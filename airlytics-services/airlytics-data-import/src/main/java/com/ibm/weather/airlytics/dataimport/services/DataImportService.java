package com.ibm.weather.airlytics.dataimport.services;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;
import com.ibm.weather.airlytics.dataimport.dto.ImportJobDefinition;
import com.ibm.weather.airlytics.dataimport.dto.JobStatusReport;
import com.ibm.weather.airlytics.dataimport.integrations.AirlockDataImportClient;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class DataImportService {

    private static final Logger logger = LoggerFactory.getLogger(DataImportService.class);

    private static final Object lock = new Object();

    private static final Gauge rowsGauge =
            Gauge.build()
                    .name("airlytics_ai_import_rows_gauge")
                    .help("Gauge for changes in rows imported per import")
                    .labelNames("env","product")
                    .register();

    private static final Summary importTimes =
            Summary.build()
                    .name("airlytics_ai_import_duration_summary")
                    .help("Time for import")
                    .quantile(0.95, 0.01)
                    .labelNames("env","product")
                    .register();

    @Value("${airlock.airlytics.env:}")
    private String airlyticsEnv;

    @Value("${airlock.airlytics.product:}")
    private String airlyticsProduct;

    private S3FileService s3Service;

    private UserFeaturesDao userFeaturesDao;

    private AirlockDataImportClient airlockClient;

    private DataImportConfigService featureConfigService;

    private String prodDbSchema;

    @Autowired
    public DataImportService(
            S3FileService s3Service,
            UserFeaturesDao userFeaturesDao,
            AirlockDataImportClient airlockClient,
            DataImportConfigService featureConfigService,
            @Value("${spring.datasource.jdbc-url}") String dbUrl) {
        this.s3Service = s3Service;
        this.userFeaturesDao = userFeaturesDao;
        this.airlockClient = airlockClient;
        this.featureConfigService = featureConfigService;
        this.prodDbSchema = dbUrl.substring(dbUrl.lastIndexOf('=') + 1);
    }

    public List<String> getTableNames() throws AirlockException {
        DataImportAirlockConfig config = getAirlockConfig();

        if(config.getPiFeatureTables() == null) {
            return config.getFeatureTables();
        }

        if(config.getFeatureTables() == null) {
            return config.getPiFeatureTables();
        }
        Collections.sort(config.getFeatureTables());
        Collections.sort(config.getPiFeatureTables());
        return CollectionUtils.collate(config.getFeatureTables(), config.getPiFeatureTables());
    }

    public long executeJob(DataImportConfig jobRequest) throws AirlockException, DataImportServiceException {
        Summary.Timer timer = importTimes.labels(airlyticsEnv, airlyticsProduct).startTimer();

        try {
            DataImportAirlockConfig config = getAirlockConfig();
            ImportJobDefinition job = s3Service.buildJobDefinition(jobRequest, config);
            long successCnt = 0L;

            if(job.isOverwrite()) {
                successCnt = userFeaturesDao.runImportReplaceJob(job, config);
            } else {
                successCnt = userFeaturesDao.runImportUpsertJob(job, config);
            }
            rowsGauge.labels(airlyticsEnv, airlyticsProduct).set(Long.valueOf(successCnt).doubleValue());
            return successCnt;
        } finally {
            timer.observeDuration();
        }
    }

    @Async
    public void asyncExecuteJob(DataImportConfig jobRequest) {
        Summary.Timer timer = importTimes.labels(airlyticsEnv, airlyticsProduct).startTimer();
        ImportJobDefinition job = null;

        try {
            JobStatusReport report = new JobStatusReport(
                    JobStatusReport.JobStatus.RUNNING,
                    "Job Started",
                    null,
                    null,
                    null);
            airlockClient.asyncUpdateJobStatus(jobRequest.getProductId(), jobRequest.getUniqueId(), report);
            logger.info("Waiting to start a new job {} for {}", jobRequest.getUniqueId(), jobRequest.getS3File());
            int successCnt;

            // since it's a DB-heavy operation, allow one job at a time
            synchronized (lock) {
                logger.info("Preprocessing job request {}", (new Gson()).toJson(jobRequest));
                DataImportAirlockConfig config = getAirlockConfig();
                job = s3Service.buildJobDefinition(jobRequest, config);

                try {

                    if(job.isOverwrite()) {
                        successCnt = userFeaturesDao.runImportReplaceJob(job, config);
                    } else {
                        successCnt = userFeaturesDao.runImportUpsertJob(job, config);
                    }
                } catch (Exception e) {
                    // AWS S3 import is sometimes unstable, let's try one more time
                    //Pause for 30 seconds
                    Thread.sleep(30_000);
                    logger.warn("Retrying DB sequence for job " + jobRequest.getUniqueId() + " after exception", e);

                    if(job.isOverwrite()) {
                        successCnt = userFeaturesDao.runImportReplaceJob(job, config);
                    } else {
                        successCnt = userFeaturesDao.runImportUpsertJob(job, config);
                    }
                }
            }
            rowsGauge.labels(airlyticsEnv, airlyticsProduct).set(Long.valueOf(successCnt).doubleValue());

            report = new JobStatusReport(
                    JobStatusReport.JobStatus.COMPLETED,
                    "Completed",
                    null,
                    successCnt,
                    job.getAffectedColumnNames());
            airlockClient.updateJobStatus(jobRequest.getProductId(), jobRequest.getUniqueId(), report);

        } catch (AirlockException e) {
            logger.error("Error reporting import status to Airlock", e);
        } catch (Exception e) {
            logger.error("Import failed for job " + jobRequest.getUniqueId(), e);
            JobStatusReport report = new JobStatusReport(
                    JobStatusReport.JobStatus.FAILED,
                    "Import Failed",
                    e.getMessage(),
                    null,
                    (job != null) ? job.getAffectedColumnNames() : null);
            try {
                airlockClient.updateJobStatus(jobRequest.getProductId(), jobRequest.getUniqueId(), report);
            } catch (AirlockException airlockException) {
                logger.error("Error reporting import failure to Airlock", airlockException);
            }
        } finally {
            timer.observeDuration();
        }
    }

    private DataImportAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<DataImportAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }

    public void setAirlyticsVarsForTest(String env, String product) {
        this.airlyticsEnv = env;
        this.airlyticsProduct = product;
    }
}
