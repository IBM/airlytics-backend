package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;
import com.ibm.weather.airlytics.dataimport.dto.GetDataImportsResponse;
import com.ibm.weather.airlytics.dataimport.dto.JobStatusReport;
import com.ibm.weather.airlytics.dataimport.integrations.AirlockDataImportClient;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class JobRecoveryService {

    private static final Logger logger = LoggerFactory.getLogger(JobRecoveryService.class);

    private DataImportConfigService featureConfigService;

    private DataImportService dataImportService;

    private AirlockDataImportClient airlockClient;

    private DataImportAirlockConfig featureConfig;

    @Autowired
    public JobRecoveryService(DataImportConfigService featureConfigService, DataImportService dataImportService, AirlockDataImportClient airlockClient) {
        this.featureConfigService = featureConfigService;
        this.dataImportService = dataImportService;
        this.airlockClient = airlockClient;
    }

    @PostConstruct
    public void init() throws DataImportServiceException {
        Optional<DataImportAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new DataImportServiceException("Error reading Airlock feature config");
        }
        this.featureConfig =  optConfig.get();
    }

    @Scheduled(initialDelay = 60_000, fixedDelay = Long.MAX_VALUE)
    public void recovery() {

        if(featureConfig.isResumeInterruptedJobs()) {

            if(featureConfig.getProductId() != null) {
                processProduct(featureConfig.getProductId());
            } else {
                logger.warn("No products configured for job recovery");
            }
        } else {
            logger.warn("Job recovery is disabled");
        }
    }

    private void processProduct(String productId) {

        try {
            Optional<GetDataImportsResponse> optImports = airlockClient.getJobs(productId);

            if(optImports.isPresent()) {
                List<DataImportConfig> jobs = optImports.get().getJobs();

                if(jobs != null) {
                    processPending(jobs);
                    processRunning(jobs);
                }
            }
        } catch (AirlockException e) {
            logger.error("Error obtaining jobs for product " + productId, e);
        }
    }

    private void processPending(List<DataImportConfig> jobs) {
        List<DataImportConfig> pendingJobs =
                jobs.stream()
                        .filter(j -> j.getStatus() == null || j.getStatus() == JobStatusReport.JobStatus.PENDING)
                        .collect(Collectors.toList());

        for(DataImportConfig dic : pendingJobs) {
            logger.info("Restarting pending job {}", dic.getUniqueId());

            try {
                JobStatusReport report = new JobStatusReport(
                        JobStatusReport.JobStatus.PENDING,
                        "Pending",
                        null,
                        null,
                        null);
                airlockClient.updateJobStatus(dic.getProductId(), dic.getUniqueId(), report);
            } catch (Exception e) {
                logger.error("Status report failed for job " + dic.getUniqueId() + " before the job started", e);
            }
            dataImportService.asyncExecuteJob(dic);
        }
    }

    private void processRunning(List<DataImportConfig> jobs) {
        List<DataImportConfig> runningJobs =
                jobs.stream()
                        .filter(j -> j.getStatus() == JobStatusReport.JobStatus.RUNNING)
                        .collect(Collectors.toList());

        for(DataImportConfig dic : runningJobs) {

            try {
                JobStatusReport report = new JobStatusReport(
                        JobStatusReport.JobStatus.FAILED,
                        "Failed",
                        "The server restarted, while executing the job. Try again later",
                        null,
                        null);
                airlockClient.updateJobStatus(dic.getProductId(), dic.getUniqueId(), report);
            } catch (Exception e) {
                logger.error("Status report failed for failed job " + dic.getUniqueId(), e);
            }
        }
    }
}
