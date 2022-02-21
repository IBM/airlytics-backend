package com.ibm.weather.airlytics.jobs.aggregate.services;

import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.jobs.aggregate.db.AthenaDao;
import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import com.ibm.weather.airlytics.jobs.aggregate.dto.SecondaryAggregationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.SortedSet;

@Service
public class JobExecutionService implements ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(JobExecutionService.class);

    private ApplicationContext context;

    private EventAggregatorConfigService featureConfigService;

    private S3Service s3Service;

    private AthenaDriver athena;

    private AthenaDao athenaDao;

    private AirlockDataImportClient airlockDataImport;

    @Autowired
    public JobExecutionService(
            EventAggregatorConfigService featureConfigService,
            S3Service s3Service,
            AthenaDriver athena,
            AthenaDao athenaDao,
            AirlockDataImportClient airlockDataImport) {
        this.featureConfigService = featureConfigService;
        this.s3Service = s3Service;
        this.athena = athena;
        this.athenaDao = athenaDao;
        this.airlockDataImport = airlockDataImport;
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.context = ctx;
    }

    @Scheduled(initialDelay = 10_000L, fixedDelay = Long.MAX_VALUE)
    public void runJob() {
        logger.info("Starting Aggregation job");

        try {
            EventAggregatorAirlockConfig featureConfig = featureConfigService.getAirlockConfig();

            AthenaClient athenaClient = athena.getAthenaClient(featureConfig);
            SortedSet<LocalDate> dates = s3Service.getMissingDays(featureConfig);
            LocalDate startDay = featureConfig.getHistoryStartDayDate();
            LocalDate today = LocalDate.now();
            LocalDate endDay = today.minusDays(1L);
            boolean updated = false;

            if(dates.isEmpty()) {
                logger.warn("No days to process so far");
            } else {

                boolean isReady =
                        !featureConfig.isCheckDataForLastDay() ||
                            athenaDao.isDateComplete(athenaClient, featureConfig, endDay);

                if(!isReady) {
                    logger.warn("Previous day's data is still being processed");
                } else {
                    boolean isFirstRun = dates.size() == ChronoUnit.DAYS.between(startDay, today);

                    if (isFirstRun) {
                        logger.warn("Initial run, starting calculations from {}", startDay);
                    }
                    logger.info("Cleanup...");
                    s3Service.missingDaysCleanup(featureConfig, dates);

                    for (LocalDate day : dates) {
                        int cnt = athenaDao.performDailyJob(athenaClient, featureConfig, day, isFirstRun);

                        if(cnt > 0) {

                            if (featureConfig.getSecondaryAggregations() != null) {

                                for (SecondaryAggregationConfig aggregationConfig : featureConfig.getSecondaryAggregations()) {
                                    athenaDao.performSecondaryAggregation(athenaClient, featureConfig, day, aggregationConfig, isFirstRun);
                                }
                            }
                        } else {
                            logger.warn("No relevant events found on {}. Secondary aggregations will not run.", day);
                        }
                        isFirstRun = false;// next day is not considered 1st run
                        s3Service.markSuccess(featureConfig, day);
                    }
                    updated = true;
                }
            }

            if(featureConfig.isForceExportToCsv() || (updated && featureConfig.isExportToCsv())) {
                logger.info("Producing CSV...");
                athenaDao.exportCsv(athenaClient, featureConfig, endDay);

                if(featureConfig.isImportToUserDb()) {
                    logger.info("Creating Data Import Job in Airlock...");
                    String csvFolder = athenaDao.getCsvFullPrefix(featureConfig, endDay);
                    String s3FileUrl = s3Service.getCsvS3Url(featureConfig, csvFolder);

                    if(s3FileUrl != null) {
                        airlockDataImport.createDataImport(featureConfig, s3FileUrl);
                    } else {
                        throw new RuntimeException("Error producing CSV in " + csvFolder);
                    }
                }
            }
            logger.info("Aggregation job finished");
            // shutdown
            int exitCode = SpringApplication.exit(this.context, () -> 0);
            System.exit(exitCode);
        } catch (Exception e) {
            logger.error("Error performing Aggregation job", e);
            int exitCode = SpringApplication.exit(this.context, () -> 1);
            System.exit(exitCode);
            //throw new RuntimeException("Error performing Aggregation job");
        }
    }
}
