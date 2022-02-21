package com.ibm.weather.airlytics.cohorts.services;

import com.ibm.weather.airlytics.cohorts.db.UserCohortsDao;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.Optional;

@Service
public class ScheduledCohortsService {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledCohortsService.class);

    private AirCohortsConfigService featureConfigService;

    private AsyncCohortsService asyncCohortsService;

    private CohortCalculationService calculationService;

    private AirlockCohortsClient airlockClient;

    private UserCohortsDao userCohortsDao;

    @Autowired
    public ScheduledCohortsService(
            AirCohortsConfigService featureConfigService,
            AsyncCohortsService asyncCohortsService,
            CohortCalculationService calculationService,
            AirlockCohortsClient airlockClient,
            UserCohortsDao userCohortsDao) {
        this.featureConfigService = featureConfigService;
        this.asyncCohortsService = asyncCohortsService;
        this.calculationService = calculationService;
        this.airlockClient = airlockClient;
        this.userCohortsDao = userCohortsDao;
    }

    @Scheduled(initialDelay = 60_000, fixedDelay = Long.MAX_VALUE)
    public void recovery() {
        logger.info("Starting recovery");

        try {
            Optional<AirCohortsAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

            if(!optConfig.isPresent()) {
                throw new CohortServiceException("Error reading Airlock feature config");
            }
            AirCohortsAirlockConfig config = optConfig.get();

            for(String productId : config.getProductIds()) {
                ProductCohortsConfig pc = runRecoveredProductCohorts(productId, config.getRecoveryRetries());
            }
        } catch (Exception e) {
            logger.error("Error during cohorts calculation recovery", e);
        }
    }

    @Scheduled(initialDelayString = "${cohorts.check.initDelay}", fixedRateString = "${cohorts.check.rate}")
    public void runScheduledCohortCalculations() {
        logger.info("Starting scheduled run");

        try {
            Optional<AirCohortsAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

            if(!optConfig.isPresent()) {
                throw new CohortServiceException("Error reading Airlock feature config");
            }
            AirCohortsAirlockConfig config = optConfig.get();

            for(String productId : config.getProductIds()) {
                ProductCohortsConfig pc = runScheduledProductCohorts(productId);
            }
        } catch (Exception e) {
            logger.error("Error during scheduled cohorts calculation", e);
        }
    }

    public ProductCohortsConfig runScheduledProductCohorts(String productId) throws CohortServiceException, AirlockException {

        ProductCohortsConfig pc = calculationService.fetchProductCohortsConfig(productId);
        ProductCohortsConfig result = new ProductCohortsConfig();
        result.setDbApplicationName(pc.getDbApplicationName());
        result.setCohorts(new LinkedList<>());

        for(CohortConfig cc : pc.getCohorts()) {

            if(calculationService.isValidCohortConfig(cc)) {

                if(cc.getUpdateFrequency() != CohortCalculationFrequency.MANUAL) {
                    Optional<Instant> lastCalcTime = userCohortsDao.getLastCalculationTime(cc.getUniqueId());
                    Instant now = Instant.now();

                    switch(cc.getUpdateFrequency()) {
                        case HOURLY:
                            if(lastCalcTime.isPresent() && lastCalcTime.get().isAfter(now.minus(1, ChronoUnit.HOURS)))  continue;
                            break;
                        case DAILY:
                            if(lastCalcTime.isPresent() && lastCalcTime.get().isAfter(now.minus(1, ChronoUnit.DAYS)))  continue;
                            break;
                        case WEEKLY:
                            if(lastCalcTime.isPresent() && lastCalcTime.get().isAfter(now.minus(7, ChronoUnit.DAYS)))  continue;
                            break;
                        case MONTHLY:
                            if(lastCalcTime.isPresent() && lastCalcTime.get().isAfter(now.minus(30, ChronoUnit.DAYS)))  continue;
                            break;
                    }
                } else {
                    continue; // MANUAL are not recalculated automatically
                }
                logger.info("Scheduled execution for cohort {}", cc.getUniqueId());
                asyncCohortsService.asyncRunCohortCalculation(cc);
                result.getCohorts().add(cc);
            }
        }
        return result;
    }

    public ProductCohortsConfig runRecoveredProductCohorts(String productId, int maxRetries) throws CohortServiceException, AirlockException {

        ProductCohortsConfig pc = calculationService.fetchProductCohortsConfig(productId);
        ProductCohortsConfig result = new ProductCohortsConfig();
        result.setDbApplicationName(pc.getDbApplicationName());
        result.setCohorts(new LinkedList<>());

        for(CohortConfig cc : pc.getCohorts()) {

            if(calculationService.isValidCohortConfig(cc)) {

                if("PENDING".equals(cc.getCalculationStatus())) {
                    int retry = cc.getRetriesNumber() == null ? 0 : cc.getRetriesNumber().intValue();

                    if(retry < maxRetries) {
                        retry++;
                        logger.info("Recovery execution for cohort {}, retry {} out of {}", cc.getUniqueId(), retry, maxRetries);
                        reportRetry(cc.getUniqueId(), retry);
                        cc.setRetriesNumber(retry);
                        asyncCohortsService.asyncRunCohortCalculation(cc);
                        result.getCohorts().add(cc);
                    } else {
                        logger.error("Stop re-trying calculation for cohort {} after {} retries", cc.getUniqueId(), retry);
                        reportFailure(cc.getUniqueId(), retry);
                    }
                }
            }
        }
        return result;
    }

    private void reportRetry(String cohortId, int retry) {
        BasicJobStatusReport report = new BasicJobStatusReport(
                BasicJobStatusReport.JobStatus.PENDING,
                "Pending Retry",
                null,
                retry);
        try {
            airlockClient.updateJobStatus(cohortId, report);
        } catch (AirlockException airlockException) {
            logger.error("Error reporting calculation retry to Airlock", airlockException);
        }
    }

    private void reportFailure(String cohortId, int retry) {
        BasicJobStatusReport report = new BasicJobStatusReport(
                BasicJobStatusReport.JobStatus.FAILED,
                "Calculation Failed",
                null,
                retry);
        try {
            airlockClient.updateJobStatus(cohortId, report);
        } catch (AirlockException airlockException) {
            logger.error("Error reporting calculation failure to Airlock", airlockException);
        }
    }
}
