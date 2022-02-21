package com.ibm.weather.airlytics.cohorts.services;

import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AsyncCohortsService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncCohortsService.class);

    private AirCohortsConfigService featureConfigService;

    private CohortCalculationService calculationService;

    private KafkaProfileExportService kafkaService;

    private AirlockCohortsClient airlockClient;

    @Autowired
    public AsyncCohortsService(
            AirCohortsConfigService featureConfigService,
            CohortCalculationService calculationService,
            KafkaProfileExportService kafkaService,
            AirlockCohortsClient airlockClient) {
        this.calculationService = calculationService;
        this.kafkaService = kafkaService;
        this.featureConfigService = featureConfigService;
        this.airlockClient = airlockClient;
    }

    @Async("calculationTaskAsyncExecutor")
    public void asyncRunCohortCalculation(String cohortId) {
        int retry = 0;

        try {
            CohortConfig cc = calculationService.runCohortCalculation(cohortId);
            retry = cc.getRetriesNumber() == null ? 0 : cc.getRetriesNumber().intValue();

            reportAndExport(cc);
        } catch (CohortServiceException e) {
            reportFailure(cohortId, retry);
        } catch (AirlockException e) {
            logger.error("Error reporting calculation status to Airlock", e);
        }
    }

    @Async("calculationTaskAsyncExecutor")
    public void asyncRunCohortCalculation(CohortConfig cc) {

        try {
            calculationService.runCohortCalculation(cc);

            reportAndExport(cc);
        } catch (CohortServiceException e) {
            int retry = cc.getRetriesNumber() == null ? 0 : cc.getRetriesNumber().intValue();
            reportFailure(cc.getUniqueId(), retry);
        } catch (AirlockException e) {
            logger.error("Error reporting calculation status to Airlock", e);
        }
    }

    @Async("calculationTaskAsyncExecutor")
    public void asyncCohortDeletionFromThirdParties(String productId, String cohortId, List<UserCohortExport> deletedExports) {

        try {
            BasicJobStatusReport report = new BasicJobStatusReport(
                    BasicJobStatusReport.JobStatus.COMPLETED,
                    "Completed",
                    null,
                    null);
            airlockClient.updateJobStatus(cohortId, report);

            boolean exported = kafkaService.deleteCohortFromThirdParties(productId, cohortId, deletedExports);
            // export job status will be reported asynchronously by the exporter

            if(!exported) {
                report = new BasicJobStatusReport(
                        BasicJobStatusReport.JobStatus.FAILED,
                        "Export Failed",
                        null,
                        null);
                airlockClient.updateJobStatus(cohortId, report);
            }
        } catch (AirlockException e) {
            logger.error("Error reporting calculation status to Airlock", e);
        }
    }

    @Async("calculationTaskAsyncExecutor")
    public void asyncCompleteCohortDeletion(CohortConfig cc, boolean deleteFromThirdParties) {

        try {
            boolean exported = true;

            if(deleteFromThirdParties) {
                // export job status will be reported asynchronously by the exporter
                exported = kafkaService.exportCohort(cc, true);
            }

            if(exported) {
                calculationService.deleteUserCohort(cc.getUniqueId());
            } else {
                logger.error("Cohort deletion export failed for cohort " + cc.getUniqueId());
            }
        } catch (CohortServiceException e) {
            logger.error("Cohort deletion failed for cohort " + cc.getUniqueId());
        }
    }

    @Async("calculationTaskAsyncExecutor")
    public void asyncCohortRename(String cohortId, String exportKey, String oldExportName, String newExportName) {
        int retry = 0;
        try {
            CohortConfig cc = calculationService.fetchCohortConfig(cohortId);
            retry = cc.getRetriesNumber() == null ? 0 : cc.getRetriesNumber().intValue();

            BasicJobStatusReport report = new BasicJobStatusReport(
                    BasicJobStatusReport.JobStatus.COMPLETED,
                    "Completed",
                    null,
                    retry);
            airlockClient.updateJobStatus(cc.getUniqueId(), report);

            boolean exported = kafkaService.renameCohort(cc, exportKey, oldExportName, newExportName);
            // export job status will be reported asynchronously by the exporter

            if(!exported) {
                report = new BasicJobStatusReport(
                        BasicJobStatusReport.JobStatus.FAILED,
                        "Export Failed",
                        null,
                        retry);
                airlockClient.updateJobStatus(cc.getUniqueId(), report);
            }
        } catch (CohortServiceException e) {
            reportFailure(cohortId, retry);
        } catch (AirlockException e) {
            logger.error("Error reporting calculation status to Airlock", e);
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

    private void reportAndExport(CohortConfig cc) throws AirlockException {
        int retry = cc.getRetriesNumber() == null ? 0 : cc.getRetriesNumber().intValue();
        BasicJobStatusReport report = new BasicJobStatusReport(
                BasicJobStatusReport.JobStatus.COMPLETED,
                "Completed",
                cc.getUsersNumber(),
                retry);
        airlockClient.updateJobStatus(cc.getUniqueId(), report);

        boolean exported = kafkaService.exportCohort(cc);// export job status will be reported asynchronously by the exporter
    }
}
