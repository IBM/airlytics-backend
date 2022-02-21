package com.ibm.weather.airlytics.cohorts.services;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.cohorts.db.UserCohortsDao;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.AirlockCohortsClient;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.kafka.KafkaProducer;
import io.prometheus.client.Counter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaProfileExportService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProfileExportService.class);

    private static final Counter usersCounter =
            Counter.build()
                    .name("airlytics_cohorts_users_kafka_exported")
                    .help("Counter for user-cohort pairs sent to Kafka")
                    .labelNames("env","product")
                    .register();

    private static final Gson gson = new Gson();

    @Value("${airlock.airlytics.env:}")
    private String airlyticsEnv;

    @Value("${airlock.airlytics.product:}")
    private String airlyticsProduct;

    private UserCohortsDao userCohortsDao;

    private AirlockCohortsClient airlockClient;

    private AirCohortsConfigService featureConfigService;

    private KafkaProducer producer = null;

    private int kafkaMaxMessages;

    @Autowired
    public KafkaProfileExportService(UserCohortsDao userCohortsDao, AirlockCohortsClient airlockClient, AirCohortsConfigService featureConfigService) {
        this.userCohortsDao = userCohortsDao;
        this.airlockClient = airlockClient;
        this.featureConfigService = featureConfigService;
    }

    @PostConstruct
    synchronized public void init() throws Exception {
        AirCohortsAirlockConfig featureConfig = getAirlockConfig();

        if(featureConfig.isKafkaEnabled()) {
            // create, if missing, re-create, if changed
//            if(producer == null ||
//                    producer.isChanged(
//                            featureConfig.getKafkaTopic(),
//                            featureConfig.getKafkaBootstrapServers(),
//                            featureConfig.getKafkaSecurityProtocol())
//            ) {
                if (producer != null) {
                    producer.close();
                }
                Map<String, Object> addKafkaProperties = new HashMap<>();
                addKafkaProperties.put("batch.size", featureConfig.getKafkaBatchSize());
                addKafkaProperties.put("linger.ms", featureConfig.getKafkaLingerMs());
                producer = new KafkaProducer(
                        featureConfig.getKafkaTopic(),
                        featureConfig.getKafkaBootstrapServers(),
                        featureConfig.getKafkaSecurityProtocol(),
                        addKafkaProperties);
//            }
            this.kafkaMaxMessages = featureConfig.getKafkaMaxMessages();
        } else if (producer != null) { // send outstanding, and nullify the producer
            producer.close();
            producer = null;
        }
    }

    public boolean exportCohort(CohortConfig cohortConfig) {
        return exportCohort(cohortConfig, false);
    }

    public boolean exportCohort(CohortConfig cohortConfig, boolean deletion) {
        long start = System.currentTimeMillis();
        List<UserCohortExport> exports = new LinkedList<>();

        if(MapUtils.isNotEmpty(cohortConfig.getExports())) {

            for(String exportType : cohortConfig.getExports().keySet()) {

                if(CohortExportConfig.DB_ONLY_EXPORT.equalsIgnoreCase(exportType)) {
                    continue;
                }
                CohortExportConfig exportConfig = cohortConfig.getExports().get(exportType);

                if(StringUtils.isNotBlank(exportConfig.getExportName())) {
                    exports.add(new UserCohortExport(exportType, exportConfig.getExportName()));
                }
            }
        }

        if(CollectionUtils.isEmpty(exports)) {
            logger.warn("No exports are configured for cohort {} {}",
                    cohortConfig.getUniqueId(),
                    cohortConfig.getName());
            return true; // success, since there was nothing to export
        }

        try {
            init();
        } catch (Exception e) {
            logger.error("Export failed for cohort " + cohortConfig.getUniqueId(), e);
            return false; // failure
        }
        int size = 0;
        boolean success = true;
        Optional<ValueType> optValueType = ValueType.forNameOrLabel(cohortConfig.getValueType());

        if(!optValueType.isPresent()) {
            logger.warn("Unrecognized value type {} for cohort {} {}. Using STRING",
                    cohortConfig.getValueType(),
                    cohortConfig.getUniqueId(),
                    cohortConfig.getName());
            optValueType = Optional.of(ValueType.STRING);
        }
        final ValueType valueType = optValueType.get();

        if(producer != null) {

            synchronized (producer) { // one export session per producer instance
                AtomicInteger rowCnt = new AtomicInteger(0);
                producer.resetErrorCounter();

                size = userCohortsDao.processUsersCohortsForExport(
                        Collections.singletonList(cohortConfig.getUniqueId()),
                        (uc) -> {
                            uc.setValueType(valueType);
                            uc.setEnabledExports(exports);
                            uc.setProductId(cohortConfig.getProductId());
                            uc.setEventId(UUID.randomUUID().toString());
                            producer.sendRecord(uc.getUserId(), uc.getEventId(), gson.toJson(uc));
                            rowCnt.incrementAndGet();

                            if(this.kafkaMaxMessages > 0 && rowCnt.get() % this.kafkaMaxMessages == 0) {
                                producer.flush();
                            }

                            if(rowCnt.get() % 1_000_000 == 0) {
                                logger.info("Sent {} messages for cohort {} {} to Kafka",
                                        rowCnt.get(),
                                        cohortConfig.getUniqueId(),
                                        cohortConfig.getName());
                            }
                        }
                );

                if (size == 0) {
                    logger.warn("No users found in cohort {} {}",
                            cohortConfig.getUniqueId(),
                            cohortConfig.getName());
                    updateAirlockJobs(cohortConfig, exports, 0, true);
                    return true; // success, since there was nothing to export
                }
                producer.flush();

                if (producer.getErrorsCount() > 0) { // Shall we introduce some tolerance?
                    logger.error("{} errors sending to Kafka for cohort {}", producer.getErrorsCount(), cohortConfig.getUniqueId());
                    success = false;
                }
            }
        } else {
            logger.warn("Sending to Kafka is disabled, not exporting cohort " + cohortConfig.getUniqueId());
            size = userCohortsDao.processUsersCohortsForExport(
                    Collections.singletonList(cohortConfig.getUniqueId()),
                    (uc) -> {});

            if (size == 0) {
                logger.warn("No users found in cohort {} {}",
                        cohortConfig.getUniqueId(),
                        cohortConfig.getName());
                updateAirlockJobs(cohortConfig, exports, 0, true);
                return true; // success, since there was nothing to export
            }
        }

        if(success) {
            // export was successful
            usersCounter.labels(airlyticsEnv, airlyticsProduct).inc(size);

            try {
                userCohortsDao.markCohortsExported(Collections.singletonList(cohortConfig.getUniqueId()));
            } catch (Exception e) {
                logger.error("Error updating exported cohort " + cohortConfig.getUniqueId(), e);
            }
        }

        if(!deletion) {
            updateAirlockJobs(cohortConfig, exports, size, success);
        }
        logger.info("Export for cohort {} {} took {}ms, exporting {} user-cohorts with {}",
                cohortConfig.getUniqueId(),
                cohortConfig.getName(),
                (System.currentTimeMillis() - start),
                size,
                (success ? "success" : "failure"));
        return success;
    }

    public boolean renameCohort(CohortConfig cohortConfig, String exportType, String oldExportName, String newExportName) {
        long start = System.currentTimeMillis();
        List<UserCohortExport> exports = new LinkedList<>();

        if(MapUtils.isNotEmpty(cohortConfig.getExports()) && !CohortExportConfig.DB_ONLY_EXPORT.equalsIgnoreCase(exportType)) {
            CohortExportConfig exportConfig = cohortConfig.getExports().get(exportType);

            if(exportConfig != null && StringUtils.isNotBlank(exportConfig.getExportName())) {
                exports.add(new UserCohortExport(exportType, newExportName, oldExportName));
            }
        }

        if(CollectionUtils.isEmpty(exports)) {
            logger.warn("No exports are configured for renamed cohort {} {}",
                    cohortConfig.getUniqueId(),
                    cohortConfig.getName());
            return true; // success, since there was nothing to export
        }

        try {
            init();
        } catch (Exception e) {
            logger.error("Export failed for renamed cohort " + cohortConfig.getUniqueId(), e);
            return false; // failure
        }
        int size = 0;
        boolean success = true;

        if(producer != null) {

            synchronized (producer) { // one export session per producer instance
                AtomicInteger rowCnt = new AtomicInteger(0);
                producer.resetErrorCounter();

                size = userCohortsDao.processAllUsersCohorts(
                        Collections.singletonList(cohortConfig.getUniqueId()),
                        (uc) -> {
                            uc.setEnabledExports(exports);
                            uc.setProductId(cohortConfig.getProductId());
                            uc.setEventId(UUID.randomUUID().toString());
                            producer.sendRecord(uc.getUserId(), uc.getEventId(), gson.toJson(uc));
                            rowCnt.incrementAndGet();

                            if(this.kafkaMaxMessages > 0 && rowCnt.get() % this.kafkaMaxMessages == 0) {
                                producer.flush();
                            }

                            if(rowCnt.get() % 1_000_000 == 0) {
                                logger.info("Sent {} messages for cohort {} {} to Kafka",
                                        rowCnt.get(),
                                        cohortConfig.getUniqueId(),
                                        cohortConfig.getName());
                            }
                        });

                if (size == 0) {
                    logger.warn("No users found in renamed cohort {} {}",
                            cohortConfig.getUniqueId(),
                            cohortConfig.getName());
                    updateAirlockJobs(cohortConfig, exports, 0, true);
                    return true; // success, since there was nothing to export
                }
                producer.flush();

                if (producer.getErrorsCount() > 0) { // Shall we introduce some tolerance?
                    logger.error("{} errors sending to Kafka for renamed cohort {}", producer.getErrorsCount(), cohortConfig.getUniqueId());
                    success = false;
                }
            }
        } else {
            logger.warn("Sending to Kafka is disabled, not exporting renamed cohort " + cohortConfig.getUniqueId());
            size = userCohortsDao.processUsersCohortsForExport(
                    Collections.singletonList(cohortConfig.getUniqueId()),
                    (uc) -> {});

            if (size == 0) {
                logger.warn("No users found in renamed cohort {} {}",
                        cohortConfig.getUniqueId(),
                        cohortConfig.getName());
                updateAirlockJobs(cohortConfig, exports, 0, true);
                return true; // success, since there was nothing to export
            }
        }

        if(success) {
            // export was successful
            usersCounter.labels(airlyticsEnv, airlyticsProduct).inc(size);
        }
        updateAirlockJobs(cohortConfig, exports, size, success);
        logger.info("Export for renamed cohort {} {} took {}ms, exporting {} user-cohorts with {}",
                cohortConfig.getUniqueId(),
                cohortConfig.getName(),
                (System.currentTimeMillis() - start),
                size,
                (success ? "success" : "failure"));
        return success;
    }

    public boolean deleteCohortFromThirdParties(String productId, String cohortId, List<UserCohortExport> deletedExports) {
        long start = System.currentTimeMillis();

        if(CollectionUtils.isEmpty(deletedExports)) {
            logger.warn("No exports are configured for deleted cohort {}", cohortId);
            return true; // success, since there was nothing to export
        }

        try {
            init();
        } catch (Exception e) {
            logger.error("Export failed for deleted cohort " + cohortId, e);
            return false; // failure
        }
        int size = 0;
        boolean success = true;

        if(producer != null) {

            synchronized (producer) { // one export session per producer instance
                AtomicInteger rowCnt = new AtomicInteger(0);
                producer.resetErrorCounter();

                size = userCohortsDao.processAllUsersCohorts(
                        Collections.singletonList(cohortId),
                        (uc) -> {
                            uc.setEnabledExports(deletedExports);
                            uc.setProductId(productId);
                            uc.setPendingDeletion(true);
                            uc.setEventId(UUID.randomUUID().toString());
                            producer.sendRecord(uc.getUserId(), uc.getEventId(), gson.toJson(uc));
                            rowCnt.incrementAndGet();

                            if(this.kafkaMaxMessages > 0 && rowCnt.get() % this.kafkaMaxMessages == 0) {
                                producer.flush();
                            }

                            if(rowCnt.get() % 1_000_000 == 0) {
                                logger.info("Sent {} messages for cohort {} to Kafka",
                                        rowCnt.get(),
                                        cohortId);
                            }
                        });

                if (size == 0) {
                    logger.warn("No users found in deleted cohort {}", cohortId);
                    updateAirlockJobs(cohortId, Long.valueOf(size), deletedExports, size, success);
                    return true; // success, since there was nothing to export
                }
                producer.flush();

                if (producer.getErrorsCount() > 0) { // Shall we introduce some tolerance?
                    logger.error("{} errors sending to Kafka for deleted cohort {}", producer.getErrorsCount(), cohortId);
                    success = false;
                }
            }
        } else {
            logger.warn("Sending to Kafka is disabled, not exporting deleted cohort " + cohortId);
            size = userCohortsDao.processUsersCohortsForExport(
                    Collections.singletonList(cohortId),
                    (uc) -> {});

            if (size == 0) {
                logger.warn("No users found in deleted cohort {}", cohortId);
                updateAirlockJobs(cohortId, Long.valueOf(size), deletedExports, size, success);
                return true; // success, since there was nothing to export
            }
        }

        if(success) {
            // export was successful
            usersCounter.labels(airlyticsEnv, airlyticsProduct).inc(size);
        }
        updateAirlockJobs(cohortId, Long.valueOf(size), deletedExports, size, success);
        logger.info("Export for deleted cohort {} took {}ms, exporting {} user-cohorts with {}",
                cohortId,
                (System.currentTimeMillis() - start),
                size,
                (success ? "success" : "failure"));
        return success;
    }

    private AirCohortsAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<AirCohortsAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }

    private void updateAirlockJobs(CohortConfig cohortConfig, List<UserCohortExport> exports, int deltaSize, boolean success) {
        updateAirlockJobs(cohortConfig.getUniqueId(), cohortConfig.getUsersNumber(), exports, deltaSize, success);
    }

    private void updateAirlockJobs(String cohortId, Long usersNumber, List<UserCohortExport> exports, int deltaSize, boolean success) {
        // basic report
        ExportJobStatusReport.JobStatus status = success ?
                (deltaSize > 0 ?
                        ExportJobStatusReport.JobStatus.PENDING :
                        ExportJobStatusReport.JobStatus.COMPLETED) :
                ExportJobStatusReport.JobStatus.FAILED;
        String msg = success ?
                (deltaSize > 0 ?
                        "Pending" :
                        "Completed") :
                "Export Failed";
        ExportJobStatusReport report = new ExportJobStatusReport(
                null,
                status,
                msg,
                usersNumber);

        // details
        JobStatusDetails jobProgress = new JobStatusDetails();
        jobProgress.setPendingImports(success ? deltaSize : 0);
        jobProgress.setFailedImports(success ? 0: deltaSize);
        jobProgress.setTotalImports(deltaSize); // meaning deltaSize out of deltaSize (all) are pending
        report.setAirlyticsStatusDetails(jobProgress);

        for(UserCohortExport export : exports) {
            report.setExportKey(export.getExportType());
            airlockClient.asyncUpdateJobStatus(cohortId, report);
        }
    }
}
