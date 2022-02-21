package com.ibm.airlytics.consumer.cohorts.localytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import com.ibm.airlytics.utilities.SimpleRateLimiter;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class LocalyticsCohortsConsumer extends AirlyticsConsumer {

    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(LocalyticsCohortsConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_localytics_cohorts_records_processed_total")
            .help("Total records processed by the Loaclytics cohorts consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    final private ObjectMapper mapper = new ObjectMapper();

    private LocalyticsCohortsConsumerConfig config;
    private LocalyticsCohortsConsumerConfig newConfig;

    private LocalyticsExporter localyticsExporter;

    private ExecutorService airlockReportExecutor;

    // We aggregate several packages from Kafka before sending a whole batch to Localytics
    private long lastMessageSeen = 0L;
    private List<UserCohort> currentBatch = new LinkedList<>();
    private Map<String, Integer> progress = new HashMap<>();
    private RateLimiter jobStatusRateLimiter;
    private SimpleRateLimiter batchUploadRateLimiter;
    private boolean isUnitTest = false;

    public LocalyticsCohortsConsumer() {
        isUnitTest = true;
    }

    public LocalyticsCohortsConsumer(LocalyticsCohortsConsumerConfig config) {
        super(config);
        setConfig(config);
        init();
        LOGGER.info("Localytics cohorts consumer created with configuration:\n" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if(consumerRecords.isEmpty()) {
            sendBatchIfReady();
            return 0;
        }
        updateToNewConfigIfExists();
        int recordsCount = 0;
        int relevantCount = 0;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {

            if(!isRunning()) {
                break;
            }
            Optional<UserCohort> userCohort = processRecord(record);

            if(userCohort.isPresent()) {
                relevantCount++;
                lastMessageSeen = System.currentTimeMillis();
                currentBatch.add(userCohort.get());
            }
            recordsCount++;
        }

        sendBatchIfReady();

        if( currentBatch.size() == 0 && lastMessageSeen == 0 && !isUnitTest) {
            commit();
        }
        return recordsCount;
    }

    private void sendBatchIfReady() {

        if( currentBatch.size() > 0 && lastMessageSeen > 0) {
            LOGGER.debug("Batch size now - " + currentBatch.size());

            if ((config.getMaxBatchSize() > 0 && currentBatch.size() >= config.getMaxBatchSize()) ||
                    (lastMessageSeen > 0L && (System.currentTimeMillis() - lastMessageSeen) >= config.getMaxWaitForBatchMs())) {
                // flush the batch
                try {
                    LOGGER.info("Sending now - " + currentBatch.size());
                    sendCurrentBatch();

                    if (!isUnitTest) {
                        commit();
                        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(currentBatch.size());
                    }

                    if(!progress.isEmpty()) {
                        localyticsExporter.reportAirlyticsSuccess(progress);
                        progress = new HashMap<>();
                    }
                    lastMessageSeen = 0L;
                    currentBatch = new LinkedList<>();
                } catch (LocalyticsException e) {
                    LOGGER.error("Sending to Localytics failed. Stopping the consumer", e);
                    stop();
                }
            }
        }
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        LocalyticsCohortsConsumerConfig config = new LocalyticsCohortsConsumerConfig();
        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Localytics Cohorts Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Localytics cohorts consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("Stopping Localytics cohorts consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(LocalyticsCohortsConsumerConfig config) {
        this.config = config;
    }

    void init() {
        if(this.airlockReportExecutor == null) {
            this.airlockReportExecutor = Executors.newFixedThreadPool(10);
        }

        if(this.batchUploadRateLimiter == null) {
            this.batchUploadRateLimiter = new SimpleRateLimiter(10, TimeUnit.HOURS);
        }

        if(this.jobStatusRateLimiter == null) {
            this.jobStatusRateLimiter = RateLimiter.create(1.0);// Localytics allows 60 requests/minute;
        }
        this.localyticsExporter = new LocalyticsExporter(this.config, this.batchUploadRateLimiter, this.jobStatusRateLimiter, this.airlockReportExecutor);
    }

    private Optional<UserCohort> processRecord(ConsumerRecord<String, JsonNode> record) {
        JsonNode eventJson = record.value();

        try {
            UserCohort userCohort = mapper.treeToValue(eventJson, UserCohort.class);

            if(userCohort.getEnabledExports() != null) {
                Optional<UserCohortExport> exportConfig = localyticsExporter.getUserCohortExport(userCohort, LocalyticsExporter.LOCALYTICS_EXPORT_KEY);

                if(exportConfig.isPresent()) {
                    return Optional.of(userCohort);
                }
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing UserCohort event " + eventJson.toString() + ". Localytics cohorts consumer will be stopped.", e);
            stop();
        }
        return Optional.empty();
    }

    private void sendCurrentBatch() throws LocalyticsException {

        Map<String, List<UserCohort>> cohortsPerProduct = currentBatch.stream().collect(Collectors.groupingBy(UserCohort::getProductId));

        for(String productId : cohortsPerProduct.keySet()) {

            List<UserCohort> renames = new LinkedList<>();
            List<UserCohort> exports = new LinkedList<>();

            for (UserCohort uc : cohortsPerProduct.get(productId)) {
                Optional<UserCohortExport> export = localyticsExporter.getUserCohortExport(uc, LocalyticsExporter.LOCALYTICS_EXPORT_KEY);

                if (export.isPresent()) {

                    if (StringUtils.isNotBlank(export.get().getOldFieldName())) {
                        renames.add(uc);
                    } else {
                        exports.add(uc);
                    }
                }
            }

            if (!renames.isEmpty()) {
                Map<String, Integer> counts = localyticsExporter.sendRenamesBatch(renames, productId);
                concatenateMaps(progress, counts);
            }

            if (!exports.isEmpty()) {
                Map<String, Integer> counts = localyticsExporter.sendExportedDeltasBatch(exports, productId);
                concatenateMaps(progress, counts);
            }
        }
    }

    private void concatenateMaps(Map<String, Integer> destination, Map<String, Integer> mapToAdd) {
        mapToAdd.forEach((k,v) -> {

            if(destination.containsKey(k)) {
                destination.put(k, destination.get(k) + v);
            } else {
                destination.put(k, v);
            }
        });
    }
}
