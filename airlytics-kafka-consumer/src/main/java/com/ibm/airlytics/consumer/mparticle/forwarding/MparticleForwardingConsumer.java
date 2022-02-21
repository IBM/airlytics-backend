package com.ibm.airlytics.consumer.mparticle.forwarding;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.mparticle.MparticleJson;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.mparticle.ApiClient;
import com.mparticle.client.EventsApi;
import com.mparticle.model.Batch;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class MparticleForwardingConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(MparticleForwardingConsumer.class.getName());

    private static final String BATCH_LOG_PATH = "/usr/src/app/data/airlytics-datalake-prod/mparticle-log";

    private static final Counter recordsReceivedCounter = Counter.build()
            .name("airlytics_mparticle_fwd_events_received_total")
            .help("Total records received by the mparticle forwarding consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_mparticle_fwd_events_processed_total")
            .help("Total records processed by the mparticle forwarding consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    private static final Histogram proxyResponseTimes = Histogram.build()
            .name("airlytics_mparticle_response_time_seconds")
            .help("Mparticle events API response times in seconds.").register();

    private static final int BULK_SIZE = 100;

    final private MparticleJson mpJson = new MparticleJson();

    private MparticleForwardingConsumerConfig config;

    private MparticleForwardingConsumerConfig newConfig;

    private ExecutorService executor;

    private EventsApi mparticleClient;

    private RateLimiter rateLimiter;

    private DataSerializer batchLog = null;

    public MparticleForwardingConsumer() {}

    public MparticleForwardingConsumer(MparticleForwardingConsumerConfig config) {
        super(config);

        setConfig(config);
        init();
        LOGGER.info("Mparticle forwarding consumer created with configuration:" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if (consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();

        if(!isRunning()) { // updating config failed
            return 0;
        }

        List<Future<Boolean>> futures = new LinkedList<>();
        List<Batch> batches = new LinkedList<>();
        boolean success = true;

        int recordsProcessed = convertRecords(consumerRecords, batches);
        Map<String, List<Batch>> batchesPerUser = groupByUser(batches);
        List<Batch> mergedBatches = mergeUserBatches(batchesPerUser);

        List<List<Batch>> bulks = Lists.partition(mergedBatches, BULK_SIZE);
        LOGGER.info("Submitting " + mergedBatches.size() + " batches in " + bulks.size() + " bulks");
        int recordsBatched = 0;

        for(List<Batch> bulk : bulks) {
            futures.add(submitBulk(bulk));

            for(Batch b : bulk) {
                recordsBatched += b.getEvents().size();
            }
        }
        LOGGER.info("Submitted " + recordsBatched + " out of " + recordsProcessed + " received");

        if(recordsBatched != recordsProcessed) {
            LOGGER.warn("Some records were lost while batching");
        }

        for(Future<Boolean> future : futures) {

            try {

                if(!success) {// failed at a previous iteration
                    future.cancel(true);
                } else {
                    success = future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Exception in the thread sending events to the Amplitude API", e);
                success = false;
            }
        }

        if(!success) {
            stop();
            return 0;
        }

        commit(); // tell Kafka to shift the offset

        return recordsProcessed;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        MparticleForwardingConsumerConfig config = new MparticleForwardingConsumerConfig();

        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Mparticle Forwarding Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {

                if(newConfig.getMaxPollRecords() != config.getMaxPollRecords()) {
                    LOGGER.warn("Kafka batch size changed. Restarting the consumer...");
                    stop();
                    return;
                }
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Mparticle forwarding consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping mparticle forwarding consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(MparticleForwardingConsumerConfig config) {

        if(config.getPercentageUsersForwarded() < 0 || config.getPercentageUsersForwarded() > 100) {
            throw new IllegalArgumentException("Invalid getPercentageUsersForwarded " + config.getPercentageUsersForwarded());
        }

        if(config.isMparticleIntegrationEnabled() &&
                StringUtils.isAnyBlank(
                        config.getMparticleApiBaseUrl(),
                        config.getMparticleKey(),
                        config.getMparticleSecret())) {
            throw new IllegalArgumentException("Invalid Mparticle API settings");
        }
        this.config = config;
    }

    void init() {

        try {
            ApiClient client = new ApiClient(config.getMparticleKey(), config.getMparticleSecret());
            client.getAdapterBuilder().baseUrl(config.getMparticleApiBaseUrl());
            this.mparticleClient = client.createService(EventsApi.class);

            if(config.isMparticleIntegrationEnabled()) {
                this.rateLimiter = RateLimiter.create(config.getApiRateLimit());
            } else {
                this.rateLimiter = RateLimiter.create(Integer.MAX_VALUE);
            }

            if(CollectionUtils.isNotEmpty(config.getLogEvents())) {
                this.batchLog = new FSSerializer(BATCH_LOG_PATH, 3);
            }

            if(this.executor != null) {
                // shutdown the previous executor to avoid threads leakage
                this.executor.shutdown();

                try {

                    if (!this.executor.awaitTermination(1, TimeUnit.SECONDS)) {
                        this.executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    this.executor.shutdownNow();
                }
            }
            this.executor = Executors.newFixedThreadPool(this.config.getApiParallelThreads());
        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid configuration " + config.toString());
            throw e;
        }
    }

    private boolean isAcceptedMessage(ConsumerRecord<String, JsonNode> record) {

        if(config.getPercentageUsersForwarded() == 100) {
            return true;
        }

        if(config.getPercentageUsersForwarded() > 0) {
            int partitionsToSend = (int)Math.floor(config.getPercentageUsersForwarded() *  getNumberOfPartitions(config.getTopics().get(0)) / 100.0);
            return record.partition() < partitionsToSend;
        }
        return false;
    }

    private int convertRecords(ConsumerRecords<String, JsonNode> consumerRecords, List<Batch> batches) {
        int recordsProcessed = 0;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            processRecord(record, batches);
            ++recordsProcessed;
        }
        LOGGER.info("Converted " + recordsProcessed + " records");
        return recordsProcessed;
    }

    private Map<String, List<Batch>> groupByUser(List<Batch> batches) {
        Map<String, List<Batch>> batchesPerUser = new LinkedHashMap<>();

        for(Batch b : batches) {
            String uid = b.getUserIdentities().getCustomerId();
            List<Batch> userBatches = batchesPerUser.get(uid);

            if(userBatches == null) {
                userBatches = new LinkedList<>();
                batchesPerUser.put(uid, userBatches);
            }
            userBatches.add(b);
        }
        LOGGER.info("Found " + batchesPerUser.size() + " users");
        return batchesPerUser;
    }

    private List<Batch> mergeUserBatches(Map<String, List<Batch>> batchesPerUser) {
        List<Batch> mergedBatches = new ArrayList<>(batchesPerUser.size());

        for(List<Batch> userBatches : batchesPerUser.values()) {
            Batch mergedBatch = userBatches.get(0);

            if(userBatches.size() > 1) {

                for(int i = 1; i < userBatches.size(); i++) {
                    Batch b = userBatches.get(i);

                    if(MapUtils.isNotEmpty(b.getUserAttributes())) {

                        if(mergedBatch.getUserAttributes() == null) {
                            mergedBatch.setUserAttributes(b.getUserAttributes());
                        } else {
                            mergedBatch.getUserAttributes().putAll(b.getUserAttributes());
                        }
                    }

                    if(CollectionUtils.isNotEmpty(b.getEvents())) {
                        b.getEvents().forEach(mergedBatch::addEventsItem);
                    }
                }
            }
            mergedBatches.add(mergedBatch);
        }
        return mergedBatches;
    }

    private void processRecord(
            ConsumerRecord<String, JsonNode> record,
            List<Batch> bulk) {

        JsonNode eventJson = record.value();
        Batch batch = mpJson.getGson().fromJson(eventJson.toString(), Batch.class);
        Map<String, Long> eventsCountPerType =
                batch.getEvents()
                        .stream()
                        .collect(
                                Collectors.groupingBy(
                                        event -> getMPEventName(event),
                                        Collectors.counting()));
        reportReceived(eventsCountPerType);

        if(isAcceptedMessage(record)) {
            bulk.add(batch);
        }
    }

    private Future<Boolean> submitBulk(final List<Batch> bulk) {
        return executor.submit(() -> sendCurrentBatchToProxy(bulk));
    }

    private boolean sendCurrentBatchToProxy(final List<Batch> bulk) {

        if(bulk.isEmpty()) {
            return true;
        }
        int nRetries = 4;
        int retryCounter = 0;
        IOException lastIoException = null;

        Map<String, Long> eventsCountPerType =
                bulk
                        .stream()
                        .flatMap(b -> b.getEvents().stream())
                        .collect(
                                Collectors.groupingBy(
                                        event -> getMPEventName(event),
                                        Collectors.counting()));

        String bulkJson = null;
        String loggedBatchJson = null;
        String logFilePath = null;

        if(CollectionUtils.isNotEmpty(config.getLogEvents())) {

            for(String eventType : config.getLogEvents()) {

                if(eventsCountPerType.containsKey(eventType)) {
                    bulkJson = mpJson.getGson().toJson(bulk);
                    logFilePath = "/" + LocalDate.now().toString();
                    break;
                }
            }

            if(bulkJson != null) {
                List<Batch> loggedBatches =
                        bulk.stream()
                                .filter(batch ->
                                        batch.getEvents()
                                                .stream()
                                                .filter(event -> config.getLogEvents().contains(getMPEventName(event)))
                                                .findAny()
                                                .isPresent()
                                )
                                .collect(Collectors.toList());
                loggedBatchJson = mpJson.getGson().toJson(loggedBatches);
            }
        }

        while(retryCounter < nRetries) {

            try {
                rateLimiter.acquire();
                Response<Void> bulkResponse = null;

                if(config.isMparticleIntegrationEnabled()) {
                    Histogram.Timer timer = proxyResponseTimes.startTimer();

                    try {
                        Call<Void> bulkResult = mparticleClient.bulkUploadEvents(bulk);
                        bulkResponse = bulkResult.execute();

                        if(bulkJson != null) {
                            batchLog.writeData(
                                    logFilePath + "-bulks",
                                    Instant.now().toString() + "\t" + bulkJson + "\t" + bulkResponse.code() + "\n",
                                    true);

                            batchLog.writeData(
                                    logFilePath,
                                    Instant.now().toString() + "\t" + loggedBatchJson + "\t" + bulkResponse.code() + "\n",
                                    true);
                        }
                    } finally {
                        timer.observeDuration();
                    }
                }

                if(bulkResponse == null || bulkResponse.isSuccessful()) {
                    // all succeeded
                    reportForwarded(RESULT_SUCCESS, eventsCountPerType);
                    return true;
                } else if(bulkResponse.code() == 429) {// throttling
                    LOGGER.warn("Mparticle API throttling response: " + bulkResponse);
                    retryCounter++;

                    int secs = retryCounter;
                    String sSecs = bulkResponse.headers().get("Retry-After");

                    if(sSecs != null) {

                        try {
                            secs = Integer.parseInt(sSecs);
                            LOGGER.info("Retry-After " + secs);
                        } catch(Exception nfe) {
                            // unparsable - do notyhing
                        }
                    }

                    try {
                        Thread.sleep(secs * 1000L);
                    } catch (InterruptedException e1) {
                        // it seems, the process is being stopped, so, stop retrying
                    }
                    continue;
                } else if(bulkResponse.code() > 500) {// server failure
                    LOGGER.error("Mparticle API " + bulkResponse.code() + " response: " + bulkResponse);
                    retryCounter++;

                    try {
                        Thread.sleep(retryCounter * 1000L);
                    } catch (InterruptedException e1) {
                        // it seems, the process is being stopped, so, stop retrying
                    }
                    continue;
                } else {
                    LOGGER.error("Mparticle API error response: " + bulkResponse);

                    // all failed
                    reportForwarded(RESULT_ERROR, eventsCountPerType);
                    break;
                }
            } catch (IOException e) {

                retryCounter++;
                lastIoException = e;

                try {
                    Thread.sleep(retryCounter * 1000L);
                } catch (InterruptedException e1) {
                    // it seems, the process is being stopped, so, stop retrying
                }
                continue;
            }
        }

        if (retryCounter >= nRetries) {

            if(lastIoException != null) {
                LOGGER.error(lastIoException.getMessage(), lastIoException.getCause());
            } else {
                LOGGER.error("Error sending events.");
            }
            // all failed
            reportForwarded(RESULT_ERROR, eventsCountPerType);
        }
        return false;
    }

    private String getMPEventName(Object event) {
        if (event instanceof Map) {

            if ("CUSTOM_EVENT".equalsIgnoreCase(((Map<?, ?>) event).get("event_type").toString())) {
                Object oData = ((Map<?, ?>) event).get("data");

                if (oData != null && oData instanceof Map) {
                    Object oName = ((Map<?, ?>) oData).get("event_name");

                    if (oName != null) {
                        return oName.toString();
                    }
                }
            } else if ("COMMERCE_EVENT".equalsIgnoreCase(((Map<?, ?>) event).get("event_type").toString())) {
                Object oData = ((Map<?, ?>) event).get("data");

                if (oData != null && oData instanceof Map) {
                    Object oAttrs = ((Map<?, ?>) oData).get("custom_attributes");

                    if (oAttrs != null && oAttrs instanceof Map) {
                        Object oName = ((Map<?, ?>) oAttrs).get("purchaseType");

                        if (oName != null) {
                            return oName.toString();
                        }

                    }
                }
            }
        }
        return "event";
    }

    private void reportReceived(Map<String, Long> eventsPerType) {
        eventsPerType.forEach((k, v) -> reportReceived(k, v.intValue()));
    }

    private void reportReceived(String eventType, int size) {
        recordsReceivedCounter.labels(
                AirlockManager.getEnvVar(), AirlockManager.getProduct(), eventType).inc(size);
    }

    private void reportForwarded(String result, Map<String, Long> eventsPerType) {
        eventsPerType.forEach((k, v) -> reportForwarded(result, k, v.intValue()));
    }

    private void reportForwarded(String result, String eventType, int size) {
        recordsProcessedCounter.labels(
                result, AirlockManager.getEnvVar(), AirlockManager.getProduct(), eventType).inc(size);
    }
}
