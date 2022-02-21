package com.ibm.airlytics.consumer.braze.forwarding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class BrazeForwardingConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeForwardingConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_braze_fwd_records_processed_total")
            .help("Total records processed by the braze forwarding consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    private static final int BRAZE_BATCH_SIZE = 75;

    final private ObjectMapper mapper = new ObjectMapper();

    private BrazeForwardingConsumerConfig config;

    private BrazeForwardingConsumerConfig newConfig;

    private ExecutorService executor;

    private BrazeApiClient brazeClient;

    public BrazeForwardingConsumer() {}

    public BrazeForwardingConsumer(BrazeForwardingConsumerConfig config) {
        super(config);

        setConfig(config);
        init();
        LOGGER.info("Braze forwarding consumer created with configuration:" + config.toString());
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

        int recordsProcessed = 0;
        List<Future<Boolean>> futures = new LinkedList<>();
        List<JsonNode> attributes = new LinkedList<>();
        List<JsonNode> events = new LinkedList<>();
        List<JsonNode> purchases = new LinkedList<>();
        boolean success = true;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {

            try {
                processRecord(record, attributes, events, purchases);

                if (attributes.size() == BRAZE_BATCH_SIZE || events.size() == BRAZE_BATCH_SIZE || purchases.size() == BRAZE_BATCH_SIZE) {
                    futures.add(submitBatch(attributes, events, purchases));
                    attributes = new LinkedList<>();
                    events = new LinkedList<>();
                    purchases = new LinkedList<>();
                }
            } catch (JsonProcessingException e) {
                LOGGER.error("Error parsing Amplitude event " + record.value().toString(), e);
                success = false;
                break;
            }
            ++recordsProcessed;
        }

        if(success && (!attributes.isEmpty() || !events.isEmpty() || !purchases.isEmpty())) {
            // last batch
            futures.add(submitBatch(attributes, events, purchases));
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
        BrazeForwardingConsumerConfig config = new BrazeForwardingConsumerConfig();

        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Braze Forwarding Consumer", e);
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
                LOGGER.info("Braze forwarding consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping braze forwarding consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(BrazeForwardingConsumerConfig config) {

        if(config.getPercentageUsersForwarded() < 0 || config.getPercentageUsersForwarded() > 100) {
            throw new IllegalArgumentException("Invalid getPercentageUsersForwarded " + config.getPercentageUsersForwarded());
        }

        if(config.isBrazeIntegrationEnabled() &&
                StringUtils.isAnyBlank(
                        config.getBrazeApiBaseUrl(),
                        config.getBrazeApiPath(),
                        config.getBrazeKey(),
                        config.getBrazeAppId())) {
            throw new IllegalArgumentException("Invalid Braze API settings");
        }
        this.config = config;
    }

    void init() {

        try {
            this.brazeClient = new BrazeApiClient(config);

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

    private void processRecord(
            ConsumerRecord<String, JsonNode> record,
            List<JsonNode> attributes,
            List<JsonNode> events,
            List<JsonNode> purchases)
            throws JsonProcessingException {
        if(isAcceptedMessage(record)) {
            JsonNode eventJson = record.value();

            if(eventJson.has("name")) {

                if(!eventJson.has("app_id") || eventJson.get("app_id").isNull()) {
                    ((ObjectNode)eventJson).put("app_id", config.getBrazeAppId());
                }
                events.add(eventJson);
            } else if(eventJson.has("currency")) {

                if(!eventJson.has("app_id") || eventJson.get("app_id").isNull()) {
                    ((ObjectNode)eventJson).put("app_id", config.getBrazeAppId());
                }
                purchases.add(eventJson);
            } else {
                attributes.add(eventJson);
            }
        }
    }

    private Future<Boolean> submitBatch(final List<JsonNode> attributes, final List<JsonNode> events, final List<JsonNode> purchases) {
        return executor.submit(() -> sendCurrentBatchToProxy(attributes, events, purchases));
    }

    private boolean sendCurrentBatchToProxy(final List<JsonNode> attributes, final List<JsonNode> events, final List<JsonNode> purchases) {

        if(attributes.isEmpty() && events.isEmpty() && purchases.isEmpty()) {
            return true;
        }
        int nRetries = 2;
        int retryCounter = 0;
        BrazeApiException lastIoException = null;

        Map<String, Long> eventsCountPerType =
                events
                        .stream()
                        .collect(
                                Collectors.groupingBy(
                                        event -> event.get("name").textValue(),
                                        Collectors.counting()));

        if(!attributes.isEmpty()) {
            eventsCountPerType.put("user-attributes", new Long(attributes.size()));
        }

        if(!purchases.isEmpty()) {
            eventsCountPerType.put("subscription", new Long(purchases.size()));
        }

        while(retryCounter < nRetries) {

            try {
                this.brazeClient.uploadEvents(attributes, events, purchases);
                // all succeeded
                report(RESULT_SUCCESS, eventsCountPerType);
                return true;
            } catch (BrazeApiException e) {

                if(e.isIOException()) { // connection problem
                    retryCounter++;
                    lastIoException = e;

                    try {
                        Thread.sleep(retryCounter * 500L);
                    } catch (InterruptedException e1) {
                        // it seems, the process is being stopped, so, stop retrying
                    }
                    continue;
                } else if (e.getResponseCode() == 429) { // throttling
                    LOGGER.error("Braze API throttling response: " + e.getResponseBody());
                } else if(e.getResponseCode() == 400) { // some of the events were invalid
                    LOGGER.error("Braze API invalid request response. Request: " + e.getRequestBody() + " Response: " + e.getResponseBody());
                } else if(e.getResponseCode() == 401) { // invalifd aPI key
                    LOGGER.error("Braze API returned unauthorized response 401: " + e.getResponseBody());
                } else { // log error only for responses other than above
                    LOGGER.error("Error sending events: " + e.getRequestBody() + ":" + e.getMessage(), e);
                }
                // all failed
                report(RESULT_ERROR, eventsCountPerType);
                break;
            }
        }

        if (retryCounter >= nRetries) {

            if(lastIoException != null) {
                LOGGER.error(lastIoException.getMessage(), lastIoException.getCause());
            } else {
                LOGGER.error("Error sending events.");
            }
            // all failed
            report(RESULT_ERROR, eventsCountPerType);
        }
        return false;
    }

    private void report(String result, Map<String, Long> eventsPerType) {
        eventsPerType.forEach((k, v) -> report(result, k, v.intValue()));
    }

    private void report(String result, String eventType, int size) {
        recordsProcessedCounter.labels(
                result, AirlockManager.getEnvVar(), AirlockManager.getProduct(), eventType).inc(size);
    }
}
