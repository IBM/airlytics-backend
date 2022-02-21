package com.ibm.airlytics.consumer.amplitude.forwarding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Lists;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class AmplitudeForwardingConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AmplitudeForwardingConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_amplitude_fwd_records_processed_total")
            .help("Total records processed by the amplitude forwarding consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    final private ObjectMapper mapper = new ObjectMapper();

    private AmplitudeForwardingConsumerConfig config;

    private AmplitudeForwardingConsumerConfig newConfig;

    private ExecutorService executor;

    private AmplitudeApiClient amplitudeClient;

    private AmplitudeApiClient amplitudeBatchClient;

    public AmplitudeForwardingConsumer() {}

    public AmplitudeForwardingConsumer(AmplitudeForwardingConsumerConfig config) {
        super(config);

        setConfig(config);
        init();
        LOGGER.info("Amplitude forwarding consumer created with configuration:" + config.toString());
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
        List<AmplitudeEvent> currentBatch = new LinkedList<>();
        List<AmplitudeEvent> throttledBatch = Collections.synchronizedList(new LinkedList<>());
        int batchSize = config.getRelevantBatchSize();
        boolean success = true;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {

            try {
                processRecord(record).ifPresent(currentBatch::add);

                if (currentBatch.size() >= batchSize) {
                    futures.add(submitBatch(currentBatch, throttledBatch));
                    currentBatch = new LinkedList<>();
                }
            } catch (JsonProcessingException e) {
                LOGGER.error("Error parsing Amplitude event " + record.value().toString(), e);
                success = false;
                break;
            }
            ++recordsProcessed;
        }

        if(success) {
            // last batch
            futures.add(submitBatch(currentBatch, throttledBatch));
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

        if(success && !throttledBatch.isEmpty()) {
            List<AmplitudeEvent> stillThrottled = new LinkedList<>();
            // upload throttled with batch API

            List<List<AmplitudeEvent>> batchOfBatches = Lists.partition(throttledBatch, config.getAmplitudeBatchApiBatchSize());

            for(List<AmplitudeEvent> subBatch : batchOfBatches) {
                success = sendCurrentBatchToAmplitude(subBatch, stillThrottled, true);

                if(!success) {
                    break;
                }
            }

            if(success && !stillThrottled.isEmpty()) {
                Map<String, Long> eventsPerUser =
                        currentBatch
                                .stream()
                                .collect(
                                        Collectors.groupingBy(
                                                AmplitudeEvent::getUser_id,
                                                Collectors.counting()));
                List<String> report = new LinkedList<>();
                eventsPerUser.forEach((k, v) -> report.add(k + " - " + v));
                LOGGER.error("Cannot process throttles events for users: " + report);
                success = false;// all attempts to overcome throttling failed, restart
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
        AmplitudeForwardingConsumerConfig config = new AmplitudeForwardingConsumerConfig();

        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Amplitude Forwarding Consumer", e);
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
                LOGGER.info("Amplitude forwarding consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping amplitude forwarding consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(AmplitudeForwardingConsumerConfig config) {

        int batchSize = config.getRelevantBatchSize();

        if(batchSize < 1) {
            throw new IllegalArgumentException(
                    "Invalid " +
                            (config.isUseEventApi() ? "amplitudeEventApiBatchSize " : "amplitudeEventApiBatchSize ") +
                            batchSize);
        }

        if(config.getPercentageUsersForwarded() < 0 || config.getPercentageUsersForwarded() > 100) {
            throw new IllegalArgumentException("Invalid getPercentageUsersForwarded " + config.getPercentageUsersForwarded());
        }
        this.config = config;
    }

    void init() {

        try {
            this.amplitudeClient = new AmplitudeApiClient(config, !config.isUseEventApi());
            this.amplitudeBatchClient = new AmplitudeApiClient(config, true);

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

    private Optional<AmplitudeEvent> processRecord(ConsumerRecord<String, JsonNode> record) throws JsonProcessingException {

        if(isAcceptedMessage(record)) {
            JsonNode eventJson = record.value();
            AmplitudeEvent event = mapper.treeToValue(eventJson, AmplitudeEvent.class);

            if(CollectionUtils.isNotEmpty(config.getIgnoredUsers()) &&
                    (config.getIgnoredUsers().contains(event.getUser_id().toUpperCase()) || config.getIgnoredUsers().contains(event.getUser_id().toLowerCase()))) {
                LOGGER.warn("Ignoring event from user " + event.getUser_id());
                return Optional.empty();
            }
            return Optional.of(event);
        }
        return Optional.empty();
    }

    private Future<Boolean> submitBatch(final List<AmplitudeEvent> currentBatch, final List<AmplitudeEvent> throttledBatch) {
        return executor.submit(() -> sendCurrentBatchToAmplitude(currentBatch, throttledBatch, false));
    }

    private boolean sendCurrentBatchToAmplitude(final List<AmplitudeEvent> currentBatch, final List<AmplitudeEvent> throttledBatch, final boolean isForceBatchApi) {

        if(currentBatch.isEmpty()) {
            return true;
        }
        int nRetries = 2;
        int retryCounter = 0;
        AmplitudeApiException lastIoException = null;

        Map<String, Long> eventsPerType =
                currentBatch
                        .stream()
                        .collect(
                                Collectors.groupingBy(
                                        AmplitudeEvent::getEvent_type,
                                        Collectors.counting()));

        while(retryCounter < nRetries) {

            try {

                if(isForceBatchApi) {
                    this.amplitudeBatchClient.uploadEvents(currentBatch);
                } else {
                    this.amplitudeClient.uploadEvents(currentBatch);
                }
                // all succeeded
                report(RESULT_SUCCESS, eventsPerType);
                return true;
            } catch (AmplitudeApiException e) {

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
                    LOGGER.warn("Amplitude API throttled request: " + e.getRequestBody());
                    LOGGER.warn("Amplitude API throttling response: " + e.getResponseBody());
                    try {
                        JsonNode resp = mapper.readTree(e.getResponseBody());

                        if(resp.has("throttled_events")) {
                            ArrayNode lst = (ArrayNode)resp.get("throttled_events");
                            lst.elements().forEachRemaining(idx -> {
                                AmplitudeEvent event = currentBatch.get(idx.asInt());
                                throttledBatch.add(event);
                                eventsPerType.put(event.getEvent_type(), eventsPerType.get(event.getEvent_type()) - 1);
                            });
                            report(RESULT_SUCCESS, eventsPerType);
                        } else {
                            throttledBatch.addAll(currentBatch);
                        }
                        return true;
                    } catch (JsonProcessingException jsone) {
                        LOGGER.error("Error parsing throttling response: " + e.getResponseBody());
                    }
                } else if(e.getResponseCode() == 400) { // some of the events were invalid
                    LOGGER.error("Amplitude API invalid request response. Request: " + e.getRequestBody() + " Response: " + e.getResponseBody());
                } else if(e.getResponseCode() == 413) { // request too large
                    int maxBatch = isForceBatchApi ? config.getAmplitudeBatchApiBatchSize() : config.getAmplitudeEventApiBatchSize();
                    LOGGER.error("Request to Amplitude is too large. Batch size " + currentBatch.size() + ", max batch size " + maxBatch);
                    List<String> eventIds = currentBatch.stream().map(ae -> ae.getInsert_id()).collect(Collectors.toList());
                    LOGGER.warn("Amplitude API 413 events: " + eventIds);
                    LOGGER.warn("Amplitude API 413 response: " + e.getResponseBody());
                    // TBD: shall we partition the current batch and send again?
                    // Currently, we will stop the consumer due to invalid configuration
                } else { // log error only for responses other than above
                    LOGGER.error("Error sending events: " + e.getRequestBody() + ":" + e.getMessage(), e);
                }
                // all failed
                report(RESULT_ERROR, eventsPerType);
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
            report(RESULT_ERROR, eventsPerType);
        }
        return false;
    }

    private void report(String result, Map<String, Long> eventsPerType) {
        eventsPerType.entrySet().stream().forEach(e -> report(result, e.getKey(), e.getValue().intValue()));
    }

    private void report(String result, String eventType, int size) {

        if(size > 0) {
            recordsProcessedCounter.labels(
                    result, AirlockManager.getEnvVar(), AirlockManager.getProduct(), eventType).inc(size);
        }
    }

}
