package com.ibm.airlytics.consumer.cloning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.cloning.config.CloningConsumerConfig;
import com.ibm.airlytics.consumer.cloning.obfuscation.Obfuscator;
import com.ibm.airlytics.eventproxy.EventApiClient;
import com.ibm.airlytics.eventproxy.EventApiException;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;

public class CloningConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(CloningConsumer.class.getName());

    private static final String NOTIFICATIONS_SOURCE = "NOTIFICATIONS";

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_cloning_records_processed_total")
            .help("Total records processed by the cloning consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, AirlyticsConsumerConstants.SOURCE)
            .register();

    private static final Histogram proxyResponseTimes = Histogram.build()
            .name("airlytics_cloning_consumer_proxy_response_ms")
            .help("Airlytics events API response time in milliseconds.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, AirlyticsConsumerConstants.SOURCE)
            .register();

    final private ObjectMapper mapper = new ObjectMapper();

    private CloningConsumerConfig config;

    private CloningConsumerConfig newConfig;

    private Obfuscator obfuscator;

    private EventApiClient proxyClient;

    private ExecutorService executor;

    private RateLimiter rateLimiter;

    private String rawDataSource;

    public CloningConsumer() {}

    public CloningConsumer(CloningConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        rawDataSource = Environment.getAlphanumericEnv(AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM, false);
        if (!"ERROR".equals(rawDataSource) && !"SUCCESS".equals(rawDataSource)  && !"NOTIFICATIONS".equals(rawDataSource)) {
            throw new IllegalArgumentException("Illegal value for " + AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM + " environment parameter. Can be one of the following: ERROR, SUCCESS, NOTIFICATIONS");
        }
        LOGGER.info("cloning consumer created with configuration:" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if(consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();
        int recordsProcessed = 0;
        List<Future<Boolean>> futures = new LinkedList<>();
        List<ConsumerRecord<String, JsonNode>> noHeadersBatch = new LinkedList<>();
        List<ConsumerRecord<String, JsonNode>> headersBatch = new LinkedList<>();
        int batchSize = NOTIFICATIONS_SOURCE.equals(rawDataSource) ? 1 : config.getEventProxyIntegrationConfig().getEventApiBatchSize();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            processRecord(record, noHeadersBatch, headersBatch);

            if (noHeadersBatch.size() >= batchSize) {
                futures.add(submitBatchToProxy(noHeadersBatch, null));
                noHeadersBatch = new LinkedList<>();
            }

            if (headersBatch.size() >= batchSize) {

                splitHeadersBatch(headersBatch).forEach(batch -> {

                    if(!batch.isEmpty()) {
                        futures.add(submitBatchToProxy(batch, batch.get(0).headers()));
                    }
                });
                headersBatch = new LinkedList<>();
            }
            ++recordsProcessed;
        }
        // last batch
        futures.add(submitBatchToProxy(noHeadersBatch, null));

        if (headersBatch.size() > 0) {
            splitHeadersBatch(headersBatch).forEach(batch -> {

                if(!batch.isEmpty()) {
                    futures.add(submitBatchToProxy(batch, batch.get(0).headers()));
                }
            });
        }
        boolean success = true;

        for(Future<Boolean> future : futures) {

            try {

                if(!success) {// failed at a previous iteration
                    future.cancel(true);
                } else {
                    success = future.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Exception in the thread sending events to the eventas API", e);
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
        CloningConsumerConfig config = new CloningConsumerConfig();
        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Cloning Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("cloning consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping cloning consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    // used for unit-testing
    void setConfig(CloningConsumerConfig config) {

        if(config.getPercentageUsersCloned() < 0 || config.getPercentageUsersCloned() > 100 || config.getPercentageUsersCloned() == 0) {
            throw new IllegalArgumentException("Invalid percentageUsersCloned " + config.getPercentageUsersCloned());
        }

        if(config.getEventProxyIntegrationConfig().getEventApiBatchSize() < 1) {
            throw new IllegalArgumentException("Invalid eventBatchSize " + config.getEventProxyIntegrationConfig().getEventApiBatchSize());
        }

        if(config.getEventProxyIntegrationConfig().getEventApiParallelThreads() < 1) {
            throw new IllegalArgumentException("Invalid eventApiParallelThreads " + config.getEventProxyIntegrationConfig().getEventApiParallelThreads());
        }
        this.config = config;
    }

    void init() {

        if(!NOTIFICATIONS_SOURCE.equals(rawDataSource)) {// Normal Airlytics event
            this.obfuscator = new Obfuscator(this.config);
        }

        if (this.config.getEventProxyIntegrationConfig().getEventApiRateLimit() > 0) {
            this.rateLimiter = RateLimiter.create(this.config.getEventProxyIntegrationConfig().getEventApiRateLimit());
        }
        this.proxyClient = new EventApiClient(this.config.getEventApiClientConfig(), true);

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
        this.executor = Executors.newFixedThreadPool(this.config.getEventProxyIntegrationConfig().getEventApiParallelThreads());
    }

    private void  processRecord(
            ConsumerRecord<String, JsonNode> record,
            List<ConsumerRecord<String, JsonNode>> noHeadersBatch,
            List<ConsumerRecord<String, JsonNode>> headersBatch) {

        if(isAcceptedMessage(record)) {

            if(!NOTIFICATIONS_SOURCE.equals(rawDataSource)) {// Normal Airlytics event
                JsonNode event = record.value();
                ((ObjectNode) event).remove("error");

                applyAttributeFilters(
                        event,
                        EVENT_ATTRIBUTES_FIELD,
                        config.getIgnoreEventAttributes(),
                        config.getIncludeEventAttributes());

                applyAttributeFilters(
                        event,
                        EVENT_CUSTOM_DIMENSIONS_FIELD,
                        config.getIgnoreCustomDimensions(),
                        config.getIncludeCustomDimensions());

                if (obfuscator.hasRules()) {
                    obfuscator.obfuscate(event);
                }

                if(record.headers() != null && record.headers().toArray().length > 0) {
                    headersBatch.add(record);
                } else {
                    noHeadersBatch.add(record);
                }
            } else if(AirlockManager.getProduct().toLowerCase().contains("android")) {// Android Notification event
                // for purchase notifications from Android, encode the message
                JsonNode event = record.value();

                try {
                    String data = mapper.writeValueAsString(event);
                    String encodedData = Base64.getEncoder().encodeToString(data.getBytes());
                    ObjectNode message = mapper.createObjectNode();
                    message.put("data", encodedData);
                    ObjectNode wrapper = mapper.createObjectNode();
                    wrapper.set("message", message);
                    ConsumerRecord<String, JsonNode> transformed =
                            new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.key(), wrapper);

                    noHeadersBatch.add(transformed);
                } catch (JsonProcessingException e) {
                    // should not happen
                }
            } else {
                noHeadersBatch.add(record);
            }
        }
    }

    private boolean isAcceptedMessage(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();

        if(!NOTIFICATIONS_SOURCE.equals(rawDataSource)) {

            if (!event.has(USER_ID_FIELD)) {
                LOGGER.error("Ignoring illegal event without '" + USER_ID_FIELD + "': " + event.toString());
                return false;
            }

            if (!event.has(EVENT_NAME_FIELD)) {
                LOGGER.error("Ignoring illegal event without '" + EVENT_NAME_FIELD + "': " + event.toString());
                return false;
            }
            String eventName = event.get(EVENT_NAME_FIELD).textValue();

            if(config.getIgnoreEventTypes() != null && config.getIgnoreEventTypes().contains(eventName)) {
                return false;
            }

            if(config.getIncludeEventTypes() != null && !config.getIncludeEventTypes().contains(eventName)) {
                return false;
            }

            if(config.getEventProxyIntegrationConfig().getObfuscation() != null) {
                String userId = event.get(USER_ID_FIELD).textValue();
                String prefix = config.getEventProxyIntegrationConfig().getObfuscation().getHashPrefix();
                boolean isResend = config.getEventProxyIntegrationConfig().getObfuscation().isResendObfuscated();

                if (StringUtils.isNotBlank(prefix) && userId.startsWith(prefix)) {
                    // in test, do not re-send those already processed in a previous iteration
                    return isResend;
                }
            }
        }

        if(config.getPercentageUsersCloned() == 100) {
            return true;
        }

        if(config.getPercentageUsersCloned() > 0) {
            return record.partition() < config.getPercentageUsersCloned();
        }
        return false;
    }

    private void applyAttributeFilters(JsonNode event, String parentField, List<String> ignored, List<String> included) {
        ObjectNode eventAttributes = (ObjectNode)event.findValue(parentField);

        if ( eventAttributes != null &&
                (ignored != null || included != null) ) {
            Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
            List<String> attributeNames = new LinkedList<>();

            while (eventFieldsIter.hasNext()) {
                attributeNames.add(eventFieldsIter.next());
            }

            for (String attribute : attributeNames) {

                if(ignored != null && ignored.contains(attribute)) {
                    eventAttributes.remove(attribute);
                } else if(included != null && !included.contains(attribute)) {
                    eventAttributes.remove(attribute);
                }
            }
        }
    }

    private boolean sendCurrentBatchToProxy(List<ConsumerRecord<String, JsonNode>> currentBatch, Headers headers, boolean is202Retry) {

        if(currentBatch.isEmpty()) {
            return true;
        }
        JsonNode json = batchToJson(currentBatch);

        if(json == null) {
            return false;
        }
        int retryCounter = 0;
        IOException lastIoException = null;

        while(retryCounter < config.getEventProxyIntegrationConfig().getEventApiRetries()) {
            Histogram.Timer timer = proxyResponseTimes.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).startTimer();

            try {
                acquirePermit();
                this.proxyClient.post(json.toString(), headers);
                // all succeeded
                recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).inc(currentBatch.size());
                return true;
            } catch (IOException e) {
                retryCounter++;
                lastIoException = e;

                try {
                    Thread.sleep(retryCounter * 1000L);
                } catch (InterruptedException e1) {
                    // it seems, the process is being stopped, so, stop retrying
                }
            } catch (EventApiException e) {

                if (e.is202Accepted()) {

                    if (is202Retry || retryOn202(currentBatch, headers, e)) {
                        return true;// do not report the error
                    }
                } else { // log error only for responses other than 202
                    LOGGER.error("Error sending events: " + json + ":" + e.getMessage(), e);
                }
                retryCounter++;
            } finally {
                timer.observeDuration();
            }
        }

        if (retryCounter >= config.getEventProxyIntegrationConfig().getEventApiRetries()) {

            if(lastIoException != null) {
                LOGGER.error("I/O error sending events: " + json + ":" + lastIoException.getMessage(), lastIoException);
            } else {
                LOGGER.error("Error sending events: " + json);
            }
            // all failed
            recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).inc(currentBatch.size());
        }
        return false;
    }

    private void acquirePermit() {

        if (this.rateLimiter != null) {
            this.rateLimiter.acquire();
        }
    }

    private Future<Boolean> submitBatchToProxy(final List<ConsumerRecord<String, JsonNode>> currentBatch, Headers headers) {
        return executor.submit(() -> sendCurrentBatchToProxy(currentBatch, headers, false));
    }

    private boolean retryOn202(List<ConsumerRecord<String, JsonNode>> currentBatch, Headers headers, EventApiException e) {
        // some events may need to be retried
        if(e.is202Accepted() && e.getResponseBody() != null) {

            try {
                String responseBody = e.getResponseBody();
                JsonNode responseJson = mapper.readTree(responseBody);

                if(responseJson instanceof ArrayNode) {
                    ArrayNode events = (ArrayNode)responseJson;
                    List<String> retries = new LinkedList<>();

                    for(int i = 0; i < events.size(); i++) {
                        JsonNode event = events.get(i);

                        if(event.has("eventId")) {

                            if (event.has("shouldRetry")) {

                                if ("true".equalsIgnoreCase(event.get("shouldRetry").asText())) {
                                    retries.add(event.get("eventId").textValue());
                                    continue;
                                }
                            }

                            if(event.has("error")) {
                                JsonNode jsonError = event.get("error");

                                if(jsonError instanceof ArrayNode) {
                                    ArrayNode jsonErrors = (ArrayNode) jsonError;

                                    if (jsonErrors.size() > 0) {
                                        // one failed
                                        recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).inc();
                                        continue;
                                    }
                                }
                                else if(jsonError.has("message") && !jsonError.get("message").isNull()) {
                                    // one failed
                                    recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).inc();
                                    continue;
                                }
                            }
                            // one succeeded
                            recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct(), rawDataSource).inc();
                        }
                    }

                    if(!retries.isEmpty()) {
                        List<ConsumerRecord<String, JsonNode>> newBatch =
                                currentBatch.stream()
                                        .filter(r -> retries.contains(r.value().get("eventId").asText()))
                                        .collect(Collectors.toList());
                        return sendCurrentBatchToProxy(newBatch, headers, true);
                    }
                    return true;
                }
            }
            catch (Exception e1) {
                LOGGER.error("Unrecognized response format from event API: " + e.getResponseBody());
            }
        }
        return false;
    }

    private JsonNode batchToJson(List<ConsumerRecord<String, JsonNode>> currentBatch)  {

        if(NOTIFICATIONS_SOURCE.equals(rawDataSource)) {

            if(currentBatch.size() == 1) {
                ConsumerRecord<String, JsonNode> record = currentBatch.get(0);
                return record.value();
            } else {
                LOGGER.error("Purchase Notifications API doe not support batches");
            }
        } else {
            ArrayNode events = mapper.createArrayNode();
            currentBatch.forEach(r -> events.add(r.value()));
            ObjectNode wrapper = mapper.createObjectNode();
            wrapper.set("events", events);
            return wrapper;
        }
        return null;
    }

    private Collection<List<ConsumerRecord<String, JsonNode>>> splitHeadersBatch(List<ConsumerRecord<String, JsonNode>> currentBatch) {
        Map<String, List<ConsumerRecord<String, JsonNode>>> byCurrentDeviceTime = new TreeMap<>();

        for(ConsumerRecord<String, JsonNode> record : currentBatch) {
            String key = "";

            if(record.headers() != null) {

                for(Header header : record.headers().toArray()) {

                    if("x-current-device-time".equalsIgnoreCase(header.key())) {
                        key = new String(header.value(), StandardCharsets.UTF_8);
                        key = key + record.value().get("userId").asText();
                    }
                }
            }
            addToMappedList(byCurrentDeviceTime, key, record);
        }
        return byCurrentDeviceTime.values();
    }

    private <T> void addToMappedList(Map<String, List<T>> map, String key, T value) {
        List<T> list = map.get(key);

        if(list == null) {
            list =  new LinkedList<>();
            map.put(key, list);
        }
        list.add(value);
    }
}