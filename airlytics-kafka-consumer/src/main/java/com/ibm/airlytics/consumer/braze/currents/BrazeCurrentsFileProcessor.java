package com.ibm.airlytics.consumer.braze.currents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.EvictingQueue;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.dto.EnvironmentDefinition;
import com.ibm.airlytics.consumer.integrations.dto.FileProcessingRequest;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractThirdPartyEventsFileProcessor;
import com.ibm.airlytics.eventproxy.EventApiClient;
import com.ibm.airlytics.eventproxy.EventApiException;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.datamarker.FileProgressTracker;
import com.ibm.airlytics.utilities.datamarker.ReadProgressMarker;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;

public class BrazeCurrentsFileProcessor extends AbstractThirdPartyEventsFileProcessor {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeCurrentsFileProcessor.class.getName());

    //count braze currents events: labels: env, product
    static final Counter eventsCounter = Counter.build()
            .name("airlytics_braze_currents_events_counter")
            .help("Total Braze Currents events")
            .labelNames(AirlyticsConsumerConstants.ENV, PRODUCT).register();

    private static final String APP_ID_MATCHER = "app_id";
    private static final String S3_FILE_SEPARATOR = "/";
    private static final String EVENT_TYPE_PATH_PREFIX = "event_type=";
    private static final String EVENT_TYPE_INNER_PREFIX = "dataexport.S3.inner.";

    final private ObjectMapper mapper = new ObjectMapper();

    final private BrazeCurrentsConsumerConfig config;
    final private String awsSecret;
    final private String awsKey;

    private Queue<Long> processingTimes = EvictingQueue.create(1000);

    public BrazeCurrentsFileProcessor(BrazeCurrentsConsumerConfig config, ReadProgressMarker readProgressMarker) {
        super(
                config.getEventApiClientConfig(),
                config.getEventProxyIntegrationConfig(),
                config.getProducts(),
                readProgressMarker,
                100);

        this.eventTransformer = new BrazeCurrentsEventTransformer(config);

        this.config = config;
        this.awsSecret = Environment.getEnv(AWS_ACCESS_SECRET_PARAM, true);
        this.awsKey = Environment.getEnv(AWS_ACCESS_KEY_PARAM, true);
    }

    /**
     * Call it before re-creating a file processor with a new Airlock config
     */
    public void shutdown() {
        // nothing to shutdown, for now
    }

    public Collection<String> processFile(FileProcessingRequest request) throws IOException, EventApiException {
        long jobStartTS = System.currentTimeMillis();

        if(config.getPercentageUsersSent() <= 0) {
            LOGGER.warn("Configured to send " + config.getPercentageUsersSent() + " users");
            return Collections.emptyList();
        }

        String[] pathSegments = request.getFileName().split(S3_FILE_SEPARATOR);
        String eventType = null;

        for(String segment : pathSegments) {

            if(segment.startsWith(EVENT_TYPE_PATH_PREFIX)) {
                eventType = segment.substring(EVENT_TYPE_PATH_PREFIX.length());
            }
        }

        if(eventType == null) {
            LOGGER.warn("Could not extract event type from path " + request.getFileName());
        } else if(CollectionUtils.isNotEmpty(config.getIncludeEventTypes()) && !config.getIncludeEventTypes().contains(EVENT_TYPE_INNER_PREFIX+eventType)) {
            LOGGER.warn("Event type " + eventType + " from path " + request.getFileName() + " is not listed for processing");
            return Collections.emptyList();
        }

        final String fullPath = request.getBucketName() + S3_FILE_SEPARATOR + request.getFileName();
        String fileUrl = "s3://" + fullPath;
        LOGGER.info("Processing " + fullPath);

        Map<String, ProductDefinition> productPerApp = new HashMap<>();
        Map<String, Boolean> isDevPerApp = new HashMap<>();
        Map<String, EventApiClient> proxyClientPerApp = new HashMap<>();
        Map<String, String> progressFileNamePerApp = new HashMap<>();
        Map<String, FileProgressTracker> fileTrackerPerApp = new HashMap<>();
        Map<String, Integer> lastRecordSentPerApp = new HashMap<>();
        Map<String, Integer> currentEventPerApp = new HashMap<>();
        Map<String, List<AirlyticsEvent>> currentBatchPerApp = new HashMap<>();
        int idx = -1;
        int sentCount = 0;
        boolean success = true;

        try(DataFileStream<GenericRecord> avroReader = getAvroReader(request)) {

            while (avroReader.hasNext()) {
                GenericRecord rec = avroReader.next();
                ++idx;

                if (this.config.getPercentageUsersSent() < 100) {
                    Object oUserId = rec.get("external_user_id");

                    if(oUserId == null) {
                        LOGGER.info("Record missing external_user_id: " + rec.toString());
                        continue;
                    }
                    String userId = oUserId.toString();

                    if (getUserPartition(userId) > this.config.getPercentageUsersSent()) {
                        continue;
                    }
                }

                if(!rec.hasField("app_id")) {
                    LOGGER.warn("Record missing app_id: " + rec.toString());
                    continue;
                }
                Object oAppId = rec.get("app_id");

                if(oAppId == null) {
                    LOGGER.warn("Record missing app_id: " + rec.toString());
                    continue;
                }
                final String appId = oAppId.toString().toLowerCase();
                ProductDefinition product = getProductDefinition(productPerApp, appId);

                if (product == null) {
                    LOGGER.warn("No matching product for app_id" + appId);
                    continue;
                }
                Boolean isDev = getIsDev(isDevPerApp, appId);
                EventApiClient proxyClient = getEventApiClient(proxyClientPerApp, appId, product, isDev);
                List<AirlyticsEvent> currentBatch = getCurrentBatch(currentBatchPerApp, appId);

                Optional<AirlyticsEvent> optEvent = this.eventTransformer.transform(rec.getSchema().getFullName(), rec, product);

                if (optEvent.isPresent()) {
                    AirlyticsEvent event = optEvent.get();
                    currentBatch.add(event);
                } else {
                    LOGGER.warn("Could not transform record " + idx + " in file " + fullPath);
                }

                if (currentBatch.size() >= this.config.getEventProxyIntegrationConfig().getEventApiBatchSize()) {
                    String progressFileName = getProgressFileName(fileUrl, progressFileNamePerApp, appId, product, isDev);
                    FileProgressTracker fileTracker = getFileProgressTracker(fileTrackerPerApp, appId);
                    Integer lastRecordSent = getLastRecordSent(lastRecordSentPerApp, appId, product, isDev, progressFileName, fileTracker);
                    Integer currentEvent = getCurrentEvent(currentEventPerApp, appId, product, isDev, fileTracker);

                    if (!this.readProgressMarker.shouldSkipBatch(fileTracker, isDev, product.getId(), currentBatch.size(), lastRecordSent, currentEvent)) {
                        success = sendCurrentBatchToProxy(proxyClient, product, currentBatch, isDev, false);

                        if(success) {
                            sentCount += currentBatch.size();
                            currentEvent += currentBatch.size();
                            currentEventPerApp.put(appId, currentEvent);
                            readProgressMarker.markBatch(fileTracker, isDev, product.getId(), currentEvent, progressFileName);
                        }
                    } else {
                        LOGGER.info("Skipping previously sent batch of " + currentBatch.size() + " events");
                    }

                    if(success) {
                        currentBatchPerApp.put(appId, new LinkedList<>());
                    } else {
                        break;
                    }
                }
            }

            if(success) {

                // last batches
                for(Map.Entry<String, List<AirlyticsEvent>> e : currentBatchPerApp.entrySet()) {
                    String appId = e.getKey();
                    List<AirlyticsEvent> currentBatch = e.getValue();
                    ProductDefinition product = getProductDefinition(productPerApp, appId);
                    Boolean isDev = getIsDev(isDevPerApp, appId);
                    EventApiClient proxyClient = getEventApiClient(proxyClientPerApp, appId, product, isDev);
                    String progressFileName = getProgressFileName(fileUrl, progressFileNamePerApp, appId, product, isDev);
                    FileProgressTracker fileTracker = getFileProgressTracker(fileTrackerPerApp, appId);
                    Integer lastRecordSent = getLastRecordSent(lastRecordSentPerApp, appId, product, isDev, progressFileName, fileTracker);
                    Integer currentEvent = getCurrentEvent(currentEventPerApp, appId, product, isDev, fileTracker);

                    if (!this.readProgressMarker.shouldSkipBatch(fileTracker, isDev, product.getId(), currentBatch.size(), lastRecordSent, currentEvent)) {
                        success = sendCurrentBatchToProxy(proxyClient, product, currentBatch, isDev, false);

                        if (success) {
                            sentCount += currentBatch.size();
                            currentEvent += currentBatch.size();
                            currentEventPerApp.put(appId, currentEvent);
                            readProgressMarker.markBatch(fileTracker, isDev, product.getId(), currentEvent, progressFileName);
                        }
                    } else {
                        LOGGER.info("Skipping previously sent batch of " + currentBatch.size() + " events");
                    }
                }
            }
        }

        if(!success) {
            throw new EventApiException("Processing failed for file " + fullPath);
        }
        long took = System.currentTimeMillis() - jobStartTS;
        this.processingTimes.add(took);
        long average = new Double(this.processingTimes.stream().mapToLong(l -> l).average().orElse(0.0)).longValue();
        LOGGER.info(
                "Finished processing " + fullPath +
                        ", processed " + (idx + 1) + " rows" +
                        ", sent " + sentCount + " events" +
                        ", took " + took + "ms" +
                        ", with average " + average + "ms");
        List<String> filesToDelete = new LinkedList<>();

        for(FileProgressTracker ft : fileTrackerPerApp.values()) {
            filesToDelete.addAll(ft.getProgressFilesToDelete());
        }
        return filesToDelete;
    }

    private Integer getCurrentEvent(Map<String, Integer> currentEventPerApp, String appId, ProductDefinition product, Boolean isDev, FileProgressTracker fileTracker) {
        Integer currentEvent = currentEventPerApp.get(appId);

        if(currentEvent ==  null) {
            currentEvent = this.readProgressMarker.getCurrentEvent(fileTracker, product.getId(), isDev);
            currentEventPerApp.put(appId, currentEvent);
        }
        return currentEvent;
    }

    private Integer getLastRecordSent(Map<String, Integer> lastRecordSentPerApp, String appId, ProductDefinition product, Boolean isDev, String progressFileName, FileProgressTracker fileTracker) throws IOException {
        Integer lastRecordSent = lastRecordSentPerApp.get(appId);

        if(lastRecordSent == null) {
            lastRecordSent = this.readProgressMarker.getLastRecordSent(fileTracker, isDev, product.getId(), progressFileName);
            lastRecordSentPerApp.put(appId, lastRecordSent);
        }
        return lastRecordSent;
    }

    private FileProgressTracker getFileProgressTracker(Map<String, FileProgressTracker> fileTrackerPerApp, String appId) {
        FileProgressTracker fileTracker = fileTrackerPerApp.get(appId);

        if(fileTracker == null) {
            fileTracker = new FileProgressTracker();
            fileTrackerPerApp.put(appId, fileTracker);
        }
        return fileTracker;
    }

    private String getProgressFileName(String fileUrl, Map<String, String> progressFileNamePerApp, String appId, ProductDefinition product, Boolean isDev) {
        String progressFileName = progressFileNamePerApp.get(appId);

        if(progressFileName == null) {
            progressFileName = this.readProgressMarker.getProgressFileName(fileUrl.substring(fileUrl.lastIndexOf(ReadProgressMarker.S3_FILE_SEPARATOR)), product.getId(), isDev);
            progressFileNamePerApp.put(appId, progressFileName);
        }
        return progressFileName;
    }

    private List<AirlyticsEvent> getCurrentBatch(Map<String, List<AirlyticsEvent>> currentBatchPerApp, String appId) {
        List<AirlyticsEvent> currentBatch = currentBatchPerApp.get(appId);

        if(currentBatch == null) {
            currentBatch = new LinkedList<>();
            currentBatchPerApp.put(appId, currentBatch);
        }
        return currentBatch;
    }

    private EventApiClient getEventApiClient(Map<String, EventApiClient> proxyClientPerApp, String appId, ProductDefinition product, Boolean isDev) throws EventApiException {
        EventApiClient proxyClient = proxyClientPerApp.get(appId);

        if(proxyClient == null) {
            proxyClient = getProxyClient(product.getId(), isDev);

            if (this.config.getEventProxyIntegrationConfig().isEventApiEnabled() && proxyClient == null) {
                throw new EventApiException("Event API is not configured for product " + product.getId());
            }
            proxyClientPerApp.put(appId, proxyClient);
        }
        return proxyClient;
    }

    private Boolean getIsDev(Map<String, Boolean> isDevPerApp, String appId) {
        Boolean isDev = isDevPerApp.get(appId);

        if(isDev == null) {
            isDev = isDev(appId);
            isDevPerApp.put(appId, isDev);
        }
        return isDev;
    }

    private ProductDefinition getProductDefinition(Map<String, ProductDefinition> productPerApp, String appId) {
        ProductDefinition product = productPerApp.get(appId);

        if(product == null) {
            product =
                    this.products.stream()
                            .filter(p ->
                                    p.getConditions().get(APP_ID_MATCHER) != null &&
                                            p.getConditions()
                                                    .get(APP_ID_MATCHER)
                                                    .stream()
                                                    .anyMatch(m -> m.equalsIgnoreCase(appId))

                            )
                            .findAny()
                            .orElse(null);
            productPerApp.put(appId, product);
        }
        return product;
    }

    public boolean isDev(String appId) {
        boolean isDev = false;

        if(this.config.getEnvironments() != null && !this.config.getEnvironments().isEmpty()) {
            EnvironmentDefinition env =
                    this.config.getEnvironments().stream()
                            .filter(p ->
                                    p.getConditions().get(APP_ID_MATCHER) != null &&
                                            p.getConditions()
                                                    .get(APP_ID_MATCHER)
                                                    .stream()
                                                    .anyMatch(m -> m.equalsIgnoreCase(appId))

                            )
                            .findAny()
                            .orElse(null);
            isDev = env != null && env.isDevEnvironment();
        }
        return isDev;
    }

    private DataFileStream<GenericRecord> getAvroReader(FileProcessingRequest request) throws IOException {
        Configuration conf = new Configuration();

        //s3
        if (awsKey != null) {
            conf.set("fs.s3a.access.key", awsKey);
        }
        if (awsSecret != null) {
            conf.set("fs.s3a.secret.key", awsSecret);
        }
        conf.set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        String bucket = request.getBucketName();
        String filePath = request.getFileName();

        if(!filePath.startsWith(S3_FILE_SEPARATOR)) {
            filePath = S3_FILE_SEPARATOR + filePath;
        }
        int retryCounter = 0;

        while (retryCounter < config.getMaxAwsRetries()) {

            try {
                FileSystem fs = FileSystem.get(URI.create("s3a://" + bucket + S3_FILE_SEPARATOR), conf);
                Path path = new Path(filePath);

                // create avro generic record reader
                GenericDatumReader<GenericRecord> avroGenericRecordReader = new GenericDatumReader<>();

                BufferedInputStream bufferedStream = new BufferedInputStream(fs.open(path));
                return new DataFileStream<>(bufferedStream, avroGenericRecordReader);
            } catch (IOException e) {
                LOGGER.info("(" + retryCounter + ") Cannot open file " + bucket + filePath + " for reading: " + e.getMessage());
                retryCounter++;

                if (retryCounter == config.getMaxAwsRetries()) {
                    throw e;
                }

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException interruptedException) {
                    // ignore
                }
            }
        }
        return null;// unreachable
    }

    private boolean sendCurrentBatchToProxy(EventApiClient proxyClient, ProductDefinition product, List<AirlyticsEvent> currentBatch, boolean isDev, boolean is202Reply) {

        if(currentBatch.isEmpty()) {
            return true;
        }
        String json = batchToJson(currentBatch);

        if(json == null) {
            return false;
        }
        int retryCounter = 0;
        IOException lastIoException = null;

        while(retryCounter < config.getEventProxyIntegrationConfig().getEventApiRetries()) {

            try {
                acquirePermit(product.getId());

                if(proxyClient != null) {
                    proxyClient.post(json);
                } else {
                    LOGGER.info("Event API is disabled - otherwise, we would send " + currentBatch.size() + " events");
                    LOGGER.info(json);
                }
                eventsCounter.labels(isDev ? "DEV" : "PROD", product.getPlatform()).inc(currentBatch.size());
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

                    if (is202Reply || retryOn202(proxyClient, product, currentBatch, isDev, e)) {
                        return true;// do not report the error
                    }
                } else { // log error only for responses other than 202
                    LOGGER.error("Error sending events: " + json + ":" + e.getMessage(), e);
                }
                retryCounter++;
            }
        }

        if (retryCounter >= config.getEventProxyIntegrationConfig().getEventApiRetries()) {

            if(lastIoException != null) {
                LOGGER.error("I/O error sending events to proxy:" + lastIoException.getMessage(), lastIoException);
            } else {
                LOGGER.error("Error sending events to proxy.");
            }
        }
        return false;
    }

    private boolean retryOn202(EventApiClient proxyClient, ProductDefinition product, List<AirlyticsEvent> currentBatch, boolean isDev, EventApiException e) {
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
                                        LOGGER.warn("Event Proxy returned error message: " + jsonError.toString());
                                    }
                                }
                                else if(jsonError.has("message") && !jsonError.get("message").isNull()) {
                                    // one failed
                                    LOGGER.warn("Event Proxy returned error message: " + jsonError.toString());
                                }
                            }
                        }
                    }

                    if(!retries.isEmpty()) {
                        List<AirlyticsEvent> newBatch =
                                currentBatch.stream()
                                        .filter(ev -> retries.contains(ev.getEventId()))
                                        .collect(Collectors.toList());
                        return sendCurrentBatchToProxy(proxyClient, product, newBatch, isDev, true);
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

    private String batchToJson(List<AirlyticsEvent> currentBatch) {
        ArrayNode events = mapper.createArrayNode();

        for(AirlyticsEvent e : currentBatch) {
            JsonNode node = mapper.valueToTree(e);
            obfuscateIfNeeded(node);
            events.add(node);
        }
        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.set("events", events);

        try {
            return mapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            // safe to ignore
        }
        // generally unreachable
        return null;
    }
}
