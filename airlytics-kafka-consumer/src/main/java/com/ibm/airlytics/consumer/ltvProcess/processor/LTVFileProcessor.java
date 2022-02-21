package com.ibm.airlytics.consumer.ltvProcess.processor;

import com.amazonaws.util.Base32;
import com.google.common.primitives.Longs;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.integrations.dto.FileProcessingRequest;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.consumer.integrations.mappings.AbstractThirdPartyEventsFileProcessor;
import com.ibm.airlytics.consumer.ltvProcess.*;
import com.ibm.airlytics.eventproxy.EventApiClient;
import com.ibm.airlytics.eventproxy.EventApiException;
import com.ibm.airlytics.utilities.AesGcmEncryption;
import com.ibm.airlytics.utilities.datamarker.FileProgressTracker;
import com.ibm.airlytics.utilities.datamarker.ReadProgressMarker;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.ibm.airlytics.utilities.Environment;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.eco.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.json.JSONObject;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;

public class LTVFileProcessor extends AbstractThirdPartyEventsFileProcessor {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(com.ibm.airlytics.consumer.ltvProcess.processor.LTVFileProcessor.class.getName());

    private LTVProcessConsumerConfig config;
    private String awsSecret;
    private String awsKey;
    private final AesGcmEncryption aes = new AesGcmEncryption();
    private static final double USD_TO_MICROS = 1000000;
    private static long USEC_TO_MILLI = 1000;
    private static String IOS_PRODUCT_PROD_ID = "<put_product_id_here>";
    private static String ANDROID_PRODUCT_PROD_ID = "<put_other_product_id_here>";
    private static String IOS_APP_VERSION_PREFIX1 = "<version_prefix>";
    private static String IOS_APP_VERSION_PREFIX2 = "<version_prefix>";
    private static String ANDROID_APP_VERSION_PREFIX = "<android_app_version_prefix>";

    public LTVFileProcessor(LTVProcessConsumerConfig config, ReadProgressMarker readProgressMarker) {
        super(
                config.getEventApiClientConfig(),
                config.getEventProxyIntegrationConfig(),
                config.getProducts(),
                readProgressMarker,
                config.getPartitionsNumber());

        this.config = config;
        this.awsSecret = Environment.getEnv(AWS_ACCESS_SECRET_PARAM, true);
        this.awsKey = Environment.getEnv(AWS_ACCESS_KEY_PARAM, true);
    }

    public List<String> processFile(FileProcessingRequest request, Counter ltvEventsCounter, Counter ltvErrorEventsCounter, Summary revenueSummary) throws Exception {
        String filePath = request.getBucketName()+ File.separator+request.getFileName();

        LOGGER.info(" ***** Start processing file: " + filePath);
        Set<String> progressFilesToDeleteSet = new HashSet<>();

        ReadFileTask rft = new ReadFileTask(filePath , ltvEventsCounter, ltvErrorEventsCounter, revenueSummary);

        try {
            ReadFileTask.Result res = rft.call();
            if (res.error != null && !res.error.isEmpty()) {
                LOGGER.error("error reading file:"+filePath+":"+res.error);
                return null;
            } else {
                progressFilesToDeleteSet = res.progressFilesToDeleteSet;
            }
        } catch (Exception e) {
            LOGGER.error("error (exception) reading file:"+filePath+":"+e.getMessage());
            throw e;
        }

        List<String> progressFilesToDeleteList = new ArrayList<>(progressFilesToDeleteSet);
        return progressFilesToDeleteList;
    }

    private class ReadFileTask implements Callable {

        class Result {
            String error = null;
            String path;
            Set<String> progressFilesToDeleteSet = new HashSet<>();
        }

        private String filePath;
        private long totalReadTimeMs = 0;
        private long totalSendTimeMS = 0;
        private Counter ltvEventsCounter;
        private Counter ltvErrorEventsCounter;
        private Summary revenueSummary;

        public ReadFileTask(String filePath, Counter ltvEventsCounter, Counter ltvErrorEventsCounter, Summary revenueSummary) {
            this.filePath = filePath;
            this.revenueSummary = revenueSummary;
            this.ltvErrorEventsCounter = ltvErrorEventsCounter;
            this.ltvEventsCounter = ltvEventsCounter;
        }

        public Result call() throws IOException, InterruptedException, EventApiException {
            Instant startTaskTime = Instant.now();
            Result result = new Result();
            result.path = filePath;
            Map<String, List<AirlyticsEvent>> events = new HashMap<>();
            Map<String, List<AirlyticsEvent>> devEvents = new HashMap<>();
            FileProgressTracker tracker = new FileProgressTracker();
            result.progressFilesToDeleteSet = tracker.getProgressFilesToDelete();
            long eventsNumber = 0;
            long nonEventsNumber = 0;
            int counter = 0;

            try {
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
                filePath = "s3a://" + filePath;


                Path path = new Path(filePath);

                //exception can be thrown when the file size is 0. This happens when the file was opened for wrting by the
                //persistence consumer but was not closed yet
                int retryCounter = 0;
                ParquetFileReader reader = null;
                while (retryCounter < config.getIoActionRetries()) {
                    try {
                        reader = new ParquetFileReader(HadoopInputFile.fromPath(path, conf), ParquetReadOptions.builder().build());
                        break;
                    } catch (Exception e) {
                        LOGGER.info("(" + retryCounter + ") Cannot open file " + filePath + " for reading: " + e.getMessage());
                        retryCounter++;
                        if (retryCounter == config.getIoActionRetries()) {
                            throw e;
                        }
                        Thread.sleep(3000);
                    }
                }

                try {
                    ParquetMetadata md = reader.getFooter();
                    MessageType schema = md.getFileMetaData().getSchema();

                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

                    PageReadStore pages;

                    Instant startReadTime = Instant.now();

                    while (null != (pages = reader.readNextRowGroup())) {
                        final long rows = pages.getRowCount();
                        RecordReader<Group> recordReader = columnIO.getRecordReader(
                                pages, new GroupRecordConverter(schema));
                        totalReadTimeMs += Duration.between(startReadTime, Instant.now()).toMillis();

                        for (int i = 0; i < rows; i++) {

                            counter++;

                            startReadTime = Instant.now();
                            Group g = recordReader.read();
                            totalReadTimeMs += Duration.between(startReadTime, Instant.now()).toMillis();

                            Pair<AirlyticsEvent, Boolean> ltvResult = createLtvEvent(g, filePath, counter);
                            AirlyticsEvent event = ltvResult.getLeft();
                            if (event != null) {
                                eventsNumber++;

                                boolean devUser = ltvResult.getRight();
                                EventApiClient eventApiClient = devUser ? devApiClients.get(event.getProductId()) : apiClients.get(event.getProductId());
                                Map<String, List<AirlyticsEvent>> currEvents =devUser ? devEvents : events;
                                if(eventApiClient != null) {
                                    addAirlyticsEventToList(currEvents, event, eventApiClient, tracker, devUser);
                                } else if(config.getEventProxyIntegrationConfig().isEventApiEnabled()) {
                                    LOGGER.error("Event API is not configured for product " + event.getProductId());
                                    throw new EventApiException("Event API is not configured for product " + event.getProductId());
                                } else {
                                    LOGGER.warn((devUser? "DEV" : "PROD") + " proxy client is not configured for product " + event.getProductId());
                                }
                            } else {
                                nonEventsNumber++;
                            }
                        }

                        startReadTime = Instant.now();
                    }
                } finally {
                    reader.close();

                    //send prod events
                    for (Map.Entry<String, List<AirlyticsEvent>> entry : events.entrySet()) {
                        String prodId = entry.getKey();
                        List<AirlyticsEvent> currEvents = entry.getValue();
                        if (currEvents.size() > 0) {
                            sendBatchToProxy(currEvents, prodId, apiClients.get(prodId), tracker, false);
                            LOGGER.debug("filePath:"+filePath+":sent events: "+currEvents.size()+" prod events to prodID:"+prodId);
                        }
                    }

                    //send dev events
                    for (Map.Entry<String, List<AirlyticsEvent>> entry : devEvents.entrySet()) {
                        String prodId = entry.getKey();
                        List<AirlyticsEvent> currEvents = entry.getValue();
                        if (currEvents.size() > 0) {
                            sendBatchToProxy(currEvents, prodId, devApiClients.get(prodId), tracker, true);
                            LOGGER.debug("filePath:"+filePath+":sent events: "+currEvents.size()+" dev events to prodID:"+prodId);
                        }
                    }
                    long totalTimeMs = Duration.between(startTaskTime, Instant.now()).toMillis();
                    LOGGER.info("***** Finished processing file: '" + filePath + "', eventsNumber = " + eventsNumber + "', nonEventsNumber = " + nonEventsNumber + " total time = " + totalTimeMs + " ms, readTime = " + totalReadTimeMs + " ms, sendTime = " + totalSendTimeMS + " ms" );
                    LOGGER.info("***** Sending " + (eventsNumber / ((totalTimeMs / 1000)+1)) + " events/sec");

                }
            } catch (EventApiException e) {
                result.error = "error sending event to proxy: " + filePath + ": " + e.getMessage();
                LOGGER.error(result.error);
                throw e;
            } catch (IOException | InterruptedException e) {
                result.error = "error reading file: " + filePath + ": " + e.getMessage();
                LOGGER.error(result.error);
                throw e;
            }
            return result;
        }

        private String obfuscateValue(String value) {
            if (StringUtils.isNotBlank(value)) {
                String prefix = config.getHashPrefix();
                String obf = UUID.nameUUIDFromBytes(value.getBytes()).toString();
                if (StringUtils.isNotBlank(prefix)) {// replace bytes at the start
                    obf = prefix + obf.substring(prefix.length());
                }
                return obf;
            }
            return "";
        }

        private void addAirlyticsEventToList(Map<String, List<AirlyticsEvent>> eventsMap, AirlyticsEvent event,
                                       EventApiClient eventApiClient,
                                       FileProgressTracker fileTracker, boolean devUser) throws IOException, EventApiException {
            String prodId = event.getProductId();

            if (!eventsMap.containsKey(prodId)) {
                eventsMap.put(prodId, new ArrayList<>());
            }
            List<AirlyticsEvent> eventsList = eventsMap.get(event.getProductId());
            eventsList.add(event);

            if (eventsList.size() == config.getEventProxyIntegrationConfig().getEventApiBatchSize()) {
                sendBatchToProxy(eventsList, prodId, eventApiClient, fileTracker, devUser);
                eventsList.clear();
            }
        }

        private void sendBatchToProxy(List<AirlyticsEvent> eventsList, String prodId,
                                      EventApiClient eventApiClient,
                                      FileProgressTracker fileTracker, boolean devUser) throws IOException, EventApiException {

            String progressFileName = readProgressMarker.getProgressFileName(filePath, prodId, devUser);
            Integer lastRecordSent = readProgressMarker.getLastRecordSent(fileTracker, devUser, prodId, progressFileName);
            Integer currentEvent = readProgressMarker.getCurrentEvent(fileTracker, prodId, devUser);

            if (readProgressMarker.shouldSkipBatch(fileTracker, devUser, prodId, eventsList.size(), lastRecordSent, currentEvent)) {
                return;
            }

            //send events list to proxy
           // LOGGER.info("***** sending " + eventsList.size() + " events of prodId " + prodId + ", env = " + (devUser? "dev":"prod" ) + ", read from " + filePath);

            if (lastRecordSent > -1 && lastRecordSent > currentEvent) {
                //need to send only the non sent events from this batch
                List<AirlyticsEvent> notSentEvents = new ArrayList<>();
                for (int i = 0; i < eventsList.size(); i++) {
                    if (i + currentEvent + 1 > lastRecordSent) {
                        notSentEvents.add(eventsList.get(i));
                    }
                }
                sendEventsBatch(prodId, notSentEvents, eventApiClient);
            } else { //if (lastRecordSent == -1 || lastRecordSent<=currentEvent) {
                sendEventsBatch(prodId, eventsList, eventApiClient);
            }

            currentEvent += eventsList.size();

            readProgressMarker.markBatch(fileTracker, devUser, prodId, currentEvent, progressFileName);
        }

        public void sendEventsBatch(String prodId, List<AirlyticsEvent> events, EventApiClient eventApiClient) throws EventApiException {
            LOGGER.debug("SENDING events to "+prodId+" with client "+eventApiClient);
            JSONObject batch = new JSONObject();
            batch.put("events", events);

            //do not retry upon 202 or io error
            for (int i = 0; i < config.getEventProxyIntegrationConfig().getEventApiRetries(); i++) {
                try {
                    Instant startSendTime = Instant.now();
                    acquirePermit(prodId);

                    if(eventApiClient != null) {
                        eventApiClient.post(batch.toString());
                    } else {
                        LOGGER.info("Event API is disabled - otherwise, we would send " + batch.toString());
                    }
                    totalSendTimeMS += Duration.between(startSendTime, Instant.now()).toMillis();
                    break;
                } catch (EventApiException e) {
                    if (e.is202Accepted()) {
                        LOGGER.error("Error sending events to proxy. 202 response: " + e.getMessage() + ", " + e.getResponseBody());
                        //do not retry upon 202
                        throw new EventApiException("Error sending events to proxy. 202 response:" + e.getMessage());
                    } else { // log error only for responses other than 202
                        LOGGER.error("Error sending events to proxy. Response error: " + e.getMessage());
                        if (i == config.getEventProxyIntegrationConfig().getEventApiRetries() - 1) {
                            throw new EventApiException("Error sending events. Reached maximum number of attempts to send the event:" + e.getMessage());
                        }
                    }
                } catch (IOException ioe) {
                    LOGGER.error("Error sending events to proxy. IO exception: " + ioe.getMessage());
                    throw new EventApiException("IO Error sending events to proxy:" + ioe.getMessage());
                }
            }
        }

        private Collection<List<Object>> splitEvents(List<Object> events) {
            int batchSize = events.size();
            int maxBatchSize = config.getEventProxyIntegrationConfig().getEventApiBatchSize();
            if (maxBatchSize > 0 && batchSize > maxBatchSize) {
                //split into batches
                final AtomicInteger counter = new AtomicInteger();
                final Collection<List<Object>> result = events.stream()
                        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / config.getEventProxyIntegrationConfig().getEventApiBatchSize()))
                        .values();
                return result;
            } else {
                Collection<List<Object>> toRet = new ArrayList<>();
                toRet.add(events);
                return toRet;
            }
        }

        private Pair<AirlyticsEvent, Boolean> createLtvEvent(Group g, String filePath, long eventPos) {
            Pair<AirlyticsEvent, Boolean> nullRet = new ImmutablePair<>(null, false);
            AirlyticsEvent event = new AirlyticsEvent();
            event.setName("ad-impression");
            ProductDefinition prod = getProduct(g);
            if (prod == null || prod.getId() == null) {
//                LOGGER.debug("row is with unsupported product");
                return nullRet;
            }
            Triple<String, Boolean, Pair<String, Long>> ltvResult = getUserId(g);
            String userId = ltvResult.getLeft();
            Boolean devUser = ltvResult.getMiddle();
            Pair<String, Long> sessionPair = ltvResult.getRight();
            String sessionId = sessionPair != null ? sessionPair.getLeft() : null;
            Long sessionStartTime = sessionPair != null ? sessionPair.getRight() : null;
            if (userId == null) {
//                LOGGER.warn("could not create user id");
                return nullRet;
            }

            //in internal - send events for only part of the users
            if (config.getPercentageUsersSent()<100) {
                int partition = getUserPartition(userId);
                if (config.getPercentageUsersSent() > 0 && partition > config.getPercentageUsersSent()) {
                    //in internal env send only users of the configured percentage
                    return nullRet;
                }
            }

            Boolean unfilledImpression = getUnfilledImpression(g);
            if (unfilledImpression == null || unfilledImpression) {
                this.ltvEventsCounter.labels(AirlockManager.getEnvVar(), prod.getPlatform(), "unfilled_impression").inc();
                return nullRet;
            }

            String productId = prod.getId();
            String platform = prod.getPlatform();
            String eventId = getUUID(filePath, eventPos);
            String appVersion = getAppVersion(g, productId);

            if (productId.equals(IOS_PRODUCT_PROD_ID)) {
                eventId = (eventId == null) ? null : eventId.toUpperCase();
                userId = (userId == null) ? null : userId.toUpperCase();
                sessionId = (sessionId == null) ? null : sessionId.toUpperCase();
            } else if (productId.equals(ANDROID_PRODUCT_PROD_ID)) {
                eventId = (eventId == null) ? null : eventId.toLowerCase();
                userId = (userId == null) ? null : userId.toLowerCase();
                sessionId = (sessionId == null) ? null : sessionId.toLowerCase();
            }

            event.setEventId(eventId);
            event.setSchemaVersion(prod.getSchemaVersion());
            event.setProductId(productId);
            event.setPlatform(platform);
            event.setUserId(userId);
            if (sessionId != null) {
                event.setSessionId(sessionId);
            }
            if (sessionStartTime != null) {
                event.setSessionStartTime(sessionStartTime);
            }
            if (appVersion != null) {
                event.setAppVersion(appVersion);
            }
            long event_time_usec2 = g.getLong("event_time_usec2", 0);
            long event_time_milli = event_time_usec2 / USEC_TO_MILLI;
            event.setEventTime(event_time_milli);

            //setup attributes
            Map<String, Object> attributes = populateAttributes(g, prod);
            event.setAttributes(attributes);

            if (config.isObfuscateValues()) {
                //obfuscate userid and eventid - in internal to match the cloned users
                event.setUserId(obfuscateValue(event.getUserId()));
                event.setEventId(obfuscateValue(event.getEventId()));
            }

            this.ltvEventsCounter.labels(AirlockManager.getEnvVar(), prod.getPlatform(), "filled_impression").inc();
            return new ImmutablePair<>(event, devUser);
        }

        private Map<String, Object> populateAttributes(Group g, ProductDefinition prod) {
            Map<String, Object> attributes = new HashMap<>();
            List<LTVEventAttribute> attributeList = config.getEventAttributes();
            for (LTVEventAttribute attrDef : attributeList) {
                Object value = getAttributeValue(g, attrDef);
                if (value != null) {
                    if (attrDef.getName().equals("revenue")) {
                        //convert revenue to micros
                        Double val = (Double) value * USD_TO_MICROS;
                        long rVal = Math.round(val);
                        attributes.put(attrDef.getAttributeName(), rVal);
                        this.revenueSummary.labels(AirlockManager.getEnvVar(), prod.getPlatform()).observe(rVal);
                    } else if (attrDef.getName().equals("seller_reserve_price")) {
                        //convert seller_reserve_price to micros
                        double val = (Float) value * USD_TO_MICROS;
                        attributes.put(attrDef.getAttributeName(), Math.round(val));
                    } else {
                        attributes.put(attrDef.getAttributeName(), value);
                    }
                } else {
                    attributes.put(attrDef.getAttributeName(), JSONObject.NULL);
                }
            }
            return attributes;
        }

        private Object getAttributeValue(Group g, LTVEventAttribute attrDef) {
            String type = attrDef.getType().toUpperCase();
            String name = attrDef.getName();
            try {
                switch (type) {
                    case "BINARY":
                        return g.getBinary(name, 0).toStringUsingUTF8();
                    case "BOOLEAN":
                        return g.getBoolean(name, 0);
                    case "FLOAT":
                        return g.getFloat(name, 0);
                    case "INT32":
                    case "INT64":
                        return g.getInteger(name, 0);
                    case "LONG":
                        return g.getLong(name, 0);
                    case "DOUBLE":
                        return g.getDouble(name, 0);
                }
            } catch (Exception e) {
                if (e.getMessage() == null || e.getMessage().equals("null")) {
                    // LOGGER.warn("error populating attribute "+name+":"+e.getMessage());
                } else {
                    LOGGER.error("error populating attribute " + name + ":" + e.getMessage());
                }

            }

            return null;
        }

        private boolean getUnfilledImpression(Group g) {
            return g.getBoolean("unfilled_impression", 0);
        }


        private String getAppVersion(Group g, String productId) {
            String appVer = null;
            try {
                appVer = g.getString("app_version", 0);
            } catch (NullPointerException e) {
               // LOGGER.debug("no app_version value");
                return null;
            }
            if (productId.equals(IOS_PRODUCT_PROD_ID)) {
                if (appVer.contains(IOS_APP_VERSION_PREFIX1) || appVer.contains(IOS_APP_VERSION_PREFIX2)) {
                    appVer = appVer.substring(IOS_APP_VERSION_PREFIX1.length()); //both prefixes has the same lengthh
                    return appVer.replace("_", ".");
                }
            } else if (productId.equals(ANDROID_PRODUCT_PROD_ID)) {
                if (appVer.contains(ANDROID_APP_VERSION_PREFIX)) {
                    appVer = appVer.substring(ANDROID_APP_VERSION_PREFIX.length());
                    return appVer.replace("_", ".");
                }
            }
            return appVer;
        }


        private Triple<String, Boolean, Pair<String, Long>> getUserId(Group g) {
            Pair<String, Long> blankSessionPair = new ImmutablePair<>(null, null);
            String encrypted = getLtvValue(g);
            if (encrypted == null || encrypted.isEmpty()) {
                //LOGGER.debug("empty ltv value");
                return new ImmutableTriple<>(null, null, blankSessionPair);
            }
            //LOGGER.info("found ltv value:"+encrypted);
            //replace special characters
            encrypted = encrypted.replace("-", "=");

            byte[] cipher;
            try {
                cipher = Base32.decode(encrypted);
            } catch (Exception e) {
                //LOGGER.warn("error decrypting ltv value from Base64:"+e.getMessage());
                this.ltvErrorEventsCounter.labels(AirlockManager.getEnvVar()).inc();
                return new ImmutableTriple<>(null, null, blankSessionPair);
            }

            try {
                byte[] decrypted = aes.decrypt(config.getEncryptionKey().getBytes(), cipher, false);
                byte devByte = Arrays.copyOfRange(decrypted, 0, 1)[0];
                boolean devUser = (devByte & 1) > 0;
                String userId = AesGcmEncryption.bytesToUuid(Arrays.copyOfRange(decrypted, 1, 17)).toString();
                String sessionId = null;
                Long sessionStartTime = null;

                //future fields
                if (decrypted.length >= 33) {
                    sessionId = AesGcmEncryption.bytesToUuid(Arrays.copyOfRange(decrypted, 17, 33)).toString();
                }
                if (decrypted.length >= 41) {
                    sessionStartTime = Longs.fromByteArray(Arrays.copyOfRange(decrypted, 33, 41));
                }
                Pair<String, Long> sessionPair = new ImmutablePair<>(sessionId, sessionStartTime);
                //LOGGER.info("decrypted ltv value ("+encrypted+") to userId:"+userId+", isDev:"+devUser+", sessionId:"+sessionId+","+"sessionStartTime:"+sessionStartTime);
                return new ImmutableTriple<>(userId, devUser, sessionPair);

            } catch (EncryptionException e) {
                this.ltvErrorEventsCounter.labels(AirlockManager.getEnvVar()).inc();
                LOGGER.error("error decrypting ltv " + encrypted + ":" + e.getMessage());
            }
            return new ImmutableTriple<>(null, null, null);
        }

        private String addBase64Paddings(String encrypted) {
            if (encrypted == null) return null;
            StringBuilder sb = new StringBuilder(encrypted);
            int strLength = encrypted.length();
            int multiplesOf4 = (int) (Math.floor(strLength / 4) * 4);
            int reminder = strLength - multiplesOf4;
            if (reminder > 0) {
                for (int i = reminder; i < 4; ++i) {
                    sb.append("=");
                }
            }
            return sb.toString();
        }

        private String getLtvValue(Group g) {
            StringBuilder sb = new StringBuilder();
            try {
                String ltv = g.getString("ltv", 0);
                sb.append(ltv);
            } catch (NullPointerException e) {
                //LOGGER.debug("no ltv value");
                return null;
            }
            boolean isDone = sb.toString().isEmpty();
            int n = 2;
            while (!isDone) {
                try {
                    String ltvn = g.getString("ltv" + n, 0);
                    if (ltvn == null || ltvn.isEmpty()) {
                        isDone = true;
                    } else {
                        sb.append(ltvn);
                    }
                } catch (ParquetRuntimeException e) {
                    isDone = true;
                }
            }
            return sb.toString();

        }

        private ProductDefinition getProduct(Group g) {
            List<ProductDefinition> products = config.getProducts();
            for (ProductDefinition prod : products) {
                boolean productMatched = true;
                for (Map.Entry<String, List<String>> entry : prod.getConditions().entrySet()) {
                    String key = entry.getKey();
                    String property = "";
                    try {
                        property = g.getString(key, 0);
                    } catch (NullPointerException e) {
                        //LOGGER.debug("cannot read property " + key + " from row");
                    }

                    List<String> possibleValues = entry.getValue();
                    boolean propertyMatched = false;
                    for (String value : possibleValues) {
                        if (value.equals(property)) {
                            propertyMatched = true;
                        }
                    }
                    if (!propertyMatched) {
                        productMatched = false;
                    }
                }
                if (productMatched) {
                    return prod;
                }
            }
            return null;
        }

        private String getUUID(String filePath, long eventPos) {
            String uuidSource = filePath + eventPos;
            UUID uuid = UUID.nameUUIDFromBytes(uuidSource.getBytes());
            return uuid.toString();
        }

    }
}
