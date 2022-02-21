package com.ibm.airlytics.retentiontrackerqueryhandler.dsr;

import com.amazonaws.util.Base32;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.S3Serializer;
import com.ibm.airlytics.retentiontrackerqueryhandler.kafka.Producer;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.AesGcmEncryption;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.DataSerializer;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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
import org.apache.parquet.schema.PrimitiveType;
import org.json.JSONObject;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.retentiontrackerqueryhandler.db.PersistenceConsumerConstants.AWS_ACCESS_KEY_PARAM;
import static com.ibm.airlytics.retentiontrackerqueryhandler.db.PersistenceConsumerConstants.AWS_ACCESS_SECRET_PARAM;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class DSRRequestsScrapper {
    static final Counter dsrRequestsCounter = Counter.build()
            .name("dsr_scrapper_requests_found_total")
            .help("Total number of DSR requests found.")
            .labelNames(Constants.APP_ID, Constants.ENV)
            .register();

    static final Counter dsrRequestsFoldersCounter = Counter.build()
            .name("dsr_scrapper_new_folders_found_total")
            .help("Total number of new DSR requests folders found.")
            .labelNames(Constants.ENV)
            .register();

    static final Summary dsrScrapperSummary = Summary.build()
            .name("dsr_scrapper_operation_seconds")
            .help("Total Dsr scrapping operation time seconds")
            .labelNames(Constants.ENV).register();

    private String awsSecret;
    private String awsKey;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(DSRRequestsScrapper.class.getName());
    private ThreadPoolExecutor executor;
    private DataSerializer dataSerializer;
    private DSRConfig config;
    private String s3FilePrefix;
    private final DbHandler dbHandler;
    private final DSRCursor dsrCursor;
    private final AesGcmEncryption aes = new AesGcmEncryption();
    private final ConfigurationManager configurationManager;
    private Map<String, Topics> producers;

    public DSRRequestsScrapper(DbHandler dbHandler, DSRCursor dsrCursor, ConfigurationManager configurationManager) throws IOException {
        this.dbHandler = dbHandler;
        this.dsrCursor = dsrCursor;
        this.configurationManager = configurationManager;

    }

    public void configure() throws IOException {
        config = new DSRConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator;// + config.getTopic() + File.separator;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
        this.producers = createKafKaProducers(configurationManager.getKafkaTopics());
    }

    public void configureForImpressions() throws IOException {
        config = new DSRConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator;// + config.getTopic() + File.separator;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
    }

    public Runnable getScrapperTask() {
        return new Runnable() {
            @Override
            public void run() {
                dsrScrapperSummary.labels(ConfigurationManager.getEnvVar()).time(
                        () -> retrieveRequests());
            }
        };
    }

    private class Topics {
        Producer producer;
        Producer errorsProducer;
    }
    private Map<String, Topics> createKafKaProducers(JSONObject topicsObj) {
        Map<String, Topics> map = new HashMap<>();
        for (String app : topicsObj.keySet()) {
            JSONObject currTopics = topicsObj.getJSONObject(app);
            String topic = currTopics.getString("topic");
            String errorsTopic = currTopics.getString("errorsTopic");
            Topics topics = new Topics();
            topics.producer = new Producer(topic, config.getBootstrapServers(),
                    config.getSecurityProtocol());
            topics.errorsProducer = new Producer(errorsTopic, config.getBootstrapServers(),
                    config.getSecurityProtocol());
            map.put(app, topics);
        }
        return map;
    }

    public void processImpressionFiles() {
        try {
            String cursor = dsrCursor.getCursor();
            cursor = "date=2021-08-01";
            logger.info("cursor at "+cursor);
            List<String> folders = dataSerializer.listFolders("network_impressions/");
            List<String> relevantDays = filterImpressionFolders(folders, cursor);
            for (String day : relevantDays) {
                List<String> files = getFiles(day);
                for (String file : files) {
                    processAdImpressionFile(file);
                }
                logger.debug(files.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    private void processAdImpressionFile(String file) {

    }


    private class ReadFileTask implements Callable {

        class Result {
            String error = null;
            String path;
            Set<String> progressFilesToDeleteSet = new HashSet<>();
        }

        private String filePath;

        public ReadFileTask(String filePath) {
            this.filePath = filePath;
        }

        public Result call() throws IOException, InterruptedException {
            Instant startTaskTime = Instant.now();
            Result result = new Result();
            result.path = filePath;
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
                        logger.info("(" + retryCounter + ") Cannot open file " + filePath + " for reading: " + e.getMessage());
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

                        for (int i = 0; i < rows; i++) {

                            counter++;

                            startReadTime = Instant.now();
                            Group g = recordReader.read();

                            /*Pair<AirlyticsEvent, Boolean> ltvResult = createLtvEvent(g, filePath, counter);
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
                            }*/
                        }

                        startReadTime = Instant.now();
                    }
                } finally {
                    reader.close();

                    logger.info("***** Finished processing file: '" + filePath + "', eventsNumber = " + eventsNumber + "', nonEventsNumber = " + nonEventsNumber + " total time = ");

                }
            } catch (Exception e) {
                result.error = "error sending event to proxy: " + filePath + ": " + e.getMessage();
                logger.error(result.error);
                throw e;
            }
            return result;
        }

        private Object getAttributeValue(Group g) {
            String type = "BINARY";
            String name = "ad_unit_id";
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
                    logger.error("error populating attribute " + name + ":" + e.getMessage());
                }

            }

            return null;
        }

        private boolean getUnfilledImpression(Group g) {
            return g.getBoolean("unfilled_impression", 0);
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
                return new ImmutableTriple<>(null, null, blankSessionPair);
            }

            try {
                byte[] decrypted = aes.decrypt("J@NcRfUjXn2r5u8x".getBytes(), cipher, false);
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

            } catch (Exception e) {
                logger.error("error decrypting ltv " + encrypted + ":" + e.getMessage());
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

        private String getUUID(String filePath, long eventPos) {
            String uuidSource = filePath + eventPos;
            UUID uuid = UUID.nameUUIDFromBytes(uuidSource.getBytes());
            return uuid.toString();
        }

    }
    public void retrieveRequests() {
        List<String> days = null;
        try {
            days = getRelevantDays();
            ///////////DELETE ME
//            if (days.size() > 0) {
//                days = Lists.newArrayList(days.get(0));
//            }
            ////////////////////// 213D
            dsrRequestsFoldersCounter.labels(ConfigurationManager.getEnvVar()).inc(days.size());
            if (days.size() == 0) {
                logger.info("cannot find new DSR requests");
                return;
            }
        } catch (IOException | ParseException e) {
            logger.error("error getting relevant days to scan:"+e.getMessage());
            e.printStackTrace();
            return;
        }
        String lastDay = null;
        try {
            lastDay = getMinimumDate(days.get(days.size()-1), getYesterdayDateString());
        } catch (ParseException e) {
            logger.error("error getting last day");
            e.printStackTrace();
        }
        logger.info("last day is:"+lastDay);
        List<Future<ReadDayTask.DayResult>> futures = new ArrayList<>();
        for (String day : days) {
            ReadDayTask rdt = new ReadDayTask("requests/"+day, day);
            Future<ReadDayTask.DayResult> future = executor.submit(rdt);
            futures.add(future);
        }
        List<String> errors = new ArrayList<>();
        List<JSONObject> requests = new ArrayList<>();
        for (Future<ReadDayTask.DayResult> future : futures) {
            try {
                ReadDayTask.DayResult result = future.get();
                if (result.errors != null) {
                    errors.addAll(result.errors);
                }
                requests.addAll(result.requests);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error in executing ReadDayTask:"+e.getMessage());
                e.printStackTrace();
            }
        }
        try {
            deliverRequests(requests);
            dsrCursor.setCursor(lastDay);
        } catch (IOException e) {
            logger.error("failed delivering requests:"+e.getMessage());
        }
        logger.info("finished retrieving requests");
    }

    private String getMinimumDate(String s1, String s2) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = dateFormat.parse(removePrefix(s1,"date="));
        Date d2 = dateFormat.parse(removePrefix(s2, "date="));
        if (d1.before(d2)) {
            return s1;
        } else {
            return s2;
        }
    }


    private List<String> getFiles(String folder) throws IOException, ParseException {
        List<String> subFolders = dataSerializer.listFoldersInFolder(config.getS3RootFolder()+"/"+folder);
        List<String> toRet = new ArrayList<>();
        for (String subF : subFolders) {
            List<String> files =  dataSerializer.listFilesInFolder(subF);
            for (String file : files) {
                toRet.add(subF+"/"+file);
            }
        }
        return toRet;
    }
    private List<String> getRelevantDays() throws IOException, ParseException {
        try {
            String cursor = dsrCursor.getCursor();
            logger.info("cursor at "+cursor);
            List<String> folders = dataSerializer.listFolders("requests/");
            List<String> days = filterFolders(folders, cursor);
            logger.info("folders found:"+days);
            return days;
        } catch (IOException | ParseException e) {
            logger.error("error listing folders"+e.getMessage());
            e.printStackTrace();
            throw  e;
        }
    }

    private List<String> filterImpressionFolders(List<String> folders, String lastDayStr) throws ParseException {
        Date lastDay = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        if (lastDayStr != null && !lastDayStr.isEmpty()) {
            String last = removePrefix(lastDayStr,
                    "date=");
            lastDay = dateFormat.parse(last);
        }
        List<String> toRet = new ArrayList<>();

        for (String folder : folders) {
            String dateStr = removePrefix(folder, "imprsn_date=");
            Date currDate = dateFormat.parse(dateStr);
            if (isDateIncluded(currDate, lastDay)) {
                toRet.add(folder);
            }
        }
        return toRet;
    }

    private List<String> filterFolders(List<String> folders, String lastDayStr) throws ParseException {
        Date lastDay = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        if (lastDayStr != null && !lastDayStr.isEmpty()) {
            String last = removePrefix(lastDayStr, "date=");
            lastDay = dateFormat.parse(last);
        }
        List<String> toRet = new ArrayList<>();

        for (String folder : folders) {
            String dateStr = removePrefix(folder, "date=");
            Date currDate = dateFormat.parse(dateStr);
            if (isDateIncluded(currDate, lastDay)) {
                toRet.add(folder);
            }
        }
        return toRet;
    }
    private boolean isDateIncluded(Date date, Date lastDay) throws ParseException {
        if (lastDay != null && !date.after(lastDay)) {
            return false;
        }

        if (date.after(getYesterdayDate())) {
//            return false;
        }
        return true;
    }

    private Date getYesterdayDate() throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date today = dateFormat.parse(removePrefix(getYesterdayDateString(),"date="));
        return today;
    }

    private String removePrefix(String str, String prefix) {
        if (str.startsWith(prefix)) {
            return str.substring(prefix.length());
        } else {
            return str;
        }
    }

    public void deliverFakeRequests() throws IOException {
        JSONObject requestObj = new JSONObject("{\"rType\":\"Portability\",\"regime\":\"USA\",\"dayPath\":\"date=2021-11-19\",\"userId\":\"twc::enc_1_v8AODH%2BiILnHIxGNHFqElFhSEGio4amsHXCBLG%2FmxdQSJmotnC6E3w0BPA3ohoMd\",\"requested\":\"2021-11-19T15:43:25.691614118Z\",\"s3DataUrl\":\"\",\"isAuth\":false,\"adId\":\"\",\"partners\":[{\"ID\":\"2C0FBEA0851D71B9-60001909C0003D32\",\"Name\":\"Omniture\"},{\"ID\":\"user:628639f3-9f70-4c6c-8583-6db0d58147af\",\"Name\":\"Amplitude\"},{\"ID\":\"user:628639f3-9f70-4c6c-8583-6db0d58147af\",\"Name\":\"airlytics\"},{\"ID\":\"628639f3-9f70-4c6c-8583-6db0d58147af\",\"Name\":\"Piano\"},{\"ID\":\"628639f3-9f70-4c6c-8583-6db0d58147af\",\"Name\":\"TWC_Services_Data\"}],\"adIdKey\":null,\"requestId\":\"b424940b-db1e-4b97-8fb8-1442a2962e40\",\"appId\":\"twc-web\",\"origUserId\":\"628639f3-9f70-4c6c-8583-6db0d58147af\"}");
        String appId = requestObj.getString("appId");
        String requestId = requestObj.getString("requestId");
        Topics topics = this.producers.get(appId);
        if (topics != null) {
            dsrRequestsCounter.labels(appId, ConfigurationManager.getEnvVar()).inc();
            Producer producer = topics.producer;
            ObjectMapper objectMapper = new ObjectMapper();

            JsonNode jsonNode = objectMapper.readTree(requestObj.toString());
            logger.debug("sending request for topic "+producer.getTopic()+" with id:"+requestId);
            producer.sendRecord(requestId, jsonNode);
        } else {
            logger.debug("skipping request for appId "+appId);
        }
    }

    public void addPermissionsToFolder(String path) {
        try {
            List<String> files = dataSerializer.listFilesInFolder(path);
            for (String file : files) {
                String filePath = path+"/"+file;
                this.dataSerializer.addFullPermission(filePath);
            }

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }
    private void deliverRequests(List<JSONObject> requests) throws IOException {
        logger.info("deliverRequests for maximum "+requests.size()+" requests");
        for (JSONObject requestObj : requests) {
            String appId = requestObj.getString("appId");
            String requestId = requestObj.getString("requestId");
            Topics topics = this.producers.get(appId);
            if (topics != null) {
                dsrRequestsCounter.labels(appId, ConfigurationManager.getEnvVar()).inc();
                Producer producer = topics.producer;
                ObjectMapper objectMapper = new ObjectMapper();

                JsonNode jsonNode = objectMapper.readTree(requestObj.toString());
                logger.info("sending request for topic "+producer.getTopic()+" with id:"+requestId);
                producer.sendRecord(requestId, jsonNode);
            } else {
                logger.info("skipping request for appId "+appId);
            }
        }
    }

    private Date yesterday() {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }

    private Date today() {
        final Calendar cal = Calendar.getInstance();
        return cal.getTime();
    }

    private String getYesterdayDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return "date="+dateFormat.format(yesterday());
    }
    private String getTodayDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return "date="+dateFormat.format(today());
    }
    class ReadDayTaskImpressions implements Callable {
        class ImpressionFilesResult {
            List<String> files = new ArrayList<>();
            String error;
        }
        String dayPath;
        String folderName;

        public ImpressionFilesResult call() {
            ImpressionFilesResult res = new ImpressionFilesResult();
            try {
                List<String> files = dataSerializer.listFilesInFolder(folderName);

            } catch (IOException e) {
                e.printStackTrace();
            }
            return res;
        }
    }
    class ReadDayTask implements Callable {
        class DayResult {
            List<String> errors = new ArrayList<>();
            List<JSONObject> requests = new LinkedList<>();
        }
        String folderName;
        String dayPath;
        ThreadPoolExecutor executor;
        public ReadDayTask(String folderName, String dayPath) {
            this.folderName=folderName;
            this.dayPath=dayPath;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        }

        public DayResult call() {
            logger.debug("retrieving requests for: "+folderName);
            DayResult result = new DayResult();
            String delete = folderName+"/requestType=Delete/";
            String portability = folderName+"/requestType=Portability/";
            try {
                ReadS3FolderTask portabilityTask = new ReadS3FolderTask(portability, dayPath);
                ReadS3FolderTask deleteTask = new ReadS3FolderTask(delete, dayPath);
                Future<ReadS3FolderTask.FolderResult> portabilityFuture = executor.submit(portabilityTask);
                Future<ReadS3FolderTask.FolderResult> deleteFuture = executor.submit(deleteTask);
                ReadS3FolderTask.FolderResult portabilityResult = portabilityFuture.get();
                ReadS3FolderTask.FolderResult deleteResult = deleteFuture.get();
                result.requests.addAll(portabilityResult.requests);
                result.requests.addAll(deleteResult.requests);
                result.errors.addAll(portabilityResult.errors);
                result.errors.addAll(deleteResult.errors);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("failed retrieving requests:"+e.getMessage());
            }
            return result;
        }
    }

    class ReadS3FolderTask implements Callable {

        class FolderResult {
            List<String> errors = new ArrayList<>();
            List<JSONObject> requests = new LinkedList<>();
        }
        private String folderName;
        private String dayPath;
        ThreadPoolExecutor executor;
        public ReadS3FolderTask(String folderName, String dayPath) {
            this.folderName=folderName;
            this.dayPath=dayPath;
        }

        public FolderResult call() throws IOException {
            logger.debug("ReadS3FolderTask: retrieving requests for: "+folderName);
            FolderResult result = new FolderResult();
            List<String> files = dataSerializer.listFilesInFolder(folderName);
//            int nThreads = files.size() > 0 ? files.size() : 1;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
            logger.debug("found "+files.size()+" files for "+folderName);
            List<Future<ReadS3FileTask.FileResult>> results = new ArrayList<>();
            for (String file : files) {
                String filePath = folderName.endsWith(File.separator) ? folderName + file : folderName+File.separator+file;
                ReadS3FileTask rt = new ReadS3FileTask(filePath, dayPath);
                Future<ReadS3FileTask.FileResult> res = executor.submit(rt);
                results.add(res);
            }
            ArrayList<String> errors = new ArrayList<>();
            try {
                for (Future<ReadS3FileTask.FileResult> future : results) {
                    ReadS3FileTask.FileResult fileResult = future.get();
                    if (fileResult.error != null) {
                        result.errors.add(fileResult.path+":"+fileResult.error);
                    }
                    if (fileResult.request != null) {
                        result.requests.add(fileResult.request);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error("error reading file results:"+e.getMessage());
            }

            return result;
        }
    }

    private class ParquetFields {
        //Map between field name to field type
        HashMap<String, PrimitiveType.PrimitiveTypeName> fieldsMap;

        public ParquetFields() {
            fieldsMap = new HashMap<String, PrimitiveType.PrimitiveTypeName>();
        }

        public synchronized void addField(String name, PrimitiveType.PrimitiveTypeName type) {
            if (!fieldsMap.containsKey(name)) {
                fieldsMap.put(name, type);
            }
        }

        public Set<String> getFieldNames() {
            return fieldsMap.keySet();
        }

        public PrimitiveType.PrimitiveTypeName getFieldType (String fieldName) {
            return fieldsMap.get(fieldName);
        }

    }

    class ReadS3FileTask implements Callable {
        class FileResult {
            String error;
            String path;
            JSONObject request;
        }

        String filePath;
        String dayPath;
        private ParquetFields parquetFields = new ParquetFields();
        public ReadS3FileTask(String filePath, String dayPath) {
            this.filePath=filePath;
            this.dayPath=dayPath;
        }

        public FileResult call() {
            logger.debug("reading file:"+filePath);
            FileResult result = new FileResult();
            result.path = filePath;
            try {
                String content = dataSerializer.getFileContent(filePath);
                JSONObject obj = new JSONObject(content);
                obj.put("dayPath", this.dayPath);
                result.request = obj;
            } catch (IOException e) {
                logger.error("error reading file "+filePath+":"+e.getMessage());
                result.error = e.getMessage();
            }
            return result;
        }
    }
}
