package com.ibm.airlytics.consumer.dsr.retriever;

import com.ibm.airlytics.consumer.dsr.DSRRequest;
import com.ibm.airlytics.consumer.dsr.DSRResponse;
import com.ibm.airlytics.consumer.dsr.db.DbHandler;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
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
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;

public class ParquetRetriever {
    private final DbHandler dbHandler;
    private RetrieverConfig config;
    private static final AirlyticsLogger logger = AirlyticsLogger.getLogger(ParquetRetriever.class.getName());
    private String awsSecret;
    private String awsKey;
    private String s3FilePrefix;
    private ThreadPoolExecutor executor;
    private String folderPath;
    private ParquetFields parquetFields = new ParquetFields();
    private Map<String, DSRResponse> users;
    private DataSerializer dataSerializer;
    private DataSerializer dataSerializerPi;
    private DataSerializer dataSerializerDev;
    private DataSerializer dataSerializerPiDev;

    public ParquetRetriever(DbHandler dbHandler) throws IOException {
        this.dbHandler = dbHandler;
        config = new RetrieverConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator + config.getTopic() + File.separator;
        String folderPath = ""; //TODO::
        this.folderPath = folderPath.endsWith(File.separator) ? folderPath : folderPath + File.separator;
        logger.info("s3Bucket:"+config.getS3Bucket());
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
        logger.info("s3BucketPi:"+config.getS3BucketPi());
        this.dataSerializerPi = new S3Serializer(config.getS3RegionPi(), config.getS3BucketPi(), config.getIoActionRetries());
        logger.info("s3BucketDev:"+config.getS3BucketDev());
        this.dataSerializerDev = new S3Serializer(config.getS3regionDev(), config.getS3BucketDev(), config.getIoActionRetries());
        logger.info("s3BucketPiDev:"+config.getS3BucketPiDev());
        this.dataSerializerPiDev = new S3Serializer(config.getS3RegionPiDev(), config.getS3BucketPiDev(), config.getIoActionRetries());
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
    }
    private class DatesRange {
        Timestamp firstSession=null;
        Timestamp lastSesion=null;

        public DatesRange(){}

        public DatesRange(DatesRange other) {
            super();
            if (other != null) {
                this.firstSession=other.firstSession;
                this.lastSesion=other.lastSesion;
            }
        }
    }

    private class ShardData {
        List<String> userIds = new ArrayList<>();
        DatesRange datesRange = null;
    }

    private class ShardResult {
        JSONArray results=new JSONArray();
        List<String> errors;
    }
    public List<DSRData> getUserData(List<DSRRequest> requests) throws SQLException, ClassNotFoundException, IOException {
        if (requests.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Integer, ShardData> shards = new HashMap<Integer, ShardData>();
        String dayPath = "";
        if (requests.size() > 0) {
            dayPath = requests.get(0).getDayPath();
        }
        this.users = new ConcurrentHashMap<>();
        List<String> airlyticsIds = new ArrayList<>();
        List<String> upsIds = new ArrayList<>();
        List<String> registeredIds = new ArrayList<>();
        for (DSRRequest request : requests) {
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            String upsId = request.getUpsId();
            if (registeredId != null) {
                registeredIds.add(registeredId);
            } else if (deviceId != null) {
                airlyticsIds.add(deviceId);
            } else {
                //if both null the request should be filtered earlier
                upsIds.add(upsId);
            }

        }
        Map<String, List<String>> registeredUsersMap = null;
        if (registeredIds.size() > 0) {
            registeredUsersMap = dbHandler.getDeviceIdsByRegisteredUser(registeredIds);
            for (List<String> deviceIds : registeredUsersMap.values()) {
                airlyticsIds.addAll(deviceIds);
            }
        }
        Map<String, JSONObject> userDataByIds = dbHandler.getUsersJSONsByIds(airlyticsIds, config.getExcludedColumns());
        Map<String, JSONObject> usersDataByUps = dbHandler.getUsersJSONs(upsIds, config.getExcludedColumns());
        int i=0;
        for (DSRRequest request : requests) {
            boolean isUPS = false;
            List<String> ids = new ArrayList<>();
            String upsId = request.getUpsId();
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            if (registeredId!=null) {
                ids.addAll(registeredUsersMap.getOrDefault(registeredId, new ArrayList<>()));
            } else if (deviceId!=null) {
                ids.add(deviceId);
            } else {
                ids.add(upsId);
                isUPS = true;
            }
            for (String currId : ids) {
                JSONObject userObj = null;
                if (isUPS) {
                    userObj = usersDataByUps.get(currId);
                } else {
                    userObj = userDataByIds.get(currId);
                }
                if (userObj != null) {
                    logger.info("user with ups_id: "+upsId+" is in the DB");
                    userObj.put("events", new JSONArray());
                    int shard = userObj.getInt("shard"); //TODO:: make sure shard and id fields are in the db
                    String userId = userObj.getString("id");
                    ShardData shardData = shards.getOrDefault(shard, new ShardData());
                    shardData.userIds.add(userId);
                    DatesRange currDr = getDatesRange(userObj);
                    DSRResponse userResponse = new DSRResponse(request, userObj, new ArrayList<>());
                    users.put(userId, userResponse);
                    shardData.datesRange = updateRanges(shardData.datesRange, currDr);
                    shards.put(shard, shardData);
                } else {
                    logger.info("skipping user with updId:"+upsId+" and airlyticsId:"+airlyticsId);
                }
            }
            ++i;
        }
        logger.info("found "+users.size()+" users");
        List<ReadShardTask> readTasks = new ArrayList<>();
        for (Map.Entry<Integer, ShardData> entry : shards.entrySet()) {
            int shard = entry.getKey().intValue();
            ShardData data = entry.getValue();
            String shardPath = "shard="+shard;
            String folderPath = config.getS3RootFolder()+"/"+config.getProductFolder()+"/"+shardPath;
            logger.info(folderPath);
            //regular events
            ReadShardTask rst = new ReadShardTask(folderPath, data, dataSerializer, config.getS3Bucket());
            readTasks.add(rst);
            //pi events
            ReadShardTask pirst = new ReadShardTask(folderPath, data, dataSerializerPi, config.getS3BucketPi());
            readTasks.add(pirst);
        }

        List<String> errors = new ArrayList<>();
        try{
            for (ReadShardTask rsTask: readTasks) {
                ShardResult res = rsTask.call();
                if (res.errors != null) {
                    errors.addAll(res.errors);
                }
            }
        } catch (Exception e) {
            logger.error("error getting shard results:"+e.getMessage());
            e.printStackTrace();
        }
        List<DSRData> toRet = new ArrayList<>();
        List<DSRResponse> currResponses = new ArrayList<>();
        synchronized (users) {
            for (DSRResponse user : users.values()) {
                if (currResponses.isEmpty()) {
                    currResponses.add(user);
                    dayPath = user.getRequest().getDayPath();
                } else if (user.getRequest().getDayPath().equals(dayPath)) {
                    currResponses.add(user);
                } else {
                    toRet.add(new DSRData(new ArrayList<>(currResponses), this.parquetFields, dayPath));
                    currResponses.clear();
                    dayPath = null;
                }
            }
            if (!currResponses.isEmpty()) {
                //add the last day path
                toRet.add(new DSRData(new ArrayList<>(currResponses), this.parquetFields, dayPath));
            }
            return toRet;
        }
    }

    public List<DSRData> getDevUserData(List<DSRRequest> requests) throws SQLException, ClassNotFoundException, IOException {
        if (requests.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Integer, ShardData> shards = new HashMap<Integer, ShardData>();
        String dayPath = "";
        if (requests.size() > 0) {
            dayPath = requests.get(0).getDayPath();
        }
        this.parquetFields = new ParquetFields();
        this.users = new ConcurrentHashMap<>();
        List<String> airlyticsIds = new ArrayList<>();
        List<String> upsIds = new ArrayList<>();
        List<String> registeredIds = new ArrayList<>();
        for (DSRRequest request : requests) {
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            String upsId = request.getUpsId();
            if (registeredId != null) {
                registeredIds.add(registeredId);
            } else if (deviceId != null) {
                airlyticsIds.add(deviceId);
            } else {
                //if both null the request should be filtered earlier
                upsIds.add(upsId);
            }

        }
        Map<String, List<String>> registeredUsersMap = null;
        if (registeredIds.size() > 0) {
            registeredUsersMap = dbHandler.getDeviceIdsByRegisteredUser(registeredIds);
            for (List<String> deviceIds : registeredUsersMap.values()) {
                airlyticsIds.addAll(deviceIds);
            }
        }
        Map<String, JSONObject> userDataByIds = dbHandler.getDevUsersJSONsByIds(airlyticsIds, config.getExcludedColumns());
        Map<String, JSONObject> usersDataByUps = dbHandler.getDevUsersJSONs(upsIds, config.getExcludedColumns());
        int i=0;
        for (DSRRequest request : requests) {
            boolean isUPS = false;
            List<String> ids = new ArrayList<>();
            String upsId = request.getUpsId();
            String airlyticsId = request.getAirlyticsId();
            String deviceId = stripDeviceId(airlyticsId);
            String registeredId = stripRegisteredId(airlyticsId);
            if (registeredId!=null) {
                ids.addAll(registeredUsersMap.getOrDefault(registeredId, new ArrayList<>()));
            } else if (deviceId!=null) {
                ids.add(deviceId);
            } else {
                ids.add(upsId);
                isUPS = true;
            }
            for (String currId : ids) {
                JSONObject userObj = null;
                if (isUPS) {
                    userObj = usersDataByUps.get(currId);
                } else {
                    userObj = userDataByIds.get(currId);
                }
                if (userObj != null) {
                    logger.info("user with ups_id: "+upsId+" is in the DB");
                    userObj.put("events", new JSONArray());
                    int shard = userObj.getInt("shard");
                    String userId = userObj.getString("id");
                    ShardData shardData = shards.getOrDefault(shard, new ShardData());
                    shardData.userIds.add(userId);
                    DatesRange currDr = getDatesRange(userObj);
                    DSRResponse userResponse = new DSRResponse(request, userObj, new ArrayList<>());
                    users.put(userId, userResponse);
                    shardData.datesRange = updateRanges(shardData.datesRange, currDr);
                    shards.put(shard, shardData);
                } else {
                    logger.info("skipping user with updId:"+upsId+" and airlyticsId:"+airlyticsId);
                }
            }
            ++i;
        }
        logger.info("found "+users.size()+" users");
        List<ReadShardTask> readTasks = new ArrayList<>();
        for (Map.Entry<Integer, ShardData> entry : shards.entrySet()) {
            int shard = entry.getKey().intValue();
            ShardData data = entry.getValue();
            String shardPath = "shard="+shard;
            String folderPath = config.getS3RootFolder()+"/"+config.getDevProductFolder()+"/"+shardPath;
            logger.info(folderPath);
            //regular events
            ReadShardTask rst = new ReadShardTask(folderPath, data, dataSerializerDev, config.getS3BucketDev());
            readTasks.add(rst);
            //pi events
            ReadShardTask pirst = new ReadShardTask(folderPath, data, dataSerializerPiDev, config.getS3BucketPiDev());
            readTasks.add(pirst);
        }

        List<String> errors = new ArrayList<>();
        try{
            for (ReadShardTask rsTask : readTasks) {
                ShardResult res = rsTask.call();
                if (res.errors != null) {
                    errors.addAll(res.errors);
                }
            }
        } catch (Exception e) {
            logger.error("error getting shard results:"+e.getMessage());
            e.printStackTrace();
        }
        List<DSRData> toRet = new ArrayList<>();
        List<DSRResponse> currResponses = new ArrayList<>();
        synchronized (users) {
            for (DSRResponse user : users.values()) {
                if (currResponses.isEmpty()) {
                    currResponses.add(user);
                    dayPath = user.getRequest().getDayPath();
                } else if (user.getRequest().getDayPath().equals(dayPath)) {
                    currResponses.add(user);
                } else {
                    toRet.add(new DSRData(new ArrayList<>(currResponses), this.parquetFields, dayPath));
                    currResponses.clear();
                    dayPath = null;
                }
            }
            if (!currResponses.isEmpty()) {
                //add the last day path
                toRet.add(new DSRData(new ArrayList<>(currResponses), this.parquetFields, dayPath));
            }
            return toRet;
        }
    }
    private String stripRegisteredId(String airlyticsId) {
        if (airlyticsId == null || config.getRegisteredIdPrefix() == null || config.getRegisteredIdPrefix().isEmpty()) {
            return null;
        }
        if (airlyticsId.startsWith(config.getRegisteredIdPrefix())) {
            return airlyticsId.replaceFirst("^"+config.getRegisteredIdPrefix(),"");
        }
        return null;
    }

    private String stripDeviceId(String airlyticsId) {
        if (airlyticsId == null || config.getDeviceIdPrefix() == null || config.getDeviceIdPrefix().isEmpty()) {
            return airlyticsId;
        }
        if (airlyticsId.startsWith(config.getDeviceIdPrefix())) {
            return airlyticsId.replaceFirst("^"+config.getDeviceIdPrefix(),"");
        }
        return airlyticsId;
    }

    private DatesRange updateRanges(DatesRange range1, DatesRange range2) {
        if (range1 == null) {
            return range2;
        } else if (range2 == null) {
            return range1;
        }
        DatesRange toRet = new DatesRange(range1);
        if (range2.firstSession != null && toRet.firstSession != null && range2.firstSession.before(toRet.firstSession)) {
            toRet.firstSession = range2.firstSession;
        }
        if (range2.lastSesion == null || toRet.lastSesion == null) {
            toRet.lastSesion = null;
        } else if (range2.lastSesion.after(toRet.lastSesion)) {
            toRet.lastSesion = range2.lastSesion;
        }
        return toRet;
    }

    private DatesRange getDatesRange(JSONObject userObj) {
        DatesRange toRet = new DatesRange();
        Timestamp firstSession=null;
        if (userObj.getString("first_session") != null) {
            firstSession = Timestamp.valueOf(userObj.getString("first_session"));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(firstSession.getTime());
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            firstSession = new Timestamp(cal.getTimeInMillis());
            logger.info("firstSession:"+firstSession.toString());
        }
        Timestamp lastSession=null;
        if (userObj.getString("last_session") != null) {
            lastSession = Timestamp.valueOf(userObj.getString("last_session"));
            logger.info("lastSession:"+lastSession.toString());
        }
        toRet.firstSession = firstSession;
        toRet.lastSesion = lastSession;
        return toRet;
    }

    private List<String> filterFolders(DatesRange dates, List<String> folders, String prefix) {
        Timestamp firstSession=dates.firstSession;
        Timestamp lastSession=dates.lastSesion;
        List<String> toRet = new ArrayList<>();
        for (String folder : folders) {
            String folderDate = getDayString(folder);
            Timestamp folderTime = Timestamp.valueOf(folderDate);
            if (firstSession != null && folderTime.before(firstSession) ||
                    lastSession != null && folderTime.after(lastSession)) {
                logger.debug("pruning folder "+folder);
            } else {
                toRet.add(prefix+folder);
            }
        }
        return toRet;
    }
    private List<String> filterFolders(JSONObject userObj, List<String> folders) {
        Object s = userObj.getString("first_session");
        Timestamp firstSession=null;
        if (userObj.getString("first_session") != null) {
            firstSession = Timestamp.valueOf(userObj.getString("first_session"));
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(firstSession.getTime());
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            firstSession = new Timestamp(cal.getTimeInMillis());
            logger.info("firstSession:"+firstSession.toString());
        }
        Timestamp lastSession=null;
        if (userObj.getString("last_session") != null) {
            lastSession = Timestamp.valueOf(userObj.getString("last_session"));
            logger.info("lastSession:"+lastSession.toString());
        }
        List<String> toRet = new ArrayList<>();
        for (String folder : folders) {
            String folderDate = getDayString(folder);
            Timestamp folderTime = Timestamp.valueOf(folderDate);
            if (firstSession != null && folderTime.before(firstSession) ||
                lastSession != null && folderTime.after(lastSession)) {
                logger.debug("pruning folder "+folder);
            } else {
                toRet.add(folder);
            }
        }
        return toRet;
    }

    private String getDayString(String original) {
        String identifier = "day=";
        int index = original.indexOf(identifier);
        String toRet = original.substring(index+identifier.length())+ " 00:00:00";
        return toRet;
    }

    private void addEvent(JSONObject eventObj, String userId) {

        List<JSONObject> events = users.get(userId).getEvents();
        synchronized (events) {
            logger.info("adding event! "+eventObj.getString("eventId"));
            events.add(eventObj);
        }
    }

    public class ParquetFields {
        public HashMap<String, PrimitiveType.PrimitiveTypeName> getFieldsMap() {
            return fieldsMap;
        }

        //Map between field name to field type
        HashMap<String, PrimitiveType.PrimitiveTypeName> fieldsMap;
        HashMap<String, PrimitiveType.PrimitiveTypeName> uppercaseFieldsMap;

        public ParquetFields() {
            fieldsMap = new HashMap<String, PrimitiveType.PrimitiveTypeName>();
            uppercaseFieldsMap = new HashMap<>();
            fieldsMap.put(DSR_REQUEST_ID_SCHEMA_FIELD, PrimitiveType.PrimitiveTypeName.BINARY);
            uppercaseFieldsMap.put(DSR_REQUEST_ID_SCHEMA_FIELD.toUpperCase(), PrimitiveType.PrimitiveTypeName.BINARY);
        }

        public synchronized void addField(String name, PrimitiveType.PrimitiveTypeName type) {
            if (!fieldsMap.containsKey(name) && !uppercaseFieldsMap.containsKey(name.toUpperCase())) {
                fieldsMap.put(name, type);
                uppercaseFieldsMap.put(name.toUpperCase(), type);
            }
        }

        public Set<String> getFieldNames() {
            return fieldsMap.keySet();
        }

        public PrimitiveType.PrimitiveTypeName getFieldType (String fieldName) {
            return fieldsMap.get(fieldName);
        }

        public String generateParquetSchema() {
            StringBuilder sb = new StringBuilder();
            sb.append("message event_record {\n" );
            String requiredStr = "optional";
            for (Map.Entry<String, PrimitiveType.PrimitiveTypeName> entry : fieldsMap.entrySet()) {
                switch (entry.getValue()) {
                    case BINARY:
                        sb.append(requiredStr + " binary "+ entry.getKey() + " (STRING);\n");
                        break;
                    case INT32:
                        sb.append(requiredStr + " int32 "+ entry.getKey() + ";\n");
                        break;
                    case INT64:
                        sb.append(requiredStr + " int64 "+ entry.getKey() + ";\n");
                        break;
                    case FLOAT:
                        sb.append(requiredStr + " float "+ entry.getKey() + ";\n");
                        break;
                    case DOUBLE:
                        sb.append(requiredStr + " double "+ entry.getKey() + ";\n");
                        break;
                    case BOOLEAN:
                        sb.append(requiredStr + " boolean "+ entry.getKey() + ";\n");
                        break;
                }
            }

            sb.append("}\n");
            return sb.toString();
        }
    }

    class Result {
        String error = null;
        String path;
        JSONObject result;
    }

    private class ReadShardTask implements Callable {
        private String folderName;
        private ShardData shardData;
        private ThreadPoolExecutor executor;
        private DataSerializer dataSerializer;
        private String s3Bucket;
        public ReadShardTask(String folderName, ShardData shardData, DataSerializer dataSerializer, String s3Bucket) {
            this.folderName=folderName;
            this.shardData=shardData;
            this.s3Bucket = s3Bucket;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
            this.dataSerializer = dataSerializer;
        }

        public ShardResult call() throws IOException {
            ShardResult result = new ShardResult();
            List<Result> results = new ArrayList<>();
            List<String> folders = dataSerializer.listFolders(folderName+File.separator);
            List<String> relevantFolders = filterFolders(shardData.datesRange, folders, folderName+File.separator);
            for (String folder : relevantFolders) {
                ReadFolderTask mf = new ReadFolderTask(folder, shardData.userIds, dataSerializer, s3Bucket);
                results.add(mf.call());
            }
            ArrayList<String> errors = new ArrayList<>();
            for (Result res : results) {
                if (res.error != null) {
                    errors.add(res.path + ": " + res.error);
                }
            }

            return result;
        }
    }
    private class ReadFolderTask implements Callable {
        private String folderName;
        private List<String> userIds;
        private ThreadPoolExecutor executor;
        private DataSerializer dataSerializer;
        private String s3Bucket;
        public ReadFolderTask(String folderName, List<String> userIds, DataSerializer dataSerializer, String s3Bucket) {
            this.folderName=folderName;
            this.userIds=userIds;
            this.s3Bucket = s3Bucket;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
            this.dataSerializer=dataSerializer;
        }
        public Result call() {
            logger.info("looking for userID:"+userIds);
            Result result = new Result();
            result.path = folderName;
            try {
                List<String> files = dataSerializer.listFilesInFolder(folderName, "", false);
                files = filterNonParquetFiles(files);
                List<Future<Result>> results = new ArrayList<>();
                for (String fileName : files) {
                    ReadFileTask mf = new ReadFileTask(folderName+"/"+fileName, userIds, s3Bucket);
                    Future<Result> future = executor.submit(mf);
                    results.add(future);
                }
                ArrayList<String> errors = new ArrayList<>();
                for (Future<Result> future : results) {
                    Result res = future.get();
                    if (res.error != null) {
                        if (result.error == null) {
                            result.error = res.error;
                        } else {
                            result.error += ","+res.error;
                        }
                    }
                }

            } catch (InterruptedException | ExecutionException | IOException e) {
                logger.error("error retrieving data from parquet files:"+e.getMessage());
            }
            return result;
        }

        private List<String> filterNonParquetFiles(List<String> files) {
            if (files == null) return files;
            List<String> result = files.stream()                // convert list to stream
                    .filter(file -> file.endsWith(PARQUET_FILE_EXTENSION))     // only parquet files
                    .collect(Collectors.toList());              // collect the output and convert streams to a List
            return result;
        }
    }
    private class ReadFileTask implements Callable {
        private String fileName;
        private List<String> userIds;
        private String s3Bucket;
        public ReadFileTask(String fileName, List<String> userIds, String s3Bucket) {
            this.fileName=fileName;
            this.userIds=userIds;
            this.s3Bucket=s3Bucket;
        }

        public Result call() {
            Instant startReadTime = Instant.now();
            Result result = new Result();
            result.path = fileName;
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
                String filePath = "s3a://" + s3Bucket + File.separator + folderPath + fileName;
                Path path = new Path(filePath);
                HadoopInputFile hif = HadoopInputFile.fromPath(path, conf);
                ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(path, conf),
                        ParquetReadOptions.builder().build());

                ParquetMetadata md = reader.getFooter();
                MessageType schema = md.getFileMetaData().getSchema();

                //add fields in schema to the parquet fields list
                List<ColumnDescriptor> descriptorList = schema.getColumns();
                for (ColumnDescriptor colDescriptor : descriptorList) {
                    parquetFields.addField(colDescriptor.getPath()[0], colDescriptor.getPrimitiveType().getPrimitiveTypeName());
                }

                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                try {
                    boolean userFound = false;
                    PageReadStore pages;
                    int counter = 0;

                    while (null != (pages = reader.readNextRowGroup()) && !userFound) {
                        final long rows = pages.getRowCount();
                        //LOGGER.info("rows = " + rows);
                        RecordReader<Group> recordReader = columnIO.getRecordReader(
                                pages, new GroupRecordConverter(schema));
                        String currUser=null;
                        List<String> usersToCover = new ArrayList<>(this.userIds);
                        for (int i = 0; i < rows; i++) {
                            try {
                                Group g = recordReader.read();
                                String userId = g.getBinary("userId", 0).toStringUsingUTF8();
                                if (currUser != null && !currUser.equals(userId)) {
                                    //finished with the last user
                                    usersToCover.remove(currUser);
                                    if (usersToCover.isEmpty()) {
                                        //we have gone past our userIds, we can stop now
                                        return result;
                                    }
                                }
                                if (this.userIds.contains(userId)) {
                                    currUser = userId;
                                    JSONObject eventObj = createEventJSON(g);
                                    logger.info("found event with userID:"+userId);
                                    String eventName = eventObj.getString("name");
                                    if (eventName!= null && config.getExcludedEventNames()!=null &&
                                        config.getExcludedEventNames().contains(eventName)) {
                                        logger.debug("excluding event "+eventName);
                                    } else {
                                        addEvent(eventObj, userId);
                                    }

                                } else if (usersToCover.isEmpty()) {
                                    //we have gone past our userIds, we can stop now
                                    userFound = true;
                                    return result;
                                }
                                counter++;
                            } catch (Exception e) {
                                logger.info("Error reading record from parquet file " + fileName + ". Skipping. " + e.getMessage());
                            }
                        }
                    }

                    logger.debug("Read '" + filePath + "', number of records = " + counter + " time = " + Duration.between(startReadTime, Instant.now()).toMillis() + " ms");
                    // System.out.println("Number of groups added: " + counter);
                } finally {
                    reader.close();
                }
            }
            catch (Exception e) {
                result.error = "error reading file: " + fileName + ": " + e.getMessage();
                logger.error(result.error);
            }
            return result;
        }

        private JSONObject createEventJSON(Group g) {
            JSONObject obj = new JSONObject();
            Set<String> fieldNames =  parquetFields.getFieldNames();
            int i=0;
            for (String fieldName : parquetFields.getFieldNames()) {
                PrimitiveType.PrimitiveTypeName type = parquetFields.getFieldType(fieldName);
                try {
                    Object value = null;
                    switch (type) {
                        case BOOLEAN:
                            value = g.getBoolean(fieldName, 0);
                            break;
                        case FLOAT:
                            value = g.getFloat(fieldName, 0);
                            break;
                        case DOUBLE:
                            value = g.getDouble(fieldName, 0);
                            break;
                        case INT32:
                            value = g.getInteger(fieldName, 0);
                            break;
                        case INT64:
                            value = g.getLong(fieldName, 0);
                            break;
                        default:
                            value = g.getBinary(fieldName, 0).toStringUsingUTF8();
                    }
                    if (value!= null) {
                        value = String.valueOf(value);
                    }
                    obj.put(fieldName, value);
                } catch (Exception e) {
                    logger.debug("skipping field "+fieldName+":"+e.getMessage());
                }
                i++;
            }
            return  obj;
        }

    }


}
