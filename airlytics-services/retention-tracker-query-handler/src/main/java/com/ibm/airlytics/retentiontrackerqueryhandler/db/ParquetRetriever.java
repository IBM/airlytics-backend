package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.DataSerializer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.S3Serializer;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
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
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Component
@DependsOn("Airlock")
public class ParquetRetriever {
    private final DbHandler dbHandler;
    private RetrieverConfig config;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(ParquetRetriever.class.getName());

    private String awsSecret;
    private String awsKey;
    private String s3FilePrefix;
    private ThreadPoolExecutor executor;
    private String folderPath;
    private ParquetFields parquetFields = new ParquetFields();
    private Map<String, JSONObject> users;
    private JSONArray events = new JSONArray();
    private DataSerializer dataSerializer;

    public ParquetRetriever(DbHandler dbHandler) throws IOException {
        this.dbHandler = dbHandler;
        config = new RetrieverConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(PersistenceConsumerConstants.AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(PersistenceConsumerConstants.AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = config.getS3Bucket() + File.separator + config.getS3RootFolder() + File.separator + config.getTopic() + File.separator;
        String folderPath = ""; //TODO::
        this.folderPath = folderPath.endsWith(File.separator) ? folderPath : folderPath + File.separator;
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
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
    public JSONArray getUserData(List<String> userIds, String product) throws SQLException, ClassNotFoundException, IOException {
        Map<Integer, ShardData> shards = new HashMap<Integer, ShardData>();
        this.users = new ConcurrentHashMap<>();
        for (String userId : userIds) {
            JSONObject userObj = dbHandler.getUserJSON(userId);
            userObj.put("events", new JSONArray());
            int shard = userObj.getInt("shard");
            ShardData shardData = shards.getOrDefault(shard, new ShardData());
            shardData.userIds.add(userId);
            DatesRange currDr = getDatesRange(userObj);
            userObj.put("events", new JSONArray());
            users.put(userId, userObj);
            shardData.datesRange = updateRanges(shardData.datesRange, currDr);
            shards.put(shard, shardData);
        }
        List<Future<ShardResult>> shardResults = new ArrayList<>();
        for (Map.Entry<Integer, ShardData> entry : shards.entrySet()) {
            int shard = entry.getKey().intValue();
            ShardData data = entry.getValue();
            String shardPath = "shard="+shard;
            String folderPath = "parquet/"+product+"/"+shardPath;
            ReadShardTask rst = new ReadShardTask(folderPath, data);
            Future<ShardResult> future = executor.submit(rst);
            shardResults.add(future);
        }

        List<String> errors = new ArrayList<>();
        try{
            for (Future<ShardResult> future: shardResults) {
                ShardResult res = future.get();
                if (res.errors != null) {
                    errors.addAll(res.errors);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        synchronized (users) {
            JSONArray toRet = new JSONArray();
            for (JSONObject userObj : users.values()) {
                toRet.put(userObj);
            }
            return toRet;
        }
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
            logger.debug("firstSession:"+firstSession.toString());
        }
        Timestamp lastSession=null;
        if (userObj.getString("last_session") != null) {
            lastSession = Timestamp.valueOf(userObj.getString("last_session"));
            logger.debug("lastSession:"+lastSession.toString());
        }
        toRet.firstSession = firstSession;
        toRet.lastSesion = lastSession;
        return toRet;
    }

    private List<String> filterFolders(DatesRange dates, List<String> folders) {
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
                toRet.add(folder);
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
            logger.debug("firstSession:"+firstSession.toString());
        }
        Timestamp lastSession=null;
        if (userObj.getString("last_session") != null) {
            lastSession = Timestamp.valueOf(userObj.getString("last_session"));
            logger.debug("lastSession:"+lastSession.toString());
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
        JSONObject userJSON = users.get(userId);
        synchronized (userJSON) {
            logger.debug("adding event! "+eventObj.getString("eventId"));
            userJSON.getJSONArray("events").put(eventObj);
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
        public ReadShardTask(String folderName, ShardData shardData) {
            this.folderName=folderName;
            this.shardData=shardData;
        }

        public ShardResult call() throws IOException {
            ShardResult result = new ShardResult();
            List<Future<Result>> results = new ArrayList<>();
            List<String> folders = dataSerializer.listFoldersInFolder(folderName);
            List<String> relevantFolders = filterFolders(shardData.datesRange, folders);
            for (String folder : relevantFolders) {
                ReadFolderTask mf = new ReadFolderTask(folder, shardData.userIds);
                Future<Result> future = executor.submit(mf);
                results.add(future);
            }
            ArrayList<String> errors = new ArrayList<>();
            try {
                for (Future<Result> future : results) {
                    Result res = future.get();
                    if (res.error != null) {
                        errors.add(res.path + ": " + res.error);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return result;
        }
    }
    private class ReadFolderTask implements Callable {
        private String folderName;
        private List<String> userIds;
        public ReadFolderTask(String folderName, List<String> userIds) {
            this.folderName=folderName;
            this.userIds=userIds;
        }
        public Result call() {
            logger.debug("looking for userID:"+userIds);
            Result result = new Result();
            result.path = folderName;
            try {
                List<String> files = dataSerializer.listFilesInFolder(folderName);
                List<Future<Result>> results = new ArrayList<>();
                for (String fileName : files) {
                    ReadFileTask mf = new ReadFileTask(folderName+"/"+fileName, userIds);
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
    }
    private class ReadFileTask implements Callable {
        private String fileName;
        private List<String> userIds;
        public ReadFileTask(String fileName, List<String> userIds) {
            this.fileName=fileName;
            this.userIds=userIds;
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
                String filePath = "s3a://" + config.getS3Bucket() + File.separator + folderPath + fileName;
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
                                if (this.userIds.contains(userId)) {
                                    if (currUser != null && !currUser.equals(userId)) {
                                        //finished with the last user
                                        usersToCover.remove(currUser);
                                    }
                                    currUser = userId;
                                    userFound = true;
                                    JSONObject eventObj = createEventJSON(g);
                                    logger.info("found event with userID:"+userId);
                                    addEvent(eventObj, userId);
                                } else if (usersToCover.isEmpty()) {
                                    //we have gone past our userIds, we can stop now
                                    return result;
                                }
                                counter++;
                                String sessionId = g.getBinary("sessionId", 0).toStringUsingUTF8();
                                String eventId = g.getBinary("eventId", 0).toStringUsingUTF8();
                                long eventTime = g.getLong("eventTime", 0);
//                                usersData.addGroup(userId, sessionId, eventTime, eventId, g);
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
