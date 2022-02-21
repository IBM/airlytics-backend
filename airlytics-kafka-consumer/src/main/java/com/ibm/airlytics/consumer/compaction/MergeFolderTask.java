package com.ibm.airlytics.consumer.compaction;

import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.persistence.FieldConfig;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.serialize.parquet.RowsLimitationParquetWriter;
import com.ibm.airlytics.utilities.EventFieldsMaps;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.eco.EconomicalGroup;
import org.apache.parquet.example.data.eco.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.CustomParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class MergeFolderTask implements Callable {
    private static final Logger LOGGER = Logger.getLogger(MergeFolderTask.class.getName());

    //Number of successful merge folder tasks
    static final Counter mergeFolderSuccessTasksCounter = Counter.build()
            .name("airlytics_compaction_merge_folder_suc_tasks_total")
            .help("Total successful merge folder tasks processed by the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //Number of failed merge folder tasks
    static final Counter mergeFolderFailTasksCounter = Counter.build()
            .name("airlytics_compaction_merge_folder_fail_tasks_total")
            .help("Total failed merge folder tasks processed by the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //counts only the newly merged events
    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_compaction_records_processed_total")
            .help("Total records processed by the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //Number of duplicated events
    static final Counter duplicatedEventsCounter = Counter.build()
            .name("airlytics_compaction_duplicated_events_total")
            .help("Total duplicated events detected by the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //counts all (newly+already merged) read events
    static final Counter totalRecordsReadCounter = Counter.build()
            .name("airlytics_compaction_records_read_total")
            .help("Total records read by the compaction consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, SOURCE).register();



    //opertaion: READ/WRITE
    //source: S3/FILE_SYSTEM
    static final Counter compactionIoOperationsCounter = Counter.build()
            .name("airlytics_compaction_io_action_total")
            .help("Total number of files read/written by the compaction consumer.")
            .labelNames(ENV, PRODUCT, OPERATION, SOURCE).register();

    //count only the io actions - so for each thread "read action" separately
    static final Counter compactionIoOperationsLatencyMSCounter = Counter.build()
            .name("airlytics_compaction_io_operations_latency_ms")
            .help("Compaction io operation latency seconds.")
            .labelNames(ENV, PRODUCT, OPERATION, SOURCE).register();

    //count global read and write actions - includes data structures building
    static final Summary compactionIoOperationsLatencyMSCounterSummary = Summary.build()
            .name("airlytics_compaction_io_operations_latency_summary_ms")
            .help("Compaction global operation latency seconds.")
            .labelNames(ENV, PRODUCT, OPERATION).register();


    private static class UsersData {
        private class SessionTime implements Comparable<SessionTime>{
            String sessionId;
            Long sessionStartTime; //the time of the first event in the session

            SessionTime (String sessionId, Long sessionStartTime) {
                this.sessionId = sessionId;
                this.sessionStartTime = sessionStartTime;
            }

            @Override
            public int compareTo(SessionTime s) {
                return sessionStartTime.compareTo(s.sessionStartTime);
            }
        }

        //map<user-><map<sessionId->list of events sorted by date>>>
        TreeMap<String, TreeMap<String, TreeMap<Long, LinkedList<Group>>>> usersDataMap;

        //list of all event ids that were already added. This is used to avoid duplications
        HashSet<UUID> eventIds;

        public synchronized void addGroup(String userId, String sessionId, long eventTime, String eventId, Group group) {
            if (containsEventId(eventId)) {
                LOGGER.debug("Event id " + eventId + " already exists. Ignoring.");
                duplicatedEventsCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }
            else {
                TreeMap<String, TreeMap<Long, LinkedList<Group>>> userMap = usersDataMap.get(userId);
                if (userMap == null) {
                    userMap = new TreeMap<>();
                    usersDataMap.put(userId, userMap);
                }

                TreeMap<Long, LinkedList<Group>> sessionMap = userMap.get(sessionId);
                if (sessionMap == null) {
                    sessionMap = new TreeMap<>();
                    userMap.put(sessionId, sessionMap);
                }

                LinkedList<Group> groupsList = sessionMap.get(eventTime);
                if (groupsList == null) {
                    groupsList = new LinkedList<>();
                    sessionMap.put(eventTime, groupsList);
                }
                groupsList.add(group);
                addEventId(eventId);
            }
        }

        public synchronized long getGroupsNumber() {
            return eventIds.size();
        }

        public UsersData() {
            eventIds = new HashSet<>();
            usersDataMap = new TreeMap<String, TreeMap<String, TreeMap<Long, LinkedList<Group>>>>();
        }

        private TreeMap<Long, LinkedList<Group>> getSessionEventsMap (String userId, String sessionId) {
            TreeMap<String, TreeMap<Long, LinkedList<Group>>> userMap = usersDataMap.get(userId);
            if (userMap!=null) {
                return userMap.get(sessionId);
            }
            return null;
        }

        public synchronized int calculateUserRows(String userId) {
            int counter = 0;

            TreeMap<String, TreeMap<Long, LinkedList<Group>>> userMap = usersDataMap.get(userId);

            for (Map.Entry<String, TreeMap<Long, LinkedList<Group>>> sessionEntry : userMap.entrySet()) {
                TreeMap<Long, LinkedList<Group>> sessionMap = sessionEntry.getValue();

                for (Map.Entry<Long, LinkedList<Group>> dateEntry : sessionMap.entrySet()) {
                    LinkedList<Group> groupsList = dateEntry.getValue();
                    for (Group g : groupsList) {
                        counter++;
                    }
                }
            }
            return counter;
        }

        public synchronized Set<String> getUsers() {
            return usersDataMap.keySet();
        }

        private Set<String> getUserSessions(String userId) {
            return usersDataMap.get(userId).keySet();
        }

        public TreeMap<String, TreeMap<String, TreeMap<Long, LinkedList<Group>>>> getUsersDataMap() {
            return usersDataMap;
        }

        public synchronized void addEventId(String eventId) {
            eventIds.add(UUID.fromString(eventId));
        }

        public synchronized boolean containsEventId(String eventId) {
            return eventIds.contains(UUID.fromString(eventId));
        }

        public synchronized List<String> getSortedUserSessions(String userId) {
            Set<String> userSessions = getUserSessions(userId);
            List<SessionTime> sessionsTimeList = new LinkedList<>();
            for (String userSession:userSessions) {
                TreeMap<Long, LinkedList<Group>> sessionMap = getSessionEventsMap(userId, userSession);
                for (Map.Entry<Long, LinkedList<Group>> dateEntry : sessionMap.entrySet()) {
                    sessionsTimeList.add(new SessionTime(userSession, dateEntry.getKey()));
                    break;
                }
            }
            Collections.sort(sessionsTimeList);
            LinkedList<String> sortedSessions = new LinkedList<>();

            for (SessionTime st:sessionsTimeList) {
                sortedSessions.add(st.sessionId);
            }
            return sortedSessions;
        }

        public synchronized void clear() {
            usersDataMap.clear();
            eventIds.clear();
        }

        public synchronized void removeUserDataFromMap(String userId) {
            if (usersDataMap.containsKey(userId)) {
                usersDataMap.put(userId, null);
            }
        }
    }

    public class ParquetFields {
        //Map between field name to field type
        HashMap<String, PrimitiveType.PrimitiveTypeName> fieldsMap;

        public ParquetFields() {
            fieldsMap = new HashMap<String, PrimitiveType.PrimitiveTypeName>();
        }

        public synchronized void addField(String name, PrimitiveType.PrimitiveTypeName type) throws MergeDataException{
            if (fieldsMap.containsKey(name) && !fieldsMap.get(name).equals(type)) { //type collision
                //replace int with long
                if (fieldsMap.get(name).equals(PrimitiveType.PrimitiveTypeName.INT32) && type.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                    fieldsMap.put(name, type);
                    return;
                }
                if (fieldsMap.get(name).equals(PrimitiveType.PrimitiveTypeName.INT64) && type.equals(PrimitiveType.PrimitiveTypeName.INT32)) {
                    return;
                }
                else {
                    if (typeCollisionsMap.containsKey(name)) {
                        typeCollisionsMap.get(name).foundTypes.add(type);
                        return;
                    }

                    //look for the field in the configured events maps
                    FieldConfig configuredType=configuredEventsMaps.getFieldConfigByFullName(name);

                    if (configuredType == null) {
                        throw new MergeDataException("Types collision for field " + name + ". Both " + type.toString() + " and " + fieldsMap.get(name).toString() + " exist. In folder " + baseFolderPath);
                    } else {
                        TypeCollision tc = new TypeCollision();
                        tc.configuredType = convertFieldTypeToPrimitiveType(configuredType);
                        tc.foundTypes.add(tc.configuredType); //add the configured type as one of the possible type
                        tc.foundTypes.add(type); //add new type
                        tc.foundTypes.add(fieldsMap.get(name)); //add prev type
                        typeCollisionsMap.put(name, tc);
                        fieldsMap.put(name, tc.configuredType);
                        LOGGER.info("Types collision for field " + name + " Both " + type.toString() + " and " + fieldsMap.get(name).toString() + " exist. Configured type = " + configuredType.getType().toString() + ". In folder: " + baseFolderPath);
                    }
                }
            }

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
                        sb.append(requiredStr + " binary "+ entry.getKey() + " (UTF8);\n");
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

    private PrimitiveType.PrimitiveTypeName convertFieldTypeToPrimitiveType(FieldConfig fieldConfig) {
        switch (fieldConfig.getType()) {
            case STRING:
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case INTEGER:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case LONG:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case FLOAT:
                return PrimitiveType.PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            default:
                return PrimitiveType.PrimitiveTypeName.BINARY;
        }
    }

    class Result {
        String error = null;
        String path;
        double maxUnmergedDataSizeM;
    }


    class TypeCollision {
        PrimitiveType.PrimitiveTypeName configuredType;
        Set<PrimitiveType.PrimitiveTypeName> foundTypes = new HashSet<PrimitiveType.PrimitiveTypeName>();
    }

    private String baseFolderPath;
    private String awsSecret;
    private String awsKey;
    private UsersData usersData = new UsersData();
    private long eventsCounter = 0;
    private ParquetFields parquetFields = new ParquetFields();
    private CompactionConsumerConfig config;
    private AtomicLong tasksCounter;
    private EventFieldsMaps configuredEventsMaps;

    //map of collide types
    //fieldName->TypeCollision
    private HashMap<String, TypeCollision> typeCollisionsMap = new HashMap<String, TypeCollision>();

    private boolean inputS3Storage;
    private boolean outputS3Storage;
    private String scoreFolder;
    private String usersToDeleteFolder;

    //base data
    private String inputBaseDataFolder;
    private String inputBaseDataFilePrefix;
    private String outputBaseDataFilePrefix;
    private String outputBaseDataFolder;
    private DataSerializer inputBaseDataSerializer;
    private DataSerializer outputBaseDataSerializer;

    //pi data
    private String inputPiDataFolder;
    private String inputPiDataFilePrefix;
    private String outputPiDataFilePrefix;
    private String outputPiDataFolder;
    private DataSerializer inputPiDataSerializer;
    private DataSerializer outputPiDataSerializer;

    private long age; //days old
    private long numberOfAlreadyMergedRecords = 0;
    private AtomicInteger runningTasksCounter;

    private Set<String> userIdsToDelete = new HashSet<>();

    double maxUnmergedDataSizeM = 0;

    private TasksQueue.Task queuedTask;

    public MergeFolderTask(TasksQueue.Task queuedTask, String awsKey, String awsSecret, CompactionConsumerConfig config, AtomicLong tasksCounter, EventFieldsMaps configuredEventsMaps, AtomicInteger runningTasksCounter) throws IOException {
        this.runningTasksCounter = runningTasksCounter;

        this.baseFolderPath = queuedTask.getSubFolder();
        this.inputBaseDataFolder = config.getInputDataFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;
        this.outputBaseDataFolder = config.getOutputDataFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;
        this.inputPiDataFolder = config.getPiInputDataFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;
        this.outputPiDataFolder = config.getPiOutputDataFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;
        this.scoreFolder = config.getScoresFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;
        this.usersToDeleteFolder = config.getUsersToDeleteFolder() + File.separator + config.getTopic() + File.separator + baseFolderPath + File.separator;

        this.age = queuedTask.getAge();

        this.inputS3Storage = config.getInputStorageType().equals(STORAGE_TYPE.S3.toString()); //if false => fileSystemStorage
        this.outputS3Storage = config.getOutputStorageType().equals(STORAGE_TYPE.S3.toString()); //if false => fileSystemStorage

        String baseFolder = config.getBaseFolder();
        if (config.getBaseFolder()!=null && !baseFolder.endsWith(File.separator)) {
            baseFolder = baseFolder+File.separator;
        }

        if(inputS3Storage) {
            this.inputBaseDataFilePrefix = config.getS3Bucket() + File.separator + inputBaseDataFolder;
            this.inputBaseDataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries()); //base and nonPi uses the same data serializer
            this.inputPiDataFilePrefix = config.getS3PiBucket() + File.separator + inputPiDataFolder;
            this.inputPiDataSerializer = new S3Serializer(config.getS3region(), config.getS3PiBucket(), config.getIoActionRetries());
        }
        else { //FILE_SYSTEM
            this.inputBaseDataFilePrefix = baseFolder + inputBaseDataFolder;
            this.inputBaseDataSerializer = new FSSerializer(baseFolder, config.getIoActionRetries());
            this.inputPiDataFilePrefix = baseFolder + inputPiDataFolder;
            this.inputPiDataSerializer = new FSSerializer(baseFolder, config.getIoActionRetries());
        }

        if (outputS3Storage) {
            this.outputBaseDataFilePrefix = config.getS3Bucket() + File.separator + outputBaseDataFolder;
            this.outputBaseDataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
            this.outputPiDataFilePrefix = config.getS3PiBucket() + File.separator + outputPiDataFolder;
            this.outputPiDataSerializer = new S3Serializer(config.getS3region(), config.getS3PiBucket(), config.getIoActionRetries());
        }
        else { //FILE_SYSTEM
            this.outputBaseDataFilePrefix = baseFolder + outputBaseDataFolder;
            this.outputBaseDataSerializer = new FSSerializer(baseFolder, config.getIoActionRetries());
            this.outputPiDataFilePrefix = baseFolder + outputPiDataFolder;
            this.outputPiDataSerializer = new FSSerializer(baseFolder, config.getIoActionRetries());
        }

        this.awsSecret = awsSecret; //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = awsKey; //if set as environment parameters use them otherwise will be taken from role
        this.config = config;
        this.tasksCounter = tasksCounter;
        this.configuredEventsMaps = configuredEventsMaps;
        this.maxUnmergedDataSizeM = calcMaxUnmergedDataSizeM();
        this.maxUnmergedDataSizeM+=queuedTask.getOriginalScore();
        this.queuedTask = queuedTask;
    }

    public TasksQueue.Task getQueuedTask() {
        return queuedTask;
    }

    private double calcMaxUnmergedDataSizeM() throws IOException {
        //look for non-pi merged file.
        if (!outputBaseDataSerializer.exists(outputBaseDataFolder + PARQUET_MERGED_FILE_NAME)) {
            return 0; //the non-pi merged file does not exist means no unmerged data exists
        }

        double nonPiSize = outputBaseDataSerializer.getFileSizeB(outputBaseDataFolder + PARQUET_MERGED_FILE_NAME, false);

        //look for pi merged file.
        if (!outputPiDataSerializer.exists(outputPiDataFolder +  PARQUET_MERGED_FILE_NAME)) {
            return nonPiSize/1024/1024; //only non-pi merged file exists
        }

        double piSize = outputPiDataSerializer.getFileSizeB(outputPiDataFolder + PARQUET_MERGED_FILE_NAME, false);

        return Math.max(piSize, nonPiSize)/1024/1024;
    }

    public double getMaxUnmergedDataSizeM() {
        return maxUnmergedDataSizeM;
    }

    public Result call() throws ExecutionException, InterruptedException, MergeDataException {
        try {
            Instant startMergeTask = Instant.now();

            Result res = doCall();
            if (res.error!=null) {
                //errors that can be ignored and retried later
                LOGGER.warn("Fail merging folder " + res.path + ":" + res.error);
                mergeFolderFailTasksCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                return res;
            }

            LOGGER.info("***** Merge folder " + baseFolderPath +
                    ", number of events written: " + eventsCounter +
                    " , total time = " + Duration.between(startMergeTask, Instant.now()).toMillis() + " ms");


            tasksCounter.incrementAndGet();
            mergeFolderSuccessTasksCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

            long numberOfNewEvents = eventsCounter-numberOfAlreadyMergedRecords;
            if (numberOfNewEvents > 0) { //can be negative if no events are added but only pi events are deleted for user with deletionRequest
                recordsProcessedCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(numberOfNewEvents);
            }

            return res;
        } finally {
            runningTasksCounter.decrementAndGet();
        }
    }

    public Result doCall() throws ExecutionException, InterruptedException, MergeDataException {
        Result res = new Result();
        res.path = inputBaseDataFolder;
        res.maxUnmergedDataSizeM = maxUnmergedDataSizeM;

        res = mergeFolder(inputBaseDataFolder, inputBaseDataSerializer, outputBaseDataFolder, outputBaseDataSerializer, inputBaseDataFilePrefix, outputBaseDataFilePrefix);
        if(res.error!=null) {
            return res;
        }

        //build the users to delete list at the last moment to avoid user added after reading files.
        LinkedList<String> usersToDeleteFileNames = null;
        try {
            usersToDeleteFileNames = buildUsersToDeleteLists();
        } catch (IOException e) {
            res.error = "Fail listing file in folder: " + e.getMessage();
            return res;
        }

        res = mergeFolder(inputPiDataFolder, inputPiDataSerializer, outputPiDataFolder, outputPiDataSerializer, inputPiDataFilePrefix, outputPiDataFilePrefix);
        if(res.error!=null) {
            return res;
        }

        LinkedList<String> scoreFileNames = null; //should be only one
        try {
            scoreFileNames = inputBaseDataSerializer.listFilesInFolder(scoreFolder, SCORE_FILE_PREFIX, false);
        } catch (IOException e) {
            res.error = "Fail listing file in folder: " + e.getMessage();
            return res;
        }

        if (scoreFileNames!=null) {
            inputBaseDataSerializer.deleteFilesIgnoreError(scoreFolder, scoreFileNames, null, age>40);
        }

        if (usersToDeleteFileNames!=null) {
            inputBaseDataSerializer.deleteFilesIgnoreError(usersToDeleteFolder, usersToDeleteFileNames, null, false);
        }

        return res;
    }

    private LinkedList<String> buildUsersToDeleteLists() throws IOException{
        StringBuilder sb = new StringBuilder();
        LinkedList<String> usersToDeleteFileNames = inputBaseDataSerializer.listFilesInFolder(usersToDeleteFolder, USER_TO_DELETE_FILE_PREFIX, false);
        for (String f:usersToDeleteFileNames) {
            int deleteRequestUserPos = f.indexOf(USER_TO_DELETE_FILE_PREFIX);
            if (deleteRequestUserPos == -1) {
                LOGGER.error("Illegal users to delete file name:" + f);
                continue;
            }
            int deleteRequestDayPos = f.indexOf(USER_TO_DELETE_DAY_MARKER);
            if (deleteRequestDayPos == -1) {
                LOGGER.error("Illegal users to delete file name:" + f);
                continue;
            }

            String userId = f.substring(deleteRequestUserPos + USER_TO_DELETE_FILE_PREFIX.length(), deleteRequestDayPos);
            userIdsToDelete.add(userId);
            sb.append(userId + " , ");
        }

        if (sb.length() > 0) {
            LOGGER.info("Users to delete from folder '" + baseFolderPath + "' :" + sb.toString());
        }
        return usersToDeleteFileNames;
    }

    private Result mergeFolder(String inputDataFolder, DataSerializer inputDataSerializer,
                               String outputDataFolder, DataSerializer outputDataSerializer,
                               String inputDataFilePrefix, String outputDataFilePrefix) throws ExecutionException, InterruptedException, MergeDataException {
        Instant startMergeFolder = Instant.now();
        Result res = new Result();
        res.path = inputDataFolder;
        res.maxUnmergedDataSizeM = maxUnmergedDataSizeM;

        LinkedList<String> inputFileNames = null;
        long prevEventsCounter = eventsCounter;
        try {
            try {
                inputFileNames = inputBaseDataSerializer.listFilesInFolder(inputDataFolder, "", false);
            } catch (IOException e) {
                res.error = "Fail listing file in folder: " + e.getMessage();
                return res;
            }

            if (inputFileNames.size() > 0 || userIdsToDelete.size()>0) { //if there are new files to merge or users to delete
                //look for the merged file - if exists should be merged with the new input files too
                //should be 0 or 1 such files
                LinkedList<String> mergedFile = null;
                try {
                    mergedFile = outputDataSerializer.listFilesInFolder(outputDataFolder, PARQUET_MERGED_FILE_NAME, false);
                } catch (IOException e) {
                    res.error = "Fail listing file in folder: " + e.getMessage();
                    return res;
                }

                String mergedFilePath = null;
                if (mergedFile != null && mergedFile.size() > 0) {
                    mergedFilePath = mergedFile.get(0);
                }

                //read folder content to map
                String readResults = readFolderContent(inputFileNames, mergedFilePath, inputDataFilePrefix, outputDataFilePrefix, inputDataSerializer, outputDataSerializer, inputDataFolder, outputDataFolder);

                if (!readResults.isEmpty()) { //error during read
                    res.error = readResults;
                    return res;
                }

                String writeResults = writeMergedData(outputDataFilePrefix);

                if (!writeResults.isEmpty()) { //error during write temp merged file
                    res.error = writeResults;
                    return res;
                }

               try {
                    //delete the input folder only if the folder is older than 40 days
                    inputDataSerializer.deleteFiles(inputDataFolder, inputFileNames, PARQUET_MERGED_FILE_NAME, age > 40);
                } catch (IOException e) {
                    res.error = "Fail deleting " + inputFileNames.size() + " merged files from folder " + inputDataFolder + ": " + e.getMessage();
                    return res;
                }
            }
            LOGGER.info("* Merge sub folder " + inputDataFolder +
                    ", number of files merged: " + inputFileNames.size() +
                    ", number of events written: " + (eventsCounter - prevEventsCounter) +
                    " , total time = " + Duration.between(startMergeFolder, Instant.now()).toMillis() + " ms");
        }  finally {
            usersData.clear();
        }

        return res;
    }

    private class ReadFileTask implements Callable {
        private String filePath;
        private String fileName;
        private STORAGE_TYPE storageType;
        private DataSerializer dataSerializer;
        private String dataFolder;

        public ReadFileTask(String filePath, STORAGE_TYPE storageType, String fileName, DataSerializer dataSerializer, String dataFolder ) {
            this.filePath = filePath;
            this.storageType = storageType;
            this.fileName = fileName;
            this.dataSerializer = dataSerializer;
            this.dataFolder = dataFolder;
        }

        public Result call() throws MergeDataException {
            Instant startReadTime = Instant.now();
            Result result = new Result();
            result.path = filePath;
            boolean isMergedFile = filePath.endsWith(PARQUET_MERGED_FILE_NAME);
            String storage = isMergedFile?config.getOutputStorageType():config.getInputStorageType();

            long readTimeMS=0;
            try {
                Configuration conf = new Configuration();

                if (storageType.equals(STORAGE_TYPE.S3)) {
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
                }
                else { //FILE_SYSTEM
                    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                }

                Path path = new Path(filePath);
                //exception can be thrown when the file size is 0. This happens when the file was opened for wrting by the
                //persistence consumer but was not closed yet
                int retryCounter = 0;
                ParquetFileReader reader = null;
                while (retryCounter < config.getIoActionRetries()) {
                    try {
                        Instant startReadOperationTime = Instant.now();
                        reader = new ParquetFileReader(HadoopInputFile.fromPath(path, conf), ParquetReadOptions.builder().build());
                        readTimeMS += Duration.between(startReadOperationTime, Instant.now()).toMillis();
                        break;
                    } catch (IOException | RuntimeException e) { //catching RuntimeException as well since files of size 0 are throwing RuntimeException
                        LOGGER.info("(" + retryCounter + ") Cannot open file " + filePath + " for reading: " + e.getMessage());
                        retryCounter++;
                        if (retryCounter == config.getIoActionRetries()) {
                            throw e;
                        }
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                    }
                }

                int counter = 0;
                try {
                    ParquetMetadata md = reader.getFooter();
                    MessageType schema = md.getFileMetaData().getSchema();

                    //add fields in schema to the parquet fields list
                    List<ColumnDescriptor> descriptorList = schema.getColumns();
                    for (ColumnDescriptor colDescriptor : descriptorList) {
                        parquetFields.addField(colDescriptor.getPath()[0], colDescriptor.getPrimitiveType().getPrimitiveTypeName());
                    }

                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

                    PageReadStore pages;

                    while (null != (pages = reader.readNextRowGroup())) {
                        final long rows = pages.getRowCount();
                        RecordReader<Group> recordReader = columnIO.getRecordReader(
                                pages, new GroupRecordConverter(schema));
                        for (int i = 0; i < rows; i++) {
                            try {
                                Instant startReadOperationTime = Instant.now();
                                Group g = recordReader.read();
                                readTimeMS += Duration.between(startReadOperationTime, Instant.now()).toMillis();
                                String userId = g.getBinary("userId", 0).toStringUsingUTF8();
                                counter++;
                                if (isMergedFile) {
                                    numberOfAlreadyMergedRecords++;
                                }

                                String eventId = g.getBinary("eventId", 0).toStringUsingUTF8();
                                long eventTime = g.getLong("eventTime", 0);

                                String sessionId;
                                try {
                                    sessionId = g.getBinary("sessionId", 0).toStringUsingUTF8();
                                }
                                catch (NullPointerException npe) {
                                    //if the session id is not specified for the event - generate random one.
                                    //this way the only event in the seeion will be this event and it will be sorted with the other user's sessions
                                    sessionId = UUID.randomUUID().toString();
                                }

                                usersData.addGroup(userId, sessionId, eventTime, eventId, g);
                            } catch (Exception e) {
                                e.printStackTrace();
                                LOGGER.error("Error reading record from parquet file " + filePath + ". Skipping. " + e.getMessage());
                            }
                        }
                    }

                    if (storageType.equals(STORAGE_TYPE.S3)) {
                        LOGGER.info("Read '" + filePath + "', number of records = " + counter + " time = " + Duration.between(startReadTime, Instant.now()).toMillis() + " ms");
                    }
                } finally {
                    totalRecordsReadCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), storage).inc(counter);
                    Instant startReadOperationTime = Instant.now();
                    reader.close();
                    readTimeMS += Duration.between(startReadOperationTime, Instant.now()).toMillis();
                    if (config.isFillIOCountersData() && result.error==null) { //no errors
                        compactionIoOperationsCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "READ", storage).inc();
                        compactionIoOperationsLatencyMSCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "READ", storage).inc(readTimeMS);
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                result.error = "IO error reading file: " + filePath + ": " + ioe.getMessage();
                LOGGER.error(result.error);
            } catch (Exception e) {
                if (e.getMessage().contains("too small length: 0") || e.getMessage().contains("length is too low: 0")) {
                    //empty file should not stop the consumer - we can report, skip and retry later
                    //usually caused because the persistence did not close the file yet
                    result.error = "cannot read file: " + filePath + ". Empty file:" + e.getMessage();
                    LOGGER.warn(result.error);
                }
                else if (e.getMessage().contains("expected magic number at tail")) { //thrown as RuntimeException
                    //file that was not fully written yet should not stop the consumer - we can report, skip and retry later
                    //usually caused because the persistence did not close the file yet
                    result.error = "cannot read file: " + filePath + ". File is not completely written yet:" + e.getMessage();
                    LOGGER.warn(result.error);
                }
                else {
                    LOGGER.error("Error reading file: " + filePath + ": " + e.getMessage());
                    e.printStackTrace();
                    throw e;
                }
            }

            return result;
        }
    }

    private String readFolderContent(LinkedList<String> inputFileNames, String mergedFilePath,
                                     String inputDataFilePrefix, String outputDataFilePrefix,
                                     DataSerializer inputDataSerializer,  DataSerializer outputDataSerializer,
                                     String inputDataFolder, String outputDataFolder) throws ExecutionException, InterruptedException {
        String results = "";
        Instant startTotalReadTime = Instant.now();
        ArrayList<Future<Result>> readings = new ArrayList<Future<Result>>();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());

        try {
            // if merged file already exists should be merged with the new input files too
            if (mergedFilePath!=null) {
                ReadFileTask task = new ReadFileTask(outputDataFilePrefix + mergedFilePath, outputS3Storage? STORAGE_TYPE.S3 : STORAGE_TYPE.FILE_SYSTEM, mergedFilePath, outputDataSerializer, outputDataFolder);
                Future<Result> future = executor.submit(task);
                readings.add(future);
            }

            for (String file:inputFileNames) {

                if (!file.endsWith(PARQUET_FILE_EXTENSION)) {
                    continue;
                }

                ReadFileTask task = new ReadFileTask(inputDataFilePrefix + file, inputS3Storage? STORAGE_TYPE.S3 : STORAGE_TYPE.FILE_SYSTEM, file, inputDataSerializer, inputDataFolder);
                Future<Result> future = executor.submit(task);
                readings.add(future);
            }

            ArrayList<String> errors = new ArrayList<>();
            for (Future<Result> item : readings) {
                try {
                    Result result = item.get();
                    if (result.error != null) {
                        errors.add(result.path + ": " + result.error);
                    }
                }
                catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                    LOGGER.error("Exception thrown during file read: " + e.getMessage());
                    throw e;
                }
            }

            if (!errors.isEmpty()) {
                //errors that can be ignored and retried later
                results = "Cannot read files: " + errorsListToString(errors);
                LOGGER.warn(results);
            }
        }
        finally {
            executor.shutdown();
        }

        long totalReadTimeMS = Duration.between(startTotalReadTime, Instant.now()).toMillis();
        compactionIoOperationsLatencyMSCounterSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "READ").observe(totalReadTimeMS);
        LOGGER.info("Total read time from '" + inputDataFolder + "' = " + totalReadTimeMS + " ms");

        return results;
    }

    private String errorsListToString(ArrayList<String> errors) {
        StringBuilder sb = new StringBuilder();
        for (String err : errors) {
            sb.append(err);
            sb.append("\n");
        }
        return  sb.toString();
    }

    private String writeMergedData(String outputDataFilePrefix) throws MergeDataException {
        String res = "";
        if (usersData.getGroupsNumber()==0) {
            return res; //can happen when there are no input files and no merged file but only a user deletion request
        }

        int retryCounter = 0;
        CustomParquetWriter<Group> writer = null;
        Instant startWriteTime = Instant.now();

        int counter = 0;
        long writeTimeMS = 0;
        String filePath = outputDataFilePrefix + PARQUET_MERGED_FILE_NAME;

        while (retryCounter < config.getIoActionRetries()) {
            try {
                Configuration conf = new Configuration();

                if (outputS3Storage) {
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
                }
                else {
                    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                }

                MessageType schema = parseMessageType( parquetFields.generateParquetSchema());
                GroupWriteSupport.setSchema(schema, conf);
                Instant startWriteOperationTime = Instant.now();
                writer = RowsLimitationParquetWriter.builder(new Path(filePath))
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(config.getParquetRowGroupSize())
                        .withPageSize(config.getParquetRowGroupSize())
                        .withDictionaryEncoding(true)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .withConf(conf)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build();
                writeTimeMS += Duration.between(startWriteOperationTime, Instant.now()).toMillis();

                Set<String> userIds = usersData.getUsers();
                for (String userId:userIds) {
                    if (userIdsToDelete.contains(userId)) { //only when merging pi the map has values
                        continue;
                    }
                    List<String> userSortedSessionIds = usersData.getSortedUserSessions(userId);
                    int userRowsNum = usersData.calculateUserRows(userId);
                    boolean firstRowForUser = true;
                    for (String sessionId : userSortedSessionIds) {
                        TreeMap<Long, LinkedList<Group>> sessionMap = usersData.getSessionEventsMap(userId, sessionId);
                        for (Map.Entry<Long, LinkedList<Group>> dateEntry : sessionMap.entrySet()) {
                            LinkedList<Group> groupsList = dateEntry.getValue();
                            for (Group g : groupsList) {
                                EconomicalGroup eg = new EconomicalGroup(schema);

                                Set<String> fieldNames = parquetFields.getFieldNames();
                                for (String fieldName : fieldNames) {
                                    if (typeCollisionsMap.containsKey(fieldName)) {
                                        boolean valFound = false;
                                        for (PrimitiveType.PrimitiveTypeName foundType:typeCollisionsMap.get(fieldName).foundTypes){
                                            try {
                                                addFieldToGroup(eg, g, fieldName, foundType, typeCollisionsMap.get(fieldName).configuredType);
                                                valFound = true;
                                                break;
                                            }
                                            catch (Exception e) {
                                                //ignore - try another found types
                                            }
                                        }
                                        if (!valFound) {
                                            throw new MergeDataException("Cannot find value for field '" +fieldName+ "' that has type collision.");
                                        }
                                    }
                                    else {
                                        addFieldToGroup(eg, g, fieldName, parquetFields.getFieldType(fieldName), null);
                                    }
                                }

                                try {
                                    counter++;
                                    startWriteOperationTime = Instant.now();
                                    if (firstRowForUser) {
                                        writer.write(eg, userRowsNum); //validate that there is enough space in the current rowGroup. If not, create new row group and write
                                        firstRowForUser = false;
                                    } else {
                                        writer.write(eg);
                                    }
                                    writeTimeMS += Duration.between(startWriteOperationTime, Instant.now()).toMillis();
                                    eventsCounter++;
                                }
                                catch (Exception e) {
                                    LOGGER.error("error in event: " + eg.toString() + ":" + e.getMessage() + "\n file path = " + filePath + "\n original event = " + g.toString() + "\n file schema = " + schema.toString(), e);
                                    e.printStackTrace();
                                    throw e;
                                }
                            }
                        }
                    }

                    usersData.removeUserDataFromMap(userId); //no need of the user events any more
                }

                LOGGER.info("Write '" + filePath + "' number of events: " + counter + " , time = " + Duration.between(startWriteTime, Instant.now()).toMillis() + " ms");
                return res;
            } catch (IOException e) {
                ++retryCounter;
                res = "Fail writing file to S3 trial number: " + retryCounter + ": " + e.getMessage();
                LOGGER.error(res, e);
                e.printStackTrace();
                eventsCounter = 0;
            } catch (MergeDataException me) {
                LOGGER.error("Error writing file + " + filePath + ". MergeDataException:" +  me.getMessage());
                me.printStackTrace();
                throw  me;
            } finally {
                if (writer != null && res.isEmpty()) { //if an exception is thrown - do not close writer so the merged file wont be written
                    try {
                        Instant startWriteOperationTime = Instant.now();
                        writer.close();

                        writeTimeMS += Duration.between(startWriteOperationTime, Instant.now()).toMillis();
                        if (config.isFillIOCountersData()) {
                            compactionIoOperationsCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "WRITE", config.getOutputStorageType()).inc();
                            compactionIoOperationsLatencyMSCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "WRITE", config.getOutputStorageType()).inc(writeTimeMS);
                        }
                        compactionIoOperationsLatencyMSCounterSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct(), "WRITE").observe(Duration.between(startWriteTime, Instant.now()).toMillis());
                    } catch (IOException e) {
                        LOGGER.error("Fail closing parquet file: " + e.getMessage(), e);
                        e.printStackTrace();
                    }

                }
            }
        }

        return res;
    }

    private void addFieldToGroup(EconomicalGroup destination, Group source, String fieldName, PrimitiveType.PrimitiveTypeName fieldType, PrimitiveType.PrimitiveTypeName configuredType) throws MergeDataException {
        if (source.getType().containsField(fieldName)) {
            if (((EconomicalGroup)source).getValue(source.getType().getFieldIndex(fieldName), 0)!=null) {
                switch (fieldType) {
                    case BINARY:
                        Binary bval = source.getBinary(fieldName, 0);
                        if (configuredType ==  null || configuredType.equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
                            destination.add(fieldName, bval);
                        }
                        else {
                            switch (configuredType) {
                                case INT32:
                                    destination.add(fieldName, Integer.valueOf(bval.toStringUsingUTF8()));
                                    break;
                                case INT64:
                                    destination.add(fieldName, Long.valueOf(bval.toStringUsingUTF8()));
                                    break;
                                case FLOAT:
                                    destination.add(fieldName, Float.valueOf(bval.toStringUsingUTF8()));
                                    break;
                                case DOUBLE:
                                    destination.add(fieldName, Double.valueOf(bval.toStringUsingUTF8()));
                                    break;
                                case BOOLEAN:
                                    destination.add(fieldName, Boolean.valueOf(bval.toStringUsingUTF8()));
                                    break;
                            }
                        }
                        break;
                    case INT32:
                        int ival = source.getInteger(fieldName, 0);
                        if (configuredType == null || configuredType.equals(PrimitiveType.PrimitiveTypeName.INT32)) {
                            destination.add(fieldName, ival);
                        }
                        else {
                            switch (configuredType) {
                                case BINARY:
                                    destination.add(fieldName, String.valueOf(ival));
                                    break;
                                case INT64:
                                    destination.add(fieldName, Long.valueOf(ival));
                                    break;
                                case FLOAT:
                                    destination.add(fieldName, Float.valueOf(ival));
                                    break;
                                case DOUBLE:
                                    destination.add(fieldName, Double.valueOf(ival));
                                    break;
                                case BOOLEAN:
                                    destination.add(fieldName, ival!=0?true:false);
                                    break;
                            }
                        }
                        break;
                    case INT64:
                        if (configuredType == null || configuredType.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                            try {
                                destination.add(fieldName, source.getLong(fieldName, 0));
                            } catch (ClassCastException c) {
                                //for cases that field is int in the input file but should be long
                                long lval = Long.valueOf(source.getInteger(fieldName, 0));
                                destination.add(fieldName, lval);
                            }
                        }
                        else {
                            long lval = source.getLong(fieldName, 0);
                            Long l = new Long(lval);
                            switch (configuredType) {
                                case BINARY:
                                    destination.add(fieldName, String.valueOf(lval));
                                    break;
                                case INT32:
                                    destination.add(fieldName, l.intValue());
                                    break;
                                case FLOAT:
                                    destination.add(fieldName, l.floatValue());
                                    break;
                                case DOUBLE:
                                    destination.add(fieldName, l.doubleValue());
                                    break;
                                case BOOLEAN:
                                    destination.add(fieldName, lval!=0L?true:false);
                                    break;
                            }
                        }
                        break;
                    case FLOAT:
                        float fval = source.getFloat(fieldName, 0);
                        if (configuredType == null || configuredType.equals(PrimitiveType.PrimitiveTypeName.FLOAT)) {
                            destination.add(fieldName, fval);
                        }
                        else{
                            Float f = new Float(fval);
                            switch (configuredType) {
                                case BINARY:
                                    destination.add(fieldName, String.valueOf(fval));
                                    break;
                                case INT64:
                                    destination.add(fieldName, f.longValue());
                                    break;
                                case INT32:
                                    destination.add(fieldName, f.intValue());
                                    break;
                                case DOUBLE:
                                    destination.add(fieldName,f.doubleValue());
                                    break;
                                case BOOLEAN:
                                    destination.add(fieldName, fval!=0?true:false);
                                    break;
                            }
                        }

                        break;
                    case DOUBLE:
                        double dval = source.getDouble(fieldName, 0);
                        if (configuredType == null || configuredType.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
                            destination.add(fieldName, dval);
                        }
                        else {
                            Double d = new Double(dval);
                            switch (configuredType) {
                                case BINARY:
                                    destination.add(fieldName, String.valueOf(dval));
                                    break;
                                case INT64:
                                    destination.add(fieldName, d.longValue());
                                    break;
                                case INT32:
                                    destination.add(fieldName, d.intValue());
                                    break;
                                case FLOAT:
                                    destination.add(fieldName,d.floatValue());
                                    break;
                                case BOOLEAN:
                                    destination.add(fieldName, dval!=0?true:false);
                                    break;
                            }
                        }
                        break;
                    case BOOLEAN:
                        Boolean boolval = source.getBoolean(fieldName, 0);
                        if (configuredType == null || configuredType.equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
                            destination.add(fieldName, boolval);
                        }
                        else {
                            switch (configuredType) {
                                case BINARY:
                                    destination.add(fieldName, String.valueOf(boolval));
                                    break;
                                case INT64:
                                    destination.add(fieldName, boolval?1L:0L);
                                    break;
                                case INT32:
                                    destination.add(fieldName, boolval?1:0);
                                    break;
                                case FLOAT:
                                    destination.add(fieldName, boolval?1:0);
                                    break;
                                case DOUBLE:
                                    destination.add(fieldName, boolval?1:0);
                                    break;
                            }
                        }
                        break;
                }
            }
        }
    }
}
