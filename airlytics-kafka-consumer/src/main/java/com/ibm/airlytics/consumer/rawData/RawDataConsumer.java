package com.ibm.airlytics.consumer.rawData;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.exceptions.FileCreationException;
import com.ibm.airlytics.consumer.persistence.FieldConfig;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.serialize.parquet.RowsLimitationParquetWriter;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.EventFieldsMaps;
import com.ibm.airlytics.utilities.Hashing;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.log4j.Logger;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.eco.EconomicalGroup;
import org.apache.parquet.hadoop.CustomParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class RawDataConsumer extends AirlyticsConsumer {
    private static final Logger LOGGER = Logger.getLogger(RawDataConsumer.class.getName());

    private enum DATA_SOURCE {
        ERROR,
        SUCCESS,
        PURCHASES,
        NOTIFICATIONS,
        DEDUP;

        public static DATA_SOURCE fromString (String sourceStr) {
            try {
                return DATA_SOURCE.valueOf(sourceStr);
            }
            catch (Exception e) {
                return null;
            }
        }
    }

    private static final SimpleDateFormat gFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private static final String EVENT_FIELD_NAME = "event";
    private static final String TIMESTAMP_FIELD_NAME = "receivedTime";
    private static final String OFFSET_FIELD_NAME = "offset";
    private static final String PARTITION_FIELD_NAME = "partition";
    private static final String SHARD_FIELD_NAME = "shard";
    private static final String DEVICE_TIME_FIELD_NAME = "deviceTime";
    private static final String DEVICE_TIME_HEADER_KEY = "x-current-device-time";
    private static final String ERROR_MESSAGE_HEADER_KEY = "error-message";
    private static final String ERROR_MESSAGE_FIELD_NAME = "errorMessage";
    private static final String ERROR_TYPE_HEADER_KEY = "error-type";
    private static final String ERROR_TYPE_FIELD_NAME = "errorType";
    public static final String ORIGINAL_EVENT_TIME_FIELD_NAME = "originalEventTime";
    public static final String ORIGINAL_EVENT_TIME_HEADER_KEY = "original-event-time";


    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_rawdata_records_processed_total")
            .help("Total records processed by the raw data consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //Number of write trials. Includes both fail and success writes.
    static final Counter filesWrittenCounter = Counter.build()
            .name("airlytics_rawdata_files_written_total")
            .help("Total files written by the raw data consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Summary filesSizeBytesSummary = Summary.build()
            .name("airlytics_rawdata_file_size_bytes")
            .help("File size in bytes.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    class S3ParquetWriteTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }

        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        int partition;
        String day;

        S3ParquetWriteTask(int partition, String day) {
            this.partition = partition;
            this.day=day;
        }

        public Result call() {
            Result res = new Result();
            int retryCounter = 0;

            //calculate minimum record offset in the partition&day
            Long minOffset = null;
            LinkedHashMap<Long, ConsumerRecord<String, JsonNode>> offestMap = eventsMap.get(partition).get(day);
            for (Long offset : offestMap.keySet()) { //sorted by offset so first offset is the minimal
                minOffset = offset;
                break;
            }

            String filePath = composeFilePath(partition, day, minOffset, INFORMATION_SOURCE.BASE);
            String piFilePath = composeFilePath(partition, day, minOffset, INFORMATION_SOURCE.PI);

            while (retryCounter < config.getWriteRetries()) {
                String writeResult = RESULT_SUCCESS;

                CustomParquetWriter<Group> baseWriter = null;
                CustomParquetWriter<Group> piWriter = null;

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

                    res.filePath = filePath;
                    res.error = null;

                    MessageType schema = parseMessageType(parquetSchema);
                    GroupWriteSupport.setSchema(schema, conf);

                    Set<Long> offsets = eventsMap.get(partition).get(day).keySet();
                    for (Long offset : offsets) { //sorted by offset
                        ConsumerRecord<String, JsonNode> record = eventsMap.get(partition).get(day).get(offset);
                        JsonNode event = record.value();

                        String recordResult = RESULT_SUCCESS;

                        try {
                            EconomicalGroup group = new EconomicalGroup(schema);
                            buildGroup(group, record, event);

                            //add the event json as string to the event field
                            group.add(EVENT_FIELD_NAME, event.toString());

                            //check if the field contains PI fields
                            boolean piEvent = isPiEvent(event);
                            if (piEvent) {
                                group.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.PI.toString());
                                if (piWriter == null) {
                                    piWriter = createWriter(piFilePath, conf);
                                }
                                piWriter.write(group);

                                if (baseWriter == null) {
                                    baseWriter = createWriter(filePath, conf);
                                }
                                EconomicalGroup nonPiGroup = new EconomicalGroup(schema);
                                buildGroup(nonPiGroup, record, event);

                                //add the clean event json as string to the event field
                                removePiData(event);
                                nonPiGroup.add(EVENT_FIELD_NAME, event.toString());
                                nonPiGroup.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.SANS_PI.toString());

                                baseWriter.write(nonPiGroup);
                            } else {
                                if (baseWriter == null) {
                                    baseWriter = createWriter(filePath, conf);
                                }
                                if (source.equals(DATA_SOURCE.SUCCESS) || source.equals(DATA_SOURCE.DEDUP) || source.equals(DATA_SOURCE.ERROR) ) {
                                    group.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.BASE.toString());
                                }
                                baseWriter.write(group);
                            }
                        } catch (FileCreationException fce) {
                            fce.printStackTrace();
                            throw fce; //IOException during file creation is not an event error but a global io error that requires retry
                        } catch (Exception e) {
                            e.printStackTrace();
                            LOGGER.error("error in event: " + event.toString() + ":\n" + e.getMessage(), e);
                            recordResult = RESULT_ERROR;
                        }

                        recordsProcessedCounter.labels(recordResult, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    }

                    filesWrittenCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

                    if (baseWriter!=null) {
                        filesSizeBytesSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(baseWriter.getDataSize());
                    }
                    if (piWriter!=null) {
                        filesSizeBytesSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(piWriter.getDataSize());
                    }

                    break; //retry only upon error.
                } catch (IOException e) {
                    e.printStackTrace();
                    //FileCreationException will be caught here
                    LOGGER.error("Fail writing file " + res.filePath + " to S3: " + e.getMessage(), e);
                    e.printStackTrace();
                    ++retryCounter;
                    res.error = e.getMessage();
                    writeResult = RESULT_ERROR;
                } finally {
                    filesWrittenCounter.labels(writeResult, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    if (baseWriter != null) {
                        try {
                            baseWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            res.error = "Fail closing parquet file " + res.filePath+": " + e.getMessage();
                            LOGGER.error(res.error);
                            return res;
                        }
                    }

                    if (piWriter != null) {
                        try {
                            piWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            res.error = "Fail closing PI parquet file " + piFilePath + ": " + e.getMessage();
                            LOGGER.error(res.error);
                            return res;
                        }
                    }

                }
            }

            if (res.error != null) {
                return res;
            }

            try {
                writeScoreFile(partition, day);
            } catch (Exception e) {
                e.printStackTrace();
                res.error = "Fail writing score file to S3 for partition " + partition + " for day " + day + " : " + e.getMessage();
                LOGGER.error(res.error);
                return res;
            }

            return res;
        }

        private CustomParquetWriter<Group> createWriter(String filePath, Configuration conf) throws FileCreationException {
            try {
                return RowsLimitationParquetWriter.builder(new Path(filePath))
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withRowGroupSize(config.getParquetRowGroupSize())
                        .withPageSize(config.getParquetRowGroupSize())
                        .withDictionaryEncoding(true)
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        .withConf(conf)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build();
            } catch (IOException e) {
                e.printStackTrace();
                throw new FileCreationException("Fail craeting file " + filePath, e);
            }
        }

        private boolean isPiEvent(JsonNode event) {
            if (source.equals(DATA_SOURCE.PURCHASES) || source.equals(DATA_SOURCE.NOTIFICATIONS)) {
                return false; //these data sources does not contain pi data
            }

            if (event.get("name") == null) {
                return false;
            }


            String eventName = event.get("name").textValue();
            //if tpiMarker exists and the value is false. The event is not pi
            if (configuredEventsMaps.getEventsWithPIMarker().containsKey(eventName)) {
                String piMarkerFieldName = configuredEventsMaps.getEventsWithPIMarker().get(eventName);
                if (event.get(EVENT_ATTRIBUTES_FIELD).get(piMarkerFieldName)!=null && event.get(EVENT_ATTRIBUTES_FIELD).get(piMarkerFieldName).booleanValue()==false) {
                    return false;
                }
            }

            if (eventsPiFieldsMap.containsKey(eventName)) {
                String eventVersion = EventFieldsMaps.getEventSchemaVersion(event);
                JsonNode eventAttributes = event.findValue(EVENT_ATTRIBUTES_FIELD);
                if (eventAttributes!=null) {
                    Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
                    while(eventFieldsIter.hasNext()) {
                        //look for the field in the specific schema_version and if is not there look in the default_version
                        String eventFieldName = eventFieldsIter.next();
                        if (eventVersion!=null && !eventVersion.isEmpty()){
                             Set<String> versionFields = eventsPiFieldsMap.get(eventName).get(eventVersion);
                             if (versionFields!=null && versionFields.contains(eventFieldName)) {
                                 return true;
                             }
                        }
                        Set<String> defVersionFields = eventsPiFieldsMap.get(eventName).get(DEFAULT_VERSION);
                        if (defVersionFields!=null && defVersionFields.contains(eventFieldName)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private void removePiData(JsonNode event) {
            String eventName = event.get("name").textValue();
            String eventVersion = EventFieldsMaps.getEventSchemaVersion(event);
            if (eventsPiFieldsMap.containsKey(eventName)) {
                JsonNode eventAttributes = event.findValue(EVENT_ATTRIBUTES_FIELD);
                if (eventAttributes!=null) {
                    Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
                    ArrayList<String> eventFields = new ArrayList<>();
                    while(eventFieldsIter.hasNext()) {
                        eventFields.add(eventFieldsIter.next());
                    }

                    for (String eventFieldName : eventFields) {
                        if (eventsPiFieldsMap.get(eventName).get(eventVersion)!=null && eventsPiFieldsMap.get(eventName).get(eventVersion).contains(eventFieldName)) {
                            ((ObjectNode)event.findValue(EVENT_ATTRIBUTES_FIELD)).remove(eventFieldName);
                        }
                        else {
                            if (eventsPiFieldsMap.get(eventName).get(DEFAULT_VERSION)!=null && eventsPiFieldsMap.get(eventName).get(DEFAULT_VERSION).contains(eventFieldName)) {
                                ((ObjectNode)event.findValue(EVENT_ATTRIBUTES_FIELD)).remove(eventFieldName);
                            }
                        }
                    }


                    if (eventName.equals("user-attributes") && event.get(EVENT_PREVIOUS_VALUES_FIELD)!=null) {
                        JsonNode previousValues = event.findValue(EVENT_PREVIOUS_VALUES_FIELD);

                        Iterator<String> previousValuesFieldsIter = previousValues.fieldNames();
                        ArrayList<String> previousValuesFields = new ArrayList<>();
                        while(previousValuesFieldsIter.hasNext()) {
                            previousValuesFields.add(previousValuesFieldsIter.next());
                        }

                        for (String previousValuesFieldName : previousValuesFields) {
                            if (eventsPiFieldsMap.get(eventName).get(eventVersion)!=null && eventsPiFieldsMap.get(eventName).get(eventVersion).contains(previousValuesFieldName)) {
                                ((ObjectNode)event.findValue(EVENT_PREVIOUS_VALUES_FIELD)).remove(previousValuesFieldName);
                            }
                            else {
                                if (eventsPiFieldsMap.get(eventName).get(DEFAULT_VERSION)!=null && eventsPiFieldsMap.get(eventName).get(DEFAULT_VERSION).contains(previousValuesFieldName)) {
                                    ((ObjectNode)event.findValue(EVENT_PREVIOUS_VALUES_FIELD)).remove(previousValuesFieldName);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void buildGroup(EconomicalGroup group, ConsumerRecord<String, JsonNode> record, JsonNode event) {
            Set<String> commonFieldNames = config.getCommonFieldTypesMap().keySet();
            for (String commonFieldName : commonFieldNames) {
                if (commonFieldName.equals(EVENT_PREVIOUS_VALUES_FIELD)) {
                    continue; //the previousValues field is not written as a seperate column in the rawdata files
                }
                String parquetFieldName = config.getCommonFieldTypesMap().get(commonFieldName).getParquetField(); //set when building the schema
                addValue(group, parquetFieldName, commonFieldName, event, config.getCommonFieldTypesMap().get(commonFieldName).getType());
            }

            //add the event timestamp
            group.add(TIMESTAMP_FIELD_NAME,  record.timestamp());

            //add the event offset
            group.add(OFFSET_FIELD_NAME,  record.offset());

            //add the event partition
            group.add(PARTITION_FIELD_NAME,  record.partition());


           //add deviceTime taken from header
           if (record.headers()!=null && record.headers().toArray().length > 0) {
                Header header = record.headers().lastHeader(DEVICE_TIME_HEADER_KEY);
                if (header != null && (source.equals(DATA_SOURCE.SUCCESS) || source.equals(DATA_SOURCE.DEDUP) || source.equals(DATA_SOURCE.ERROR))) {
                    try {
                        long l = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
                        group.add(DEVICE_TIME_FIELD_NAME, l);
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOGGER.error("Error converting byte[] to long. Ignoring : " + e.getMessage());
                    }
                }

               header = record.headers().lastHeader(ORIGINAL_EVENT_TIME_HEADER_KEY);
               if (header != null && (source.equals(DATA_SOURCE.SUCCESS) || source.equals(DATA_SOURCE.DEDUP) || source.equals(DATA_SOURCE.ERROR))) {
                   try {
                       long l = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
                       group.add(ORIGINAL_EVENT_TIME_FIELD_NAME, l);
                   } catch (Exception e) {
                       e.printStackTrace();
                       LOGGER.error("Error converting byte[] to long. Ignoring : " + e.getMessage());
                   }
               }

               if (source.equals(DATA_SOURCE.ERROR)) {
                   header = record.headers().lastHeader(ERROR_TYPE_HEADER_KEY);
                   if (header != null) {
                       try {
                           String et = new String(header.value(), StandardCharsets.UTF_8);
                           group.add(ERROR_TYPE_FIELD_NAME, et);
                       } catch (Exception e) {
                           e.printStackTrace();
                           LOGGER.error("Error setting error type. Ignoring : " + e.getMessage());
                       }
                   }

                   header = record.headers().lastHeader(ERROR_MESSAGE_HEADER_KEY);
                   if (header != null) {
                       try {
                           String et = new String(header.value(), StandardCharsets.UTF_8);
                           group.add(ERROR_MESSAGE_FIELD_NAME, et);
                       } catch (Exception e) {
                           e.printStackTrace();
                           LOGGER.error("Error setting error message. Ignoring : " + e.getMessage());
                       }
                   }
               }
            }

            if (source.equals(DATA_SOURCE.SUCCESS) || source.equals(DATA_SOURCE.DEDUP) || source.equals(DATA_SOURCE.ERROR) ) {
                //add the event shard
                JsonNode userId = event.get("userId");
                group.add(SHARD_FIELD_NAME, (userId == null || userId.textValue().isEmpty()) ? -1 : hashing.hashMurmur2(userId.textValue()));
            }
        }
        
        private void writeScoreFile(int partition, String day) throws IOException, ParseException {
            String folderPath = s3BaseFolder +
                    "partition=" + partition + File.separator +
                    "day=" + day + File.separator;
            double sizeM = baseDataSerializer.getFolderSizeM(folderPath, PARQUET_MERGED_FILE_NAME, PARQUET_FILE_EXTENSION);
            double weight = sizeM;


            //delete previous score file if exists
            LinkedList<String> scoreFiles = baseDataSerializer.listFilesInFolder(folderPath,  SCORE_FILE_PREFIX, false);

            for (String scoreFile:scoreFiles) {
                baseDataSerializer.deleteFile(folderPath+scoreFile);
            }

            String scoreFile = folderPath + SCORE_FILE_PREFIX + weight;
            baseDataSerializer.writeData(scoreFile , UUID.randomUUID().toString());
        }

        private void addValue(Group group, String parquetFieldName, String recordFieldName, JsonNode source, FieldConfig.FIELD_TYPE type) throws IllegalArgumentException{
            if (source.get(recordFieldName) != null && !source.get(recordFieldName).isNull()) {
                try {
                    switch (type) {
                        case STRING:
                            group.add(parquetFieldName, source.get(recordFieldName).textValue());
                            break;
                        case INTEGER:
                            group.add(parquetFieldName, source.get(recordFieldName).intValue());
                            break;
                        case LONG:
                            group.add(parquetFieldName, source.get(recordFieldName).longValue());
                            break;
                        case FLOAT:
                            group.add(parquetFieldName, source.get(recordFieldName).floatValue());
                            break;
                        case DOUBLE:
                            group.add(parquetFieldName, source.get(recordFieldName).doubleValue());
                            break;
                        case BOOLEAN:
                            group.add(parquetFieldName, source.get(recordFieldName).booleanValue());
                            break;
                        case JSON_STRING:
                            group.add(parquetFieldName, source.get(recordFieldName).toString());
                            break;
                    }
                } catch (NullPointerException e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException("recordFieldName: " + recordFieldName + " is not in the expected type. There is a mismatch between the configured schema and event record.");
                }
            }
        }
    }

    private Hashing hashing;
    private RawDataConsumerConfig config;
    private RawDataConsumerConfig newConfig;
    private String awsSecret;
    private String awsKey;
    private String s3BaseFilePrefix;
    private String s3BaseFolder;
    private String s3PiFilePrefix;
    private String s3PiFolder;
    private long totalRecords = 0;
    private long recordCounter = 0;
    private Instant dumpIntervalStart;
    private String parquetSchema;
    private String writeErrorMsg = "";
    private DataSerializer baseDataSerializer;
    private DATA_SOURCE source; //can be ERROR, SUCCESS, PURCHASES, NOTIFICATIONS

    private ThreadPoolExecutor executor;  //parallelize partitions and days writing to S3

    //Map <partition -> map <day->map<offset -> event>>>
    //ConcurrentHashMap for the partitions and days map since are accessed during file writing from multiple threads
    //LinkedHashMap for the offset map since the offset order is the sort order
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, LinkedHashMap<Long, ConsumerRecord<String, JsonNode>>>> eventsMap = new ConcurrentHashMap<>();

    private EventFieldsMaps configuredEventsMaps = new EventFieldsMaps();

    //map <eventname->Map<version, Set<PiFieldNames>>
    private HashMap<String, HashMap<String, HashSet<String>>> eventsPiFieldsMap = new HashMap<>();

    public RawDataConsumer(RawDataConsumerConfig config) {
        super(config);
        this.config = config;

        String sourceStr = Environment.getAlphanumericEnv(AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM, false);
        source = DATA_SOURCE.fromString(sourceStr);
        if (source == null) {
            throw new IllegalArgumentException("Illegal value for " + AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM + " environment parameter. Can be eitehr ERROR, SUCCESS, PURCHASES or NOTIFICATIONS.");
        }

        //S3 configuration
        this.awsSecret = Environment.getEnv(AWS_ACCESS_SECRET_PARAM, true);
        this.awsKey = Environment.getEnv(AWS_ACCESS_KEY_PARAM, true);

        this.s3BaseFolder = config.getS3RootFolder() + File.separator + config.getTopic() + File.separator;
        this.s3BaseFilePrefix = config.getS3Bucket() + File.separator + this.s3BaseFolder;
        this.s3PiFolder = config.getS3PiRootFolder() + File.separator + config.getTopic() + File.separator;
        this.s3PiFilePrefix = config.getS3PiBucket() + File.separator + this.s3PiFolder;

        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getWriteThreads());
        dumpIntervalStart = Instant.now();

        configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());
        parquetSchema = buildParquetSchema(config);

        buildEventsPiFieldsMap();

        this.baseDataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getWriteRetries());

        this.hashing = new Hashing(config.getShardsNumber());

        LOGGER.info("parquetSchema = \n" + parquetSchema);
        LOGGER.info("rawdata consumer created with configuration:\n" + config.toString());
    }

    private String buildParquetSchema(RawDataConsumerConfig configuration) {
        Set<String> parquetFields = new HashSet<>(); //to avoid fields duplication since space=>underscore
        StringBuilder sb = new StringBuilder();
        sb.append("message event_record {\n" );
        //add common fields
        Set<String> commonFieldNames = configuration.getCommonFieldTypesMap().keySet();
        for (String commonFieldName : commonFieldNames) {
            addFieldToSchema(commonFieldName, configuration.getCommonFieldTypesMap().get(commonFieldName), sb, parquetFields);
        }

        //add event field that contains the event json as string
        sb.append("required binary " + EVENT_FIELD_NAME + " (UTF8);\n");

        //add timestamp field that contains the time the event was processed by the proxy
        sb.append("optional int64 " + TIMESTAMP_FIELD_NAME + ";\n");

        //add offset field that contains the event offset in kafka partition
        sb.append("required int64 " + OFFSET_FIELD_NAME + ";\n");

        //add partition field that contains the event partition in kafka partition
        sb.append("required int32 " + PARTITION_FIELD_NAME + ";\n");

        if (source.equals(DATA_SOURCE.SUCCESS) || source.equals(DATA_SOURCE.DEDUP) || source.equals(DATA_SOURCE.ERROR) ) {
            //add timestamp field that contains the time the event was processed by the proxy
            sb.append("optional int64 " + DEVICE_TIME_FIELD_NAME + ";\n");

            //add timestamp field that contains the time the event was processed by the proxy
            sb.append("optional int64 " + ORIGINAL_EVENT_TIME_FIELD_NAME + ";\n");

            //add shard field that contains the event's shard (calculated by userId)
            sb.append("required int32 " + SHARD_FIELD_NAME + ";\n");

            //add event field that contains the source of the event: BASE,PI or SANS_PI
            sb.append("required binary " + SOURCE_FIELD_NAME + " (UTF8);\n");
        }

        if (source.equals(DATA_SOURCE.ERROR)){
            //add event field that contains the error  type
            sb.append("optional binary " + ERROR_TYPE_FIELD_NAME + " (UTF8);\n");

            //add event field that contains the error message
            sb.append("optional binary " + ERROR_MESSAGE_FIELD_NAME + " (UTF8);\n");
        }

        sb.append("}\n");
        return sb.toString();
    }

    private String concatEventField(String eventName, String eventFieldName) {
        return eventName + "." + eventFieldName;
    }

    private String getParquetFieldName (String fieldName, FieldConfig fieldConfig) {
        String parquetFieldName = fieldName;
        if (fieldConfig.getParquetField()!=null && !fieldConfig.getParquetField().isEmpty()) {
            parquetFieldName = fieldConfig.getParquetField();
        }
        //spaces are illegal in parquet field name - replace with underscores
        return parquetFieldName.replace(" ", "_");
    }

    private void addFieldToSchema(String fieldName, FieldConfig fieldConfig, StringBuilder sb, Set<String> parquetFields) {
        String parquetFieldName = getParquetFieldName(fieldName, fieldConfig);
        fieldConfig.setParquetField(parquetFieldName);

        if (parquetFields.contains(parquetFieldName)) {
            return;
        }

        parquetFields.add(parquetFieldName);

        //all common fields are optional in the raw data consumer
        String requiredStr = " optional";

        switch (fieldConfig.getType()) {
            case STRING:
            case JSON_STRING:
                sb.append(requiredStr + " binary "+ parquetFieldName + " (UTF8);\n");
                break;
            case INTEGER:
                sb.append(requiredStr + " int32 "+ parquetFieldName + ";\n");
                break;
            case LONG:
                sb.append(requiredStr + " int64 "+ parquetFieldName + ";\n");
                break;
            case FLOAT:
                sb.append(requiredStr + " float "+ parquetFieldName + ";\n");
                break;
            case DOUBLE:
                sb.append(requiredStr + " double "+ parquetFieldName + ";\n");
                break;
            case BOOLEAN:
                sb.append(requiredStr + " boolean "+ parquetFieldName + ";\n");
                break;
        }
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {
        updateSchemaUsingNewConfigIfExists();

        int recordsProcessed = 0;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            addRecordToMap(record);
            ++recordCounter;
            ++totalRecords;
            ++recordsProcessed;
        }

        if (recordCounter >= config.getMaxDumpRecords() || (Duration.between(dumpIntervalStart, Instant.now()).toMillis()>config.getMaxDumpIntervalMs())) {
            if (!dumpMapToParquetFile()) {
                LOGGER.error("Stopping server due to S3 write errors.");
                stop();
                return recordsProcessed;
            }

            commit();
            resetEventsMap();

            LOGGER.info("commit & reset map. Total number of records = " + totalRecords);
            long duration = Duration.between(dumpIntervalStart, Instant.now()).toMillis();
            LOGGER.info("dump " + recordCounter + " records took  " + duration + " millisecond. " + (recordCounter/(duration/1000)) + " rec/sec");
            dumpIntervalStart = Instant.now();
            recordCounter = 0;
        }

        return recordsProcessed;
    }

    @Override
    public void newConfigurationAvailable() {
        RawDataConsumerConfig config = new RawDataConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error init Airlock config: " + e.getMessage(), e);
        }
    }

    // Note that this will ONLY update the parquet schema. For other config changes to take effect - restart the consumer
    private synchronized void updateSchemaUsingNewConfigIfExists() {
        if (newConfig != null) {
            parquetSchema = buildParquetSchema(newConfig);
            config.setCommonFieldTypesMap(newConfig.getCommonFieldTypesMap());
            config.setEventsFieldTypesMap(newConfig.getEventsFieldTypesMap());

            //build versioned fields map from the new configuration
            configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());
            buildEventsPiFieldsMap();

            LOGGER.info("updateSchemaUsingNewConfigIfExists, parquetSchema = \n" + parquetSchema);
            newConfig = null;
        }
    }

    private void buildEventsPiFieldsMap() {
        LOGGER.info("in buildEventsPiFieldsMap");
        eventsPiFieldsMap.clear();
        Set<String> eventNames = configuredEventsMaps.getConfiguredEventsFieldConfigs().keySet();
        for (String eventName : eventNames) {
            HashMap<String, HashMap<String, FieldConfig>> eventFieldsMap = configuredEventsMaps.getConfiguredEventsFieldConfigs().get(eventName);
            if (eventFieldsMap != null) {
                Set<String> eventFieldNames = eventFieldsMap.keySet();
                for (String eventFieldName : eventFieldNames) {
                    HashMap<String, FieldConfig> fieldVersions = eventFieldsMap.get(eventFieldName);
                    Set<String> versions = fieldVersions.keySet();
                    for (String version : versions) {
                        FieldConfig fieldConfig = fieldVersions.get(version);
                        if (fieldConfig.isPersonalInformation()) {
                            //add the field to the eventsPiFieldsMap map
                            HashMap<String, HashSet<String>> eventPiFields = eventsPiFieldsMap.get(eventName);
                            if (eventPiFields == null) {
                                eventPiFields = new HashMap<String, HashSet<String>>();
                                eventsPiFieldsMap.put(eventName, eventPiFields);
                            }

                            HashSet<String> versionPiFields = eventPiFields.get(version);
                            if (versionPiFields == null) {
                                versionPiFields = new HashSet<String>();
                                eventPiFields.put(version, versionPiFields);
                            }

                            versionPiFields.add(eventFieldName);
                            LOGGER.info("Field " + eventPiFields + " of version " + version + " in event " + eventName + " is PII");
                        }
                    }
                }
            }
        }
    }

    private void resetEventsMap() {
        eventsMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, LinkedHashMap<Long, ConsumerRecord<String, JsonNode>>>>();
    }

    //dump map to parquet files in s3
    //return true is all succeeded - false is there are errors
    private boolean dumpMapToParquetFile() {
        LOGGER.info("**** in dump to file ****");
        ArrayList<Future<S3ParquetWriteTask.Result>> writings = new ArrayList<Future<S3ParquetWriteTask.Result>>();
        Set<Integer> partitions = eventsMap.keySet();
        for (int partition:partitions) {
            Set<String> days = eventsMap.get(partition).keySet();
            for (String day:days) {
                S3ParquetWriteTask w = new S3ParquetWriteTask(partition, day);
                Future<S3ParquetWriteTask.Result> future = executor.submit(w);
                writings.add(future);
            }
        }

        LOGGER.info("writing " + writings.size() + " files.");
        ArrayList<String> errors = new ArrayList<>();
        for (Future<S3ParquetWriteTask.Result> item : writings)
        {
            try {
                S3ParquetWriteTask.Result result = item.get();
                if (result.error != null) {
                    errors.add(result.filePath + ": " + result.error);
                    writeErrorMsg = "Fail writing parquet file '" + result.filePath + "' to S3: " + result.error;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                errors.add(e.toString());
            }
        }

        if (!errors.isEmpty()) {
            LOGGER.error("errors during writing: " + errors.toString());
            return false;
        }

        return true;
    }

    private String composeFilePath(int partition, String day, long firstRecordOffset, INFORMATION_SOURCE piType) {
        StringBuilder sb = new StringBuilder("s3a://");

        switch (piType) {
            case BASE:
            case SANS_PI:
                sb.append(s3BaseFilePrefix);
                break;
            case PI:
                sb.append(s3PiFilePrefix);
                break;
        }
        sb.append("partition=" + partition + File.separator);
        sb.append("day=" + day + File.separator);
        sb.append(partition + "_" + day + "_" + firstRecordOffset + PARQUET_FILE_EXTENSION);

        return sb.toString();
    }

    private synchronized void addRecordToMap(ConsumerRecord<String, JsonNode> record) {
        long recordOffset = record.offset();
        long recordTimestamp = record.timestamp();
        int partition = record.partition();

        Date eventProcessDate = new Date(recordTimestamp);
        String eventDateStr = gFormatter.format(eventProcessDate);

        ConcurrentHashMap<String, LinkedHashMap<Long, ConsumerRecord<String, JsonNode>>> partitionsMap = eventsMap.get(partition);
        if (partitionsMap == null) {
            partitionsMap = new ConcurrentHashMap<String, LinkedHashMap<Long, ConsumerRecord<String, JsonNode>>>();
            eventsMap.put(partition, partitionsMap);
        }

        LinkedHashMap<Long, ConsumerRecord<String, JsonNode>> dayMap = partitionsMap.get(eventDateStr);
        if (dayMap == null) {
            dayMap = new LinkedHashMap<Long, ConsumerRecord<String, JsonNode>>();
            partitionsMap.put(eventDateStr, dayMap);
        }

        dayMap.put(recordOffset, record);
    }

    private String healthMessage = "";

    @Override
    public boolean isHealthy() {
        if (!super.isHealthy())
            return false;

        if (!writeErrorMsg.isEmpty()) {
            healthMessage = writeErrorMsg;
            return false;
        }
        healthMessage = "Healthy!";
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

}
