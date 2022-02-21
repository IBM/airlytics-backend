package com.ibm.airlytics.consumer.persistence;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.exceptions.FileCreationException;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.serialize.parquet.RowsLimitationParquetWriter;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.EventFieldsMaps;
import com.ibm.airlytics.utilities.Hashing;
import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.eco.EconomicalGroup;
import org.apache.parquet.hadoop.CustomParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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

public class PersistenceConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(PersistenceConsumer.class.getName());

    private static final SimpleDateFormat gFormatter = new SimpleDateFormat("yyyy-MM-dd");

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_persistence_records_processed_total")
            .help("Total records processed by the persistence consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //Number of write trials. Includes both fail and success writes.
    static final Counter filesWrittenCounter = Counter.build()
            .name("airlytics_persistence_files_written_total")
            .help("Total files written by the persistence consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    static final Summary filesSizeBytesSummary = Summary.build()
            .name("airlytics_persistence_file_size_bytes")
            .help("File size in bytes.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    class S3ParquetWriteTask implements Callable {
        class Result {
            String error = null;
            String filePath;
        }

        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        int shard;
        String day;

        S3ParquetWriteTask(int shard, String day) {
            this.shard = shard;
            this.day=day;
        }

        public Result call() {
            Result res = new Result();
            int retryCounter = 0;
            Long minOffset = null;

            //calculate minimum record offset in the shard&day
            LinkedHashMap<String, ArrayList<JsonNode>> usersMap = eventsMap.get(shard).get(day);
            for (Map.Entry<String, ArrayList<JsonNode>> userMap : usersMap.entrySet()) {
                List<JsonNode> events = userMap.getValue();
                for (JsonNode event : events) {
                    long curOffset = event.get(OFFSET_FIELD_NAME).asLong();
                    if (minOffset == null || minOffset>curOffset)
                        minOffset = curOffset;
                }
            }

            //Instant writeIntervalStart = Instant.now();
            CustomParquetWriter<Group> baseWriter = null;
            CustomParquetWriter<Group> piWriter = null;

            String filePath = composeFilePath(shard, day, minOffset, INFORMATION_SOURCE.BASE);
            String piFilePath = composeFilePath(shard, day, minOffset, INFORMATION_SOURCE.PI);

            LOGGER.info("about writing file: " + filePath + ", pi = " + piFilePath);

            double addedFilesWeightB = 0;
            //int fileRecordCounter = 0;
            while (retryCounter < config.getWriteRetries()) {
                String writeResult = RESULT_SUCCESS;
                //fileRecordCounter = 0;

                try {
                    Configuration conf = new Configuration();

                    if (s3Storage) {
                        //s3
                        if (awsKey != null) {
                            conf.set("fs.s3a.access.key", awsKey);
                        }
                        if (awsSecret != null) {
                            conf.set("fs.s3a.secret.key", awsSecret);
                        }
                        conf.set("fs.s3a.impl", org.apache.hadoop.fs.s3a.S3AFileSystem.class.getName());
                        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                    }
                    else {
                        conf.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
                    }

                    res.filePath = filePath;
                    res.error = null;

                    MessageType schema = parseMessageType(parquetSchema);
                    GroupWriteSupport.setSchema(schema, conf);

                    Set<String> users = eventsMap.get(shard).get(day).keySet();
                    for (String user : users) {
                        List<JsonNode> events = eventsMap.get(shard).get(day).get(user);
                        for (JsonNode event : events) {
                            String recordResult = RESULT_SUCCESS;

                            try {
                                ArrayList<String> intentialNullValueFields  = new ArrayList<>();

                                EconomicalGroup group = new EconomicalGroup(schema);
                                boolean configuredCommonFieldsContainsPI = addConfiguredCommonFieldsToGroup(group, event, configuredEventsMaps.getConfiguredCommonFieldConfigs(), intentialNullValueFields, configuredEventsMaps.getConfiguredEventsFieldConfigs(), true);
                                boolean configuredEventsFieldsContainsPI = addConfiguredEventsFieldsToGroup(group, event, configuredEventsMaps.getConfiguredEventsFieldConfigs(), intentialNullValueFields, true);
                                boolean configuredCustomDimensionsFieldsContainsPI = addConfiguredCustomDimensionsFieldsToGroup(group, event, configuredEventsMaps.getConfiguredCustomDimensionsFieldConfigs(), intentialNullValueFields, true);

                                //auto detected fields
                                boolean detectedCommonFieldsContainsPI = false;
                                boolean detectedEventsFieldsContainsPI = false;
                                boolean detectedCustomDimensionsFieldsContainsPI = false;
                                if (config.isAutomaticallyAddNewFields()) {
                                    detectedCommonFieldsContainsPI = addDetectedCommonFieldsToGroup(group, event, detectedNewCommonFieldConfigs, intentialNullValueFields, true);
                                    detectedEventsFieldsContainsPI = addDetectedEventsFieldsToGroup(group, event, detectedNewEventsFieldConfigs, intentialNullValueFields, true);
                                    detectedCustomDimensionsFieldsContainsPI = addDetectedCustomDimensionsFieldsToGroup(group, event, detectedNewCustomDimensionsFieldConfigs, intentialNullValueFields, true);
                                }

                                if(intentialNullValueFields.size()>0) {
                                    group.add(NULL_VALUE_FIELDS_FIELD_NAME, arrayToString(intentialNullValueFields));
                                }
                                group.add(WRITE_TIME_FIELD_NAME, System.currentTimeMillis());
                                group.add(TIMESTAMP_FIELD_NAME, event.get(TIMESTAMP_FIELD_NAME).longValue());
                                if (event.get(DEVICE_TIME_FIELD_NAME)!=null) {
                                    group.add(DEVICE_TIME_FIELD_NAME, event.get(DEVICE_TIME_FIELD_NAME).longValue());
                                }

                                if (event.get(ORIGINAL_EVENT_TIME_FIELD_NAME)!=null) {
                                    group.add(ORIGINAL_EVENT_TIME_FIELD_NAME, event.get(ORIGINAL_EVENT_TIME_FIELD_NAME).longValue());
                                }


                                if (!isNotPiByMarker(event) &&
                                        (configuredCommonFieldsContainsPI||configuredEventsFieldsContainsPI||
                                        configuredCustomDimensionsFieldsContainsPI||detectedCommonFieldsContainsPI||
                                        detectedEventsFieldsContainsPI||detectedCustomDimensionsFieldsContainsPI)) {
                                    group.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.PI.toString());

                                    //this group contains PI
                                    if (piWriter == null) {
                                        piWriter = createWriter(piFilePath, conf);
                                    }
                                    piWriter.write(group);

                                    //write the event without the pi to the base folder
                                    if (baseWriter == null) {
                                        baseWriter = createWriter(filePath, conf);
                                    }

                                    intentialNullValueFields  = new ArrayList<>();
                                    EconomicalGroup nonPiGroup = new EconomicalGroup(schema);

                                    addConfiguredCommonFieldsToGroup(nonPiGroup, event, configuredEventsMaps.getConfiguredCommonFieldConfigs(), intentialNullValueFields, configuredEventsMaps.getConfiguredEventsFieldConfigs(), false);
                                    addConfiguredEventsFieldsToGroup(nonPiGroup, event, configuredEventsMaps.getConfiguredEventsFieldConfigs(), intentialNullValueFields, false);
                                    addConfiguredCustomDimensionsFieldsToGroup(nonPiGroup, event, configuredEventsMaps.getConfiguredCustomDimensionsFieldConfigs(), intentialNullValueFields, false);

                                    //auto detected fields
                                    if (config.isAutomaticallyAddNewFields()) {
                                        addDetectedCommonFieldsToGroup(nonPiGroup, event, detectedNewCommonFieldConfigs, intentialNullValueFields, false);
                                        addDetectedEventsFieldsToGroup(nonPiGroup, event, detectedNewEventsFieldConfigs, intentialNullValueFields, false);
                                        addDetectedCustomDimensionsFieldsToGroup(nonPiGroup, event, detectedNewCustomDimensionsFieldConfigs, intentialNullValueFields, false);
                                    }

                                    if(intentialNullValueFields.size()>0) {
                                        nonPiGroup.add(NULL_VALUE_FIELDS_FIELD_NAME, arrayToString(intentialNullValueFields));
                                    }
                                    nonPiGroup.add(WRITE_TIME_FIELD_NAME, System.currentTimeMillis());
                                    nonPiGroup.add(TIMESTAMP_FIELD_NAME, event.get(TIMESTAMP_FIELD_NAME).longValue());
                                    if (event.get(DEVICE_TIME_FIELD_NAME)!=null) {
                                        nonPiGroup.add(DEVICE_TIME_FIELD_NAME, event.get(DEVICE_TIME_FIELD_NAME).longValue());
                                    }
                                    if (event.get(ORIGINAL_EVENT_TIME_FIELD_NAME)!=null) {
                                        nonPiGroup.add(ORIGINAL_EVENT_TIME_FIELD_NAME, event.get(ORIGINAL_EVENT_TIME_FIELD_NAME).longValue());
                                    }

                                    nonPiGroup.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.SANS_PI.toString());
                                    baseWriter.write(nonPiGroup);
                                }
                                else {
                                    if (baseWriter == null) {
                                        baseWriter = createWriter(filePath, conf);
                                    }
                                    group.add(SOURCE_FIELD_NAME, INFORMATION_SOURCE.BASE.toString());
                                    baseWriter.write(group);
                                }
                               // fileRecordCounter++;
                            } catch (FileCreationException fce) {
                                throw fce; //IOException during file creation is not an event error but a global io error that requires retry
                            } catch (Exception e) {
                                LOGGER.error("error in event: " + event.toString() + ":\n" + e.getMessage(), e);
                                e.printStackTrace();
                                long receivedTimestamp = event.get(TIMESTAMP_FIELD_NAME).longValue();

                                ((ObjectNode)event).remove(TIMESTAMP_FIELD_NAME);
                                ((ObjectNode)event).remove(OFFSET_FIELD_NAME);
                                ((ObjectNode)event).remove(DEVICE_TIME_FIELD_NAME);
                                ((ObjectNode)event).remove(ORIGINAL_EVENT_TIME_FIELD_NAME);

                                errorsProducer.sendRecord(user, event, receivedTimestamp);
                                recordResult = RESULT_ERROR;
                            }

                            recordsProcessedCounter.labels(recordResult, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                        }
                    }

                    filesWrittenCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    if (baseWriter!=null) {
                        filesSizeBytesSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(baseWriter.getDataSize());
                    }
                    if (piWriter!=null) {
                        filesSizeBytesSummary.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(piWriter.getDataSize());
                    }

                    //System.out.println("@@@ writing file : " + filePath);
                    break; //retry only upon error.
                } catch (IOException e) {
                    LOGGER.error("(" + retryCounter + "), Fail writing file to S3: " + e.getMessage(), e);
                    e.printStackTrace();
                    ++retryCounter;
                    res.error = e.getMessage();
                    writeResult = RESULT_ERROR;
                } finally {
                    filesWrittenCounter.labels(writeResult, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

                    if (baseWriter != null) {
                        try {
                            baseWriter.close();
                            LOGGER.info("closing file: " + filePath);
                            double fileSize = dataSerializer.getFileSizeB(filePath, true);
                            addedFilesWeightB += fileSize;

                        } catch (IOException e) {
                            res.error = "Fail closing parquet file " + res.filePath+": " + e.getMessage();
                            LOGGER.error(res.error);
                            return res;
                        }
                    }

                    if (piWriter != null) {
                        try {
                            piWriter.close();
                            LOGGER.info("closing pi file: " + piFilePath);
                            double fileSize = dataSerializer.getFileSizeB(piFilePath, true);
                            addedFilesWeightB += fileSize;
                        } catch (IOException e) {
                            res.error = "Fail closing PI parquet file " + piFilePath + ": " + e.getMessage();
                            LOGGER.error(res.error);
                            return res;
                        }
                    }

                    if (config.getReadAfterWrite()) {
                        if (baseWriter != null) {
                            res.error = validateDataPropriety(filePath);
                            if (res.error != null) {
                                LOGGER.error(res.error);
                                return res;
                            }
                        }
                        if (piWriter != null) {
                            res.error = validateDataPropriety(piFilePath);
                            if (res.error != null) {
                                LOGGER.error(res.error);
                                return res;
                            }
                        }
                    }
                }
            }

            try {
                if (res.error == null) { //write score file only if file writing was successful
                    writeScoreFile(shard, day, addedFilesWeightB);
                }
            } catch (Exception e) {
                res.error = "Fail writing score file to S3 to shard " + shard + " to day " + day + ": " + e.getMessage();
                LOGGER.error(res.error);
                return res;
            }


           /* double fileSize = 0;
            try {
                fileSize = dataSerializer.getFileSizeM(composeFilePathNoPrefix(shard, day, minOffset));
            }
            catch (IOException e) {
                LOGGER.error("cannot calculate file size:" + e.getMessage());
            }

            long writeDuration = Duration.between(writeIntervalStart, Instant.now()).toMillis();
            LOGGER.info("write " + res.filePath + " took  " + writeDuration + " millisecond. File size = " + fileSize + ", Number of records = " + fileRecordCounter);
*/
            return res;
        }

        //validate that the data in the file is not corrupted by reading the file's metadata
        //return the error string upon error.
        //return null upon success.
        private String validateDataPropriety(String filePath) {
            Configuration conf = new Configuration();

            if (s3Storage) {
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
            while (retryCounter < config.getWriteRetries()) {
                try {
                    reader = new ParquetFileReader(HadoopInputFile.fromPath(path, conf), ParquetReadOptions.builder().build());
                    break;
                } catch (Exception e) {
                   /* try {
                        if (reader != null) {
                            reader.close();
                        }
                    } catch (Exception ex) {}
                    */
                    //LOGGER.info("(" + retryCounter + ") Cannot open file " + filePath + " for reading: " + e.getMessage());
                    retryCounter++;
                    if (retryCounter == config.getWriteRetries()) {
                        return "Cannot open file " + filePath + " for metadata reading: " + e.getMessage();
                    }
                    try {
                        if (retryCounter == 1) {
                            Thread.sleep(5000); //at the first time sleep for 5 secs
                        } else {
                            Thread.sleep(25000); //then sleep for 25 secs
                        }
                    } catch (InterruptedException ie) {}
                }
            }

            try {
                ParquetMetadata md = reader.getFooter();
                MessageType schema = md.getFileMetaData().getSchema();
                reader.close();
            }
            catch (Exception e) {
                /*try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (Exception ex) {}*/
                e.printStackTrace();
                return "cannot read meta data from file '" + filePath + "': " + e.getMessage();
            }
            return null;
        }

        private boolean addConfiguredEventsFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> eventsFieldsMap, ArrayList<String> intentialNullValueFields, boolean includePI) {
            boolean containsPI = false;
            if (event.get("name")!=null) {
                String eventName = event.get("name").asText();
                HashMap<String, HashMap<String, FieldConfig>> eventFieldsMap = eventsFieldsMap.get(eventName);
                if (eventFieldsMap!=null) {
                    Set<String> eventFieldNames = eventFieldsMap.keySet();
                    for (String eventFieldName : eventFieldNames) {
                        HashMap<String, FieldConfig> fieldVersions = eventsFieldsMap.get(eventName).get(eventFieldName);
                        FieldConfig fieldConfig = getFieldConfig(event, fieldVersions);

                        boolean added = addValue(group, eventFieldName, event.get(EVENT_ATTRIBUTES_FIELD), fieldConfig, intentialNullValueFields, includePI);
                        containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
                    }
                }
            }
            return containsPI;
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
                throw new FileCreationException("Fail creating file " + filePath, e);
            }
        }

        private String getEventSchemaVersion ( JsonNode event) {
            if (event.get(SCHEMA_VERSION_FIELD_NAME)!=null && event.get(SCHEMA_VERSION_FIELD_NAME).isTextual()) {
                return event.get(SCHEMA_VERSION_FIELD_NAME).asText();
            }
            return null;
        }

        private FieldConfig getFieldConfig(JsonNode event, HashMap<String, FieldConfig> fieldVersions) {
            if (fieldVersions.size() > 1) { //more than 1 version for the event
                String eventVersion = getEventSchemaVersion(event);
                if (eventVersion!=null && !eventVersion.isEmpty() && fieldVersions.containsKey(eventVersion)) {
                    return fieldVersions.get(eventVersion);
                }
            }

            return fieldVersions.get(DEFAULT_VERSION); //we know that default exists - validation during configured fields maps building
        }

        private boolean addConfiguredCommonFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, HashMap<String, FieldConfig>> commonFieldsMap, ArrayList<String> intentialNullValueFields, HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> eventsFieldsMap, boolean includePI) {
            boolean containsPI = false;
            Set<String> commonFieldNames = commonFieldsMap.keySet();
            for (String commonFieldName : commonFieldNames) {
                HashMap<String, FieldConfig> fieldVersions = commonFieldsMap.get(commonFieldName);
                FieldConfig fieldConfig = getFieldConfig(event, fieldVersions);

                if (!includePI && commonFieldName.equals(EVENT_PREVIOUS_VALUES_FIELD)) {
                    //previous values may contain pi fields. So should be cleaned if pi data should not be included
                    if (!fieldConfig.getDoNotSave() && event.get(EVENT_PREVIOUS_VALUES_FIELD)!=null) {
                        JsonNode previousValues = event.get(EVENT_PREVIOUS_VALUES_FIELD);
                        removeUserAttributesPiData(previousValues, event, eventsFieldsMap);
                        group.add(fieldConfig.getParquetField(), previousValues.toString());
                    }
                    continue;
                }
                else {
                    boolean added = addValue(group, commonFieldName, event, fieldConfig, intentialNullValueFields, includePI);
                    containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
                }
            }
            return containsPI;
        }

        //remove pi data from previous values json. The previous values field contains the previous user attributes values
        //so we are looking for pi in user-attributes event fields.
        private void removeUserAttributesPiData(JsonNode previousValuesJson, JsonNode event, HashMap<String, HashMap<String, HashMap<String, FieldConfig>>> eventsFieldsMap) {
            String eventName = "user-attributes";

            HashMap<String, HashMap<String, FieldConfig>> eventFieldsMap = eventsFieldsMap.get(eventName);
            if (eventFieldsMap!=null) {
                Set<String> eventFieldNames = eventFieldsMap.keySet();
                for (String eventFieldName : eventFieldNames) {
                    HashMap<String, FieldConfig> fieldVersions = eventsFieldsMap.get(eventName).get(eventFieldName);
                    FieldConfig fieldConfig = getFieldConfig(event, fieldVersions);
                    if (fieldConfig.isPersonalInformation() && previousValuesJson.get(eventFieldName)!=null) {
                        ((ObjectNode)previousValuesJson).remove(eventFieldName);
                    }
                }
            }
        }

        private boolean addConfiguredCustomDimensionsFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, HashMap<String, FieldConfig>> customDimensionsFieldsMap, ArrayList<String> intentialNullValueFields, boolean includePI) {
            if (event.get(EVENT_CUSTOM_DIMENSIONS_FIELD) == null) {
                //incase customDimensions field does not exist in the events
                return false;
            }

            boolean containsPI = false;
            Set<String> customDimensionsFieldNames = customDimensionsFieldsMap.keySet();
            for (String customDimensionsFieldName : customDimensionsFieldNames) {
                HashMap<String, FieldConfig> fieldVersions = customDimensionsFieldsMap.get(customDimensionsFieldName);
                FieldConfig fieldConfig = getFieldConfig(event, fieldVersions);

                boolean added = addValue(group, customDimensionsFieldName, event.get(EVENT_CUSTOM_DIMENSIONS_FIELD), fieldConfig, intentialNullValueFields, includePI);
                containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
            }
            return containsPI;
        }

        private boolean addDetectedEventsFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, HashMap<String, FieldConfig>> eventsFieldsMap, ArrayList<String> intentialNullValueFields, boolean includePI) {
            boolean containsPI = false;
            if (event.get("name")!=null) {
                String eventName = event.get("name").asText();
                Map<String, FieldConfig> eventFieldsMap = eventsFieldsMap.get(eventName);
                if (eventFieldsMap!=null) {
                    Set<String> eventFieldNames = eventFieldsMap.keySet();
                    for (String eventFieldName : eventFieldNames) {
                        FieldConfig fieldConfig = eventFieldsMap.get(eventFieldName);
                        boolean added = addValue(group, eventFieldName, event.get(EVENT_ATTRIBUTES_FIELD), fieldConfig, intentialNullValueFields, includePI);
                        containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
                    }
                }
            }
            return containsPI;
        }

        private boolean addDetectedCommonFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, FieldConfig> commonFieldsMap, ArrayList<String> intentialNullValueFields, boolean includePI) {
            Set<String> commonFieldNames = commonFieldsMap.keySet();
            boolean containsPI = false;
            for (String commonFieldName : commonFieldNames) {
                FieldConfig fieldConfig = commonFieldsMap.get(commonFieldName);
                boolean added = addValue(group, commonFieldName, event, fieldConfig, intentialNullValueFields, includePI);
                containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
            }
            return containsPI;
        }

        private boolean addDetectedCustomDimensionsFieldsToGroup(EconomicalGroup group, JsonNode event, HashMap<String, FieldConfig> customDimensionsFieldsMap, ArrayList<String> intentialNullValueFields, boolean includePI) {
            if (event.get(EVENT_CUSTOM_DIMENSIONS_FIELD) == null) {
                //incase customDimensions field does not exist in the events
                return false;
            }

            Set<String> customDimensionsFieldNames = customDimensionsFieldsMap.keySet();
            boolean containsPI = false;
            for (String customDimensionsFieldName : customDimensionsFieldNames) {
                FieldConfig fieldConfig = customDimensionsFieldsMap.get(customDimensionsFieldName);
                boolean added = addValue(group, customDimensionsFieldName, event.get(EVENT_CUSTOM_DIMENSIONS_FIELD), fieldConfig, intentialNullValueFields, includePI);
                containsPI = containsPI || (added && fieldConfig.isPersonalInformation());
            }
            return containsPI;
        }

        private String arrayToString(ArrayList<String> arr) {
            if (arr == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (String item:arr) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append(";");
                }
                sb.append(item);
            }
            return sb.toString();
        }

        private void writeScoreFile(int shard, String day, double addedFilesWeightB) throws IOException, ParseException {
            String scoresFolderPath = scoreTopicFolder + "shard=" + shard + File.separator + "day=" + day + File.separator;

            //delete previous score file if exists
            LinkedList<String> scoreFiles = dataSerializer.listFilesInFolder(scoresFolderPath,  SCORE_FILE_PREFIX, false);

            double currentScore = 0;
            for (String scoreFile:scoreFiles) {
                currentScore = Math.max(currentScore, getScoreFromFileName(scoreFile));
                dataSerializer.deleteFile(scoresFolderPath+scoreFile);
            }

            double addedFilesWeightM = Math.round((addedFilesWeightB/1024/1024) * 1000) / 1000.0; //round size to 3 decimal digits;

            double weight = currentScore + addedFilesWeightM;

            String scoreFile = scoresFolderPath + SCORE_FILE_PREFIX + weight;
            dataSerializer.writeData(scoreFile , UUID.randomUUID().toString());
        }

        private double getScoreFromFileName(String fileName) {
            String scoreStr = fileName.substring(fileName.indexOf(SCORE_FILE_PREFIX) + SCORE_FILE_PREFIX.length());
            return Double.valueOf(scoreStr);
        }

        private boolean addValue(Group group, String recordFieldName, JsonNode source,
                              FieldConfig fieldConfig, ArrayList<String> intentialNullValueFields, boolean includePI) throws IllegalArgumentException {
            if (fieldConfig.getDoNotSave()) {
                return false;
            }

            if (fieldConfig.isPersonalInformation() && !includePI) {
                return false;
            }

            String parquetFieldName = fieldConfig.getParquetField(); //set when building the schema

            if (source.get(recordFieldName) != null && !source.get(recordFieldName).isNull()) {
                try {
                    switch (fieldConfig.getType()) {
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
                    return true;
                } catch (NullPointerException e) {
                    throw new IllegalArgumentException("recordFieldName: " + recordFieldName + " is not in the expected type. There is a mismatch between the configured schema and event record.");
                }
            }
            else {
                if (fieldConfig.isRequired()) {
                    throw new IllegalArgumentException("'" + recordFieldName + "' is missing from event while the parquet field '" + parquetFieldName + "' that is attached to it is required.");
                }

                if (source.get(recordFieldName)!=null && source.get(recordFieldName).isNull()){
                    intentialNullValueFields.add(parquetFieldName);
                }
                return false;
            }
        }
    }

    private Hashing hashing;
    private PersistenceConsumerConfig config;
    private PersistenceConsumerConfig newConfig;
    private String awsSecret;
    private String awsKey;
    private String baseDataFilePrefix;
    private String piDataFilePrefix;
    private String baseDataTopicFolder;
    private String piDataTopicFolder;
    private String scoreTopicFolder;
    private long totalRecords = 0;
    private long recordCounter = 0;
    private Instant dumpIntervalStart;
    private String parquetSchema;
    private String writeErrorMsg = "";
    private DataSerializer dataSerializer;
    private boolean s3Storage;

    private EventFieldsMaps configuredEventsMaps = new EventFieldsMaps();

    //Map between a newly detected field name and its configuration
    private HashMap<String, FieldConfig> detectedNewCommonFieldConfigs = new HashMap<>();

    //Map between a newly detected field name and its configuration
    private HashMap<String, FieldConfig> detectedNewCustomDimensionsFieldConfigs = new HashMap<>();

    //Map between event to map between newly detected field name and its configuration
    private HashMap<String, HashMap<String, FieldConfig>> detectedNewEventsFieldConfigs = new HashMap<>();

    private ThreadPoolExecutor executor;  //parallelize shards and days writing to S3

    //Map <shard -> map <day->map<userId -> eventsList>>>
    //ConcurrentHashMap for the shards and days map since are accessed during file writing from multiple threads
    //LinkedHashMap for the users map since the users order is important - the offset of the first event of the first user determines the file name
    private ConcurrentHashMap<Integer, ConcurrentHashMap<String, LinkedHashMap<String, ArrayList<JsonNode>>>> eventsMap = new ConcurrentHashMap<>();

    public PersistenceConsumer(PersistenceConsumerConfig config) {
        super(config);

        validateConfig(config);
        this.config = config;

        this.baseDataTopicFolder = config.getDataFolder() + File.separator + config.getTopic() + File.separator;
        this.piDataTopicFolder = config.getPiDataFolder() + File.separator + config.getTopic() + File.separator;
        this.scoreTopicFolder = config.getScoresFolder() + File.separator + config.getTopic() + File.separator;

        this.s3Storage = config.getStorageType().equals(STORAGE_TYPE.S3.toString()); //if false => fileSystemStorage
        if(s3Storage) {
            //S3 configuration
            this.awsSecret = Environment.getEnv(AWS_ACCESS_SECRET_PARAM, true);
            this.awsKey = Environment.getEnv(AWS_ACCESS_KEY_PARAM, true);
            this.baseDataFilePrefix = config.getS3Bucket() + File.separator + baseDataTopicFolder;
            this.piDataFilePrefix = config.getS3PIBucket() + File.separator + piDataTopicFolder;
            this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getWriteRetries());
        }
        else { //FILE_SYSTEM
            String baseFolder = config.getBaseFolder();
            if (!baseFolder.endsWith(File.separator)) {
                baseFolder = baseFolder+File.separator;
            }
            this.baseDataFilePrefix = baseFolder + baseDataTopicFolder;
            this.piDataFilePrefix = baseFolder + piDataTopicFolder;
            this.dataSerializer = new FSSerializer(baseFolder, config.getWriteRetries());
        }

        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getWriteThreads());
        this.hashing = new Hashing(config.getShardsNumber());
        dumpIntervalStart = Instant.now();

        configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());

        LOGGER.info("*** Persistence consumer created with configuration:\n" + config.toString());
    }

    private void validateConfig(PersistenceConsumerConfig config) throws IllegalArgumentException{
        if (config.getStorageType()==null || config.getStorageType().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'storageType' configuration parameter.");
        }
        if (config.getStorageType().equals(STORAGE_TYPE.S3.toString())) {
            if(config.getS3Bucket()==null || config.getS3Bucket().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 's3Bucket' configuration parameter.");
            }
            if(config.getS3region()==null || config.getS3region().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 's3region' configuration parameter.");
            }
        }
        else if (config.getStorageType().equals(STORAGE_TYPE.FILE_SYSTEM.toString())) {
            if(config.getBaseFolder()==null || config.getBaseFolder().isEmpty()) {
                throw new IllegalArgumentException("Missing value for 'baseFolder' configuration parameter.");
            }
        }
        else {
            throw new IllegalArgumentException("Illegal value for 'storageType' configuration parameter. Can be either S3 or FILE_SYSTEM.");
        }

        if (config.getDataFolder()==null || config.getDataFolder().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'dataFolder' configuration parameter.");
        }
        if (config.getScoresFolder()==null || config.getScoresFolder().isEmpty()) {
            throw new IllegalArgumentException("Missing value for 'scoresFolder' configuration parameter.");
        }
    }

    private String buildParquetSchema() {
        Set<String> parquetFields = new HashSet<>(); //to avoid fields duplication since space=>underscore
        StringBuilder sb = new StringBuilder();
        sb.append("message event_record {\n" );

        String automaticallyAddNewFieldsStr = config.isAutomaticallyAddNewFields() ? ". Filed was added to schema." : ". Filed was not added to schema.";

        //add common fields
        Set<String> commonFieldNames = configuredEventsMaps.getConfiguredCommonFieldConfigs().keySet();
        for (String commonFieldName : commonFieldNames) {
            HashMap<String,FieldConfig> fieldVersions = configuredEventsMaps.getConfiguredCommonFieldConfigs().get(commonFieldName);
            Set<String> versions = fieldVersions.keySet();
            for (String version:versions) {
                addFieldToSchema(commonFieldName, fieldVersions.get(version), sb, parquetFields);
            }

        }

        //add customDimensions fields
        Set<String> customDimensionsFieldNames = configuredEventsMaps.getConfiguredCustomDimensionsFieldConfigs().keySet();
        for (String customDimensionsFieldName : customDimensionsFieldNames) {
            HashMap<String,FieldConfig> fieldVersions = configuredEventsMaps.getConfiguredCustomDimensionsFieldConfigs().get(customDimensionsFieldName);
            Set<String> versions = fieldVersions.keySet();
            for (String version:versions) {
                addFieldToSchema("cd." + customDimensionsFieldName, fieldVersions.get(version), sb, parquetFields);
            }
        }

        //add events fields
        Set<String> eventNames = configuredEventsMaps.getConfiguredEventsFieldConfigs().keySet();
        for (String eventName : eventNames) {
            HashMap<String, HashMap<String, FieldConfig>> eventFieldsMap = configuredEventsMaps.getConfiguredEventsFieldConfigs().get(eventName);
            Set<String> eventFieldNames = eventFieldsMap.keySet();
            for (String eventFieldName : eventFieldNames) {
                HashMap<String, FieldConfig> fieldVersions = eventFieldsMap.get(eventFieldName);
                Set<String> versions = fieldVersions.keySet();
                for (String version : versions) {
                    addFieldToSchema(concatEventField(eventName, eventFieldName), fieldVersions.get(version), sb, parquetFields);
                }
            }
        }

        //schema auto detection
        //add newly detected common fields
        Set<String> detectedCommonFieldNames = detectedNewCommonFieldConfigs.keySet();
        for (String commonFieldName : detectedCommonFieldNames) {
            if (config.isAutomaticallyAddNewFields()) {
                addFieldToSchema(commonFieldName, detectedNewCommonFieldConfigs.get(commonFieldName), sb, parquetFields);
            }
            //not real error but we need to be notified upon newly detected fields
            LOGGER.error("new common field detected: " + commonFieldName + ", type = " +detectedNewCommonFieldConfigs.get(commonFieldName).getType().toString() + automaticallyAddNewFieldsStr);
        }

        //add newly detected customDimensions fields
        Set<String> detectedCustomDimensionsFieldNames = detectedNewCustomDimensionsFieldConfigs.keySet();
        for (String customDimensionsFieldName : detectedCustomDimensionsFieldNames) {
            if (config.isAutomaticallyAddNewFields()) {
                addFieldToSchema("cd." + customDimensionsFieldName, detectedNewCustomDimensionsFieldConfigs.get(customDimensionsFieldName), sb, parquetFields);
            }
            //not real error but we need to be notified upon newly detected fields
            LOGGER.error("new customDimensions field detected: " + customDimensionsFieldName + ", type = " +detectedNewCustomDimensionsFieldConfigs.get(customDimensionsFieldName).getType().toString() + automaticallyAddNewFieldsStr);
        }

        //add newly detected events fields
        Set<String> detectedEventNames = detectedNewEventsFieldConfigs.keySet();
        for (String eventName : detectedEventNames) {
            Map<String, FieldConfig> eventFieldsMap = detectedNewEventsFieldConfigs.get(eventName);
            Set<String> eventFieldNames = eventFieldsMap.keySet();
            for (String eventFieldName : eventFieldNames) {
                if (config.isAutomaticallyAddNewFields()) {
                    addFieldToSchema(concatEventField(eventName, eventFieldName), eventFieldsMap.get(eventFieldName), sb, parquetFields);
                }
                //not real error but we need to be notified upon newly detected events/fields
                LOGGER.error("new event field detected to event '" + eventName +"':" + eventFieldName + ", type = " + eventFieldsMap.get(eventFieldName).getType().toString() + automaticallyAddNewFieldsStr);
            }
        }

        //add event field that contains a list of the fields that contain intential null values
        sb.append("optional binary " + NULL_VALUE_FIELDS_FIELD_NAME + " (UTF8);\n");

        //add the time this event was first written to the parquet file
        sb.append("required int64 "+ WRITE_TIME_FIELD_NAME + ";\n");

        //add event field that contains the source of the event: BASE,PI or SANS_PI
        sb.append("required binary " + SOURCE_FIELD_NAME + " (UTF8);\n");

        //add the time this event was first written to the parquet file
        sb.append("optional int64 "+ TIMESTAMP_FIELD_NAME + ";\n");

        //add the time this event was first written to the parquet file
        sb.append("optional int64 "+ DEVICE_TIME_FIELD_NAME + ";\n");

        //add the time this event was first written to the parquet file
        sb.append("optional int64 "+ ORIGINAL_EVENT_TIME_FIELD_NAME + ";\n");

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
        if (fieldConfig.getDoNotSave()) { //skip field if should not be saved
            return;
        }

        String parquetFieldName = getParquetFieldName(fieldName, fieldConfig);
        fieldConfig.setParquetField(parquetFieldName);

        if (parquetFields.contains(parquetFieldName)) {
            return;
        }

        parquetFields.add(parquetFieldName);

        String requiredStr = fieldConfig.isRequired() ? " required" : " optional";

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
        int recordsProcessed = 0;

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            addRecordToMap(record);
            ++recordCounter;
            ++totalRecords;
            ++recordsProcessed;
        }

        if (recordCounter >= config.getMaxDumpRecords() || (Duration.between(dumpIntervalStart, Instant.now()).toMillis()>config.getMaxDumpIntervalMs())) {
            if (!dumpMapToParquetFile()) {
                LOGGER.error("Stopping server due write errors.");
                stop();
                return recordsProcessed;
            }

            /*
            LOGGER.info("Before commit. commit & reset map. Total number of records = " + totalRecords);
            long durationTmp = Duration.between(dumpIntervalStart, Instant.now()).toMillis();
            long durationSecTmp = (durationTmp/1000);
            LOGGER.info("Before commit. dump " + recordCounter + " records took  " + durationTmp + " millisecond. " + (recordCounter/durationSecTmp) + " rec/sec");
            */

            LOGGER.info("##### Before commit");
            commit();
            LOGGER.info("##### After commit");
            resetEventsMap();
            updateSchemaUsingNewConfigIfExists();

            LOGGER.info("commit & reset map. Total number of records = " + totalRecords);
            long duration = Duration.between(dumpIntervalStart, Instant.now()).toMillis();
            long durationSec = (duration/1000);
            durationSec = (durationSec == 0) ? 1 : durationSec;
            LOGGER.info("dump " + recordCounter + " records took  " + duration + " millisecond. " + (recordCounter/durationSec) + " rec/sec");
            dumpIntervalStart = Instant.now( );
            recordCounter = 0;
        }

        return recordsProcessed;
    }

    @Override
    public void newConfigurationAvailable() {
        PersistenceConsumerConfig config = new PersistenceConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
            if (newConfig.getReadAfterWrite()!=this.config.getReadAfterWrite()) {
                LOGGER.info("Update 'readAfterWrite' configuration from " + this.config.getReadAfterWrite() + " to " + newConfig.getReadAfterWrite());
                this.config.setReadAfterWrite(newConfig.getReadAfterWrite());
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error init Airlock config: " + e.getMessage(), e);
        }
    }

    // Note that this will ONLY update the parquet schema. For other config changes to take effect - restart the consumer
    private synchronized void updateSchemaUsingNewConfigIfExists() {
        if (newConfig != null) {
            config.setCommonFieldTypesMap(newConfig.getCommonFieldTypesMap());
            config.setEventsFieldTypesMap(newConfig.getEventsFieldTypesMap());
            config.setCustomDimensionsFieldTypesMap(newConfig.getCustomDimensionsFieldTypesMap());
            newConfig = null;

            //build versioned fields map from the new configuration
            configuredEventsMaps.buildConfiguredFieldsMaps(config.getCommonFieldTypesMap(), config.getEventsFieldTypesMap(), config.getCustomDimensionsFieldTypesMap());


            //building parquetSchema each dump because the schema is dynamically detected (in additional to the given configuration)
            //from the records that are dumped
        }
    }

    private void resetEventsMap() {
        eventsMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, LinkedHashMap<String, ArrayList<JsonNode>>>>();
        detectedNewEventsFieldConfigs.clear();
        detectedNewCommonFieldConfigs.clear();
        detectedNewCustomDimensionsFieldConfigs.clear();
    }

    //dump map to parquet files in s3
    //return true is all succeeded - false is there are errors
    private boolean dumpMapToParquetFile() {
        LOGGER.info("**** in dump to file ****");

        //building parquetSchema each dump because the schema is dynamically detected (in additional to the given configuration)
        //from the records that are dumped
        parquetSchema = buildParquetSchema();
        //LOGGER.info("parquetSchema = \n" + parquetSchema);

        ArrayList<Future<S3ParquetWriteTask.Result>> writings = new ArrayList<Future<S3ParquetWriteTask.Result>>();
        Set<Integer> shards = eventsMap.keySet();
        for (int shard:shards) {
            Set<String> days = eventsMap.get(shard).keySet();
            for (String day:days) {
                S3ParquetWriteTask w = new S3ParquetWriteTask(shard, day);
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
                    LOGGER.error("Errors in thread: "+ result.error);
                    errors.add(result.filePath + ": " + result.error);
                    writeErrorMsg = "Fail writing parquet file '" + result.filePath + "' to S3: " + result.error;
                }
            }
            catch (Exception e) {
                errors.add(e.toString());
            }
        }

        if (!errors.isEmpty()) {
            LOGGER.error("errors during writing: " + errors.toString());
            return false;
        }

        return true;
    }

    private String composeFilePath(int shard, String day, long firstRecordOffset, INFORMATION_SOURCE piType) {
        StringBuilder sb = new StringBuilder();
        if (s3Storage) {
            sb.append("s3a://" );
        }
        switch (piType) {
            case BASE:
            case SANS_PI:
                sb.append(baseDataFilePrefix);
                break;
            case PI:
                sb.append(piDataFilePrefix);
                break;
        }
        sb.append("shard=" + shard + File.separator);
        sb.append("day=" + day + File.separator);
        sb.append(shard + "_" + day + "_" + firstRecordOffset + PARQUET_FILE_EXTENSION);

        return sb.toString();
    }

    private String composeFilePathNoPrefix(int shard, String day, long firstRecordOffset) {
        StringBuilder sb = new StringBuilder();
        sb.append(baseDataTopicFolder);
        sb.append("shard=" + shard + File.separator);
        sb.append("day=" + day + File.separator);
        sb.append(shard + "_" + day + "_" + firstRecordOffset + PARQUET_FILE_EXTENSION);

        return sb.toString();
    }

    private synchronized void addRecordToMap(ConsumerRecord<String, JsonNode> record) {
        if (record.value().get("name") != null && config.getSkippedEvents().contains(record.value().get("name").textValue() )) {
            //should skip such events
            return;
        }

        if (record.value().get("userId") == null) {
            LOGGER.error("Illegal record: missing userId");
            return;
        }
        String userId = record.value().get("userId").textValue();

        //event day yyyy-mm-dd
        if (record.value().get("eventTime") == null) {
            LOGGER.error("Illegal record: missing eventTime");
            return;
        }

        long datetime = record.value().get("eventTime").asLong();
        Date eventDate = new Date(datetime);
        String eventDateStr = gFormatter.format(eventDate);

        int shard = hashing.hashMurmur2(userId);
        if ((shard % getNumberOfPartitions(config.getTopics().get(0))) != record.partition()) {
            LOGGER.error("Shard partition mismatch for user " + userId + ", shard = " + shard + ", partition = " + record.partition() + ", record key = " + record.key());
        }

        ConcurrentHashMap<String, LinkedHashMap<String, ArrayList<JsonNode>>> shardMap = eventsMap.get(shard);
        if (shardMap == null) {
            shardMap = new ConcurrentHashMap<String, LinkedHashMap<String, ArrayList<JsonNode>>>();
            eventsMap.put(shard, shardMap);
        }

        LinkedHashMap<String, ArrayList<JsonNode>> dayMap = shardMap.get(eventDateStr);
        if (dayMap == null) {
            dayMap = new LinkedHashMap<String, ArrayList<JsonNode>>();
            shardMap.put(eventDateStr, dayMap);
        }

        ArrayList<JsonNode> userEvents = dayMap.get(userId);
        if (userEvents == null) {
            userEvents = new ArrayList<JsonNode>();
            dayMap.put(userId, userEvents);
        }

        detectNewFields(record.value());

        ((ObjectNode)record.value()).put(OFFSET_FIELD_NAME, record.offset());
        ((ObjectNode)record.value()).put(TIMESTAMP_FIELD_NAME, record.timestamp());

        //add deviceTime taken from header
        if (record.headers()!=null && record.headers().toArray().length > 0) {
            Header header = record.headers().lastHeader(DEVICE_TIME_HEADER_KEY);
            if (header != null) {
                try {
                    long l = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
                    ((ObjectNode)record.value()).put(DEVICE_TIME_FIELD_NAME, l);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error converting byte[] to long during deviceTime header calculation. Ignoring : " + e.getMessage());
                }
            }

            header = record.headers().lastHeader(ORIGINAL_EVENT_TIME_HEADER_KEY);
            if (header != null) {
                try {
                    long l = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
                    ((ObjectNode)record.value()).put(ORIGINAL_EVENT_TIME_FIELD_NAME, l);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error converting byte[] to long during originalEventTime header calculation. Ignoring : " + e.getMessage());
                }
            }
        }
        userEvents.add(record.value());
    }

    private void detectNewFields(JsonNode value) {
        Iterator<String> filedsIter = value.fieldNames();
        while(filedsIter.hasNext()) {
            String fieldName = filedsIter.next();
            if (fieldName.equalsIgnoreCase(EVENT_ATTRIBUTES_FIELD)) {
                String eventName = value.get("name").asText();

                //event fields
                JsonNode eventAttributes = value.findValue(EVENT_ATTRIBUTES_FIELD);
                Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
                while(eventFieldsIter.hasNext()) {
                    String eventFieldName = eventFieldsIter.next();
                    Map configuredEventFields = config.getEventsFieldTypesMap().get(eventName);
                    boolean inPredefindEventFields  = configuredEventFields!=null && configuredEventFields.containsKey(eventFieldName);
                    Map detectedEventFields = detectedNewEventsFieldConfigs.get(eventName);
                    boolean inDetectedEventFields  = detectedEventFields!=null && detectedEventFields.containsKey(eventFieldName);
                    if (!inPredefindEventFields && !inDetectedEventFields) {
                        FieldConfig.FIELD_TYPE type = findValueType(eventAttributes.get(eventFieldName));
                        if (type!=null) {
                            FieldConfig fieldConfig = new FieldConfig(type, false, null, false, false, false);
                            if (!detectedNewEventsFieldConfigs.containsKey(eventName)) {
                                detectedNewEventsFieldConfigs.put(eventName, new HashMap<String, FieldConfig>());
                            }
                            detectedNewEventsFieldConfigs.get(eventName).put(eventFieldName, fieldConfig);
                        }
                    }
                }
            }
            else if (fieldName.equalsIgnoreCase(EVENT_CUSTOM_DIMENSIONS_FIELD)) {
                //customDimensions fields
                JsonNode eventCustomDimensions = value.findValue(EVENT_CUSTOM_DIMENSIONS_FIELD);
                Iterator<String> customDimensionsIter = eventCustomDimensions.fieldNames();
                while(customDimensionsIter.hasNext()) {
                    String customDimensionsFieldName = customDimensionsIter.next();
                    if (!config.getCustomDimensionsFieldTypesMap().containsKey(customDimensionsFieldName) && !detectedNewCustomDimensionsFieldConfigs.containsKey(customDimensionsFieldName)) {
                        FieldConfig.FIELD_TYPE type = findValueType(eventCustomDimensions.get(customDimensionsFieldName));
                        if (type!=null) {
                            FieldConfig fieldConfig = new FieldConfig(type, false, null, false, false, false);
                            detectedNewCustomDimensionsFieldConfigs.put(customDimensionsFieldName, fieldConfig);
                            if (value.get("eventId")!=null) {
                                LOGGER.error("new customDimensions field detected: " + customDimensionsFieldName + ". eventId = " + value.get("eventId").asText());
                            }
                        }
                    }
                }
            }
            else {
                //common fields
                if (!config.getCommonFieldTypesMap().containsKey(fieldName) && !detectedNewCommonFieldConfigs.containsKey(fieldName)) {
                    FieldConfig.FIELD_TYPE type = findValueType(value.get(fieldName));
                    if (type!=null) {
                        FieldConfig fieldConfig = new FieldConfig(type, false, null, false, false, false);
                        detectedNewCommonFieldConfigs.put(fieldName, fieldConfig);
                    }
                }
            }
        }
    }

    //event that contains piMarker and its value is false (the event may have pi fields but the piMarker is false this event is considered as non-pi)
    boolean isNotPiByMarker(JsonNode event) {
        String eventName = event.get("name").asText();
        if (configuredEventsMaps.getEventsWithPIMarker().containsKey(eventName)) {
            String piMarkerFieldName = configuredEventsMaps.getEventsWithPIMarker().get(eventName);
            if (event.get(EVENT_ATTRIBUTES_FIELD).get(piMarkerFieldName)!=null && event.get(EVENT_ATTRIBUTES_FIELD).get(piMarkerFieldName).booleanValue()==false) {
                return true;
            }

        }
        return false; //if the event is configured to have piMarker but the currnt event does not contain the pi marker field => considered as pi event
    }

    //return null if cannot detect the type
    private FieldConfig.FIELD_TYPE findValueType(JsonNode value) {
        if (value == null) {
            return null;
        }

        if (value.isBoolean()) {
            return FieldConfig.FIELD_TYPE.BOOLEAN;
        }
        if(value.isTextual()) {
            return FieldConfig.FIELD_TYPE.STRING;
        }
        if(value.isInt()) {
            return FieldConfig.FIELD_TYPE.INTEGER;
        }

        if(value.isLong()) {
            return FieldConfig.FIELD_TYPE.LONG;
        }

        if(value.isFloat()) {
            return FieldConfig.FIELD_TYPE.FLOAT;
        }

        if(value.isDouble()) {
            return FieldConfig.FIELD_TYPE.DOUBLE;
        }
        //if value is array or json object
        if (value.isObject() || value.isArray()) {
            return FieldConfig.FIELD_TYPE.JSON_STRING;
        }
        return null;
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
