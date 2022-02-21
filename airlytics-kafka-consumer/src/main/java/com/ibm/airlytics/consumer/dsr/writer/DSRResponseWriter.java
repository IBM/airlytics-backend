package com.ibm.airlytics.consumer.dsr.writer;

import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.dsr.DSRConsumer;
import com.ibm.airlytics.consumer.dsr.DSRResponse;
import com.ibm.airlytics.consumer.dsr.db.DbHandler;
import com.ibm.airlytics.consumer.dsr.retriever.DSRData;
import com.ibm.airlytics.consumer.dsr.retriever.FieldConfig;
import com.ibm.airlytics.consumer.dsr.retriever.ParquetRetriever;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.S3Serializer;
import com.ibm.airlytics.serialize.parquet.RowsLimitationParquetWriter;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.eco.EconomicalGroup;
import org.apache.parquet.hadoop.CustomParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class DSRResponseWriter {

    static final Counter recordsProcessedCounter = Counter.build()
            .name("dsr_response_db_records_processed_total")
            .help("Total records processed by the drsResponseWriter.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    static final Summary dbFilesSizeBytesSummary = Summary.build()
            .name("dsr_response_db_file_size_bytes")
            .help("File size in bytes.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();
    private static final AirlyticsLogger logger = AirlyticsLogger.getLogger(DSRResponseWriter.class.getName());
    private String unitedSchema;
    private final DbHandler dbHandler;
    private DataSerializer dataSerializer;
    private ThreadPoolExecutor executor;
    private ResponseWriterConfig config;
    private String awsSecret;
    private String awsKey;
    //Map between field name and its configuration
    private HashMap<String, FieldConfig> dbFieldConfigs = new HashMap<>();

    public DSRResponseWriter(DbHandler dbHandler) throws IOException {
        this.dbHandler = dbHandler;
        config = new ResponseWriterConfig();
        config.initWithAirlock();
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getReadThreads());
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getS3Bucket(), config.getIoActionRetries());
    }

    private String composeFolderPath(String dayPath) {
        String respFolder = config.getResponseFolder().replaceFirst("^"+config.getS3Bucket()+"/","");
        return  respFolder+dayPath+ File.separator  + config.getResponseSecondPath()+ File.separator;
    }

    private long getPrefix(DSRData data) throws IOException {
        String dayPath = data.getDayPath();
        List<String> files = dataSerializer.listFilesInFolder(composeFolderPath(dayPath),DSR_RESULT_PREFIX, false);
        int lastPart = -1;
        for (String file : files) {
            if (file.endsWith("snappy.parquet")) {
                String part = file.substring(DSR_RESULT_PREFIX.length()).substring(0,1);
                int num = Integer.parseInt(part);
                if (num > lastPart) {
                    lastPart = num;
                }
            }
        }
        return lastPart + 1;
    }

    public void writeDRSResponses(List<DSRData> dsrData) {
        try {
            if (dsrData.isEmpty()) return;

            unitedSchema = buildSchema(dsrData.get(0).getEventsFields());
            List<Future<WriteTask.WriteResult>> futures = new ArrayList<>();
            for (DSRData data : dsrData) {
                long prefix = getPrefix(data);
                WriteTask task = new WriteTask(data, prefix);
                Future<WriteTask.WriteResult> future = this.executor.submit(task);
                futures.add(future);
            }

            for (Future<WriteTask.WriteResult> future : futures) {
                WriteTask.WriteResult result = future.get();
                logger.info("DSR Response writeResult for "+result.filePath+":"+result.success);
                if (result.error != null) {
                    logger.error("error in writeResult for "+result.filePath+":"+result.error);
                }
                if (result.success) {
                    DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_PORTABILITY, DSR_REQUEST_RESULT_EXECUTED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                } else {
                    DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_PORTABILITY, DSR_REQUEST_RESULT_FAILED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                }
            }

        } catch (SQLException | ClassNotFoundException | InterruptedException | ExecutionException e) {
            logger.error("failed writing DSR responses:"+e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("failed concluding the part of the file to write:"+e.getMessage());
            e.printStackTrace();
        }
    }

    private String buildSchema(ParquetRetriever.ParquetFields eventFields) throws SQLException, ClassNotFoundException {
        StringBuilder sb = new StringBuilder();
        sb.append("message event_record {\n" );
        String requiredStr = "optional";
        List<String> fields = new ArrayList<>();
        for (Map.Entry<String, PrimitiveType.PrimitiveTypeName> entry : eventFields.getFieldsMap().entrySet()) {
//            switch (entry.getValue()) {
//                case BINARY:
//
//                    sb.append(requiredStr + " binary "+ entry.getKey() + " (STRING);\n");
//                    break;
//                case INT32:
//                    sb.append(requiredStr + " int32 "+ entry.getKey() + ";\n");
//                    break;
//                case INT64:
//                    sb.append(requiredStr + " int64 "+ entry.getKey() + ";\n");
//                    break;
//                case FLOAT:
//                    sb.append(requiredStr + " float "+ entry.getKey() + ";\n");
//                    break;
//                case DOUBLE:
//                    sb.append(requiredStr + " double "+ entry.getKey() + ";\n");
//                    break;
//                case BOOLEAN:
//                    sb.append(requiredStr + " boolean "+ entry.getKey() + ";\n");
//                    break;
//            }
            if (config.getExcludedEventsColumns() != null && config.getExcludedEventsColumns().contains(entry.getKey())) {
                continue;
            }
            sb.append(requiredStr + " binary "+ entry.getKey() + " (STRING);\n");
            fields.add(entry.getKey());
        }
        sb.append(buildDBSchema(fields));
        return sb.toString();
    }
    private String buildDBSchema(List<String> fields) throws SQLException, ClassNotFoundException {
        HashMap<String, FieldConfig> fieldConfigsMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
//        sb.append("message event_record {\n" );
        CachedRowSet schema = dbHandler.getDBSchema();
        while (schema.next()) {
            String fieldName = schema.getString("column_name");
            String type = schema.getString("data_type");
            String isNullable = schema.getString("is_nullable");
            logger.info("column_name:" + fieldName+". data_type:"+type+". isNullable:"+isNullable);
            FieldConfig fieldConfig = new FieldConfig(type, isNullable);
            if (config.getExcludedColumns() != null && config.getExcludedColumns().contains(fieldName)) {
                continue;
            }
            if (!fields.stream().anyMatch(str -> str.trim().equals(fieldName)) &&
                !fieldConfigsMap.containsKey(fieldName)) {
                addFieldToSchema(fieldName, fieldConfig, sb);
            }
            fieldConfigsMap.put(fieldName, fieldConfig);
        }
        sb.append("}\n");
        setDbFieldConfigs(fieldConfigsMap);
        return sb.toString();
    }


    private void addFieldToSchema(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        String requiredStr = fieldConfig.isRequired() ? " required" : " optional";
        //make it always optional
        requiredStr = "optional";
        switch (fieldConfig.getType()) {
            case STRING:
            case ARRAY:
            case JSON:
                sb.append(requiredStr + " binary "+ fieldName + " (STRING);\n");
                break;
            case INTEGER:
                sb.append(requiredStr + " int32 "+ fieldName + ";\n");
                break;
            case LONG:
            case NUMBER:
                sb.append(requiredStr + " int64 "+ fieldName + ";\n");
                break;
            case FLOAT:
                sb.append(requiredStr + " float "+ fieldName + ";\n");
                break;
            case DOUBLE:
                sb.append(requiredStr + " double "+ fieldName + ";\n");
                break;
            case BOOLEAN:
                sb.append(requiredStr + " boolean "+ fieldName + ";\n");
                break;
        }
    }

    public HashMap<String, FieldConfig> getDbFieldConfigs() {
        return dbFieldConfigs;
    }

    public void setDbFieldConfigs(HashMap<String, FieldConfig> dbFieldConfigs) {
        this.dbFieldConfigs = dbFieldConfigs;
    }



    class WriteTask implements Callable {
        class WriteResult {
            boolean success;
            String error;
            String filePath;
        }

        private DSRData data;
        private long part;
        WriteTask(DSRData data, long part) {
            this.data=data;
            this.part=part;
        }

        public WriteResult call() {
            logger.info("start writing dsr responses");
            WriteResult res = new WriteResult();
            String recordResult = RESULT_SUCCESS;
            int retryCounter = 0;
            Long minOffset = (long)0;
            CustomParquetWriter<Group> writer = null;
            long tryNumbers = 0;
            long successNumbers = 0;
            while (retryCounter < config.getIoActionRetries()) {
                String writeResult = RESULT_SUCCESS;

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

                    String filePath = composeFilePath(data.getDayPath());
                    res.filePath = filePath;
                    logger.info("writing data to:"+filePath);
                    res.error = null;

                    MessageType schema = parseMessageType(unitedSchema);
                    GroupWriteSupport.setSchema(schema, conf);

                    writer = RowsLimitationParquetWriter.builder(new Path(filePath))
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .withRowGroupSize(config.getParquetRowGroupSize())
                            .withPageSize(config.getParquetRowGroupSize())
                            .withDictionaryEncoding(true)
                            .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                            .withConf(conf)
                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                            .build();

                    for (DSRResponse resp : data.getUsersData()) {
                        //write user data from DB
                        try {
                            EconomicalGroup group = new EconomicalGroup(schema);
                            Set<String> fieldNames = getDbFieldConfigs().keySet();
                            // add dsr-request-id
                            group.add(DSR_REQUEST_ID_SCHEMA_FIELD, resp.getRequest().getRequestId());
                            for (String fieldName : fieldNames) {
                                addFieldToGroup(fieldName, getDbFieldConfigs().get(fieldName),group, resp.getDbData());
                            }
                            tryNumbers++;
                            writer.write(group);
                            successNumbers++;
                        } catch (Exception e) {
                            logger.error("error writing user JSON for request:"+resp.getRequest().getRequestId());
                            recordResult = RESULT_ERROR;
                            e.printStackTrace();
                        }
                        //write events
                        for (JSONObject event : resp.getEvents()) {
                            try {
                                EconomicalGroup group = new EconomicalGroup(schema);
                                // add dsr-request-id
                                group.add(DSR_REQUEST_ID_SCHEMA_FIELD, resp.getRequest().getRequestId());
                                Set<String> fieldNames = data.getEventsFields().getFieldNames();
                                for (String fieldName : fieldNames) {
                                    if (fieldName.contains("eventTime")) {
                                        fieldName = fieldName;
                                    }
                                    addFieldToGroup(fieldName, convertToFieldConfig(data.getEventsFields().getFieldType(fieldName)),group, event);
                                }
                                tryNumbers++;
                                writer.write(group);
                                successNumbers++;
                            } catch (Exception e) {
                                logger.error("error writing event:"+event+" for request:"+resp.getRequest().getRequestId()+":"+e.getMessage());
                                recordResult = RESULT_ERROR;
                                e.printStackTrace();
                            }

                        }
                        logger.info("tried to write "+tryNumbers+" rows. succeeded in "+successNumbers);

                    }
                } catch (Exception e) {
                    logger.error("error writing response:"+e.getMessage());
                    e.printStackTrace();
                } finally {
                    if (writer != null) {
                        try {
                            writer.close();
                        } catch (Exception e) {
                            res.error = "Fail closing parquet file " + res.filePath+": " + e.getMessage();
                            writeResult = RESULT_ERROR;
                            logger.error(res.error);
                            return res;
                        }
                    }
                    if (writeResult.equals(RESULT_SUCCESS)) {
                        //provide full control to the file
                        String s3Path = composeFilePathForS3(data.getDayPath());
                        //write success file
                        try {
                            dataSerializer.addFullPermission(s3Path);
                            dataSerializer.writeData(composeSuccessFilePath(data.getDayPath()), "");
                        } catch (Exception e) {
                            res.error = "Failed writing success file " + composeSuccessFilePath(data.getDayPath()) + e.getMessage();
                            writeResult = RESULT_ERROR;
                            logger.error(res.error);
                            return res;
                        }
                        res.success = true;
                        return res;
                    }
                }
            }
            return res;
        }

        private FieldConfig convertToFieldConfig(PrimitiveType.PrimitiveTypeName fieldType) {
            switch (fieldType) {
                case DOUBLE:
                case FLOAT:
                case INT64:
                case INT32:
                case BINARY:
                case BOOLEAN:
                    return new FieldConfig("character", "yes");
            }
            return null;
        }

        private String composeFilePath(String dayPath) {
            return "s3a://" + config.getResponseFolder()+dayPath+ File.separator + config.getResponseSecondPath() + File.separator + DSR_RESULT_PREFIX + part + "-" + config.getResponseFile();
        }

        private String composeFilePathForS3(String dayPath) {
            String respFolder = config.getResponseFolder().replaceFirst("^"+config.getS3Bucket()+"/","");
            return  respFolder+dayPath+ File.separator  + config.getResponseSecondPath() + File.separator + DSR_RESULT_PREFIX + part + "-" +  config.getResponseFile();
        }

        private String composeSuccessFilePath(String dayPath) {
            String respFolder = config.getResponseFolder().replaceFirst("^"+config.getS3Bucket()+"/","");
            return respFolder +dayPath+ File.separator+config.getResponseSuccessFile();
        }

        private void addFieldToGroup(String fieldName, FieldConfig fieldConfig, EconomicalGroup group, JSONObject source) throws SQLException {
            switch (fieldConfig.getType()) {
                case STRING:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                case ARRAY:
                case NUMBER:
                case JSON:
                    addValue(group, fieldName, source, fieldConfig.getType());
                    break;

                default:
                    //should not get here
                    break;
            }
        }

        private void addValue(Group group, String fieldName, JSONObject source, FieldConfig.FIELD_TYPE type) throws SQLException {
            if (!source.has(fieldName)) {
                return;
            }
            switch (type) {
                case STRING:
                    if (source.has(fieldName)) {
                        String strVal = source.getString(fieldName);
                        group.add(fieldName, strVal);
                    }
                    break;
                case ARRAY:
                    if (source.has(fieldName)) {
                        JSONArray arrVal = source.getJSONArray(fieldName);
                        if (arrVal!=null) {
                            group.add(fieldName, arrVal.toString());
                        }
                    }
                    break;
                case JSON:
                    if (source.has(fieldName)) {
                        String objVal = null;
                        try {
                            JSONObject obj = source.getJSONObject(fieldName);
                            if (obj != null) {
                                objVal = obj.toString();
                            }
                        } catch (JSONException e) {
                            JSONArray arr = source.getJSONArray(fieldName);
                            if (arr != null) {
                                objVal = arr.toString();
                            }
                        }

                        if (objVal!=null) {
                            group.add(fieldName, objVal);
                        }
                    }
                    break;
                case INTEGER:
                    if (source.has(fieldName)) {
                        int intVal = source.getInt(fieldName);
                        group.add(fieldName, intVal);
                    }
                    break;
                case NUMBER:
                    if (source.has(fieldName)) {
                        long longVal = source.getLong(fieldName);
                        group.add(fieldName, longVal);
                    }
                    break;
                case LONG:
                    if (source.has(fieldName)) {
                        String tsStr = source.getString(fieldName);
                        Timestamp tsVal = Timestamp.valueOf(tsStr);
                        group.add(fieldName, tsVal.getTime());
                    }
                    break;
                case FLOAT:

                    if (source.has(fieldName)) {
                        float floatVal = source.getFloat(fieldName);
                        group.add(fieldName, floatVal);
                    }
                    break;
                case DOUBLE:

                    if (source.has(fieldName)) {
                        double doubleVal = source.getDouble(fieldName);
                        group.add(fieldName, doubleVal);
                    }
                    break;
                case BOOLEAN:
                    if (source.has(fieldName)) {
                        boolean boolVal = source.getBoolean(fieldName);
                        group.add(fieldName, boolVal);
                    }
                    break;
            }
        }

    }
}
