package com.ibm.airlytics.retentiontrackerqueryhandler.db.split;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.S3Serializer;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.utils.TrackerTimer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.FieldConfig;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.TableViewConfig;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.athena.AthenaHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.data.DataSerializer;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.parquet.RowsLimitationParquetWriter;
import org.apache.commons.lang.StringUtils;
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
import org.json.JSONArray;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.io.File;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.retentiontrackerqueryhandler.db.PersistenceConsumerConstants.*;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

public class TableSplitter {
    TableSplitConfig tableConfig;
    private String parquetSchema;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(ParquetSplitter.class.getName());
    private DbHandler dbHandler;
    private AthenaHandler athenaHandler;
    private PersistenceSplitConfig config;
    private String awsSecret;
    private String awsKey;
    private String s3FilePrefix;
    private String tempS3FilePrefix;
    private ThreadPoolExecutor executor;  //parallelize shards writing to S3
    private DataSerializer dataSerializer;
    private DataSerializer destDataSerializer;

    public TableSplitter(TableSplitConfig tableConfig, DbHandler dbHandler, AthenaHandler athenaHandler) throws IOException {
        this.tableConfig = tableConfig;
        this.dbHandler = dbHandler;
        this.athenaHandler = athenaHandler;
        config = new PersistenceSplitConfig();
        config.initWithAirlock();
        //S3 configuration
        this.awsSecret = System.getenv(AWS_ACCESS_SECRET_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.awsKey = System.getenv(AWS_ACCESS_KEY_PARAM); //if set as environment parameters use them otherwise will be taken from role
        this.s3FilePrefix = tableConfig.getS3Bucket() + File.separator + tableConfig.getS3RootFolder() + File.separator;
        this.tempS3FilePrefix = config.getExportS3Bucket() + File.separator + config.getExportTempPrefix() + File.separator;
        int writeThreads = tableConfig.getWriteThreads();
        if (writeThreads <= 0) {
            writeThreads = config.getWriteThreads();
        }
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(writeThreads);
        this.dataSerializer = new S3Serializer(config.getS3region(), config.getExportS3Bucket(), config.getIoActionRetries());
        this.destDataSerializer = new S3Serializer(config.getS3region(), tableConfig.getS3Bucket(), config.getIoActionRetries());
    }

    private String getConvertSelectPart(TableSplitConfig tableConfig) throws SQLException, ClassNotFoundException {
        String schema = tableConfig.getSchemaName();
        String table = tableConfig.getTableName();
        HashMap<String, FieldConfig> fieldConfigsMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        List<DbHandler.DBSchemaField> fields = dbHandler.getDBSchemaFields(table, schema);
        FieldConfig shardFC = null;
        String shardFieldName = null;
        for (DbHandler.DBSchemaField field : fields) {
            String fieldName = field.fieldName;
            String type = field.type;
            String isNullable = field.isNullable;
            logger.debug("column_name:" + fieldName+". data_type:"+type+". isNullable:"+isNullable);
            FieldConfig fieldConfig = new FieldConfig(type, isNullable);
            if (fieldName.equals("shard")) {
                shardFC = fieldConfig;
                shardFieldName = fieldName;
            } else {
                if (!tableConfig.getAthenaTableName().isEmpty() || !tableConfig.getAthenaTableView().isEmpty()) {
                    addFieldToSplitSelect(fieldName, fieldConfig, sb);
                } else {
                    addFieldToConvertSelect(fieldName, fieldConfig, sb);
                }

            }
            fieldConfigsMap.put(fieldName, fieldConfig);
        }
        //put shard last if available
        if (shardFieldName != null) {
            if (!tableConfig.getAthenaTableName().isEmpty() || !tableConfig.getAthenaTableView().isEmpty()) {
                addFieldToSplitSelect(shardFieldName, shardFC, sb);
            } else {
                addFieldToConvertSelect(shardFieldName, shardFC, sb);
            }

        }
        config.setFieldTypesMap(fieldConfigsMap);
        //remove last 2 characters
        if (sb.length() > 1) {
            int length = sb.length();
            sb.deleteCharAt(length-1);
        }
        return sb.toString();
    }

    private String getAthenaFieldsConfig(String schema, String table) throws SQLException, ClassNotFoundException {
        HashMap<String, FieldConfig> fieldConfigsMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        List<DbHandler.DBSchemaField> fields = dbHandler.getDBSchemaFields(table, schema);
        for (DbHandler.DBSchemaField field : fields) {
            String fieldName = field.fieldName;
            String type = field.type;
            String isNullable = field.isNullable;
            logger.debug("column_name:" + fieldName+". data_type:"+type+". isNullable:"+isNullable);
            FieldConfig fieldConfig = FieldConfig.forAthenaSnapshot(type, isNullable);
            addFieldToAthenaSchema(fieldName, fieldConfig, sb);
            fieldConfigsMap.put(fieldName, fieldConfig);
        }
        config.setFieldTypesMap(fieldConfigsMap);
        //remove last 2 characters
        if (sb.length() > 2) {
            int length = sb.length();
            sb.deleteCharAt(length-1);
            sb.deleteCharAt(length-2);
        }
        return sb.toString();
    }

    private String buildParquetSchema(TableSplitConfig tableConfig) throws SQLException, ClassNotFoundException {
        String schema = tableConfig.getSchemaName();
        String table = tableConfig.getTableName();
        if (tableConfig.isFromAthena()) {
            table = tableConfig.getDbTableName();
        }
        HashMap<String, FieldConfig> fieldConfigsMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        sb.append("message event_record {\n" );
        List<DbHandler.DBSchemaField> fields = dbHandler.getDBSchemaFields(table, schema);
        for (DbHandler.DBSchemaField field : fields) {
            String fieldName = field.fieldName;
            String type = field.type;
            String isNullable = field.isNullable;
            logger.debug("column_name:" + fieldName+". data_type:"+type+". isNullable:"+isNullable);
            FieldConfig fieldConfig = new FieldConfig(type, isNullable);
            addFieldToSchema(fieldName, fieldConfig, sb);
            fieldConfigsMap.put(fieldName, fieldConfig);
        }
        sb.append("}\n");
        config.setFieldTypesMap(fieldConfigsMap);
        return sb.toString();
    }

    private void addFieldToRepairSelect(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        switch (fieldConfig.getType()) {
            case STRING:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case ARRAY:
            case LONG:
            case JSON:
                sb.append(fieldName+",");
                break;
            case NUMBER:
                String numTemplate = "CAST(from_utf8(%s) AS BIGINT) as %s";
                String num = String.format(numTemplate, fieldName, fieldName);
                sb.append(num+",");
                break;

        }
    }
    private void addFieldToSplitSelect(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        switch (fieldConfig.getType()) {
            case STRING:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case ARRAY:
            case LONG:
            case JSON:
            case NUMBER:
                sb.append(fieldName+",");
                break;

        }
    }
    private void addFieldToConvertSelect(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        switch (fieldConfig.getType()) {
            case STRING:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case JSON:
                sb.append(fieldName+",");
                break;
            case ARRAY:
                String customA = tableConfig.getCasts().get("ARRAY");
                String template = "'['||array_join(transform(split(substr(substr(%s, 1, LENGTH(%s)-1),2),','), x -> chr(34)||x||chr(34)),',')||']' AS %s";
                if (customA != null && !customA.isEmpty()) {
                    template = customA;
                }
                String convert = String.format(template, fieldName, fieldName, fieldName);
                sb.append(convert+",");
                break;
            case LONG:
                String customT = tableConfig.getCasts().get("LONG");
                String dateTemplate = "CAST(CAST(to_unixtime(CAST(%s AS timestamp)) AS DOUBLE)*1000 AS BIGINT) as %s";
                if (customT != null && !customT.isEmpty()) {
                    dateTemplate = customT;
                }
                String dateConvert = String.format(dateTemplate, fieldName, fieldName);
                sb.append(dateConvert+",");
                break;
            case NUMBER:
                String numTemplate = "CAST(%s AS VARCHAR) as %s";
                String num = String.format(numTemplate, fieldName, fieldName);
                sb.append(num+",");
                break;

        }
    }

    private void addFieldToAthenaSchema(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        switch (fieldConfig.getType()) {
            case STRING:
            case ARRAY:
                sb.append("`"+fieldName+"`"+" string " + ",\n");
                break;
            case INTEGER:
                sb.append("`"+fieldName+"`"+" int " + ",\n");
                break;
            case LONG:
                sb.append("`"+fieldName+"`" + " int " + ",\n");
                break;
            case NUMBER:
                sb.append("`"+fieldName+"`" + " bigint " + ",\n");
                break;
            case FLOAT:
                sb.append("`"+fieldName+"`" + " float " + ",\n");
                break;
            case DOUBLE:
                sb.append("`"+fieldName+"`" + " double " + ",\n");
                break;
            case BOOLEAN:
                sb.append("`"+fieldName+"`" + " boolean " + ",\n");
                break;
            case JSON:
                // we should not get here, JSONs are being handled as STRINGs
                break;
        }
    }
    private void addFieldToSchema(String fieldName, FieldConfig fieldConfig, StringBuilder sb) {
        String requiredStr = fieldConfig.isRequired() ? " required" : " optional";
        switch (fieldConfig.getType()) {
            case STRING:
            case ARRAY:
                sb.append(requiredStr + " binary "+ fieldName + " (STRING);\n");
                break;
            case INTEGER:
                sb.append(requiredStr + " int32 "+ fieldName + ";\n");
                break;
            case LONG:
                sb.append(requiredStr + " int64 "+ fieldName + ";\n");
                break;
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
            case JSON:
                // we should not get here, JSONs are being handled as STRINGs
                break;
        }
    }


    public class TableDumpResult {
        String filepath;
        String error = null;
    }

    private Callable<Result> getWriteTask(int shard, String day, boolean isShardNull, boolean isFromAthena) {
        if (isFromAthena) {
            return new S3ParquetFromAthenaWriteTask(day);
        } else {
            return new S3ParquetWriteTask(shard, day, isShardNull);
        }
    }

    private Callable<Result> getWriteTask(int shard, String day, boolean isFromAthena) {
        if (isFromAthena) {
            return new S3ParquetFromAthenaWriteTask(day);
        } else {
            return new S3ParquetWriteTask(shard, day);
        }
    }
    private Callable<Result> getAthenaWriteTask(int minShard, int maxShard, String day) {
        return new S3ParquetFromAthenaWriteTask(day, minShard, maxShard);
    }
    public TableDumpResult dumpTable(String day, String snapshotPath) throws InterruptedException, SQLException, ClassNotFoundException {
        TableDumpResult res = new TableDumpResult();
        logger.info("Creating table "+tableConfig.getTableName()+ "from snapshot");
        TrackerTimer timer = new TrackerTimer();
        String tableName = null;
        List<String> viewTablesNames = null;
        try {
            if (!tableConfig.getAthenaTableName().isEmpty()) {
                tableName = tableConfig.getAthenaTableName();
            }
            else if (tableConfig.getAthenaTableView().size() > 0) {
                CreateViewResult cvr = createViewFromAthena(day, snapshotPath);
                tableName = cvr.viewName;
                viewTablesNames = cvr.tableNames;
            }
            else if (tableConfig.getViewTables().size() > 0) {
                CreateViewResult cvr = createViewFromSnapshot(day, snapshotPath);
                tableName = cvr.viewName;
                viewTablesNames = cvr.tableNames;
            } else {
                tableName = createTableFromSnapshot(day, snapshotPath);
            }

            String select = getConvertSelectPart(tableConfig);

            //iterate over all relevant shards
            ArrayList<Future<Result>> writings = new ArrayList<Future<Result>>();
            for (Map.Entry<String, String> entry : tableConfig.getSplitConfig().entrySet()) {
                String application = entry.getKey();
                String platform = entry.getValue();
                if (tableConfig.getSharded()) {
                    int numShards = tableConfig.getShardsNumber();
                    if (numShards <= 0) {
                        numShards = config.getShardsNumber();
                    }
                    if (tableConfig.isDumpShardNull()) {
                        Callable<Result> w = new SplitConvertTask(day, tableName, snapshotPath, select, -1, true, tableConfig.getSharded(), application, platform);
                        Future<Result> future = executor.submit(w);
                        writings.add(future);
                    }
                    int multiShardNum = tableConfig.getMultiShardNum();
                    if (multiShardNum <= 0) {
                        multiShardNum = Constants.MULTI_SHARDS_NUM;
                    }
                    for (int shard=0; shard<numShards; shard +=multiShardNum) {
                        Callable<Result> w = new MultiShardSplitTask(day, tableName, snapshotPath, select, shard, shard+multiShardNum, application, platform,false, tableConfig.getSharded());
                        Future<Result> future = executor.submit(w);
                        writings.add(future);
                    }
                } else {
                    Callable<Result> w = new ShardConvertTask(day, tableName, snapshotPath, select, 0, false, tableConfig.getSharded());
                    Future<Result> future = executor.submit(w);
                    writings.add(future);
                }
            }

            logger.info("writing " + writings.size() + " files.");
            ArrayList<String> errors = new ArrayList<>();
            ArrayList<String> paths = new ArrayList<>();
            for (Future<Result> item : writings)
            {
                try {
                    Result result = item.get();
                    logger.info("rfinished eading results of:"+result.filePath);
                    if (result.error != null) {
                        errors.add(result.filePath + ": " + result.error);
                    } else {
                        paths.add(result.filePath);
                    }
                }
                catch (Exception e) {
                    errors.add(e.toString());
                }
            }

            if (!errors.isEmpty()) {
                logger.error("errors during writing: " + errors.toString());
                res.error = errors.toString();
                return res;
            }
            logger.info("finished dumping table "+tableConfig.getSchemaName()+"."+tableConfig.getTableName()+" in "+timer.getTotalTime()+" seconds");
            res.filepath = paths.toString();
            return res;
        } finally {
            if (tableName!=null) {
                if (viewTablesNames!=null) {
                    this.athenaHandler.dropView(tableName);
                } else {
                    if (tableConfig.getAthenaTableName().isEmpty()) {
//                        this.athenaHandler.dropTable(tableName);
                    }
                }
            }
            if (viewTablesNames!=null) {
                for (String tblName : viewTablesNames) {
//                    this.athenaHandler.dropTable(tblName);
                }
            }
            executor.shutdown();
        }

    }

    private String createTableFromSnapshot(String day, String snapshotPath) throws InterruptedException, SQLException, ClassNotFoundException {
        String schema = tableConfig.getSchemaName();
        String tableName = tableConfig.getTableName();
        String fieldsConfig = getAthenaFieldsConfig(schema, tableName);
        String athenaTableName = athenaHandler.createAthenaTable(schema, tableName, day, fieldsConfig, snapshotPath);
        return athenaTableName;
    }

//    private CreateViewResult createViewFromSnapshot(String day, String snapshotPath) throws SQLException, ClassNotFoundException, InterruptedException {
//        String schema = tableConfig.getSchemaName();
//        String view = tableConfig.getTableName();
//        Map<String, String> athenaTables = new HashMap<>();
//        for (Map.Entry<String,String> entry : tableConfig.getViewTables().entrySet()) {
//            String tableName = entry.getKey();
//            String joinColumn = entry.getValue();
//            String fieldsConfig = getAthenaFieldsConfig(schema, tableName);
//            String athenaTableName = athenaHandler.createAthenaTable(schema, tableName, day, fieldsConfig, snapshotPath);
//            athenaTables.put(athenaTableName, joinColumn);
//        }
//        String createdViewName = athenaHandler.createAthenaView(schema, view, athenaTables, day, "", snapshotPath);
//        return new CreateViewResult(createdViewName, new ArrayList<>(athenaTables.keySet()));
//    }

    private CreateViewResult createViewFromSnapshot(String day, String snapshotPath) throws SQLException, ClassNotFoundException, InterruptedException {
        String schema = tableConfig.getSchemaName();
        String view = tableConfig.getTableName();
        Map<String, TableViewConfig> athenaTables = new HashMap<>();
        for (Map.Entry<String,TableViewConfig> entry : tableConfig.getViewTables().entrySet()) {
            String tableName = entry.getKey();
            TableViewConfig viewConfig = entry.getValue();
            String fieldsConfig = getAthenaFieldsConfig(schema, tableName);
            String athenaTableName = athenaHandler.createAthenaTable(schema, tableName, day, fieldsConfig, snapshotPath);
            athenaTables.put(athenaTableName, viewConfig);
        }
        String createdViewName = athenaHandler.createAthenaView(schema, view, athenaTables, day, "", snapshotPath);
        return new CreateViewResult(createdViewName, new ArrayList<>(athenaTables.keySet()));
    }

    private CreateViewResult createViewFromAthena(String day, String snapshotPath) throws SQLException, ClassNotFoundException, InterruptedException {
        String schema = tableConfig.getSchemaName();
        String view = tableConfig.getTableName();
        Map<String, TableViewConfig> athenaTables = new HashMap<>();
        for (Map.Entry<String,TableViewConfig> entry : tableConfig.getAthenaTableView().entrySet()) {
            String tableName = entry.getKey();
            TableViewConfig viewConfig = entry.getValue();
//            String fieldsConfig = getAthenaFieldsConfig(schema, tableName);
            String athenaTableName = tableName;
            athenaTables.put(athenaTableName, viewConfig);
        }
        String createdViewName = athenaHandler.createAthenaViewForDay(schema, view, athenaTables, day, "", snapshotPath);
        return new CreateViewResult(createdViewName, new ArrayList<>(athenaTables.keySet()));
    }

    class CreateViewResult {
        String viewName;
        List<String> tableNames;

        public CreateViewResult(String viewName, List<String> tableNames) {
            this.viewName = viewName;
            this.tableNames = tableNames;
        }
    }
    class MultiShardConvertTask implements Callable {
        String day;
        String athenaTableName;
        String snapshotPath;
        String select;
        int minShard;
        int maxShard;
        boolean isShardNull;
        boolean sharded;
        private ThreadPoolExecutor executor;

        public MultiShardConvertTask(String day, String athenaTableName, String snapshotPath, String select, int minShard, int maxShard, boolean isShardNull, boolean sharded) {
            this.day = day;
            this.athenaTableName = athenaTableName;
            this.snapshotPath = snapshotPath;
            this.minShard = minShard;
            this.maxShard = maxShard;
            this.select = select;
            this.isShardNull = isShardNull;
            this.sharded = sharded;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getCopyThreads());
        }

        public Result call() throws InterruptedException, IOException {
            Result res = new Result();
            try {
                String tempDestinationFolder = composeTempFolder(tableConfig, athenaTableName, minShard, maxShard, day);
//                String location = athenaHandler.convertAthenaTableMultiSharded(athenaTableName, minShard, maxShard, tempDestinationFolder, select, tableConfig.getOrderByField());
                String location = athenaHandler.repairAthenaTableMultiSharded(athenaTableName, minShard, maxShard, tempDestinationFolder, select, tableConfig.getOrderByField(), day);

                logger.info("converted to location:"+location);
                //copy the data to the right location
                //list all files
                String locationPrefix = StringUtils.removeStart(location, "s3://"+config.getExportS3Bucket()+"/");
                ArrayList<Future<Result>> writings = new ArrayList<Future<Result>>();
                for (int shard=minShard; shard<maxShard; ++shard) {
                    String destinationFolder = composeFilePath(tableConfig, shard, day);
                    CopyFilesTask task = new CopyFilesTask(locationPrefix, day, destinationFolder, shard);
                    Future<Result> future = this.executor.submit(task);
                    writings.add(future);
                }

                ArrayList<String> errors = new ArrayList<>();
                ArrayList<String> paths = new ArrayList<>();
                for (Future<Result> item : writings)
                {
                    try {
                        Result result = item.get();
                        logger.debug("finished reading copy results of:"+result.filePath);
                        if (result.error != null) {
                            errors.add(result.filePath + ": " + result.error);
                        } else {
                            paths.add(result.filePath);
                        }
                    }
                    catch (Exception e) {
                        errors.add(e.toString());
                    }
                }

                if (!errors.isEmpty()) {
                    logger.error("errors during copy: " + errors.toString());
                    res.error = errors.toString();
                    return res;
                }
                logger.info("copied files for location:"+tempDestinationFolder);


            } catch (Exception  e) {
                e.printStackTrace();
                logger.error("error converting athena table:"+e.getMessage());
                throw e;
            }
            return res;
        }

        private String composeTempFolder(TableSplitConfig tableConfig, String athenaTableName, int minShard, int maxShard, String day) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix+"repairs"+File.separator);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append("day=" + day+File.separator);
            sb.append("shard" + minShard + "to" + maxShard);
            return sb.toString();
        }

        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day) {
            StringBuilder sb = new StringBuilder(tableConfig.getS3RootFolder()+File.separator);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
    }
    class MultiShardSplitTask implements Callable {
        String day;
        String athenaTableName;
        String snapshotPath;
        String select;
        int minShard;
        int maxShard;
        private String application;
        private String platform;
        boolean isShardNull;
        boolean sharded;
        private ThreadPoolExecutor executor;

        public MultiShardSplitTask(String day, String athenaTableName, String snapshotPath, String select, int minShard, int maxShard, String application, String platform, boolean isShardNull, boolean sharded) {
            this.day = day;
            this.athenaTableName = athenaTableName;
            this.snapshotPath = snapshotPath;
            this.minShard = minShard;
            this.maxShard = maxShard;
            this.select = select;
            this.application = application;
            this.platform = platform;
            this.isShardNull = isShardNull;
            this.sharded = sharded;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getCopyThreads());
        }

        public Result call() throws InterruptedException, IOException {
            Result res = new Result();
            try {
                String tempDestinationFolder = composeTempFolder(tableConfig, athenaTableName, minShard, maxShard, day, application);
//                String location = athenaHandler.convertAthenaTableMultiSharded(athenaTableName, minShard, maxShard, tempDestinationFolder, select, tableConfig.getOrderByField());
                String location = athenaHandler.splitAthenaTableMultiSharded(athenaTableName, minShard, maxShard, tempDestinationFolder, select, tableConfig.getOrderByField(), day, application, platform);

                logger.info("converted to location:"+location);
                //copy the data to the right location
                //list all files
                String locationPrefix = StringUtils.removeStart(location, "s3://"+config.getExportS3Bucket()+"/");
                ArrayList<Future<Result>> writings = new ArrayList<Future<Result>>();
                for (int shard=minShard; shard<maxShard; ++shard) {
                    String destinationFolder = composeFilePath(tableConfig, shard, day,application);
                    CopyFilesTask task = new CopyFilesTask(locationPrefix, day, destinationFolder, shard);
                    Future<Result> future = this.executor.submit(task);
                    writings.add(future);
                }

                ArrayList<String> errors = new ArrayList<>();
                ArrayList<String> paths = new ArrayList<>();
                for (Future<Result> item : writings)
                {
                    try {
                        Result result = item.get();
                        logger.debug("finished reading copy results of:"+result.filePath);
                        if (result.error != null) {
                            errors.add(result.filePath + ": " + result.error);
                        } else {
                            paths.add(result.filePath);
                        }
                    }
                    catch (Exception e) {
                        errors.add(e.toString());
                    }
                }

                if (!errors.isEmpty()) {
                    logger.error("errors during copy: " + errors.toString());
                    res.error = errors.toString();
                    return res;
                }
                logger.info("copied files for location:"+tempDestinationFolder);


            } catch (Exception  e) {
                e.printStackTrace();
                logger.error("error converting athena table:"+e.getMessage());
                throw e;
            }
            this.executor.shutdown();
            return res;
        }
        //
        private String composeTempFolder(TableSplitConfig tableConfig, String athenaTableName, int minShard, int maxShard, String day, String application) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix+"splits5"+File.separator);
            sb.append(application+File.separator);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append("day=" + day+File.separator);
            sb.append("shard" + minShard + "to" + maxShard);
            return sb.toString();
        }

        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day, String application) {
            StringBuilder sb = new StringBuilder(tableConfig.getS3RootFolder()+File.separator);
            sb.append(application+File.separator);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
    }
    class CopyFilesTask implements Callable {
        private String locationPrefix;
        private String day;
        private String destinationFolder;
        private int shard;

        CopyFilesTask(String locationPrefix, String day, String destinationFolder, int shard) {
            this.locationPrefix = locationPrefix;
            this.day = day;
            this.destinationFolder = destinationFolder;
            this.shard = shard;
        }
        public Result call() {
            Result res = new Result();
            res.filePath = destinationFolder;
            try {
                /// clean destination folder
                List<String> oldFiles = destDataSerializer.listFilesInFolder(destinationFolder+File.separator);
                String folder = destinationFolder+File.separator;
                if (!oldFiles.isEmpty()) {
                    logger.info("deleting "+oldFiles.size()+" old files");
                    destDataSerializer.deleteFiles(folder, oldFiles, "weshouldnotskipanyfile");
                }
                ////
                String tempShardLocation = locationPrefix+File.separator+"shard="+shard+File.separator;
                List<String> files = dataSerializer.listFilesInFolder(tempShardLocation);
                logger.info(files.size() + " files written for "+tempShardLocation);
                if (files.isEmpty()) {
                    logger.warn("no files written for "+locationPrefix);
                }
                logger.debug("copying "+files.size()+" files for shard "+shard);
                for (String file : files) {
                    dataSerializer.copyFile(destinationFolder+File.separator+file+".snappy.parquet",tableConfig.getS3Bucket(),tempShardLocation+file);
                }
            } catch (IOException e) {
                logger.error("error during copying temp files to"+destinationFolder+":"+e.getMessage());
                res.error = "error during copying temp files to"+destinationFolder+":"+e.getMessage();
            }
            return res;
        }
    }
    class ShardConvertTask implements Callable {
        String day;
        String athenaTableName;
        String snapshotPath;
        String select;
        int shard;
        boolean isShardNull;
        boolean sharded;
        private ThreadPoolExecutor executor;

        public ShardConvertTask(String day, String athenaTableName, String snapshotPath, String select, int shard, boolean isShardNull, boolean sharded) {
            this.day = day;
            this.athenaTableName = athenaTableName;
            this.snapshotPath = snapshotPath;
            this.shard = shard;
            this.select = select;
            this.isShardNull = isShardNull;
            this.sharded = sharded;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        public Result call() throws InterruptedException, IOException, ExecutionException {
            Result res = new Result();
            try {
                String tempDestinationFolder = composeTempFolder(tableConfig, athenaTableName, shard, day);
//                String destinationFolder = composeFilePath(tableConfig, shard, day);
                String location = "";
                if (!this.sharded) {
                    location = athenaHandler.convertAthenaTableNotSharded(athenaTableName, shard, tempDestinationFolder, select, tableConfig.getOrderByField(), tableConfig.getSchemaName(), tableConfig.getTableName(), day);
                }
                else if (this.isShardNull) {
                    location = athenaHandler.repairAthenaTableShardNull(athenaTableName, tempDestinationFolder, select, tableConfig.getOrderByField(), day);
                } else {
                    location = athenaHandler.convertAthenaTable(athenaTableName, shard, tempDestinationFolder, select, tableConfig.getOrderByField(), tableConfig.getSchemaName(), tableConfig.getTableName());
                }
                logger.info("converted to location:"+location);
//                copy the data to the right location
//                String destinationFolder = StringUtils.removeStart(composeFilePath(tableConfig, shard, day),config.getS3Bucket()+File.separator);
                //list all files
                String sourceFolder = composeSourceFolder(tableConfig, athenaTableName, shard, day);
                String locationPrefix = StringUtils.removeStart(sourceFolder, "s3://"+config.getExportS3Bucket()+"/");
                String destinationFolder = composeDestFilePath(tableConfig, shard, day);
                CopyFilesTask task = new CopyFilesTask(locationPrefix, day, destinationFolder, shard);
                Future<Result> future = this.executor.submit(task);
                Result result = future.get();
                logger.debug("finished reading copy results of:"+result.filePath);
                if (result.error != null) {
                    logger.error("error during copy:"+result.error);
                } else {
                    logger.info("copied files for location:"+tempDestinationFolder);
                }
                return result;

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.error("error converting athena table:"+e.getMessage());
                throw e;
            }
        }

        private String composeTempFolder(TableSplitConfig tableConfig, String athenaTableName, int shard, String day) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix+"repairs"+File.separator);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append("day=" + day+File.separator);
            sb.append("shard=" + shard);
            return sb.toString();
        }

        private String composeSourceFolder(TableSplitConfig tableConfig, String athenaTableName, int shard, String day) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append("day=" + day);
            return sb.toString();
        }

        private String composeDestFilePath(TableSplitConfig tableConfig, int shard, String day) {
            StringBuilder sb = new StringBuilder(tableConfig.getS3RootFolder()+File.separator);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day) {
            StringBuilder sb = new StringBuilder("s3://" +s3FilePrefix);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
    }
    class SplitConvertTask implements Callable {
        String day;
        String athenaTableName;
        String snapshotPath;
        String select;
        String application;
        String platform;
        int shard;
        boolean isShardNull;
        boolean sharded;
        private ThreadPoolExecutor executor;

        public SplitConvertTask(String day, String athenaTableName, String snapshotPath, String select, int shard, boolean isShardNull, boolean sharded, String application, String platform) {
            this.day = day;
            this.athenaTableName = athenaTableName;
            this.snapshotPath = snapshotPath;
            this.shard = shard;
            this.select = select;
            this.isShardNull = isShardNull;
            this.sharded = sharded;
            this.application = application;
            this.platform = platform;
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        public Result call() throws InterruptedException, IOException, ExecutionException {
            Result res = new Result();
            try {
                String tempDestinationFolder = composeTempFolder(tableConfig, athenaTableName, shard, day, application);
//                String destinationFolder = composeFilePath(tableConfig, shard, day);
                String location = "";
                if (!this.sharded) {
                    location = athenaHandler.convertAthenaTableNotSharded(athenaTableName, shard, tempDestinationFolder, select, tableConfig.getOrderByField(), tableConfig.getSchemaName(), tableConfig.getTableName(), day);
                }
                else if (this.isShardNull) {
                    location = athenaHandler.splitAthenaTableShardNull(athenaTableName, tempDestinationFolder, select, tableConfig.getOrderByField(), day, application, platform);
                } else {
                    location = athenaHandler.convertAthenaTable(athenaTableName, shard, tempDestinationFolder, select, tableConfig.getOrderByField(), tableConfig.getSchemaName(), tableConfig.getTableName());
                }
                logger.info("converted to location:"+location);
//                copy the data to the right location
//                String destinationFolder = StringUtils.removeStart(composeFilePath(tableConfig, shard, day),config.getS3Bucket()+File.separator);
                //list all files
                String sourceFolder = composeSourceFolder(tableConfig, athenaTableName, shard, day, application);
                String locationPrefix = StringUtils.removeStart(sourceFolder, "s3://"+config.getExportS3Bucket()+"/");
                String destinationFolder = composeDestFilePath(tableConfig, shard, day, application);
                CopyFilesTask task = new CopyFilesTask(locationPrefix, day, destinationFolder, shard);
                Future<Result> future = this.executor.submit(task);
                Result result = future.get();
                logger.debug("finished reading copy results of:"+result.filePath);
                if (result.error != null) {
                    logger.error("error during copy:"+result.error);
                } else {
                    logger.info("copied files for location:"+tempDestinationFolder);
                }
                return result;

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.error("error converting athena table:"+e.getMessage());
                throw e;
            }
        }

        private String composeTempFolder(TableSplitConfig tableConfig, String athenaTableName, int shard, String day, String application) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix+"splits5"+File.separator);
            sb.append(application+File.separator);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append("day=" + day+File.separator);
            sb.append("shard=" + shard);
            return sb.toString();
        }

        private String composeSourceFolder(TableSplitConfig tableConfig, String athenaTableName, int shard, String day, String application) {
            StringBuilder sb = new StringBuilder("s3://" +tempS3FilePrefix);
            sb.append(tableConfig.getSchemaName()+"."+tableConfig.getTableName()+File.separator);
            sb.append(application+File.separator);
            sb.append("day=" + day);
            return sb.toString();
        }

        private String composeDestFilePath(TableSplitConfig tableConfig, int shard, String day, String application) {
            StringBuilder sb = new StringBuilder(tableConfig.getS3RootFolder()+File.separator);
            sb.append(application+File.separator);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day) {
            StringBuilder sb = new StringBuilder("s3://" +s3FilePrefix);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day);
            return sb.toString();
        }
    }
    class Result {
        String error = null;
        String filePath;
    }

    class S3ParquetFromAthenaWriteTask implements Callable {
        String day;
        int minShard;
        int maxShard;
        S3ParquetFromAthenaWriteTask(String day) {
            this.day=day;
        }

        public S3ParquetFromAthenaWriteTask(String day, int minShard, int maxShard) {
            this(day);
            this.minShard=minShard;
            this.maxShard=maxShard;
        }

        private CustomParquetWriter<Group> getWriter(TableSplitConfig tableConfig, int shard, String day, MessageType schema) throws IOException {
            String writeResult = RESULT_SUCCESS;
            Long minOffset = (long)0;
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

            String filePath = composeFilePath(tableConfig, shard, day, minOffset);
            System.out.println("minOffset = " + minOffset + " shard = " + shard + " day = " + day);
            logger.debug("writing file:"+filePath);
            GroupWriteSupport.setSchema(schema, conf);

            CustomParquetWriter<Group> writer = RowsLimitationParquetWriter.builder(new Path(filePath))
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(config.getParquetRowGroupSize())
                    .withPageSize(config.getParquetRowGroupSize())
                    .withDictionaryEncoding(true)
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();
            return writer;
        }
        public Result call() {
            logger.info("start writing to parquet process from athena");
            Result res = new Result();
            AthenaClient client = athenaHandler.getAthenaClient();
            try {
                GetQueryResultsIterable resultIterable;
                if (!tableConfig.getSharded()) {
                    resultIterable = athenaHandler.getDbRecordsForAllTable(tableConfig.getSchemaName(), tableConfig.getTableName(), tableConfig.getOrderByField(), client);
                } else {
                    resultIterable = athenaHandler.getDbRecordsForShards(tableConfig.getSchemaName(), tableConfig.getTableName(), minShard, maxShard,client);
                }
                int retryCounter = 0;
                Long minOffset = (long)0;
                CustomParquetWriter<Group> writer = null;
                String writeResult = RESULT_SUCCESS;
                boolean isFirst = true;
                int currentShard = NO_SHARD;
                long numRecords = 0;
                MessageType schema = parseMessageType(parquetSchema);
                try {
                    for (GetQueryResultsResponse result : resultIterable) {
                        List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                        List<Row> results = result.resultSet().rows();
                        if (isFirst) {
                            //first row may be column names
                            results = removeHeaderRow(results,columnInfoList);
                        }
                        isFirst = false;
                        for (Row currRow : results) {
                            //iterate over all the rows in the batch
                            int shard = getShard(currRow, columnInfoList);
                            if (shard != currentShard) {
                                //create new writer for that shard
                                if (writer!=null) {
                                    //close previous file
                                    try {
                                        writer.close();
                                        logger.info("finished writing shard "+currentShard+" to S3 () ");
                                    } catch (IOException e) {
                                        logger.error("Fail closing parquet file for shard "+currentShard+": " + e.getMessage());
                                    }
                                }
                                writer = getWriter(tableConfig, shard, day, schema);
                                currentShard = shard;
                            }
                            //write the row
                            List<Datum> rowData = currRow.data();
                            numRecords++;
                            String recordResult = RESULT_SUCCESS;
                            try {
                                EconomicalGroup group = new EconomicalGroup(schema);
                                for (int i=0; i<columnInfoList.size(); ++i) {
                                    ColumnInfo currInfo = columnInfoList.get(i);
                                    String fieldName = currInfo.name();
                                    Datum datum = rowData.get(i);
                                    addFieldToGroup(fieldName, config.getFieldTypesMap().get(fieldName), group, datum, currInfo);
                                }
                                writer.write(group);
                            } catch (Exception e) {
                                logger.error("****************** error in record: " + currRow.toString() + ":\n" + e.getMessage());
                                System.out.println("****************** error in record: " + currRow.toString() + ":\n" + e.getMessage());
                                e.printStackTrace();
                                recordResult = RESULT_ERROR;
                            }
                            ParquetSplitter.filesWrittenCounter.labels(RESULT_SUCCESS).inc();
                            ParquetSplitter.filesSizeBytesSummary.observe(writer.getDataSize());
                        }
                    }
                    //after reading all data close the writer
                    if (writer != null) {
                        try {
                            writer.close();
                            logger.info("finished writing shard "+currentShard+" to S3 ("+numRecords+") ");
                        } catch (IOException e) {
                            logger.error("Fail closing parquet file for shard "+currentShard+": " + e.getMessage());
                        }
                    }
                    return res;
                } catch (IOException e) {
                    logger.error("Fail writing file to S3: " + e.getMessage());
                    System.out.println("Fail writing file to S3: " + e.getMessage());
                    e.printStackTrace();
                    ++retryCounter;
                    res.error = e.getMessage();
                    writeResult = RESULT_ERROR;
                } finally {
                    ParquetSplitter.filesWrittenCounter.labels(writeResult).inc();
                }
            } catch (Exception e) {
                logger.error("Fail writing Athena table "+tableConfig.getTableName()+": " + e.getMessage());
                e.printStackTrace();
            } finally {
                client.close();
            }
            return res;
        }

        private int getShard(Row currRow, List<ColumnInfo> columnInfoList) {
            for (int i=0; i<columnInfoList.size();++i) {
                ColumnInfo info = columnInfoList.get(i);
                String name = info.name();
                if (name!=null && name.equalsIgnoreCase("shard")) {
                    String shardStr = currRow.data().get(i).varCharValue();
                    if (shardStr==null) {
                        return SHARD_NULL;
                    }
                    Integer shard = new Integer(shardStr);
                    return shard;
                }
            }
            logger.error("did not find shard");
            return NO_SHARD;
        }

        private List<Row> removeHeaderRow(List<Row> results, List<ColumnInfo> columnInfoList) {
            if (results.size() <= 0) return results;
            Row first = results.get(0);
            List<Datum> allData = first.data();
            boolean equals = true;
            for (int i=0; i<columnInfoList.size();++i) {
                ColumnInfo currInfo = columnInfoList.get(i);
                String currName = currInfo.name();
                Datum currData = allData.get(i);
                String currVal = currData.varCharValue();
                if (!currName.equalsIgnoreCase(currVal)) {
                    equals = false;
                    break;
                }
            }
            if (equals) {
                List<Row> toRet = new ArrayList<>(results);
                toRet.remove(0);
                return toRet;
            }
            return results;
        }


        private Map<String, ColumnInfo> getColumnsInfoMap(List<ColumnInfo> columnInfos) {
            Map<String, ColumnInfo> toRet = new HashMap<>();
            for (ColumnInfo info : columnInfos) {
                String key = info.name();
                toRet.put(key, info);
            }
            return toRet;
        }

        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day, long firstRecordOffset) {
            StringBuilder sb = new StringBuilder("s3a://" + s3FilePrefix);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day + File.separator);
            if (tableConfig.getSharded()) {
                sb.append(shard + "_");
            }
            sb.append(day + "_" + firstRecordOffset + ".snappy.parquet");
            return sb.toString();
        }

        private void addFieldToGroup(String fieldName, FieldConfig fieldConfig, EconomicalGroup group, Datum source, ColumnInfo columnInfo) throws SQLException, ParseException {
            switch (fieldConfig.getType()) {
                case STRING:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    addValue(group, fieldName, source, fieldConfig.getType(), columnInfo);
                    break;
                case ARRAY:
                    addArray(group, fieldName, source, columnInfo);
                case JSON:
                    //not relevant for now, JSON type is treated as STRING
                    break;
            }
        }

        private void addArray(Group group, String fieldName, Datum source, ColumnInfo columnInfo) throws SQLException {
            String str = source.varCharValue();
            if (str != null) {
                if (str.startsWith("{")) {
                    str = StringUtils.removeStart(str, "{");
                }
                if (str.endsWith("}")) {
                    str = StringUtils.removeEnd(str, "}");
                }

                String[] arr = str.split(",");
                List<String> items = Arrays.asList(arr);
                JSONArray jsonArr = new JSONArray();
                for (String obj : items) {
                    jsonArr.put(obj);
                }
                String res = jsonArr.toString();
                group.append(fieldName, res);
            }
        }

        private void addValue(Group group, String fieldName, Datum source, FieldConfig.FIELD_TYPE type, ColumnInfo columnInfo) throws ParseException {
            addValueByType(group, fieldName, source, type);
        }
        private Long convertStringToDate(String dateStr) throws ParseException {
            if (dateStr!=null && !dateStr.isEmpty()) {
                SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
                SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date date = formatter1.parse(dateStr);
                    return date.getTime();
                } catch (ParseException e) {
                    try {
                        //could not parse with milliseconds, try without
                        Date date = formatter2.parse(dateStr);
                        return date.getTime();
                    } catch (ParseException e2) {
                        logger.error("error parsing date string " + dateStr + ":" + e2.getMessage() + " (first exception was:" + e.getMessage() + ")");
                        e.printStackTrace();
                        throw e;
                    }
                }
            } else {
                return null;
            }
        }

        private void addValueByType(Group group, String fieldName, Datum source, FieldConfig.FIELD_TYPE type) throws ParseException {
            String str = source.varCharValue();
            switch (type) {
                case STRING:
                    if (str != null && !str.isEmpty()) {
                        group.add(fieldName, str);
                    }
                    break;
                case INTEGER:
                    if (str != null && !str.isEmpty()) {
                        Integer intVal = new Integer(str);
                        if (intVal != null) {
                            group.add(fieldName, intVal.intValue());
                        }
                    }
                    break;
                case LONG:
                    if (str != null && !str.isEmpty()) {
                        Long longVal = convertStringToDate(str);
                        if (longVal != null) {
                            group.add(fieldName, longVal.longValue());
                        }
                    }
                    break;
                case FLOAT:
                    if (str != null && !str.isEmpty()) {
                        Float floatVal = new Float(str);
                        if (floatVal != null) {
                            group.add(fieldName, floatVal.floatValue());
                        }
                    }
                    break;
                case DOUBLE:
                    if (str != null && !str.isEmpty()) {
                        Double doubleVal = new Double(str);
                        if (doubleVal != null) {
                            group.add(fieldName, doubleVal.doubleValue());
                        }
                    }
                    break;
                case BOOLEAN:
                    if (str != null && ! str.isEmpty()) {
                        Boolean boolVal = new Boolean(str);
                        if (boolVal != null) {
                            group.add(fieldName, boolVal.booleanValue());
                        }
                    }
                    break;
            }
        }
    }
    class S3ParquetWriteTask implements Callable {


        int shard;
        String day;
        boolean isShardNull;


        S3ParquetWriteTask(int shard, String day, boolean isShardNull) {
            this(shard,day);
            this.isShardNull=isShardNull;
        }
        S3ParquetWriteTask(int shard, String day) {
            this.shard = shard;
            this.day=day;
        }

        public Result call() {
            logger.debug("start writing to parquet process");
            Result res = new Result();
            DbHandler.ResultSetWithResources resultSetWithResources = null;
            ResultSet rs = null;
            try {

                if (tableConfig.getSharded()) {
                    if (isShardNull) {
                        resultSetWithResources = dbHandler.getDBRecordsForTableForShardNull(tableConfig.getSchemaName(), tableConfig.getTableName(), tableConfig.getOrderByField());
                    } else {
                        resultSetWithResources = dbHandler.getDbRecordsForTable(tableConfig.getSchemaName(), tableConfig.getTableName(), shard, tableConfig.getOrderByField());
                    }
                } else {
                    resultSetWithResources = dbHandler.getDbRecordsForTable(tableConfig.getSchemaName(), tableConfig.getTableName(), tableConfig.getOrderByField());
                }
                rs = resultSetWithResources.getResult();
                int retryCounter = 0;
                Long minOffset = (long)0;
                CustomParquetWriter<Group> writer = null;
                while (retryCounter < config.getWriteRetries()) {
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

                        String filePath = composeFilePath(tableConfig, shard, day, minOffset);
                        System.out.println("minOffset = " + minOffset + " shard = " + shard + " day = " + day);
                        logger.debug("writing file:"+filePath);
                        res.filePath = filePath;
                        res.error = null;

                        MessageType schema = parseMessageType(parquetSchema);
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

                        long numRecords = 0;
                        while (rs.next()) {
                            numRecords++;
                            String recordResult = RESULT_SUCCESS;

                            try {
                                EconomicalGroup group = new EconomicalGroup(schema);
                                Set<String> fieldNames = config.getFieldTypesMap().keySet();
                                for (String fieldName : fieldNames) {
                                    addFieldToGroup(fieldName, config.getFieldTypesMap().get(fieldName), group, rs);
                                }
                                writer.write(group);
                            } catch (Exception e) {
                                logger.error("****************** error in record: " + rs.toString() + ":\n" + e.getMessage());
                                System.out.println("****************** error in record: " + rs.toString() + ":\n" + e.getMessage());
                                e.printStackTrace();
                                recordResult = RESULT_ERROR;
                            }

                            ParquetSplitter.recordsProcessedCounter.labels(recordResult).inc();
                        }

                        ParquetSplitter.filesWrittenCounter.labels(RESULT_SUCCESS).inc();
                        ParquetSplitter.filesSizeBytesSummary.observe(writer.getDataSize());
                        if (writer != null) {
                            try {
                                writer.close();
                                logger.debug("finished writing shard "+shard+" to S3 ("+numRecords+") "+filePath);
                            } catch (IOException e) {
                                logger.error("Fail closing parquet file for shard "+shard+": " + e.getMessage());
                            }
                        }
                        return res;
                    } catch (IOException | SQLException e) {
                        logger.error("Fail writing file to S3: " + e.getMessage());
                        System.out.println("Fail writing file to S3: " + e.getMessage());
                        e.printStackTrace();
                        ++retryCounter;
                        res.error = e.getMessage();
                        writeResult = RESULT_ERROR;
                    } finally {
                        if (resultSetWithResources != null) {
                            resultSetWithResources.closeResources();
                        }
                        ParquetSplitter.filesWrittenCounter.labels(writeResult).inc();
//                            if (writer != null) {
//                                try {
//                                    writer.close();
//                                    logger.info("finished writing shard "+shard+" to S3");
//                                } catch (IOException e) {
//                                    logger.error("Fail closing parquet file: " + e.getMessage());
//                                }
//                            }
                    }
                }

            } catch (SQLException | ClassNotFoundException e) {
                logger.error("Fail writing file to S3 for shard "+shard+": " + e.getMessage());
                System.out.println("Fail writing file to S3 for shard "+shard+": " + e.getMessage());
                e.printStackTrace();
            }
            return res;
        }

        private String composeFilePath(TableSplitConfig tableConfig, int shard, String day, long firstRecordOffset) {
            StringBuilder sb = new StringBuilder("s3a://" + s3FilePrefix);
            if (tableConfig.getSharded()) {
                sb.append("shard=" + shard + File.separator);
            }
            sb.append("day=" + day + File.separator);
            if (tableConfig.getSharded()) {
                sb.append(shard + "_");
            }
            sb.append(day + "_" + firstRecordOffset + ".snappy.parquet");
            return sb.toString();
        }

        private void addFieldToGroup(String fieldName, FieldConfig fieldConfig, EconomicalGroup group, ResultSet source) throws SQLException {
            switch (fieldConfig.getType()) {
                case STRING:
                case INTEGER:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    addValue(group, fieldName, source, fieldConfig.getType());
                    break;
                case ARRAY:
                    addArray(group, fieldName,
                            source);
                case JSON:
                    //not relevant for now, JSON type is treated as STRING
                    break;
            }
        }
        private void addArray(Group group, String fieldName, ResultSet source) throws SQLException {
            Array array = source.getArray(fieldName);
            if (array != null) {
                JSONArray jsonArr = new JSONArray();
                Object[] objsArr = (Object[]) array.getArray();
                for (Object obj : objsArr) {
                    if (obj instanceof Timestamp) {
                        Timestamp ts = (Timestamp) obj;
                        jsonArr.put(ts.getTime());
                    } else {
                        jsonArr.put(obj);
                    }
                }
                String str = jsonArr.toString();
                group.append(fieldName, str);
            }
        }
        private void addValue(Group group, String fieldName, ResultSet source, FieldConfig.FIELD_TYPE type) throws SQLException {
            switch (type) {
                case STRING:
                    String strVal = source.getString(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, strVal);
                    }
                    break;
                case INTEGER:
                    int intVal = source.getInt(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, intVal);
                    }
                    break;
                case LONG:
                    Timestamp tsVal = source.getTimestamp(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, tsVal.getTime());
                    }
                    break;
                case FLOAT:
                    float floatVal = source.getFloat(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, floatVal);
                    }
                    break;
                case DOUBLE:
                    double doubleVal = source.getDouble(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, doubleVal);
                    }
                    break;
                case BOOLEAN:
                    boolean boolVal = source.getBoolean(fieldName);
                    if (!source.wasNull()) {
                        group.add(fieldName, boolVal);
                    }
                    break;
            }
        }
    }
}
