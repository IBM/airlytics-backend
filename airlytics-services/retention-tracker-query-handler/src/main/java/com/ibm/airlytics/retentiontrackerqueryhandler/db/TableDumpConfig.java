package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import java.util.HashMap;
import java.util.Map;

public class TableDumpConfig {

    private String tableName;
    private Map<String, TableViewConfig> viewTables = new HashMap<>();
    private String dbTableName = null;
    private String schemaName;
    private String s3Bucket;
    private String s3RootFolder;
    private String orderByField;
    private Boolean sharded;
    private int shardsNumber;
    private boolean dumpShardNull;
    private int shardNullNumber = -1;
    private Boolean latestOnly = false;
    private boolean fromAthena = false;
    private int shardBatchSize = 1;
    private Map<String, String> casts = new HashMap<>();
    private int writeThreads = 0;
    private int multiShardNum = 0;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public String getS3RootFolder() {
        return s3RootFolder;
    }

    public void setS3RootFolder(String s3RootFolder) {
        this.s3RootFolder = s3RootFolder;
    }

    public Boolean getSharded() {
        return sharded;
    }

    public void setSharded(Boolean sharded) {
        this.sharded = sharded;
    }

    public String getOrderByField() {
        return orderByField;
    }

    public void setOrderByField(String orderByField) {
        this.orderByField = orderByField;
    }

    public Boolean getLatestOnly() {
        return latestOnly;
    }

    public void setLatestOnly(Boolean latestOnly) {
        this.latestOnly = latestOnly;
    }

    public int getShardsNumber() {
        return shardsNumber;
    }

    public void setShardsNumber(int shardsNumber) {
        this.shardsNumber = shardsNumber;
    }

    public boolean isDumpShardNull() {
        return dumpShardNull;
    }

    public void setDumpShardNull(boolean dumpShardNull) {
        this.dumpShardNull = dumpShardNull;
    }

    public int getShardNullNumber() {
        return shardNullNumber;
    }

    public void setShardNullNumber(int shardNullNumber) {
        this.shardNullNumber = shardNullNumber;
    }

    public boolean isFromAthena() {
        return fromAthena;
    }

    public void setFromAthena(boolean fromAthena) {
        this.fromAthena = fromAthena;
    }

    public String getDbTableName() {
        return dbTableName;
    }

    public void setDbTableName(String dbTableName) {
        this.dbTableName = dbTableName;
    }

    public int getShardBatchSize() {
        return shardBatchSize;
    }

    public void setShardBatchSize(int shardBatchSize) {
        this.shardBatchSize = shardBatchSize;
    }

    public Map<String, String> getCasts() {
        return casts;
    }

    public void setCasts(Map<String, String> casts) {
        this.casts = casts;
    }

    public Map<String, TableViewConfig> getViewTables() {
        return viewTables;
    }

    public void setViewTables(Map<String, TableViewConfig> viewTables) {
        this.viewTables = viewTables;
    }

    public int getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(int writeThreads) {
        this.writeThreads = writeThreads;
    }

    public int getMultiShardNum() {
        return multiShardNum;
    }

    public void setMultiShardNum(int multiShardNum) {
        this.multiShardNum = multiShardNum;
    }
}
