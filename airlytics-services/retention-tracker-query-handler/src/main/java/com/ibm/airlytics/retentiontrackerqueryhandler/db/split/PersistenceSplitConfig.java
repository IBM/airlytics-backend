package com.ibm.airlytics.retentiontrackerqueryhandler.db.split;

import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.AirlyticsConsumerConfig;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.FieldConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class PersistenceSplitConfig extends AirlyticsConsumerConfig {
    private int shardsNumber;
    private int maxDumpRecords;
    private int maxDumpIntervalMs;
    private int writeThreads;
    private int parquetRowGroupSize;
    private  int writeRetries;
    private int tableWriteThreads;
    private String s3Bucket;
    private String s3region;
    private String exportS3Bucket;
    private int copyThreads = 20;

    public String getExportTempPrefix() {
        return exportTempPrefix;
    }

    public void setExportTempPrefix(String exportTempPrefix) {
        this.exportTempPrefix = exportTempPrefix;
    }

    private String exportTempPrefix;

    public String getS3region() {
        return s3region;
    }

    public void setS3region(String s3region) {
        this.s3region = s3region;
    }

    public int getIoActionRetries() {
        return ioActionRetries;
    }

    public void setIoActionRetries(int ioActionRetries) {
        this.ioActionRetries = ioActionRetries;
    }

    private  int ioActionRetries;
    private String s3RootFolder;
    private List<TableSplitConfig> tables;


    //Map between field name and its configuration
    private HashMap<String, FieldConfig> fieldConfigs = new HashMap<>();

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.PERSISTENCE_CONSUMER_SPLITTER);

        fieldConfigs = new HashMap<>();
    }

    public int getShardsNumber() {
        return shardsNumber;
    }

    public void setShardsNumber(int shardsNumber) {
        this.shardsNumber = shardsNumber;
    }

    public int getMaxDumpRecords() {
        return maxDumpRecords;
    }

    public void setMaxDumpRecords(int maxDumpRecords) {
        this.maxDumpRecords = maxDumpRecords;
    }

    public int getMaxDumpIntervalMs() {
        return maxDumpIntervalMs;
    }

    public void setMaxDumpIntervalMs(int maxDumpIntervalMs) {
        this.maxDumpIntervalMs = maxDumpIntervalMs;
    }

    public int getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(int writeThreads) {
        this.writeThreads = writeThreads;
    }

    public int getParquetRowGroupSize() {
        return parquetRowGroupSize;
    }

    public void setParquetRowGroupSize(int parquetRowGroupSize) {
        this.parquetRowGroupSize = parquetRowGroupSize;
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

    public int getWriteRetries() {
        return writeRetries;
    }

    public void setWriteRetries(int writeRetries) {
        this.writeRetries = writeRetries;
    }

    public HashMap<String, FieldConfig> getFieldTypesMap()
    {
        return fieldConfigs;
    }

    public void setFieldTypesMap(HashMap<String, FieldConfig> fieldConfigs)
    {
        this.fieldConfigs = fieldConfigs;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("shardsNumber = ");
        sb.append(shardsNumber);
        sb.append("\n");
        sb.append("maxDumpRecords = ");
        sb.append(maxDumpRecords);
        sb.append("\n");
        sb.append("maxDumpIntervalMs = ");
        sb.append(maxDumpIntervalMs);
        sb.append("\n");
        sb.append("writeThreads = ");
        sb.append(writeThreads);
        sb.append("\n");
        sb.append("parquetRowGroupSize = ");
        sb.append(parquetRowGroupSize);
        sb.append("\n");
        sb.append("writeRetries = ");
        sb.append(writeRetries);
        sb.append("\n");
        sb.append("s3Bucket = ");
        sb.append(s3Bucket);
        sb.append("\n");
        sb.append("s3RootFolder = ");
        sb.append(s3RootFolder);
        sb.append("\n");
        return sb.toString();
    }

    public List<TableSplitConfig> getTables() {
        return tables;
    }

    public void setTables(List<TableSplitConfig> tables) {
        this.tables = tables;
    }

    public int getTableWriteThreads() {
        return tableWriteThreads;
    }

    public void setTableWriteThreads(int tableWriteThreads) {
        this.tableWriteThreads = tableWriteThreads;
    }

    public String getExportS3Bucket() {
        return exportS3Bucket;
    }

    public void setExportS3Bucket(String exportS3Bucket) {
        this.exportS3Bucket = exportS3Bucket;
    }

    public int getCopyThreads() {
        return copyThreads;
    }

    public void setCopyThreads(int copyThreads) {
        this.copyThreads = copyThreads;
    }
}