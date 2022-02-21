package com.ibm.airlytics.consumer.rawData;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;
import com.ibm.airlytics.consumer.persistence.FieldConfig;

import java.io.IOException;
import java.util.HashMap;

public class RawDataConsumerConfig extends AirlyticsConsumerConfig {
    private int maxDumpRecords;
    private int maxDumpIntervalMs;
    private int writeThreads;
    private int parquetRowGroupSize;
    private  int writeRetries;
    private String s3Bucket;
    private String s3RootFolder;
    private String s3region;
    private int shardsNumber;

    //personal information configuration parameters
    private String s3PiBucket;
    private String s3PiRootFolder;

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> commonFieldConfigs = new HashMap<>();

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> customDimensionsFieldConfigs = new HashMap<>();

    //Map between event to map between field name and its configuration
    private HashMap<String, HashMap<String, FieldConfig>> eventsFieldConfigs = new HashMap<>();


    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.RAWDATA_CONSUMER);

        String commonFieldConfig = getAirlockConfiguration(AirlockConstants.Consumers.COMMON_FIELDS);
        TypeReference<HashMap<String, FieldConfig>> commonTypeRef
                = new TypeReference<HashMap<String, FieldConfig>>() {};
        commonFieldConfigs = objectMapper.readValue(commonFieldConfig, commonTypeRef);

        String eventsFieldConfig = getAirlockConfiguration(AirlockConstants.Consumers.EVENTS_FIELDS);
        TypeReference<HashMap<String, HashMap<String, FieldConfig>>> eventsTypeRef
                = new TypeReference<HashMap<String, HashMap<String, FieldConfig>>>() {};
        eventsFieldConfigs = objectMapper.readValue(eventsFieldConfig, eventsTypeRef);

        String customDimensionsFieldConfig = getAirlockConfiguration(AirlockConstants.Consumers.CUSTOM_DIMENSIONS_FIELDS);
        TypeReference<HashMap<String, FieldConfig>> customDimensionsTypeRef
                = new TypeReference<HashMap<String, FieldConfig>>() {};
        customDimensionsFieldConfigs = objectMapper.readValue(customDimensionsFieldConfig, customDimensionsTypeRef);

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

    public String getS3region() {
        return s3region;
    }

    public void setS3region(String s3region) {
        this.s3region = s3region;
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

    public HashMap<String, FieldConfig> getCommonFieldTypesMap() {
        return commonFieldConfigs;
    }

    public int getShardsNumber() {
        return shardsNumber;
    }

    public void setShardsNumber(int shardsNumber) {
        this.shardsNumber = shardsNumber;
    }

    public String getS3PiBucket() {
        return s3PiBucket;
    }

    public void setS3PiBucket(String s3PiBucket) {
        this.s3PiBucket = s3PiBucket;
    }

    public String getS3PiRootFolder() {
        return s3PiRootFolder;
    }

    public void setS3PiRootFolder(String s3PiRootFolder) {
        this.s3PiRootFolder = s3PiRootFolder;
    }

    public void setCommonFieldTypesMap(HashMap<String, FieldConfig> fieldConfigs) {
        this.commonFieldConfigs = fieldConfigs;
    }

    public HashMap<String, HashMap<String, FieldConfig>> getEventsFieldTypesMap() {
        return eventsFieldConfigs;
    }

    public void setEventsFieldTypesMap(HashMap<String, HashMap<String, FieldConfig>> eventsFieldConfigs) {
        this.eventsFieldConfigs = eventsFieldConfigs;
    }

    public HashMap<String, FieldConfig> getCustomDimensionsFieldTypesMap() {
        return customDimensionsFieldConfigs;
    }

    public void setCustomDimensionsFieldTypesMap(HashMap<String, FieldConfig> fieldConfigs) {
        this.customDimensionsFieldConfigs = fieldConfigs;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
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
        sb.append("shardsNumber = ");
        sb.append(shardsNumber);
        sb.append("\n");
        sb.append("s3PiRootFolder = ");
        sb.append(s3PiRootFolder);
        sb.append("\n");
        sb.append("s3PiBucket = ");
        sb.append(s3PiBucket);
        sb.append("\n");
        return sb.toString();
    }
}