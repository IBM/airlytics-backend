package com.ibm.airlytics.consumer.persistence;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class PersistenceConsumerConfig extends AirlyticsConsumerConfig {
    private int shardsNumber;
    private int maxDumpRecords;
    private int maxDumpIntervalMs;
    private int writeThreads;
    private int parquetRowGroupSize;
    private  int writeRetries;

    private String storageType; //S3 or FILE_SYSTEM
    private String dataFolder; //the root folder of the processed data (for example parquet)

    private String scoresFolder; //the root folder of the processed data (for example parquetScores)

    //optional - not needed if storage type is FILE_SYSTEM
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3Bucket;
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3region;

    //optional - not needed if storage type is S3
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String baseFolder; //the base folder in the machine (for example /users/data)

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> commonFieldConfigs = new HashMap<>();

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> customDimensionsFieldConfigs = new HashMap<>();

    //Map between event to map between field name and its configuration
    private HashMap<String, HashMap<String, FieldConfig>> eventsFieldConfigs = new HashMap<>();

    //personal information configuration parameters
    private String s3PiBucket;
    private String piDataFolder;

    @JsonIgnoreProperties(ignoreUnknown = true)
    private boolean readAfterWrite=true;

    //optional - default is enlty list
    @JsonIgnoreProperties(ignoreUnknown = true)
    private Set<String> skippedEvents = new HashSet<>();

    private boolean automaticallyAddNewFields;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.PERSISTENCE_CONSUMER);

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

    public String getScoresFolder() {
        return scoresFolder;
    }

    public void setScoresFolder(String scoresFolder) {
        this.scoresFolder = scoresFolder;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getBaseFolder() {
        return baseFolder;
    }

    public void setBaseFolder(String baseFolder) {
        this.baseFolder = baseFolder;
    }

    public String getDataFolder() {
        return dataFolder;
    }

    public void setDataFolder(String dataFolder) {
        this.dataFolder = dataFolder;
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

    public void setCommonFieldTypesMap(HashMap<String, FieldConfig> fieldConfigs) {
        this.commonFieldConfigs = fieldConfigs;
    }

    public HashMap<String, FieldConfig> getCustomDimensionsFieldTypesMap() {
        return customDimensionsFieldConfigs;
    }

    public void setCustomDimensionsFieldTypesMap(HashMap<String, FieldConfig> fieldConfigs) {
        this.customDimensionsFieldConfigs = fieldConfigs;
    }

    public String getS3PIBucket() {
        return s3PiBucket;
    }

    public void setS3PiBucket(String s3PiBucket) {
        this.s3PiBucket = s3PiBucket;
    }

    public String getPiDataFolder() {
        return piDataFolder;
    }

    public void setPiDataFolder(String piDataFolder) {
        this.piDataFolder = piDataFolder;
    }

    public HashMap<String, HashMap<String, FieldConfig>> getEventsFieldTypesMap() {
        return eventsFieldConfigs;
    }

    public void setEventsFieldTypesMap(HashMap<String, HashMap<String, FieldConfig>> eventsFieldConfigs) {
        this.eventsFieldConfigs = eventsFieldConfigs;
    }

    public boolean getReadAfterWrite() {
        return readAfterWrite;
    }

    public void setReadAfterWrite(boolean readAfterWrite) {
        this.readAfterWrite = readAfterWrite;
    }

    public Set<String> getSkippedEvents() {
        return skippedEvents;
    }

    public void setSkippedEvents(Set<String> skippedEvents) {
        this.skippedEvents = skippedEvents;
    }

    public boolean isAutomaticallyAddNewFields() {
        return automaticallyAddNewFields;
    }

    public void setAutomaticallyAddNewFields(boolean automaticallyAddNewFields) {
        this.automaticallyAddNewFields = automaticallyAddNewFields;
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
        sb.append("s3region = ");
        sb.append(s3region);
        sb.append("\n");
        sb.append("storageType = ");
        sb.append(storageType);
        sb.append("\n");
        sb.append("dataFolder = ");
        sb.append(dataFolder);
        sb.append("\n");
        sb.append("scoresFolder = ");
        sb.append(scoresFolder);
        sb.append("\n");
        sb.append("baseFolder = ");
        sb.append(baseFolder);
        sb.append("\n");
        sb.append("s3PiBucket = ");
        sb.append(s3PiBucket);
        sb.append("\n");
        sb.append("piDataFolder = ");
        sb.append(piDataFolder);
        sb.append("\n");
        sb.append("readAfterWrite = ");
        sb.append(readAfterWrite);
        sb.append("\n");
        sb.append("automaticallyAddNewFields = ");
        sb.append(automaticallyAddNewFields);
        sb.append("\n");
        sb.append("skippedEvents = ");
        sb.append(skippedEvents.toString());
        sb.append("\n");
        return sb.toString();
    }
}