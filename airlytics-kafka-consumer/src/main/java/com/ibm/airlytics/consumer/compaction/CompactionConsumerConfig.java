package com.ibm.airlytics.consumer.compaction;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;
import com.ibm.airlytics.consumer.persistence.FieldConfig;

import java.io.IOException;
import java.util.HashMap;

public class CompactionConsumerConfig extends AirlyticsConsumerConfig {
    private int shardsNumber;
    private int readThreads;
    private int mergeThreads;
    private int parquetRowGroupSize;
    private  int ioActionRetries;
    private int maxDumpIntervalMs;
    private int refreshTasksQueueIntervalMin;
    private double dayWeightFactor;
    private double minSizeToMergeM; //if folder has less than this amount of unmerged data it wont be merged (unless it is older than 90 days)

    private String inputStorageType; //S3 or FILE_SYSTEM
    private String inputDataFolder; //the root folder of the input data (for example parquet)
    private String scoresFolder; //the root folder of the scores (for example parquetScores)

    private String outputStorageType; //S3 or FILE_SYSTEM
    private String outputDataFolder; //the root folder of the merged data (for example parquet)
    private boolean fillIOCountersData;
    private int maxConcurrentMergeSizeM = 90; //default value is 90M

    //optional - not needed if storage type is FILE_SYSTEM
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3Bucket;
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String s3region;

    //optional - not needed if storage type is S3
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String baseFolder; //the base folder in the machine (for example /users/data)

    //personal information configuration parameters
    private String s3PiBucket;
    private String piInputDataFolder;
    private String piOutputDataFolder;
    private String usersToDeleteFolder;
    private double usersDeletionAgeFactor;

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> commonFieldConfigs = new HashMap<>();

    //Map between field name and its configuration
    private HashMap<String, FieldConfig> customDimensionsFieldConfigs = new HashMap<>();

    //Map between event to map between field name and its configuration
    private HashMap<String, HashMap<String, FieldConfig>> eventsFieldConfigs = new HashMap<>();

    private int catchupHour = -1; //legal values:0 - 23 . The time of the long catchup running. This means the queue build interval is longer. If -1 : no catchup running
    private int catchupDurationMin = -1; //the duration of the catchup running. If -1 : no catchup running

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.COMPACTION_CONSUMER);

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

    public int getReadThreads() {
        return readThreads;
    }

    public void setReadThreads(int readThreads) {
        this.readThreads = readThreads;
    }

    public int getMergeThreads() {
        return mergeThreads;
    }

    public void setMergeThreads(int mergeThreads) {
        this.mergeThreads = mergeThreads;
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

    public int getMaxDumpIntervalMs() {
        return maxDumpIntervalMs;
    }

    public void setMaxDumpIntervalMs(int maxDumpIntervalMs) {
        this.maxDumpIntervalMs = maxDumpIntervalMs;
    }

    public int getRefreshTasksQueueIntervalMin() {
        return refreshTasksQueueIntervalMin;
    }

    public void setRefreshTasksQueueIntervalSec(int refreshTasksQueueIntervalMin) {
        this.refreshTasksQueueIntervalMin = refreshTasksQueueIntervalMin;
    }

    public HashMap<String, FieldConfig> getCommonFieldTypesMap() {
        return commonFieldConfigs;
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

    public void setRefreshTasksQueueIntervalMin(int refreshTasksQueueIntervalMin) {
        this.refreshTasksQueueIntervalMin = refreshTasksQueueIntervalMin;
    }

    public String getInputStorageType() {
        return inputStorageType;
    }

    public void setInputStorageType(String inputStorageType) {
        this.inputStorageType = inputStorageType;
    }

    public String getInputDataFolder() {
        return inputDataFolder;
    }

    public void setInputDataFolder(String inputDataFolder) {
        this.inputDataFolder = inputDataFolder;
    }

    public String getScoresFolder() {
        return scoresFolder;
    }

    public void setScoresFolder(String scoresFolder) {
        this.scoresFolder = scoresFolder;
    }

    public String getOutputStorageType() {
        return outputStorageType;
    }

    public void setOutputStorageType(String outputStorageType) {
        this.outputStorageType = outputStorageType;
    }

    public String getOutputDataFolder() {
        return outputDataFolder;
    }

    public void setOutputDataFolder(String outputDataFolder) {
        this.outputDataFolder = outputDataFolder;
    }

    public String getBaseFolder() {
        return baseFolder;
    }

    public void setBaseFolder(String baseFolder) {
        this.baseFolder = baseFolder;
    }

    public double getDayWeightFactor() {
        return dayWeightFactor;
    }

    public void setDayWeightFactor(double dayWeightFactor) {
        this.dayWeightFactor = dayWeightFactor;
    }

    public double getMinSizeToMergeM() {
        return minSizeToMergeM;
    }

    public void setMinSizeToMergeM(double minSizeToMergeM) {
        this.minSizeToMergeM = minSizeToMergeM;
    }

    public String getS3PiBucket() {
        return s3PiBucket;
    }

    public void setS3PiBucket(String s3PiBucket) {
        this.s3PiBucket = s3PiBucket;
    }

    public String getPiInputDataFolder() {
        return piInputDataFolder;
    }

    public void setPiInputDataFolder(String piInputDataFolder) {
        this.piInputDataFolder = piInputDataFolder;
    }

    public String getPiOutputDataFolder() {
        return piOutputDataFolder;
    }

    public void setPiOutputDataFolder(String piOutputDataFolder) {
        this.piOutputDataFolder = piOutputDataFolder;
    }

    public String getUsersToDeleteFolder() {
        return usersToDeleteFolder;
    }

    public void setUsersToDeleteFolder(String usersToDeleteFolder) {
        this.usersToDeleteFolder = usersToDeleteFolder;
    }


    public double getUsersDeletionAgeFactor() {
        return usersDeletionAgeFactor;
    }

    public void setUsersDeletionAgeFactor(double usersDeletionAgeFactor) {
        this.usersDeletionAgeFactor = usersDeletionAgeFactor;
    }

    public boolean isFillIOCountersData() {
        return fillIOCountersData;
    }

    public void setFillIOCountersData(boolean fillIOCountersData) {
        this.fillIOCountersData = fillIOCountersData;
    }


    public int getMaxConcurrentMergeSizeM() {
        return maxConcurrentMergeSizeM;
    }

    public void setMaxConcurrentMergeSizeM(int maxConcurrentMergeSizeM) {
        this.maxConcurrentMergeSizeM = maxConcurrentMergeSizeM;
    }

    public int getCatchupHour() {
        return catchupHour;
    }

    public void setCatchupHour(int catchupHour) {
        this.catchupHour = catchupHour;
    }

    public int getCatchupDurationMin() {
        return catchupDurationMin;
    }

    public void setCatchupDurationMin(int catchupDurationMin) {
        this.catchupDurationMin = catchupDurationMin;
    }




    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("shardsNumber = ");
        sb.append(shardsNumber);
        sb.append("\n");
        sb.append("readThreads = ");
        sb.append(readThreads);
        sb.append("\n");
        sb.append("parquetRowGroupSize = ");
        sb.append(parquetRowGroupSize);
        sb.append("\n");
        sb.append("ioActionRetries = ");
        sb.append(ioActionRetries);
        sb.append("\n");
        sb.append("s3Bucket = ");
        sb.append(s3Bucket);
        sb.append("\n");
        sb.append("maxDumpIntervalMs = ");
        sb.append(maxDumpIntervalMs);
        sb.append("\n");
        sb.append("refreshTasksQueueIntervalMin = ");
        sb.append(refreshTasksQueueIntervalMin);
        sb.append("\n");
        sb.append("inputStorageType = ");
        sb.append(inputStorageType);
        sb.append("\n");
        sb.append("inputDataFolder = ");
        sb.append(inputDataFolder);
        sb.append("\n");
        sb.append("outputStorageType = ");
        sb.append(outputStorageType);
        sb.append("\n");
        sb.append("outputDataFolder = ");
        sb.append(outputDataFolder);
        sb.append("\n");
        sb.append("scoresFolder = ");
        sb.append(scoresFolder);
        sb.append("\n");
        sb.append("baseFolder = ");
        sb.append(baseFolder);
        sb.append("\n");
        sb.append("\n");
        sb.append("dayWeightFactor = ");
        sb.append(dayWeightFactor);
        sb.append("\n");
        sb.append("minSizeToMergeM = ");
        sb.append(minSizeToMergeM);
        sb.append("\n");
        sb.append("s3PiBucket = ");
        sb.append(s3PiBucket);
        sb.append("\n");
        sb.append("piInputDataFolder = ");
        sb.append(piInputDataFolder);
        sb.append("\n");
        sb.append("usersToDeleteFolder = ");
        sb.append(usersToDeleteFolder);
        sb.append("\n");
        sb.append("usersDeletionAgeFactor = ");
        sb.append(usersDeletionAgeFactor);
        sb.append("\n");
        sb.append("fillIOCountersData = ");
        sb.append(fillIOCountersData);
        sb.append("\n");
        sb.append("sessionTimeoutMS = ");
        sb.append(getSessionTimeoutMS());
        sb.append("\n");
        sb.append("maxConcurrentMergeSizeM = ");
        sb.append(maxConcurrentMergeSizeM);
        sb.append("\n");
        sb.append("mergeThreads = ");
        sb.append(mergeThreads);
        sb.append("\n");
        sb.append("catchupHour = ");
        sb.append(catchupHour);
        sb.append("\n");
        sb.append("catchupDurationMin = ");
        sb.append(catchupDurationMin);
        sb.append("\n");
        return sb.toString();
    }
}