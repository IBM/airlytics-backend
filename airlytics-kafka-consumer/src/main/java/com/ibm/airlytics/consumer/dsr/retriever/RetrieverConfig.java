package com.ibm.airlytics.consumer.dsr.retriever;


import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RetrieverConfig extends AirlyticsConsumerConfig {
    private int shardsNumber;
    private int readThreads;
    private int parquetRowGroupSize;
    private int ioActionRetries;
    private String s3Bucket;
    private String s3region;
    private String s3RootFolder;
    private String s3BucketPi;
    private String s3RegionPi;
    private String s3BucketDev;
    private String s3regionDev;
    private String s3BucketPiDev;
    private String s3RegionPiDev;
    private String deviceIdPrefix = null;
    private String registeredIdPrefix = null;

    public String getS3BucketPi() {
        return s3BucketPi;
    }

    public void setS3BucketPi(String s3BucketPi) {
        this.s3BucketPi = s3BucketPi;
    }

    public String getS3RegionPi() {
        return s3RegionPi;
    }

    public void setS3RegionPi(String s3RegionPi) {
        this.s3RegionPi = s3RegionPi;
    }

    public String getS3RootFolderPi() {
        return s3RootFolderPi;
    }

    public void setS3RootFolderPi(String s3RootFolderPi) {
        this.s3RootFolderPi = s3RootFolderPi;
    }

    private String s3RootFolderPi;
    private String productFolder;
    private String devProductFolder;
    private int maxDumpIntervalMs;
    private List<String> excludedColumns;
    private List<String> excludedEventNames = new ArrayList<>();

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.RETRIEVER_CONFIG);
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

    public String getS3RootFolder() {
        return s3RootFolder;
    }

    public void setS3RootFolder(String s3RootFolder) {
        this.s3RootFolder = s3RootFolder;
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
    public String getDeviceIdPrefix() {
        return deviceIdPrefix;
    }

    public void setDeviceIdPrefix(String deviceIdPrefix) {
        this.deviceIdPrefix = deviceIdPrefix;
    }

    public String getRegisteredIdPrefix() {
        return registeredIdPrefix;
    }

    public void setRegisteredIdPrefix(String registeredIdPrefix) {
        this.registeredIdPrefix = registeredIdPrefix;
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
        sb.append("s3RootFolder = ");
        sb.append(s3RootFolder);
        sb.append("\n");
        sb.append("maxDumpIntervalMs = ");
        sb.append(maxDumpIntervalMs);
        sb.append("excludedColumns = ");
        sb.append(excludedColumns);
        return sb.toString();
    }

    public String getProductFolder() {
        return productFolder;
    }

    public void setProductFolder(String productFolder) {
        this.productFolder = productFolder;
    }

    public List<String> getExcludedColumns() {
        return excludedColumns;
    }

    public void setExcludedColumns(List<String> excludedColumns) {
        this.excludedColumns = excludedColumns;
    }

    public List<String> getExcludedEventNames() {
        return excludedEventNames;
    }

    public void setExcludedEventNames(List<String> excludedEventNames) {
        this.excludedEventNames = excludedEventNames;
    }

    public String getS3RegionPiDev() {
        return s3RegionPiDev;
    }

    public void setS3RegionPiDev(String s3RegionPiDev) {
        this.s3RegionPiDev = s3RegionPiDev;
    }

    public String getS3BucketPiDev() {
        return s3BucketPiDev;
    }

    public void setS3BucketPiDev(String s3BucketPiDev) {
        this.s3BucketPiDev = s3BucketPiDev;
    }

    public String getS3regionDev() {
        return s3regionDev;
    }

    public void setS3regionDev(String s3regionDev) {
        this.s3regionDev = s3regionDev;
    }

    public String getS3BucketDev() {
        return s3BucketDev;
    }

    public void setS3BucketDev(String s3BucketDev) {
        this.s3BucketDev = s3BucketDev;
    }

    public String getDevProductFolder() {
        return devProductFolder;
    }

    public void setDevProductFolder(String devProductFolder) {
        this.devProductFolder = devProductFolder;
    }
}