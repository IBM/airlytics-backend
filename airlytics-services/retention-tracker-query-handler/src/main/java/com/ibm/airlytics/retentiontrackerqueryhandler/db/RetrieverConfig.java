package com.ibm.airlytics.retentiontrackerqueryhandler.db;


import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;

import java.io.IOException;

public class RetrieverConfig extends AirlyticsConsumerConfig {
    private int shardsNumber;
    private int readThreads;
    private int parquetRowGroupSize;
    private  int ioActionRetries;
    private String s3Bucket;
    private String s3region;
    private String s3RootFolder;
    private int maxDumpIntervalMs;

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
        return sb.toString();
    }
}