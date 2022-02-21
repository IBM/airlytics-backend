package com.ibm.airlytics.consumer.dsr.writer;


import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.List;

public class ResponseWriterConfig extends AirlyticsConsumerConfig {
    private int readThreads;
    private int parquetRowGroupSize;
    private  int ioActionRetries;
    private String s3Bucket;
    private String s3region;
    private String productFolder;
    private int maxDumpIntervalMs;
    private String responseFolder;
    private String responseFile;
    private String responseSuccessFile;
    private String responseSecondPath;
    private List<String> excludedColumns;
    private List<String> excludedEventsColumns = null;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.RESPONSE_WRITER_CONFIG);
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
        return sb.toString();
    }

    public String getProductFolder() {
        return productFolder;
    }

    public void setProductFolder(String productFolder) {
        this.productFolder = productFolder;
    }

    public String getResponseFolder() {
        return responseFolder;
    }

    public void setResponseFolder(String responseFolder) {
        this.responseFolder = responseFolder;
    }

    public String getResponseFile() {
        return responseFile;
    }

    public void setResponseFile(String responseFile) {
        this.responseFile = responseFile;
    }

    public String getResponseSuccessFile() {
        return responseSuccessFile;
    }

    public void setResponseSuccessFile(String responseSuccessFile) {
        this.responseSuccessFile = responseSuccessFile;
    }

    public String getResponseSecondPath() {
        return responseSecondPath;
    }

    public void setResponseSecondPath(String responseSecondPath) {
        this.responseSecondPath = responseSecondPath;
    }

    public List<String> getExcludedColumns() {
        return excludedColumns;
    }

    public void setExcludedColumns(List<String> excludedColumns) {
        this.excludedColumns = excludedColumns;
    }

    public List<String> getExcludedEventsColumns() {
        return excludedEventsColumns;
    }

    public void setExcludedEventsColumns(List<String> excludedEventsColumns) {
        this.excludedEventsColumns = excludedEventsColumns;
    }
}