package com.ibm.airlytics.consumer.dsr.writer;


import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.List;

public class DeleteNotificationsWriterConfig extends AirlyticsConsumerConfig {
    private  int ioActionRetries;
    private String s3Bucket;
    private String s3region;
    private String responseFolder;
    private String responseSecondPath;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.DELETE_RESPONSE_WRITER_CONFIG);
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


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append("\n");
        sb.append("ioActionRetries = ");
        sb.append(ioActionRetries);
        sb.append("\n");
        sb.append("s3Bucket = ");
        sb.append(s3Bucket);
        sb.append("\n");
        return sb.toString();
    }


    public String getResponseFolder() {
        return responseFolder;
    }

    public void setResponseFolder(String responseFolder) {
        this.responseFolder = responseFolder;
    }

    public String getResponseSecondPath() {
        return responseSecondPath;
    }

    public void setResponseSecondPath(String responseSecondPath) {
        this.responseSecondPath = responseSecondPath;
    }

}