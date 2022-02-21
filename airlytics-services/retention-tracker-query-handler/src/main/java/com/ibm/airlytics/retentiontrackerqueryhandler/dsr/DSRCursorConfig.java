package com.ibm.airlytics.retentiontrackerqueryhandler.dsr;

import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.AirlyticsConsumerConfig;

import java.io.IOException;

public class DSRCursorConfig extends AirlyticsConsumerConfig {
    private String s3Bucket;
    private String s3RootFolder;
    private String s3region;
    private String cursorFileName;
    private int ioActionRetries;
    private String key;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.DSR.DSR_CURSOR_CONFIG);
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

    public String getS3region() {
        return s3region;
    }

    public void setS3region(String s3region) {
        this.s3region = s3region;
    }

    public String getCursorFileName() {
        return cursorFileName;
    }

    public void setCursorFileName(String cursorFileName) {
        this.cursorFileName = cursorFileName;
    }

    public int getIoActionRetries() {
        return ioActionRetries;
    }

    public void setIoActionRetries(int ioActionRetries) {
        this.ioActionRetries = ioActionRetries;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
