package com.ibm.airlytics.consumer.dsr.writer;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;

public class DeleteWriterConfig extends AirlyticsConsumerConfig {

    private String deleteFileFormat;
    private String requestFolder;
    private String baseFolder;
    private String devBaseFolder = "";
    private int writeThreads;
    private  int writeRetries;
    private int innerWriteThreads;
    private boolean dbDumpCleanupEnabled = false;
    private String deviceIdPrefix = null;
    private String registeredIdPrefix = null;

    public int getIoActionRetries() {
        return ioActionRetries;
    }

    public void setIoActionRetries(int ioActionRetries) {
        this.ioActionRetries = ioActionRetries;
    }

    private int ioActionRetries;
    
    /*
	"requestFolder":"userDeletionRequests",
	"baseFolder":"/usr/src/app/data/airlytics-datalake-prod"
     */

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.DELETE_WRITER_CONFIG);
    }


    public String getDeleteFileFormat() {
        return deleteFileFormat;
    }

    public void setDeleteFileFormat(String deleteFileFormat) {
        this.deleteFileFormat = deleteFileFormat;
    }

    public String getRequestFolder() {
        return requestFolder;
    }

    public void setRequestFolder(String requestFolder) {
        this.requestFolder = requestFolder;
    }

    public String getBaseFolder() {
        return baseFolder;
    }

    public void setBaseFolder(String baseFolder) {
        this.baseFolder = baseFolder;
    }

    public int getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(int writeThreads) {
        this.writeThreads = writeThreads;
    }

    public int getWriteRetries() {
        return writeRetries;
    }

    public void setWriteRetries(int writeRetries) {
        this.writeRetries = writeRetries;
    }

    public int getInnerWriteThreads() {
        return innerWriteThreads;
    }

    public void setInnerWriteThreads(int innerWriteThreads) {
        this.innerWriteThreads = innerWriteThreads;
    }

    public String getDevBaseFolder() {
        return devBaseFolder;
    }

    public void setDevBaseFolder(String devBaseFolder) {
        this.devBaseFolder = devBaseFolder;
    }

    public boolean isDbDumpCleanupEnabled() {
        return dbDumpCleanupEnabled;
    }

    public String getDeviceIdPrefix() {
        return deviceIdPrefix;
    }

    public String getRegisteredIdPrefix() {
        return registeredIdPrefix;
    }

    public void setDbDumpCleanupEnabled(boolean dbDumpCleanupEnabled) {
        this.dbDumpCleanupEnabled = dbDumpCleanupEnabled;
    }
}
