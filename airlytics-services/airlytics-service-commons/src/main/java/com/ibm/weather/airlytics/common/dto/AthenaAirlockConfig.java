package com.ibm.weather.airlytics.common.dto;

public class AthenaAirlockConfig extends BasicAthenaConfig {

    private String sourceAthenaDb;

    private String destAthenaDb;

    private String sourceEventsTable;

    private String targetS3Bucket;

    public String getSourceAthenaDb() {
        return sourceAthenaDb;
    }

    public void setSourceAthenaDb(String sourceAthenaDb) {
        this.sourceAthenaDb = sourceAthenaDb;
    }

    public String getDestAthenaDb() {
        return destAthenaDb;
    }

    public void setDestAthenaDb(String destAthenaDb) {
        this.destAthenaDb = destAthenaDb;
    }

    public String getSourceEventsTable() {
        return sourceEventsTable;
    }

    public void setSourceEventsTable(String sourceEventsTable) {
        this.sourceEventsTable = sourceEventsTable;
    }

    public String getTargetS3Bucket() {
        return targetS3Bucket;
    }

    public void setTargetS3Bucket(String targetS3Bucket) {
        this.targetS3Bucket = targetS3Bucket;
    }
}
