package com.ibm.weather.airlytics.common.dto;

public class BasicAthenaConfig {

    private String athenaRegion;

    private String athenaResultsOutputBucket;

    private String athenaCatalog;

    public String getAthenaRegion() {
        return athenaRegion;
    }

    public void setAthenaRegion(String athenaRegion) {
        this.athenaRegion = athenaRegion;
    }

    public String getAthenaResultsOutputBucket() {
        return athenaResultsOutputBucket;
    }

    public void setAthenaResultsOutputBucket(String athenaResultsOutputBucket) {
        this.athenaResultsOutputBucket = athenaResultsOutputBucket;
    }

    public String getAthenaCatalog() {
        return athenaCatalog;
    }

    public void setAthenaCatalog(String athenaCatalog) {
        this.athenaCatalog = athenaCatalog;
    }
}
