package com.ibm.airlytics.utilities.validationService.queryservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ServiceConfig {

    public static final String OUTPUT_DIRECTORY = "validation-output-reports";

    private String region;
    private String bucketName;
    private String accessKey;
    private String secret;

    public ServiceConfig(){
    }

    public ServiceConfig(String region, String bucketName, String accessKey, String secret){
        this.region = region;
        this.bucketName = bucketName;
        this.accessKey = accessKey;
        this.secret = secret;
    }

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }
}
