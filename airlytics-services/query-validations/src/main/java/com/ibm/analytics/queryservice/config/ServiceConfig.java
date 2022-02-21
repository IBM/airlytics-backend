package com.ibm.analytics.queryservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.analytics.queryservice.airlock.AirlockConstants;
import com.ibm.analytics.queryservice.airlock.AirlockManager;
import org.json.JSONObject;

import java.io.IOException;

public class ServiceConfig {

    private String region;
    private String bucketName;
    private String accessKey;
    private String secret;

    public ServiceConfig(){
        try {
            readFromAirlockFeature(AirlockConstants.Consumers.LTV_INPUT_CONSUMER);
        } catch (IOException e) {
//            e.printStackTrace();
        }
    }

    public ServiceConfig(String region, String bucketName, String accessKey, String secret){
        this.region = region;
        this.bucketName = bucketName;
        this.accessKey = accessKey;
        this.secret = secret;
    }

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON ("+ feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        String configResult = null;
        if (config != null){
            configResult = config.toString();
        }
        return configResult;
    }

    protected void readFromAirlockFeature(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        objectMapper.readerForUpdating(this).readValue(configuration);
    }

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
