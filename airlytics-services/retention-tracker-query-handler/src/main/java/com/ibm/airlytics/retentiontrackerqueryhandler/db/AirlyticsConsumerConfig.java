package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import org.json.JSONObject;

import java.io.IOException;

public class AirlyticsConsumerConfig {
    private String bootstrapServers;
    private int maxPollRecords;
    private int maxPollIntervalMs;
    private String securityProtocol;
    private String topic;
    private String errorsTopic;
    private String consumerGroupId;

    protected static ObjectMapper objectMapper = new ObjectMapper();

    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getInstance().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON ("+ feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config.toString();
    }

    protected void readFromAirlockFeature(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        objectMapper.readerForUpdating(this).readValue(configuration);
    }

    public void initWithAirlock() throws IOException {
        readFromAirlockFeature(AirlockConstants.Consumers.ANALYTICS_CONSUMER);
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Error serializing consumer config object";
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getTopic() {
        return topic;
    }

    public String getErrorsTopic() {
        return errorsTopic;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public void setMaxPollIntervalMs(int maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setErrorsTopic(String errorsTopic) {
        this.errorsTopic = errorsTopic;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }
}
