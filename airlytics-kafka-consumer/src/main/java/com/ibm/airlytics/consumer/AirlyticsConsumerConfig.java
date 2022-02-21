package com.ibm.airlytics.consumer;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;


@SuppressWarnings("unused")
public class AirlyticsConsumerConfig {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AirlyticsConsumerConfig.class.getName());

    private String bootstrapServers;
    protected int maxPollRecords;
    private int maxPollIntervalMs;
    private String securityProtocol;
    private List<String> topics;
    private String errorsTopic;
    private String consumerGroupId;
    private int fetchMinBytes = 1;
    private int fetchMaxWaitMs = 500;
    private String producerCompressionType = "none";
    private int lingerMs = 100;

    @JsonIgnoreProperties(ignoreUnknown = true)
    private int sessionTimeoutMS=10000; //default is 10 seconds. Note: cannot be less than heartbeat.interval.ms that is 3 seconds

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    @CheckForNull
    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getAirlock().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON (" + feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config == null ? null : config.toString();
    }

    protected void readFromAirlockFeature(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        if (configuration != null) {
            objectMapper.readerForUpdating(this).readValue(configuration);
        }
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
            LOGGER.error("Error serializing consumer config object: " + e.getMessage(), e);
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

    public List<String> getTopics() {
        return topics;
    }

    @Deprecated
    public String getTopic() {
        return getTopics().get(0);
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
        topics = Arrays.asList(topic.trim().split("\\s*,\\s*"));
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

    public int getFetchMinBytes() { return fetchMinBytes; }

    public void setFetchMinBytes(int fetchMinBytes) { this.fetchMinBytes = fetchMinBytes; }

    public int getFetchMaxWaitMs() { return fetchMaxWaitMs; }

    public void setFetchMaxWaitMs(int fetchMaxWaitMs) { this.fetchMaxWaitMs = fetchMaxWaitMs; }

    public String getProducerCompressionType() { return producerCompressionType; }

    public void setProducerCompressionType(String producerCompressionType) { this.producerCompressionType = producerCompressionType; }

    public int getLingerMs() { return lingerMs; }

    public void setLingerMs(int lingerMs) { this.lingerMs = lingerMs; }

    public int getSessionTimeoutMS() {
        return sessionTimeoutMS;
    }

    public void setSessionTimeoutMS(int sessionTimeoutMS) {
        this.sessionTimeoutMS = sessionTimeoutMS;
    }

}
