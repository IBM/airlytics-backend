package com.ibm.weather.airlytics.cohorts.dto;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class AirCohortsAirlockConfig {

    private List<String> productIds;
    private String airlockApiBaseUrl;
    private List<String> additionalTables;
    private Map<String, String> joinColumns;

    private int recoveryRetries = 10;

    private boolean kafkaEnabled;
    private String kafkaBootstrapServers;
    private String kafkaSecurityProtocol;
    private String kafkaTopic;
    private int kafkaBatchSize = 16_384;//16KB
    private int kafkaLingerMs = 100;//100ms
    private int kafkaMaxMessages = 0;//before flush, 0 - flush at the end

    public List<String> getProductIds() {
        return productIds;
    }

    public void setProductIds(List<String> productIds) {
        this.productIds = productIds;
    }

    public List<String> getAdditionalTables() {
        return additionalTables;
    }

    public void setAdditionalTables(List<String> additionalTables) {
        this.additionalTables = additionalTables;
    }

    public Map<String, String> getJoinColumns() {
        return joinColumns;
    }

    public void setJoinColumns(Map<String, String> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public String getAirlockApiBaseUrl() {
        return airlockApiBaseUrl;
    }

    public void setAirlockApiBaseUrl(String airlockApiBaseUrl) {
        this.airlockApiBaseUrl = airlockApiBaseUrl;
    }

    public int getRecoveryRetries() {
        return recoveryRetries;
    }

    public void setRecoveryRetries(int recoveryRetries) {
        this.recoveryRetries = recoveryRetries;
    }

    public boolean isKafkaEnabled() {
        return kafkaEnabled;
    }

    public void setKafkaEnabled(boolean kafkaEnabled) {
        this.kafkaEnabled = kafkaEnabled;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaSecurityProtocol() {
        return kafkaSecurityProtocol;
    }

    public void setKafkaSecurityProtocol(String kafkaSecurityProtocol) {
        this.kafkaSecurityProtocol = kafkaSecurityProtocol;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public int getKafkaBatchSize() {
        return kafkaBatchSize;
    }

    public void setKafkaBatchSize(int kafkaBatchSize) {
        this.kafkaBatchSize = kafkaBatchSize;
    }

    public int getKafkaLingerMs() {
        return kafkaLingerMs;
    }

    public void setKafkaLingerMs(int kafkaLingerMs) {
        this.kafkaLingerMs = kafkaLingerMs;
    }

    public int getKafkaMaxMessages() {
        return kafkaMaxMessages;
    }

    public void setKafkaMaxMessages(int kafkaMaxMessages) {
        this.kafkaMaxMessages = kafkaMaxMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirCohortsAirlockConfig that = (AirCohortsAirlockConfig) o;

        if (recoveryRetries != that.recoveryRetries) return false;
        if (kafkaEnabled != that.kafkaEnabled) return false;
        if (kafkaBatchSize != that.kafkaBatchSize) return false;
        if (kafkaLingerMs != that.kafkaLingerMs) return false;
        if (kafkaMaxMessages != that.kafkaMaxMessages) return false;
        if (productIds != null ? !productIds.equals(that.productIds) : that.productIds != null) return false;
        if (airlockApiBaseUrl != null ? !airlockApiBaseUrl.equals(that.airlockApiBaseUrl) : that.airlockApiBaseUrl != null)
            return false;
        if (additionalTables != null ? !additionalTables.equals(that.additionalTables) : that.additionalTables != null)
            return false;
        if (joinColumns != null ? !joinColumns.equals(that.joinColumns) : that.joinColumns != null) return false;
        if (kafkaBootstrapServers != null ? !kafkaBootstrapServers.equals(that.kafkaBootstrapServers) : that.kafkaBootstrapServers != null)
            return false;
        if (kafkaSecurityProtocol != null ? !kafkaSecurityProtocol.equals(that.kafkaSecurityProtocol) : that.kafkaSecurityProtocol != null)
            return false;
        return kafkaTopic != null ? kafkaTopic.equals(that.kafkaTopic) : that.kafkaTopic == null;
    }

    @Override
    public int hashCode() {
        int result = productIds != null ? productIds.hashCode() : 0;
        result = 31 * result + (airlockApiBaseUrl != null ? airlockApiBaseUrl.hashCode() : 0);
        result = 31 * result + (additionalTables != null ? additionalTables.hashCode() : 0);
        result = 31 * result + (joinColumns != null ? joinColumns.hashCode() : 0);
        result = 31 * result + recoveryRetries;
        result = 31 * result + (kafkaEnabled ? 1 : 0);
        result = 31 * result + (kafkaBootstrapServers != null ? kafkaBootstrapServers.hashCode() : 0);
        result = 31 * result + (kafkaSecurityProtocol != null ? kafkaSecurityProtocol.hashCode() : 0);
        result = 31 * result + (kafkaTopic != null ? kafkaTopic.hashCode() : 0);
        result = 31 * result + kafkaBatchSize;
        result = 31 * result + kafkaLingerMs;
        result = 31 * result + kafkaMaxMessages;
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AirCohortsAirlockConfig.class.getSimpleName() + "[", "]")
                .add("productIds=" + productIds)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("additionalTables=" + additionalTables)
                .add("joinColumns=" + joinColumns)
                .add("recoveryRetries=" + recoveryRetries)
                .add("kafkaEnabled=" + kafkaEnabled)
                .add("kafkaBootstrapServers='" + kafkaBootstrapServers + "'")
                .add("kafkaSecurityProtocol='" + kafkaSecurityProtocol + "'")
                .add("kafkaTopic='" + kafkaTopic + "'")
                .add("kafkaBatchSize=" + kafkaBatchSize)
                .add("kafkaLingerMs=" + kafkaLingerMs)
                .add("kafkaMaxMessages=" + kafkaMaxMessages)
                .toString();
    }
}
