package com.ibm.airlytics.consumer.kafkacloning;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventFilteringConsumerConfig;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.StringJoiner;

public class KafkaCloningConsumerConfig extends GenericEventFilteringConsumerConfig {

    private int percentageUsersCloned = 0;
    private String targetKafkaTopic;
    private String targetKafkaBootstrapServers;
    private String targetKafkaSecurityProtocol;
    private String targetKafkaCompressionType;
    private int targetKafkaBatchSize = 16_384;//16KB
    private int targetKafkaLingerMs = 100;//100ms
    private int targetKafkaMaxMessages = 0;//before flush, 0 - flush at the end

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.KAFKA_CLONING_CONSUMER);
    }

    public int getPercentageUsersCloned() {
        return percentageUsersCloned;
    }

    public void setPercentageUsersCloned(int percentageUsersCloned) {
        this.percentageUsersCloned = percentageUsersCloned;
    }

    public String getTargetKafkaTopic() {
        return targetKafkaTopic;
    }

    public void setTargetKafkaTopic(String targetKafkaTopic) {
        this.targetKafkaTopic = targetKafkaTopic;
    }

    public String getTargetKafkaBootstrapServers() {
        return targetKafkaBootstrapServers;
    }

    public void setTargetKafkaBootstrapServers(String targetKafkaBootstrapServers) {
        this.targetKafkaBootstrapServers = targetKafkaBootstrapServers;
    }

    public String getTargetKafkaSecurityProtocol() {
        return targetKafkaSecurityProtocol;
    }

    public void setTargetKafkaSecurityProtocol(String targetKafkaSecurityProtocol) {
        this.targetKafkaSecurityProtocol = targetKafkaSecurityProtocol;
    }

    public String getTargetKafkaCompressionType() {
        return targetKafkaCompressionType;
    }

    public void setTargetKafkaCompressionType(String targetKafkaCompressionType) {
        this.targetKafkaCompressionType = targetKafkaCompressionType;
    }

    public int getTargetKafkaBatchSize() {
        return targetKafkaBatchSize;
    }

    public void setTargetKafkaBatchSize(int targetKafkaBatchSize) {
        this.targetKafkaBatchSize = targetKafkaBatchSize;
    }

    public int getTargetKafkaLingerMs() {
        return targetKafkaLingerMs;
    }

    public void setTargetKafkaLingerMs(int targetKafkaLingerMs) {
        this.targetKafkaLingerMs = targetKafkaLingerMs;
    }

    public int getTargetKafkaMaxMessages() {
        return targetKafkaMaxMessages;
    }

    public void setTargetKafkaMaxMessages(int targetKafkaMaxMessages) {
        this.targetKafkaMaxMessages = targetKafkaMaxMessages;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        KafkaCloningConsumerConfig that = (KafkaCloningConsumerConfig) o;

        if (percentageUsersCloned != that.percentageUsersCloned) return false;
        if (targetKafkaBatchSize != that.targetKafkaBatchSize) return false;
        if (targetKafkaLingerMs != that.targetKafkaLingerMs) return false;
        if (targetKafkaMaxMessages != that.targetKafkaMaxMessages) return false;
        if (targetKafkaTopic != null ? !targetKafkaTopic.equals(that.targetKafkaTopic) : that.targetKafkaTopic != null)
            return false;
        if (targetKafkaBootstrapServers != null ? !targetKafkaBootstrapServers.equals(that.targetKafkaBootstrapServers) : that.targetKafkaBootstrapServers != null)
            return false;
        if (targetKafkaSecurityProtocol != null ? !targetKafkaSecurityProtocol.equals(that.targetKafkaSecurityProtocol) : that.targetKafkaSecurityProtocol != null)
            return false;
        return targetKafkaCompressionType != null ? targetKafkaCompressionType.equals(that.targetKafkaCompressionType) : that.targetKafkaCompressionType == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + percentageUsersCloned;
        result = 31 * result + (targetKafkaTopic != null ? targetKafkaTopic.hashCode() : 0);
        result = 31 * result + (targetKafkaBootstrapServers != null ? targetKafkaBootstrapServers.hashCode() : 0);
        result = 31 * result + (targetKafkaSecurityProtocol != null ? targetKafkaSecurityProtocol.hashCode() : 0);
        result = 31 * result + (targetKafkaCompressionType != null ? targetKafkaCompressionType.hashCode() : 0);
        result = 31 * result + targetKafkaBatchSize;
        result = 31 * result + targetKafkaLingerMs;
        result = 31 * result + targetKafkaMaxMessages;
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", KafkaCloningConsumerConfig.class.getSimpleName() + "[", "]")
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .add("percentageUsersCloned=" + percentageUsersCloned)
                .add("targetKafkaTopic='" + targetKafkaTopic + "'")
                .add("targetKafkaBootstrapServers='" + targetKafkaBootstrapServers + "'")
                .add("targetKafkaSecurityProtocol='" + targetKafkaSecurityProtocol + "'")
                .add("targetKafkaCompressionType='" + targetKafkaCompressionType + "'")
                .add("targetKafkaBatchSize=" + targetKafkaBatchSize)
                .add("targetKafkaLingerMs=" + targetKafkaLingerMs)
                .add("targetKafkaMaxMessages=" + targetKafkaMaxMessages)
                .toString();
    }
}
