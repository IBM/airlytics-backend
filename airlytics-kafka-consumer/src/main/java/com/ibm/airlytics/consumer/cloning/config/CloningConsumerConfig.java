package com.ibm.airlytics.consumer.cloning.config;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventFilteringConsumerConfig;
import com.ibm.airlytics.eventproxy.EventApiClientConfig;
import com.ibm.airlytics.eventproxy.EventProxyIntegrationConfig;

import java.io.IOException;
import java.util.StringJoiner;

public class CloningConsumerConfig extends GenericEventFilteringConsumerConfig {

    private EventProxyIntegrationConfig eventProxyIntegrationConfig = new EventProxyIntegrationConfig();

    private EventApiClientConfig eventApiClientConfig = new EventApiClientConfig();

    private int percentageUsersCloned = 0;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.CLONING_CONSUMER);
    }

    public String getEventApiBaseUrl() {
        return eventApiClientConfig.getEventApiBaseUrl();
    }

    public void setEventApiBaseUrl(String eventApiBaseUrl) {
        eventApiClientConfig.setEventApiBaseUrl(eventApiBaseUrl);
    }

    public String getEventApiPath() {
        return eventApiClientConfig.getEventApiPath();
    }

    public void setEventApiPath(String eventApiPath) {
        eventApiClientConfig.setEventApiPath(eventApiPath);
    }

    public String getEventApiKey() {
        return eventApiClientConfig.getEventApiKey();
    }

    public void setEventApiKey(String eventApiKey) {
        eventApiClientConfig.setEventApiKey(eventApiKey);
    }

    public int getEventApiBatchSize() {
        return eventProxyIntegrationConfig.getEventApiBatchSize();
    }

    public void setEventApiBatchSize(int eventApiBatchSize) {
        eventProxyIntegrationConfig.setEventApiBatchSize(eventApiBatchSize);
    }

    public int getEventApiRetries() {
        return eventProxyIntegrationConfig.getEventApiRetries();
    }

    public void setEventApiRetries(int eventApiRetries) {
        eventProxyIntegrationConfig.setEventApiRetries(eventApiRetries);
    }

    public int getEventApiRateLimit() {
        return eventProxyIntegrationConfig.getEventApiRateLimit();
    }

    public void setEventApiRateLimit(int eventApiRateLimit) {
        eventProxyIntegrationConfig.setEventApiRateLimit(eventApiRateLimit);
    }

    public int getEventApiParallelThreads() {
        return eventProxyIntegrationConfig.getEventApiParallelThreads();
    }

    public void setEventApiParallelThreads(int eventApiParallelThreads) {
        eventProxyIntegrationConfig.setEventApiParallelThreads(eventApiParallelThreads);
    }

    public ObfuscationConfig getObfuscation() {
        return eventProxyIntegrationConfig.getObfuscation();
    }

    public void setObfuscation(ObfuscationConfig obfuscation) {
        eventProxyIntegrationConfig.setObfuscation(obfuscation);
    }

    public EventProxyIntegrationConfig getEventProxyIntegrationConfig() {
        return eventProxyIntegrationConfig;
    }

    public void setEventProxyIntegrationConfig(EventProxyIntegrationConfig eventProxyIntegrationConfig) {
        this.eventProxyIntegrationConfig = eventProxyIntegrationConfig;
    }

    public EventApiClientConfig getEventApiClientConfig() {
        return eventApiClientConfig;
    }

    public void setEventApiClientConfig(EventApiClientConfig eventApiClientConfig) {
        this.eventApiClientConfig = eventApiClientConfig;
    }

    public int getPercentageUsersCloned() {
        return percentageUsersCloned;
    }

    public void setPercentageUsersCloned(int percentageUsersCloned) {
        this.percentageUsersCloned = percentageUsersCloned;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", CloningConsumerConfig.class.getSimpleName() + "[", "]")
                .add("eventProxyIntegrationConfig=" + eventProxyIntegrationConfig)
                .add("eventApiClientConfig=" + eventApiClientConfig)
                .add("percentageUsersCloned=" + percentageUsersCloned)
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CloningConsumerConfig that = (CloningConsumerConfig) o;

        if (percentageUsersCloned != that.percentageUsersCloned) return false;
        if (eventProxyIntegrationConfig != null ? !eventProxyIntegrationConfig.equals(that.eventProxyIntegrationConfig) : that.eventProxyIntegrationConfig != null)
            return false;
        return eventApiClientConfig != null ? eventApiClientConfig.equals(that.eventApiClientConfig) : that.eventApiClientConfig == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (eventProxyIntegrationConfig != null ? eventProxyIntegrationConfig.hashCode() : 0);
        result = 31 * result + (eventApiClientConfig != null ? eventApiClientConfig.hashCode() : 0);
        result = 31 * result + percentageUsersCloned;
        return result;
    }
}
