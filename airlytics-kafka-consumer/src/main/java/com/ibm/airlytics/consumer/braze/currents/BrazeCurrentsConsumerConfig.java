package com.ibm.airlytics.consumer.braze.currents;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.integrations.dto.EnvironmentDefinition;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.eventproxy.EventApiClientConfig;
import com.ibm.airlytics.eventproxy.EventProxyIntegrationConfig;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventMappingConsumerConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class BrazeCurrentsConsumerConfig  extends GenericEventMappingConsumerConfig  {

    private EventProxyIntegrationConfig eventProxyIntegrationConfig = new EventProxyIntegrationConfig();

    private EventApiClientConfig eventApiClientConfig = new EventApiClientConfig();

    private List<ProductDefinition> products;

    private List<EnvironmentDefinition> environments;

    private String progressFolder = "temp/";

    private int maxAwsRetries = 3;

    private int percentageUsersSent = 100; //in external 100%

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.BRAZE_CURRENTS_CONSUMER);
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

    public List<ProductDefinition> getProducts() {
        return products;
    }

    public void setProducts(List<ProductDefinition> products) {
        this.products = products;
    }

    public List<EnvironmentDefinition> getEnvironments() {
        return environments;
    }

    public void setEnvironments(List<EnvironmentDefinition> environments) {
        this.environments = environments;
    }

    public String getProgressFolder() {
        return progressFolder;
    }

    public void setProgressFolder(String progressFolder) {
        this.progressFolder = progressFolder;
    }

    public int getMaxAwsRetries() {
        return maxAwsRetries;
    }

    public void setMaxAwsRetries(int maxAwsRetries) {
        this.maxAwsRetries = maxAwsRetries;
    }

    public int getPercentageUsersSent() {
        return percentageUsersSent;
    }

    public void setPercentageUsersSent(int percentageUsersSent) {
        this.percentageUsersSent = percentageUsersSent;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BrazeCurrentsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("eventProxyIntegrationConfig=" + eventProxyIntegrationConfig)
                .add("eventApiClientConfig=" + eventApiClientConfig)
                .add("products=" + products)
                .add("environments=" + environments)
                .add("progressFolder='" + progressFolder + "'")
                .add("maxAwsRetries=" + maxAwsRetries)
                .add("percentageUsersSent=" + percentageUsersSent)
                .add("destinationTopic='" + destinationTopic + "'")
                .add("jsonObjectAttributeAccepted=" + jsonObjectAttributeAccepted)
                .add("jsonArrayAttributeAccepted=" + jsonArrayAttributeAccepted)
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .add("eventMappings=" + eventMappings)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BrazeCurrentsConsumerConfig that = (BrazeCurrentsConsumerConfig) o;

        if (maxAwsRetries != that.maxAwsRetries) return false;
        if (percentageUsersSent != that.percentageUsersSent) return false;
        if (eventProxyIntegrationConfig != null ? !eventProxyIntegrationConfig.equals(that.eventProxyIntegrationConfig) : that.eventProxyIntegrationConfig != null)
            return false;
        if (eventApiClientConfig != null ? !eventApiClientConfig.equals(that.eventApiClientConfig) : that.eventApiClientConfig != null)
            return false;
        if (products != null ? !products.equals(that.products) : that.products != null) return false;
        if (environments != null ? !environments.equals(that.environments) : that.environments != null) return false;
        return progressFolder != null ? progressFolder.equals(that.progressFolder) : that.progressFolder == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (eventProxyIntegrationConfig != null ? eventProxyIntegrationConfig.hashCode() : 0);
        result = 31 * result + (eventApiClientConfig != null ? eventApiClientConfig.hashCode() : 0);
        result = 31 * result + (products != null ? products.hashCode() : 0);
        result = 31 * result + (environments != null ? environments.hashCode() : 0);
        result = 31 * result + (progressFolder != null ? progressFolder.hashCode() : 0);
        result = 31 * result + maxAwsRetries;
        result = 31 * result + percentageUsersSent;
        return result;
    }
}
