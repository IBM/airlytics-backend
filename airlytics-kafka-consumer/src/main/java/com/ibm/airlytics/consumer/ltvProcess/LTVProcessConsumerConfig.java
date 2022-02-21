package com.ibm.airlytics.consumer.ltvProcess;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;
import com.ibm.airlytics.consumer.integrations.dto.ProductDefinition;
import com.ibm.airlytics.eventproxy.EventApiClientConfig;
import com.ibm.airlytics.eventproxy.EventProxyIntegrationConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class LTVProcessConsumerConfig extends AirlyticsConsumerConfig {
    private  int ioActionRetries;
    private int processThreads;
    private String encryptionKey;
    private String progressFolder;
    private EventProxyIntegrationConfig eventProxyIntegrationConfig = new EventProxyIntegrationConfig();
    private EventApiClientConfig eventApiClientConfig = new EventApiClientConfig();
    private List<LTVEventAttribute> eventAttributes;
    private List<ProductDefinition> products;
    private int percentageUsersSent = 100; //in external 100%
    private int partitionsNumber=100; //the number of partitions of the ios and android topics - NOT of the ltv topic. Used for sent percentage calculation
    private String hashPrefix; //Used to obfuscate userId and eventId in internal env.
    private String StartProcessDay;
     //optional - default is false
    @JsonIgnoreProperties(ignoreUnknown = true)
    private boolean obfuscateValues = false;


    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.LTV_PROCESS_CONSUMER);
        this.maxPollRecords=1; //poll only 1 event at a time - ignore airlock config
    }

    @Deprecated
    public String getEventApiBaseUrl() {
        return eventApiClientConfig.getEventApiBaseUrl();
    }

    @Deprecated
    public void setEventApiBaseUrl(String eventApiBaseUrl) {
        eventApiClientConfig.setEventApiBaseUrl(eventApiBaseUrl);
    }

    @Deprecated
    public String getEventApiPath() {
        return eventApiClientConfig.getEventApiPath();
    }

    @Deprecated
    public void setEventApiPath(String eventApiPath) {
        eventApiClientConfig.setEventApiPath(eventApiPath);
    }

    @Deprecated
    public int getTimeoutSeconds() {
        return eventApiClientConfig.getReadTimeoutSeconds();
    }

    @Deprecated
    public void setTimeoutSeconds(int timeoutSeconds) {
        eventApiClientConfig.setConnectTimeoutSeconds(timeoutSeconds);
        eventApiClientConfig.setReadTimeoutSeconds(timeoutSeconds);
    }

    @Deprecated
    public int getEventApiBatchSize() {
        return eventProxyIntegrationConfig.getEventApiBatchSize();
    }

    @Deprecated
    public void setEventApiBatchSize(int eventApiBatchSize) {
        eventProxyIntegrationConfig.setEventApiBatchSize(eventApiBatchSize);
    }

    @Deprecated
    public int getMaxBatchEvents() {
        return getEventApiBatchSize();
    }

    @Deprecated
    public void setMaxBatchEvents(int maxBatchEvents) {
        setEventApiBatchSize(maxBatchEvents);
    }

    @Deprecated
    public int getEventApiRetries() {
        return eventProxyIntegrationConfig.getEventApiRetries();
    }

    @Deprecated
    public void setEventApiRetries(int eventApiRetries) {
        eventProxyIntegrationConfig.setEventApiRetries(eventApiRetries);
    }

    @Deprecated
    public int getEventApiRateLimit() {
        return eventProxyIntegrationConfig.getEventApiRateLimit();
    }

    @Deprecated
    public void setEventApiRateLimit(int eventApiRateLimit) {
        eventProxyIntegrationConfig.setEventApiRateLimit(eventApiRateLimit);
    }

    @Deprecated
    public int getEventApiParallelThreads() {
        return eventProxyIntegrationConfig.getEventApiParallelThreads();
    }

    @Deprecated
    public void setEventApiParallelThreads(int eventApiParallelThreads) {
        eventProxyIntegrationConfig.setEventApiParallelThreads(eventApiParallelThreads);
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

    public String getProgressFolder() {
        return progressFolder;
    }

    public void setProgressFolder(String progressFolder) {
        this.progressFolder = progressFolder;
    }

    public int getIoActionRetries() {
        return ioActionRetries;
    }

    public void setIoActionRetries(int ioActionRetries) {
        this.ioActionRetries = ioActionRetries;
    }

    public int getProcessThreads() {
        return processThreads;
    }

    public void setProcessThreads(int processThreads) {
        this.processThreads = processThreads;
    }

    public List<ProductDefinition> getProducts() {
        return products;
    }

    public void setProducts(List<ProductDefinition> products) {
        this.products = products;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public List<LTVEventAttribute> getEventAttributes() {
        return eventAttributes;
    }

    public void setEventAttributes(List<LTVEventAttribute> eventAttributes) {
        this.eventAttributes = eventAttributes;
    }

    public int getPercentageUsersSent() {
        return percentageUsersSent;
    }

    public void setPercentageUsersSent(int percentageUsersSent) {
        this.percentageUsersSent = percentageUsersSent;
    }

    public int getPartitionsNumber() {
        return partitionsNumber;
    }

    public void setPartitionsNumber(int partitionsNumber) {
        this.partitionsNumber = partitionsNumber;
    }

    public String getHashPrefix() {
        return hashPrefix;
    }

    public void setHashPrefix(String hashPrefix) {
        this.hashPrefix = hashPrefix;
    }

    public boolean isObfuscateValues() {
        return obfuscateValues;
    }

    public void setObfuscateValues(boolean obfuscateValues) {
        this.obfuscateValues = obfuscateValues;
    }

    public String getStartProcessDay() {
        return StartProcessDay;
    }

    public void setStartProcessDay(String startProcessDay) {
        StartProcessDay = startProcessDay;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", LTVProcessConsumerConfig.class.getSimpleName() + "[", "]")
                .add("ioActionRetries=" + ioActionRetries)
                .add("processThreads=" + processThreads)
                .add("encryptionKey='" + encryptionKey + "'")
                .add("progressFolder='" + progressFolder + "'")
                .add("eventProxyIntegrationConfig=" + eventProxyIntegrationConfig)
                .add("eventApiClientConfig=" + eventApiClientConfig)
                .add("eventAttributes=" + eventAttributes)
                .add("products=" + products)
                .add("percentageUsersSent=" + percentageUsersSent)
                .add("partitionsNumber=" + partitionsNumber)
                .add("hashPrefix='" + hashPrefix + "'")
                .add("StartProcessDay='" + StartProcessDay + "'")
                .add("obfuscateValues=" + obfuscateValues)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LTVProcessConsumerConfig that = (LTVProcessConsumerConfig) o;

        if (ioActionRetries != that.ioActionRetries) return false;
        if (processThreads != that.processThreads) return false;
        if (percentageUsersSent != that.percentageUsersSent) return false;
        if (partitionsNumber != that.partitionsNumber) return false;
        if (obfuscateValues != that.obfuscateValues) return false;
        if (encryptionKey != null ? !encryptionKey.equals(that.encryptionKey) : that.encryptionKey != null)
            return false;
        if (progressFolder != null ? !progressFolder.equals(that.progressFolder) : that.progressFolder != null)
            return false;
        if (eventProxyIntegrationConfig != null ? !eventProxyIntegrationConfig.equals(that.eventProxyIntegrationConfig) : that.eventProxyIntegrationConfig != null)
            return false;
        if (eventApiClientConfig != null ? !eventApiClientConfig.equals(that.eventApiClientConfig) : that.eventApiClientConfig != null)
            return false;
        if (eventAttributes != null ? !eventAttributes.equals(that.eventAttributes) : that.eventAttributes != null)
            return false;
        if (products != null ? !products.equals(that.products) : that.products != null) return false;
        if (hashPrefix != null ? !hashPrefix.equals(that.hashPrefix) : that.hashPrefix != null) return false;
        return StartProcessDay != null ? StartProcessDay.equals(that.StartProcessDay) : that.StartProcessDay == null;
    }

    @Override
    public int hashCode() {
        int result = ioActionRetries;
        result = 31 * result + processThreads;
        result = 31 * result + (encryptionKey != null ? encryptionKey.hashCode() : 0);
        result = 31 * result + (progressFolder != null ? progressFolder.hashCode() : 0);
        result = 31 * result + (eventProxyIntegrationConfig != null ? eventProxyIntegrationConfig.hashCode() : 0);
        result = 31 * result + (eventApiClientConfig != null ? eventApiClientConfig.hashCode() : 0);
        result = 31 * result + (eventAttributes != null ? eventAttributes.hashCode() : 0);
        result = 31 * result + (products != null ? products.hashCode() : 0);
        result = 31 * result + percentageUsersSent;
        result = 31 * result + partitionsNumber;
        result = 31 * result + (hashPrefix != null ? hashPrefix.hashCode() : 0);
        result = 31 * result + (StartProcessDay != null ? StartProcessDay.hashCode() : 0);
        result = 31 * result + (obfuscateValues ? 1 : 0);
        return result;
    }
}
