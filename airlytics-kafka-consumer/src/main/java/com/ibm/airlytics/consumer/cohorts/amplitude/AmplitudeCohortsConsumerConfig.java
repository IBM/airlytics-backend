package com.ibm.airlytics.consumer.cohorts.amplitude;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockCohortsConsumerConfig;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

public class AmplitudeCohortsConsumerConfig extends AirlockCohortsConsumerConfig {

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final int MAX_THREADS = 200;

    private boolean amplitudeApiEnabled;
    private String apiBaseUrl;
    private String apiPath;
    private int apiBatchSize;
    private boolean triggerEventsEnabled = true;
    // Env. var. name for API key per product ID
    private Map<String, String> amplitudeApiKeys;
    private long maxWaitForBatchMs;
    private int httpMaxConnections = HTTP_MAX_IDLE;
    private int httpKeepAlive = HTTP_KEEP_ALIVE;
    private int httpTimeout = HTTP_CONNECTION_TIMEOUT;
    private int apiParallelThreads = MAX_THREADS;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.AMPLITUDE_COHORTS_CONSUMER);
    }

    public boolean isAmplitudeApiEnabled() {
        return amplitudeApiEnabled;
    }

    public void setAmplitudeApiEnabled(boolean amplitudeApiEnabled) {
        this.amplitudeApiEnabled = amplitudeApiEnabled;
    }

    public String getApiBaseUrl() {
        return apiBaseUrl;
    }

    public void setApiBaseUrl(String apiBaseUrl) {
        this.apiBaseUrl = apiBaseUrl;
    }

    public String getApiPath() {
        return apiPath;
    }

    public void setApiPath(String apiPath) {
        this.apiPath = apiPath;
    }

    public int getApiBatchSize() {
        return apiBatchSize;
    }

    public void setApiBatchSize(int apiBatchSize) {
        this.apiBatchSize = apiBatchSize;
    }

    public boolean isTriggerEventsEnabled() {
        return triggerEventsEnabled;
    }

    public void setTriggerEventsEnabled(boolean triggerEventsEnabled) {
        this.triggerEventsEnabled = triggerEventsEnabled;
    }

    public Map<String, String> getAmplitudeApiKeys() {
        return amplitudeApiKeys;
    }

    public void setAmplitudeApiKeys(Map<String, String> amplitudeApiKeys) {
        this.amplitudeApiKeys = amplitudeApiKeys;
    }

    public long getMaxWaitForBatchMs() {
        return maxWaitForBatchMs;
    }

    public void setMaxWaitForBatchMs(long maxWaitForBatchMs) {
        this.maxWaitForBatchMs = maxWaitForBatchMs;
    }

    public int getHttpMaxConnections() {
        return httpMaxConnections;
    }

    public void setHttpMaxConnections(int httpMaxConnections) {
        this.httpMaxConnections = httpMaxConnections;
    }

    public int getHttpKeepAlive() {
        return httpKeepAlive;
    }

    public void setHttpKeepAlive(int httpKeepAlive) {
        this.httpKeepAlive = httpKeepAlive;
    }

    public int getHttpTimeout() {
        return httpTimeout;
    }

    public void setHttpTimeout(int httpTimeout) {
        this.httpTimeout = httpTimeout;
    }

    public int getApiParallelThreads() {
        return apiParallelThreads;
    }

    public void setApiParallelThreads(int apiParallelThreads) {
        this.apiParallelThreads = apiParallelThreads;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AmplitudeCohortsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("maxPollRecords=" + maxPollRecords)
                .add("airlockApiEnabled=" + airlockApiEnabled)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("amplitudeApiEnabled=" + amplitudeApiEnabled)
                .add("apiBaseUrl='" + apiBaseUrl + "'")
                .add("apiPath='" + apiPath + "'")
                .add("apiBatchSize=" + apiBatchSize)
                .add("triggerEventsEnabled=" + triggerEventsEnabled)
                .add("amplitudeApiKeys=" + amplitudeApiKeys)
                .add("maxWaitForBatchMs=" + maxWaitForBatchMs)
                .add("httpMaxConnections=" + httpMaxConnections)
                .add("httpKeepAlive=" + httpKeepAlive)
                .add("httpTimeout=" + httpTimeout)
                .add("apiParallelThreads=" + apiParallelThreads)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AmplitudeCohortsConsumerConfig that = (AmplitudeCohortsConsumerConfig) o;

        if (amplitudeApiEnabled != that.amplitudeApiEnabled) return false;
        if (apiBatchSize != that.apiBatchSize) return false;
        if (triggerEventsEnabled != that.triggerEventsEnabled) return false;
        if (maxWaitForBatchMs != that.maxWaitForBatchMs) return false;
        if (httpMaxConnections != that.httpMaxConnections) return false;
        if (httpKeepAlive != that.httpKeepAlive) return false;
        if (httpTimeout != that.httpTimeout) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (apiBaseUrl != null ? !apiBaseUrl.equals(that.apiBaseUrl) : that.apiBaseUrl != null) return false;
        if (apiPath != null ? !apiPath.equals(that.apiPath) : that.apiPath != null) return false;
        return amplitudeApiKeys != null ? amplitudeApiKeys.equals(that.amplitudeApiKeys) : that.amplitudeApiKeys == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (amplitudeApiEnabled ? 1 : 0);
        result = 31 * result + (apiBaseUrl != null ? apiBaseUrl.hashCode() : 0);
        result = 31 * result + (apiPath != null ? apiPath.hashCode() : 0);
        result = 31 * result + apiBatchSize;
        result = 31 * result + (triggerEventsEnabled ? 1 : 0);
        result = 31 * result + (amplitudeApiKeys != null ? amplitudeApiKeys.hashCode() : 0);
        result = 31 * result + (int) (maxWaitForBatchMs ^ (maxWaitForBatchMs >>> 32));
        result = 31 * result + httpMaxConnections;
        result = 31 * result + httpKeepAlive;
        result = 31 * result + httpTimeout;
        result = 31 * result + apiParallelThreads;
        return result;
    }
}
