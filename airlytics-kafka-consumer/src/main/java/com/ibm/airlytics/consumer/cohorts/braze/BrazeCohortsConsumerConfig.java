package com.ibm.airlytics.consumer.cohorts.braze;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockCohortsConsumerConfig;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

public class BrazeCohortsConsumerConfig extends AirlockCohortsConsumerConfig {

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final int MAX_THREADS = 200;

    private boolean brazeApiEnabled;
    private String apiBaseUrl;
    private String apiPath;
    // Env. var. name for API key per product ID
    private Map<String, String> brazeApiKeys;
    private long maxWaitForBatchMs;
    private int httpMaxConnections = HTTP_MAX_IDLE;
    private int httpKeepAlive = HTTP_KEEP_ALIVE;
    private int httpTimeout = HTTP_CONNECTION_TIMEOUT;
    private int apiParallelThreads = MAX_THREADS;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.BRAZE_COHORTS_CONSUMER);
    }

    public boolean isBrazeApiEnabled() {
        return brazeApiEnabled;
    }

    public void setBrazeApiEnabled(boolean brazeApiEnabled) {
        this.brazeApiEnabled = brazeApiEnabled;
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

    public Map<String, String> getBrazeApiKeys() {
        return brazeApiKeys;
    }

    public void setBrazeApiKeys(Map<String, String> brazeApiKeys) {
        this.brazeApiKeys = brazeApiKeys;
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
        return new StringJoiner(", ", BrazeCohortsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("airlockApiEnabled=" + airlockApiEnabled)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("brazeApiEnabled=" + brazeApiEnabled)
                .add("apiBaseUrl='" + apiBaseUrl + "'")
                .add("apiPath='" + apiPath + "'")
                .add("brazeApiKeys=" + brazeApiKeys)
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

        BrazeCohortsConsumerConfig that = (BrazeCohortsConsumerConfig) o;

        if (brazeApiEnabled != that.brazeApiEnabled) return false;
        if (maxWaitForBatchMs != that.maxWaitForBatchMs) return false;
        if (httpMaxConnections != that.httpMaxConnections) return false;
        if (httpKeepAlive != that.httpKeepAlive) return false;
        if (httpTimeout != that.httpTimeout) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (apiBaseUrl != null ? !apiBaseUrl.equals(that.apiBaseUrl) : that.apiBaseUrl != null) return false;
        if (apiPath != null ? !apiPath.equals(that.apiPath) : that.apiPath != null) return false;
        return brazeApiKeys != null ? brazeApiKeys.equals(that.brazeApiKeys) : that.brazeApiKeys == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (brazeApiEnabled ? 1 : 0);
        result = 31 * result + (apiBaseUrl != null ? apiBaseUrl.hashCode() : 0);
        result = 31 * result + (apiPath != null ? apiPath.hashCode() : 0);
        result = 31 * result + (brazeApiKeys != null ? brazeApiKeys.hashCode() : 0);
        result = 31 * result + (int) (maxWaitForBatchMs ^ (maxWaitForBatchMs >>> 32));
        result = 31 * result + httpMaxConnections;
        result = 31 * result + httpKeepAlive;
        result = 31 * result + httpTimeout;
        result = 31 * result + apiParallelThreads;
        return result;
    }
}
