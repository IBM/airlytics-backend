package com.ibm.airlytics.consumer.braze.forwarding;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.StringJoiner;

public class BrazeForwardingConsumerConfig extends AirlyticsConsumerConfig {

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final int MAX_THREADS = 100;
    private static final String BRAZE_KEY = "BRAZE_KEY";

    private boolean brazeIntegrationEnabled;

    private String brazeApiBaseUrl;

    private String brazeApiPath;

    private String brazeAppId;

    @Deprecated private String brazeApiKey;

    private int percentageUsersForwarded = 0;

    private int httpMaxConnections = HTTP_MAX_IDLE;

    private int httpKeepAlive = HTTP_KEEP_ALIVE;

    private int httpTimeout = HTTP_CONNECTION_TIMEOUT;

    private int apiParallelThreads = MAX_THREADS;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.BRAZE_FORWARDING_CONSUMER);
    }

    public boolean isBrazeIntegrationEnabled() {
        return brazeIntegrationEnabled;
    }

    public void setBrazeIntegrationEnabled(boolean brazeIntegrationEnabled) {
        this.brazeIntegrationEnabled = brazeIntegrationEnabled;
    }

    public String getBrazeApiBaseUrl() {
        return brazeApiBaseUrl;
    }

    public void setBrazeApiBaseUrl(String brazeApiBaseUrl) {
        this.brazeApiBaseUrl = brazeApiBaseUrl;
    }

    public String getBrazeApiPath() {
        return brazeApiPath;
    }

    public void setBrazeApiPath(String brazeApiPath) {
        this.brazeApiPath = brazeApiPath;
    }

    public String getBrazeAppId() {
        return brazeAppId;
    }

    public void setBrazeAppId(String brazeAppId) {
        this.brazeAppId = brazeAppId;
    }

    public int getPercentageUsersForwarded() {
        return percentageUsersForwarded;
    }

    public void setPercentageUsersForwarded(int percentageUsersForwarded) {
        this.percentageUsersForwarded = percentageUsersForwarded;
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

    public String getBrazeKey() {

        if(System.getenv().containsKey(BRAZE_KEY)) return System.getenv(BRAZE_KEY);

        return brazeApiKey;
    }

    @Deprecated
    public String getBrazeApiKey() {

        if(System.getenv().containsKey(BRAZE_KEY)) return System.getenv(BRAZE_KEY);

        return brazeApiKey;
    }

    @Deprecated
    public void setBrazeApiKey(String brazeApiKey) {
        this.brazeApiKey = brazeApiKey;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BrazeForwardingConsumerConfig.class.getSimpleName() + "[", "]")
                .add("brazeIntegrationEnabled=" + brazeIntegrationEnabled)
                .add("brazeApiBaseUrl='" + brazeApiBaseUrl + "'")
                .add("brazeApiPath='" + brazeApiPath + "'")
                .add("brazeAppId='" + brazeAppId + "'")
                .add("percentageUsersForwarded=" + percentageUsersForwarded)
                .add("httpMaxConnections=" + httpMaxConnections)
                .add("httpKeepAlive=" + httpKeepAlive)
                .add("httpTimeout=" + httpTimeout)
                .add("apiParallelTreads=" + apiParallelThreads)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrazeForwardingConsumerConfig that = (BrazeForwardingConsumerConfig) o;

        if (brazeIntegrationEnabled != that.brazeIntegrationEnabled) return false;
        if (percentageUsersForwarded != that.percentageUsersForwarded) return false;
        if (httpMaxConnections != that.httpMaxConnections) return false;
        if (httpKeepAlive != that.httpKeepAlive) return false;
        if (httpTimeout != that.httpTimeout) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (brazeApiBaseUrl != null ? !brazeApiBaseUrl.equals(that.brazeApiBaseUrl) : that.brazeApiBaseUrl != null)
            return false;
        if (brazeApiPath != null ? !brazeApiPath.equals(that.brazeApiPath) : that.brazeApiPath != null)
            return false;
        return brazeAppId != null ? brazeAppId.equals(that.brazeAppId) : that.brazeAppId == null;
    }

    @Override
    public int hashCode() {
        int result = (brazeIntegrationEnabled ? 1 : 0);
        result = 31 * result + (brazeApiBaseUrl != null ? brazeApiBaseUrl.hashCode() : 0);
        result = 31 * result + (brazeApiPath != null ? brazeApiPath.hashCode() : 0);
        result = 31 * result + (brazeAppId != null ? brazeAppId.hashCode() : 0);
        result = 31 * result + percentageUsersForwarded;
        result = 31 * result + httpMaxConnections;
        result = 31 * result + httpKeepAlive;
        result = 31 * result + httpTimeout;
        result = 31 * result + apiParallelThreads;
        return result;
    }
}
