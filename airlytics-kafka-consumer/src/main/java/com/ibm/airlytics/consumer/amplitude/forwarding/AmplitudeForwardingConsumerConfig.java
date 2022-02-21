package com.ibm.airlytics.consumer.amplitude.forwarding;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class AmplitudeForwardingConsumerConfig extends AirlyticsConsumerConfig {

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final int MAX_THREADS = 100;
    private static final String AMPLITUDE_KEY = "AMPLITUDE_KEY";

    private boolean amplitudeIntegrationEnabled;

    private String amplitudeApiBaseUrl;

    private String amplitudeEventApiPath;

    private int amplitudeEventApiBatchSize = 100;

    private String amplitudeBatchApiPath;

    private boolean useEventApi; // which API to use for events Event API or Batch API

    private int amplitudeBatchApiBatchSize = 1000;

    private int percentageUsersForwarded = 0;

    private int httpMaxConnections = HTTP_MAX_IDLE;

    private int httpKeepAlive = HTTP_KEEP_ALIVE;

    private int httpTimeout = HTTP_CONNECTION_TIMEOUT;

    private int apiParallelThreads = MAX_THREADS;

    private List<String> ignoredUsers = null;

    @Deprecated private String amplitudeApiKey;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.AMPLITUDE_FORWARDING_CONSUMER);
    }

    public boolean isAmplitudeIntegrationEnabled() {
        return amplitudeIntegrationEnabled;
    }

    public void setAmplitudeIntegrationEnabled(boolean amplitudeIntegrationEnabled) {
        this.amplitudeIntegrationEnabled = amplitudeIntegrationEnabled;
    }

    public String getAmplitudeApiBaseUrl() {
        return amplitudeApiBaseUrl;
    }

    public void setAmplitudeApiBaseUrl(String amplitudeApiBaseUrl) {
        this.amplitudeApiBaseUrl = amplitudeApiBaseUrl;
    }

    public String getAmplitudeEventApiPath() {
        return amplitudeEventApiPath;
    }

    public void setAmplitudeEventApiPath(String amplitudeEventApiPath) {
        this.amplitudeEventApiPath = amplitudeEventApiPath;
    }

    public int getAmplitudeEventApiBatchSize() {
        return amplitudeEventApiBatchSize;
    }

    public void setAmplitudeEventApiBatchSize(int amplitudeEventApiBatchSize) {
        this.amplitudeEventApiBatchSize = amplitudeEventApiBatchSize;
    }

    public String getAmplitudeBatchApiPath() {
        return amplitudeBatchApiPath;
    }

    public void setAmplitudeBatchApiPath(String amplitudeBatchApiPath) {
        this.amplitudeBatchApiPath = amplitudeBatchApiPath;
    }

    public int getAmplitudeBatchApiBatchSize() {
        return amplitudeBatchApiBatchSize;
    }

    public void setAmplitudeBatchApiBatchSize(int amplitudeBatchApiBatchSize) {
        this.amplitudeBatchApiBatchSize = amplitudeBatchApiBatchSize;
    }

    public boolean isUseEventApi() {
        return useEventApi;
    }

    public void setUseEventApi(boolean useEventApi) {
        this.useEventApi = useEventApi;
    }

    public int getRelevantBatchSize() {
        return useEventApi ? amplitudeEventApiBatchSize : amplitudeBatchApiBatchSize;
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

    public List<String> getIgnoredUsers() {
        return ignoredUsers;
    }

    public void setIgnoredUsers(List<String> ignoredUsers) {
        this.ignoredUsers = ignoredUsers;
    }

    public String getAmplitudeKey() {

        if(System.getenv().containsKey(AMPLITUDE_KEY)) return System.getenv(AMPLITUDE_KEY);

        return amplitudeApiKey;
    }

    @Deprecated
    public String getAmplitudeApiKey() {

        if(System.getenv().containsKey(AMPLITUDE_KEY)) return System.getenv(AMPLITUDE_KEY);

        return amplitudeApiKey;
    }

    @Deprecated
    public void setAmplitudeApiKey(String amplitudeApiKey) {
        this.amplitudeApiKey = amplitudeApiKey;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AmplitudeForwardingConsumerConfig.class.getSimpleName() + "[", "]")
                .add("amplitudeIntegrationEnabled=" + amplitudeIntegrationEnabled)
                .add("amplitudeApiBaseUrl='" + amplitudeApiBaseUrl + "'")
                .add("amplitudeEventApiPath='" + amplitudeEventApiPath + "'")
                .add("amplitudeEventApiBatchSize=" + amplitudeEventApiBatchSize)
                .add("amplitudeBatchApiPath='" + amplitudeBatchApiPath + "'")
                .add("useEventApi=" + useEventApi)
                .add("amplitudeBatchApiBatchSize=" + amplitudeBatchApiBatchSize)
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

        AmplitudeForwardingConsumerConfig that = (AmplitudeForwardingConsumerConfig) o;

        if (amplitudeIntegrationEnabled != that.amplitudeIntegrationEnabled) return false;
        if (amplitudeEventApiBatchSize != that.amplitudeEventApiBatchSize) return false;
        if (useEventApi != that.useEventApi) return false;
        if (amplitudeBatchApiBatchSize != that.amplitudeBatchApiBatchSize) return false;
        if (percentageUsersForwarded != that.percentageUsersForwarded) return false;
        if (httpMaxConnections != that.httpMaxConnections) return false;
        if (httpKeepAlive != that.httpKeepAlive) return false;
        if (httpTimeout != that.httpTimeout) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (amplitudeApiBaseUrl != null ? !amplitudeApiBaseUrl.equals(that.amplitudeApiBaseUrl) : that.amplitudeApiBaseUrl != null)
            return false;
        if (amplitudeEventApiPath != null ? !amplitudeEventApiPath.equals(that.amplitudeEventApiPath) : that.amplitudeEventApiPath != null)
            return false;
        return amplitudeBatchApiPath != null ? amplitudeBatchApiPath.equals(that.amplitudeBatchApiPath) : that.amplitudeBatchApiPath == null;
    }

    @Override
    public int hashCode() {
        int result = (amplitudeIntegrationEnabled ? 1 : 0);
        result = 31 * result + (amplitudeApiBaseUrl != null ? amplitudeApiBaseUrl.hashCode() : 0);
        result = 31 * result + (amplitudeEventApiPath != null ? amplitudeEventApiPath.hashCode() : 0);
        result = 31 * result + amplitudeEventApiBatchSize;
        result = 31 * result + (amplitudeBatchApiPath != null ? amplitudeBatchApiPath.hashCode() : 0);
        result = 31 * result + (useEventApi ? 1 : 0);
        result = 31 * result + amplitudeBatchApiBatchSize;
        result = 31 * result + percentageUsersForwarded;
        result = 31 * result + httpMaxConnections;
        result = 31 * result + httpKeepAlive;
        result = 31 * result + httpTimeout;
        result = 31 * result + apiParallelThreads;
        return result;
    }
}
