package com.ibm.airlytics.consumer.cohorts.localytics;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockCohortsConsumerConfig;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

public class LocalyticsCohortsConsumerConfig extends AirlockCohortsConsumerConfig {

    private static final String LOCALYTICS_KEY = "LOCALYTICS_KEY";
    private static final String LOCALYTICS_SECRET = "LOCALYTICS_SECRET";

    private boolean localyticsApiEnabled;
    private String apiBaseUrl;
    private String localyticsOrgId;
    private Map<String, String> localyticsAppIds;
    private long maxWaitForBatchMs;
    private int maxBatchSize;
    @Deprecated private String apiKey;
    @Deprecated private String apiSecret;

    public boolean isLocalyticsApiEnabled() {
        return localyticsApiEnabled;
    }

    public void setLocalyticsApiEnabled(boolean localyticsApiEnabled) {
        this.localyticsApiEnabled = localyticsApiEnabled;
    }

    public String getApiBaseUrl() {
        return apiBaseUrl;
    }

    public void setApiBaseUrl(String apiBaseUrl) {
        this.apiBaseUrl = apiBaseUrl;
    }

    public String getLocalyticsOrgId() {
        return localyticsOrgId;
    }

    public void setLocalyticsOrgId(String localyticsOrgId) {
        this.localyticsOrgId = localyticsOrgId;
    }

    public Map<String, String> getLocalyticsAppIds() {
        return localyticsAppIds;
    }

    public void setLocalyticsAppIds(Map<String, String> localyticsAppIds) {
        this.localyticsAppIds = localyticsAppIds;
    }

    public long getMaxWaitForBatchMs() {
        return maxWaitForBatchMs;
    }

    public void setMaxWaitForBatchMs(long maxWaitForBatchMs) {
        this.maxWaitForBatchMs = maxWaitForBatchMs;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    public String getLocalyticsKey() {

        if(System.getenv().containsKey(LOCALYTICS_KEY)) return System.getenv(LOCALYTICS_KEY);

        return apiKey;
    }

    public String getLocalyticsSecret() {

        if(System.getenv().containsKey(LOCALYTICS_SECRET)) return System.getenv(LOCALYTICS_SECRET);

        return apiSecret;
    }

    @Deprecated
    public String getApiKey() {

        if(System.getenv().containsKey(LOCALYTICS_KEY)) return System.getenv(LOCALYTICS_KEY);

        return apiKey;
    }

    @Deprecated
    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    @Deprecated
    public String getApiSecret() {

        if(System.getenv().containsKey(LOCALYTICS_SECRET)) return System.getenv(LOCALYTICS_SECRET);

        return apiSecret;
    }

    @Deprecated
    public void setApiSecret(String apiSecret) {
        this.apiSecret = apiSecret;
    }

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.LOCALYTICS_COHORTS_CONSUMER);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", LocalyticsCohortsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("airlockApiEnabled=" + airlockApiEnabled)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("localyticsApiEnabled=" + localyticsApiEnabled)
                .add("apiBaseUrl='" + apiBaseUrl + "'")
                .add("localyticsOrgId='" + localyticsOrgId + "'")
                .add("localyticsAppIds=" + localyticsAppIds)
                .add("maxWaitForBatchMs=" + maxWaitForBatchMs)
                .add("maxBatchSize=" + maxBatchSize)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LocalyticsCohortsConsumerConfig that = (LocalyticsCohortsConsumerConfig) o;

        if (localyticsApiEnabled != that.localyticsApiEnabled) return false;
        if (maxWaitForBatchMs != that.maxWaitForBatchMs) return false;
        if (maxBatchSize != that.maxBatchSize) return false;
        if (apiBaseUrl != null ? !apiBaseUrl.equals(that.apiBaseUrl) : that.apiBaseUrl != null) return false;
        if (localyticsOrgId != null ? !localyticsOrgId.equals(that.localyticsOrgId) : that.localyticsOrgId != null)
            return false;
        return localyticsAppIds != null ? localyticsAppIds.equals(that.localyticsAppIds) : that.localyticsAppIds == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (localyticsApiEnabled ? 1 : 0);
        result = 31 * result + (apiBaseUrl != null ? apiBaseUrl.hashCode() : 0);
        result = 31 * result + (localyticsOrgId != null ? localyticsOrgId.hashCode() : 0);
        result = 31 * result + (localyticsAppIds != null ? localyticsAppIds.hashCode() : 0);
        result = 31 * result + (int) (maxWaitForBatchMs ^ (maxWaitForBatchMs >>> 32));
        result = 31 * result + maxBatchSize;
        return result;
    }
}
