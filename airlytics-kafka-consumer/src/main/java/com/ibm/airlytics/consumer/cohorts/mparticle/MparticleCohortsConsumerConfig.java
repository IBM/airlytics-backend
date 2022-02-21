package com.ibm.airlytics.consumer.cohorts.mparticle;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockCohortsConsumerConfig;

import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;

public class MparticleCohortsConsumerConfig extends AirlockCohortsConsumerConfig {

    private static final int MAX_THREADS = 100;

    private boolean mparticleIntegrationEnabled;
    private String mparticleApiBaseUrl;
    private int apiParallelThreads = MAX_THREADS;
    private int apiRateLimit = 270;
    // Env. var. name for API key per product ID
    private Map<String, String> mparticleApiKeys;
    private long maxWaitForBatchMs;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.MPARTICLE_COHORTS_CONSUMER);
    }

    public boolean isMparticleIntegrationEnabled() {
        return mparticleIntegrationEnabled;
    }

    public void setMparticleIntegrationEnabled(boolean mparticleIntegrationEnabled) {
        this.mparticleIntegrationEnabled = mparticleIntegrationEnabled;
    }

    public String getMparticleApiBaseUrl() {
        return mparticleApiBaseUrl;
    }

    public void setMparticleApiBaseUrl(String mparticleApiBaseUrl) {
        this.mparticleApiBaseUrl = mparticleApiBaseUrl;
    }

    public int getApiParallelThreads() {
        return apiParallelThreads;
    }

    public void setApiParallelThreads(int apiParallelThreads) {
        this.apiParallelThreads = apiParallelThreads;
    }

    public int getApiRateLimit() {
        return apiRateLimit;
    }

    public void setApiRateLimit(int apiRateLimit) {
        this.apiRateLimit = apiRateLimit;
    }

    public Map<String, String> getMparticleApiKeys() {
        return mparticleApiKeys;
    }

    public void setMparticleApiKeys(Map<String, String> mparticleApiKeys) {
        this.mparticleApiKeys = mparticleApiKeys;
    }

    public long getMaxWaitForBatchMs() {
        return maxWaitForBatchMs;
    }

    public void setMaxWaitForBatchMs(long maxWaitForBatchMs) {
        this.maxWaitForBatchMs = maxWaitForBatchMs;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MparticleCohortsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("maxPollRecords=" + maxPollRecords)
                .add("airlockApiEnabled=" + airlockApiEnabled)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("mparticleIntegrationEnabled=" + mparticleIntegrationEnabled)
                .add("mparticleApiBaseUrl='" + mparticleApiBaseUrl + "'")
                .add("apiParallelThreads=" + apiParallelThreads)
                .add("apiRateLimit=" + apiRateLimit)
                .add("mparticleApiKeys=" + mparticleApiKeys)
                .add("maxWaitForBatchMs=" + maxWaitForBatchMs)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MparticleCohortsConsumerConfig that = (MparticleCohortsConsumerConfig) o;

        if (mparticleIntegrationEnabled != that.mparticleIntegrationEnabled) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (apiRateLimit != that.apiRateLimit) return false;
        if (maxWaitForBatchMs != that.maxWaitForBatchMs) return false;
        if (mparticleApiBaseUrl != null ? !mparticleApiBaseUrl.equals(that.mparticleApiBaseUrl) : that.mparticleApiBaseUrl != null)
            return false;
        return mparticleApiKeys != null ? mparticleApiKeys.equals(that.mparticleApiKeys) : that.mparticleApiKeys == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (mparticleIntegrationEnabled ? 1 : 0);
        result = 31 * result + (mparticleApiBaseUrl != null ? mparticleApiBaseUrl.hashCode() : 0);
        result = 31 * result + apiParallelThreads;
        result = 31 * result + apiRateLimit;
        result = 31 * result + (mparticleApiKeys != null ? mparticleApiKeys.hashCode() : 0);
        result = 31 * result + (int) (maxWaitForBatchMs ^ (maxWaitForBatchMs >>> 32));
        return result;
    }
}
