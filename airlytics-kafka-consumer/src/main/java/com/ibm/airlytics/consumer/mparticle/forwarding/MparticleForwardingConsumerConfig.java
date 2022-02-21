package com.ibm.airlytics.consumer.mparticle.forwarding;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class MparticleForwardingConsumerConfig extends AirlyticsConsumerConfig {

    private static final int MAX_THREADS = 100;
    private static final String MPARTICLE_KEY = "MPARTICLE_KEY";
    private static final String MPARTICLE_SECRET = "MPARTICLE_SECRET";

    private boolean mparticleIntegrationEnabled;

    private String mparticleApiBaseUrl;

    private int percentageUsersForwarded = 0;

    private int apiParallelThreads = MAX_THREADS;

    private int apiRateLimit = 270;

    private List<String> logEvents = null;

    public String getMparticleKey() {

        if(System.getenv().containsKey(MPARTICLE_KEY)) return System.getenv(MPARTICLE_KEY);

        return null;
    }

    public String getMparticleSecret() {

        if(System.getenv().containsKey(MPARTICLE_SECRET)) return System.getenv(MPARTICLE_SECRET);

        return null;
    }

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.MPARTICLE_FORWARDING_CONSUMER);
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

    public int getPercentageUsersForwarded() {
        return percentageUsersForwarded;
    }

    public void setPercentageUsersForwarded(int percentageUsersForwarded) {
        this.percentageUsersForwarded = percentageUsersForwarded;
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

    public List<String> getLogEvents() {
        return logEvents;
    }

    public void setLogEvents(List<String> logEvents) {
        this.logEvents = logEvents;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MparticleForwardingConsumerConfig.class.getSimpleName() + "[", "]")
                .add("maxPollRecords=" + maxPollRecords)
                .add("mparticleIntegrationEnabled=" + mparticleIntegrationEnabled)
                .add("mparticleApiBaseUrl='" + mparticleApiBaseUrl + "'")
                .add("percentageUsersForwarded=" + percentageUsersForwarded)
                .add("apiParallelThreads=" + apiParallelThreads)
                .add("apiRateLimit=" + apiRateLimit)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MparticleForwardingConsumerConfig that = (MparticleForwardingConsumerConfig) o;

        if (mparticleIntegrationEnabled != that.mparticleIntegrationEnabled) return false;
        if (percentageUsersForwarded != that.percentageUsersForwarded) return false;
        if (apiParallelThreads != that.apiParallelThreads) return false;
        if (apiRateLimit != that.apiRateLimit) return false;
        if (mparticleApiBaseUrl != null ? !mparticleApiBaseUrl.equals(that.mparticleApiBaseUrl) : that.mparticleApiBaseUrl != null)
            return false;
        return logEvents != null ? logEvents.equals(that.logEvents) : that.logEvents == null;
    }

    @Override
    public int hashCode() {
        int result = (mparticleIntegrationEnabled ? 1 : 0);
        result = 31 * result + (mparticleApiBaseUrl != null ? mparticleApiBaseUrl.hashCode() : 0);
        result = 31 * result + percentageUsersForwarded;
        result = 31 * result + apiParallelThreads;
        result = 31 * result + apiRateLimit;
        result = 31 * result + (logEvents != null ? logEvents.hashCode() : 0);
        return result;
    }
}
