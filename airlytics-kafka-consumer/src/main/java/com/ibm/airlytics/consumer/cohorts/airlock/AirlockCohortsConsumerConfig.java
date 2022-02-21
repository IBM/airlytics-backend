package com.ibm.airlytics.consumer.cohorts.airlock;

import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.util.StringJoiner;

/**
 * All integrations sending AirCohorts to 3rd parties, must report back to Airlock Cohorts using Airlock API
 */
public abstract class AirlockCohortsConsumerConfig extends AirlyticsConsumerConfig {

    protected static final String AIRLOCK_API_KEY = "AIRLOCK_KEY";
    protected static final String AIRLOCK_API_PASSWORD = "AIRLOCK_PASSWORD";

    protected boolean airlockApiEnabled;
    protected String airlockApiBaseUrl;

    public boolean isAirlockApiEnabled() {
        return airlockApiEnabled;
    }

    public void setAirlockApiEnabled(boolean airlockApiEnabled) {
        this.airlockApiEnabled = airlockApiEnabled;
    }

    public String getAirlockApiBaseUrl() {
        return airlockApiBaseUrl;
    }

    public void setAirlockApiBaseUrl(String airlockApiBaseUrl) {
        this.airlockApiBaseUrl = airlockApiBaseUrl;
    }

    public String getAirlockKey() { return System.getenv(AIRLOCK_API_KEY); }

    public String getAirlockPassword() { return System.getenv(AIRLOCK_API_PASSWORD); }

    @Override
    public String toString() {
        return new StringJoiner(", ", AirlockCohortsConsumerConfig.class.getSimpleName() + "[", "]")
                .add("airlockApiEnabled=" + airlockApiEnabled)
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AirlockCohortsConsumerConfig that = (AirlockCohortsConsumerConfig) o;

        if (airlockApiEnabled != that.airlockApiEnabled) return false;
        return airlockApiBaseUrl != null ? airlockApiBaseUrl.equals(that.airlockApiBaseUrl) : that.airlockApiBaseUrl == null;
    }

    @Override
    public int hashCode() {
        int result = (airlockApiEnabled ? 1 : 0);
        result = 31 * result + (airlockApiBaseUrl != null ? airlockApiBaseUrl.hashCode() : 0);
        return result;
    }
}
