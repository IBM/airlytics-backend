package com.ibm.weather.airlytics.jobs.eventspatch.eventproxy;

import java.util.StringJoiner;

/**
 * All configurations for EventApiClient and components the use it
 */
public class EventProxyIntegrationConfig {

    private boolean eventApiEnabled = false;

    private int eventApiBatchSize = 1;

    private int eventApiRetries = 3;

    private int eventApiRateLimit = 0; // no limit

    private int eventApiParallelThreads = 20;

    public boolean isEventApiEnabled() {
        return eventApiEnabled;
    }

    public void setEventApiEnabled(boolean eventApiEnabled) {
        this.eventApiEnabled = eventApiEnabled;
    }

    public int getEventApiBatchSize() {
        return eventApiBatchSize;
    }

    public void setEventApiBatchSize(int eventApiBatchSize) {
        this.eventApiBatchSize = eventApiBatchSize;
    }

    public int getEventApiRetries() {
        return eventApiRetries;
    }

    public void setEventApiRetries(int eventApiRetries) {
        this.eventApiRetries = eventApiRetries;
    }

    public int getEventApiRateLimit() {
        return eventApiRateLimit;
    }

    public void setEventApiRateLimit(int eventApiRateLimit) {
        this.eventApiRateLimit = eventApiRateLimit;
    }

    public int getEventApiParallelThreads() {
        return eventApiParallelThreads;
    }

    public void setEventApiParallelThreads(int eventApiParallelThreads) {
        this.eventApiParallelThreads = eventApiParallelThreads;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventProxyIntegrationConfig.class.getSimpleName() + "[", "]")
                .add("eventApiEnabled=" + eventApiEnabled)
                .add("eventApiBatchSize=" + eventApiBatchSize)
                .add("eventApiRetries=" + eventApiRetries)
                .add("eventApiRateLimit=" + eventApiRateLimit)
                .add("eventApiParallelThreads=" + eventApiParallelThreads)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventProxyIntegrationConfig that = (EventProxyIntegrationConfig) o;

        if (eventApiEnabled != that.eventApiEnabled) return false;
        if (eventApiBatchSize != that.eventApiBatchSize) return false;
        if (eventApiRetries != that.eventApiRetries) return false;
        if (eventApiRateLimit != that.eventApiRateLimit) return false;
        return eventApiParallelThreads == that.eventApiParallelThreads;
    }

    @Override
    public int hashCode() {
        int result = (eventApiEnabled ? 1 : 0);
        result = 31 * result + eventApiBatchSize;
        result = 31 * result + eventApiRetries;
        result = 31 * result + eventApiRateLimit;
        result = 31 * result + eventApiParallelThreads;
        return result;
    }
}
