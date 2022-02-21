package com.ibm.airlytics.eventproxy;

import java.util.StringJoiner;

public class EventApiClientConfig {

    private String eventApiBaseUrl;

    private String eventApiPath;

    private String eventApiKey;

    private int connectTimeoutSeconds = 10;

    private int readTimeoutSeconds = 30;

    public String getEventApiBaseUrl() {
        return eventApiBaseUrl;
    }

    public void setEventApiBaseUrl(String eventApiBaseUrl) {
        this.eventApiBaseUrl = eventApiBaseUrl;
    }

    public String getEventApiPath() {
        return eventApiPath;
    }

    public void setEventApiPath(String eventApiPath) {
        this.eventApiPath = eventApiPath;
    }

    public String getEventApiKey() {
        return eventApiKey;
    }

    public void setEventApiKey(String eventApiKey) {
        this.eventApiKey = eventApiKey;
    }

    public int getConnectTimeoutSeconds() {
        return connectTimeoutSeconds;
    }

    public void setConnectTimeoutSeconds(int connectTimeoutSeconds) {
        this.connectTimeoutSeconds = connectTimeoutSeconds;
    }

    public int getReadTimeoutSeconds() {
        return readTimeoutSeconds;
    }

    public void setReadTimeoutSeconds(int readTimeoutSeconds) {
        this.readTimeoutSeconds = readTimeoutSeconds;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EventApiClientConfig.class.getSimpleName() + "[", "]")
                .add("eventApiBaseUrl='" + eventApiBaseUrl + "'")
                .add("eventApiPath='" + eventApiPath + "'")
                .add("eventApiKey='" + eventApiKey + "'")
                .add("connectTimeoutSeconds=" + connectTimeoutSeconds)
                .add("readTimeoutSeconds=" + readTimeoutSeconds)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventApiClientConfig that = (EventApiClientConfig) o;

        if (connectTimeoutSeconds != that.connectTimeoutSeconds) return false;
        if (readTimeoutSeconds != that.readTimeoutSeconds) return false;
        if (eventApiBaseUrl != null ? !eventApiBaseUrl.equals(that.eventApiBaseUrl) : that.eventApiBaseUrl != null)
            return false;
        if (eventApiPath != null ? !eventApiPath.equals(that.eventApiPath) : that.eventApiPath != null) return false;
        return eventApiKey != null ? eventApiKey.equals(that.eventApiKey) : that.eventApiKey == null;
    }

    @Override
    public int hashCode() {
        int result = eventApiBaseUrl != null ? eventApiBaseUrl.hashCode() : 0;
        result = 31 * result + (eventApiPath != null ? eventApiPath.hashCode() : 0);
        result = 31 * result + (eventApiKey != null ? eventApiKey.hashCode() : 0);
        result = 31 * result + connectTimeoutSeconds;
        result = 31 * result + readTimeoutSeconds;
        return result;
    }
}
