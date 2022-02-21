package com.ibm.weather.airlytics.amplitude.transform;

import java.util.Map;
import java.util.StringJoiner;

public class PropertyMappingConfig {

    private Map<String, String> eventProperties;

    private Map<String, String> userProperties;

    public Map<String, String> getEventProperties() {
        return eventProperties;
    }

    public void setEventProperties(Map<String, String> eventProperties) {
        this.eventProperties = eventProperties;
    }

    public Map<String, String> getUserProperties() {
        return userProperties;
    }

    public void setUserProperties(Map<String, String> userProperties) {
        this.userProperties = userProperties;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", PropertyMappingConfig.class.getSimpleName() + "[", "]")
                .add("eventProperties=" + eventProperties)
                .add("userProperties=" + userProperties)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PropertyMappingConfig that = (PropertyMappingConfig) o;

        if (eventProperties != null ? !eventProperties.equals(that.eventProperties) : that.eventProperties != null)
            return false;
        return userProperties != null ? userProperties.equals(that.userProperties) : that.userProperties == null;
    }

    @Override
    public int hashCode() {
        int result = eventProperties != null ? eventProperties.hashCode() : 0;
        result = 31 * result + (userProperties != null ? userProperties.hashCode() : 0);
        return result;
    }
}
