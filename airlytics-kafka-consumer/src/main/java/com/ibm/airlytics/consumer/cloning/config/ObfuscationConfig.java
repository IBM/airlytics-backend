package com.ibm.airlytics.consumer.cloning.config;

import java.util.List;
import java.util.StringJoiner;

public class ObfuscationConfig {

    private String hashPrefix;

    private boolean resendObfuscated = false;

    private List<EventFieldsObfuscationConfig> eventRules;

    public String getHashPrefix() {
        return hashPrefix;
    }

    public void setHashPrefix(String hashPrefix) {
        this.hashPrefix = hashPrefix;
    }

    public boolean isResendObfuscated() {
        return resendObfuscated;
    }

    public void setResendObfuscated(boolean resendObfuscated) {
        this.resendObfuscated = resendObfuscated;
    }

    public List<EventFieldsObfuscationConfig> getEventRules() { return eventRules; }

    public void setEventRules(List<EventFieldsObfuscationConfig> eventRules) { this.eventRules = eventRules; }

    @Override
    public String toString() {
        return new StringJoiner("\n ", ObfuscationConfig.class.getSimpleName() + "[", "]")
                .add("hashPrefix='" + hashPrefix + "'")
                .add("resendObfuscated=" + resendObfuscated)
                .add("fields=" + eventRules)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObfuscationConfig that = (ObfuscationConfig) o;

        if (resendObfuscated != that.resendObfuscated) return false;
        if (hashPrefix != null ? !hashPrefix.equals(that.hashPrefix) : that.hashPrefix != null) return false;
        return eventRules != null ? eventRules.equals(that.eventRules) : that.eventRules == null;
    }

    @Override
    public int hashCode() {
        int result = hashPrefix != null ? hashPrefix.hashCode() : 0;
        result = 31 * result + (resendObfuscated ? 1 : 0);
        result = 31 * result + (eventRules != null ? eventRules.hashCode() : 0);
        return result;
    }
}
