package com.ibm.weather.airlytics.amplitude.transform;

import java.util.List;
import java.util.StringJoiner;

public class GenericEventMappingConfig {

    protected boolean jsonObjectAttributeAccepted = true;

    protected boolean jsonArrayAttributeAccepted = true;

    protected boolean customDimsAcceptedByDefault = false;

    protected List<String> ignoreEventTypes;

    protected List<String> ignoreEventAttributes;

    protected List<String> ignoreCustomDimensions;

    protected List<String> includeEventTypes;

    protected List<String> includeEventAttributes;

    protected List<String> includeCustomDimensions;

    protected List<EventMappingConfig> eventMappings;

    public boolean isJsonObjectAttributeAccepted() {
        return jsonObjectAttributeAccepted;
    }

    public void setJsonObjectAttributeAccepted(boolean jsonObjectAttributeAccepted) {
        this.jsonObjectAttributeAccepted = jsonObjectAttributeAccepted;
    }

    public boolean isJsonArrayAttributeAccepted() {
        return jsonArrayAttributeAccepted;
    }

    public void setJsonArrayAttributeAccepted(boolean jsonArrayAttributeAccepted) {
        this.jsonArrayAttributeAccepted = jsonArrayAttributeAccepted;
    }

    public boolean isCustomDimsAcceptedByDefault() {
        return customDimsAcceptedByDefault;
    }

    public void setCustomDimsAcceptedByDefault(boolean customDimsAcceptedByDefault) {
        this.customDimsAcceptedByDefault = customDimsAcceptedByDefault;
    }

    public List<String> getIgnoreEventTypes() {
        return ignoreEventTypes;
    }

    public void setIgnoreEventTypes(List<String> ignoreEventTypes) {
        this.ignoreEventTypes = ignoreEventTypes;
    }

    public List<String> getIgnoreEventAttributes() {
        return ignoreEventAttributes;
    }

    public void setIgnoreEventAttributes(List<String> ignoreEventAttributes) {
        this.ignoreEventAttributes = ignoreEventAttributes;
    }

    public List<String> getIgnoreCustomDimensions() {
        return ignoreCustomDimensions;
    }

    public void setIgnoreCustomDimensions(List<String> ignoreCustomDimensions) {
        this.ignoreCustomDimensions = ignoreCustomDimensions;
    }

    public List<String> getIncludeEventTypes() {
        return includeEventTypes;
    }

    public void setIncludeEventTypes(List<String> includeEventTypes) {
        this.includeEventTypes = includeEventTypes;
    }

    public List<String> getIncludeEventAttributes() {
        return includeEventAttributes;
    }

    public void setIncludeEventAttributes(List<String> includeEventAttributes) {
        this.includeEventAttributes = includeEventAttributes;
    }

    public List<String> getIncludeCustomDimensions() {
        return includeCustomDimensions;
    }

    public void setIncludeCustomDimensions(List<String> includeCustomDimensions) {
        this.includeCustomDimensions = includeCustomDimensions;
    }

    public List<EventMappingConfig> getEventMappings() {
        return eventMappings;
    }

    public void setEventMappings(List<EventMappingConfig> eventMappings) {
        this.eventMappings = eventMappings;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", GenericEventMappingConfig.class.getSimpleName() + "[", "]")
                .add("jsonObjectAttributeAccepted=" + jsonObjectAttributeAccepted)
                .add("jsonArrayAttributeAccepted=" + jsonArrayAttributeAccepted)
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .add("eventMappings=" + eventMappings)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericEventMappingConfig that = (GenericEventMappingConfig) o;

        if (jsonObjectAttributeAccepted != that.jsonObjectAttributeAccepted) return false;
        if (jsonArrayAttributeAccepted != that.jsonArrayAttributeAccepted) return false;
        if (customDimsAcceptedByDefault != that.customDimsAcceptedByDefault) return false;
        if (ignoreEventTypes != null ? !ignoreEventTypes.equals(that.ignoreEventTypes) : that.ignoreEventTypes != null)
            return false;
        if (ignoreEventAttributes != null ? !ignoreEventAttributes.equals(that.ignoreEventAttributes) : that.ignoreEventAttributes != null)
            return false;
        if (ignoreCustomDimensions != null ? !ignoreCustomDimensions.equals(that.ignoreCustomDimensions) : that.ignoreCustomDimensions != null)
            return false;
        if (includeEventTypes != null ? !includeEventTypes.equals(that.includeEventTypes) : that.includeEventTypes != null)
            return false;
        if (includeEventAttributes != null ? !includeEventAttributes.equals(that.includeEventAttributes) : that.includeEventAttributes != null)
            return false;
        if (includeCustomDimensions != null ? !includeCustomDimensions.equals(that.includeCustomDimensions) : that.includeCustomDimensions != null)
            return false;
        return eventMappings != null ? eventMappings.equals(that.eventMappings) : that.eventMappings == null;
    }

    @Override
    public int hashCode() {
        int result = (jsonObjectAttributeAccepted ? 1 : 0);
        result = 31 * result + (jsonArrayAttributeAccepted ? 1 : 0);
        result = 31 * result + (customDimsAcceptedByDefault ? 1 : 0);
        result = 31 * result + (ignoreEventTypes != null ? ignoreEventTypes.hashCode() : 0);
        result = 31 * result + (ignoreEventAttributes != null ? ignoreEventAttributes.hashCode() : 0);
        result = 31 * result + (ignoreCustomDimensions != null ? ignoreCustomDimensions.hashCode() : 0);
        result = 31 * result + (includeEventTypes != null ? includeEventTypes.hashCode() : 0);
        result = 31 * result + (includeEventAttributes != null ? includeEventAttributes.hashCode() : 0);
        result = 31 * result + (includeCustomDimensions != null ? includeCustomDimensions.hashCode() : 0);
        result = 31 * result + (eventMappings != null ? eventMappings.hashCode() : 0);
        return result;
    }
}
