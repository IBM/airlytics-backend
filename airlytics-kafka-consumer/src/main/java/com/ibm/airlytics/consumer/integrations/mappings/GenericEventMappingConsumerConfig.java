package com.ibm.airlytics.consumer.integrations.mappings;

import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.util.List;
import java.util.StringJoiner;

/**
 * Generic event mappings configuration. Extend it in a concrete third party integration mapping consumer configuration.
 */
public class GenericEventMappingConsumerConfig extends GenericEventFilteringConsumerConfig {

    protected String destinationTopic;

    protected boolean jsonObjectAttributeAccepted = true;

    protected boolean jsonArrayAttributeAccepted = true;

    protected List<EventMappingConfig> eventMappings;

    public String getDestinationTopic() {
        return destinationTopic;
    }

    public void setDestinationTopic(String destinationTopic) {
        this.destinationTopic = destinationTopic;
    }

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

    public List<EventMappingConfig> getEventMappings() {
        return eventMappings;
    }

    public void setEventMappings(List<EventMappingConfig> eventMappings) {
        this.eventMappings = eventMappings;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", GenericEventMappingConsumerConfig.class.getSimpleName() + "[", "]")
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .add("destinationTopic='" + destinationTopic + "'")
                .add("jsonObjectAttributeAccepted=" + jsonObjectAttributeAccepted)
                .add("jsonArrayAttributeAccepted=" + jsonArrayAttributeAccepted)
                .add("eventMappings=" + eventMappings)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        GenericEventMappingConsumerConfig that = (GenericEventMappingConsumerConfig) o;

        if (jsonObjectAttributeAccepted != that.jsonObjectAttributeAccepted) return false;
        if (jsonArrayAttributeAccepted != that.jsonArrayAttributeAccepted) return false;
        if (destinationTopic != null ? !destinationTopic.equals(that.destinationTopic) : that.destinationTopic != null)
            return false;
        return eventMappings != null ? eventMappings.equals(that.eventMappings) : that.eventMappings == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (destinationTopic != null ? destinationTopic.hashCode() : 0);
        result = 31 * result + (jsonObjectAttributeAccepted ? 1 : 0);
        result = 31 * result + (jsonArrayAttributeAccepted ? 1 : 0);
        result = 31 * result + (eventMappings != null ? eventMappings.hashCode() : 0);
        return result;
    }
}
