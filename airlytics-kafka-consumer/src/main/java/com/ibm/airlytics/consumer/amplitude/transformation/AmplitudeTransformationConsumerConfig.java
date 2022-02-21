package com.ibm.airlytics.consumer.amplitude.transformation;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventMappingConsumerConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class AmplitudeTransformationConsumerConfig extends GenericEventMappingConsumerConfig {

    private List<String> lastSeenEvents = null;

    /** @deprecated use getDestinationTopic() */
    @Deprecated
    public String getAmplitudeEventsTopic() {
        return getDestinationTopic();
    }

    /** @deprecated use setDestinationTopic() */
    @Deprecated
    public void setAmplitudeEventsTopic(String amplitudeEventsTopic) {
        setDestinationTopic(amplitudeEventsTopic);
    }

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.AMPLITUDE_TRANSFORMATION_CONSUMER);
    }

    public List<String> getLastSeenEvents() {
        return lastSeenEvents;
    }

    public void setLastSeenEvents(List<String> lastSeenEvents) {
        this.lastSeenEvents = lastSeenEvents;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AmplitudeTransformationConsumerConfig.class.getSimpleName() + "[", "]")
                .add("maxPollRecords=" + maxPollRecords)
                .add("lastSeenEvents=" + lastSeenEvents)
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

        AmplitudeTransformationConsumerConfig that = (AmplitudeTransformationConsumerConfig) o;

        return lastSeenEvents != null ? lastSeenEvents.equals(that.lastSeenEvents) : that.lastSeenEvents == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (lastSeenEvents != null ? lastSeenEvents.hashCode() : 0);
        return result;
    }
}
