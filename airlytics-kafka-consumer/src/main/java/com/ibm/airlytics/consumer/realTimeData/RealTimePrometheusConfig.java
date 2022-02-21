package com.ibm.airlytics.consumer.realTimeData;

import java.util.*;

public class RealTimePrometheusConfig {

    static class EventConfig {
        private List<String> attributes;
        private Long timeThresholdSeconds;
        private Long staleThresholdSeconds;
        public List<String> getAttributes() {
            return attributes;
        }

        public void setAttributes(List<String> attributes) {
            this.attributes = attributes;
        }

        public Long getTimeThresholdSeconds() {
            return timeThresholdSeconds;
        }

        public void setTimeThresholdSeconds(Long timeThresholdSeconds) {
            this.timeThresholdSeconds = timeThresholdSeconds;
        }

        public Long getStaleThresholdSeconds() {
            return staleThresholdSeconds;
        }

        public void setStaleThresholdSeconds(Long staleThresholdSeconds) {
            this.staleThresholdSeconds = staleThresholdSeconds;
        }

        public boolean attributesEquals(EventConfig other) {
            return Objects.equals(this.attributes,other.attributes);
        }
        public boolean allEquals(EventConfig other) {
            return this.attributesEquals(other) && Objects.equals(this.timeThresholdSeconds, other.timeThresholdSeconds) && Objects.equals(this.staleThresholdSeconds, other.staleThresholdSeconds);
        }
    }

    private Map<String, EventConfig> events;
    public Map<String, EventConfig> getEvents() {
        return events;
    }

    public void setEvents(Map<String, EventConfig> events) {
        this.events = events;
    }

    public EventConfig getEventConfig(String eventName) {
        return this.events.get(eventName);
    }
}
