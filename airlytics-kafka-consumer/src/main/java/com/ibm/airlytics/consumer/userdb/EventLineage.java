package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;
import java.util.stream.Collectors;

public class EventLineage extends LinkedHashMap<UserEvent, ConsumerRecord<String, JsonNode>> {

    public Set<UserEvent> getEvents() {
        return Collections.unmodifiableSet(keySet());
    }
    public Collection<ConsumerRecord<String, JsonNode>> getRecords() {
        return Collections.unmodifiableCollection(values());
    }

    public String getEventIds() {
        return formatEventIds(keySet());
    }

    public static String formatEventIds(Set<UserEvent> events) {
        if (events == null)
            return "null User Events";
        else
            return events.stream().map(event -> event.getEventId()).collect(Collectors.joining(","));
    }

    public Integer getUserPartition() {
        if (size() > 0)
            return getRecords().iterator().next().partition();
        else return null;
    }
}
