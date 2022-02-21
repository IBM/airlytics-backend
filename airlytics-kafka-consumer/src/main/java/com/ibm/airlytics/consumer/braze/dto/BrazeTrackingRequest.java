package com.ibm.airlytics.consumer.braze.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class BrazeTrackingRequest {

    private List<JsonNode> attributes;

    private List<JsonNode> events;

    private List<JsonNode> purchases;

    public BrazeTrackingRequest() {
    }

    public BrazeTrackingRequest(List<JsonNode> attributes, List<JsonNode> events, List<JsonNode> purchases) {
        this.attributes = attributes;
        this.events = events;
        this.purchases = purchases;
    }

    public List<JsonNode> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<JsonNode> attributes) {
        this.attributes = attributes;
    }

    public List<JsonNode> getEvents() {
        return events;
    }

    public void setEvents(List<JsonNode> events) {
        this.events = events;
    }

    public List<JsonNode> getPurchases() {
        return purchases;
    }

    public void setPurchases(List<JsonNode> purchases) {
        this.purchases = purchases;
    }
}
