package com.ibm.airlytics.consumer.cohorts.braze;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.braze.dto.BrazeTrackingResponse;
import com.ibm.airlytics.consumer.braze.forwarding.BrazeApiClient;
import com.ibm.airlytics.consumer.braze.forwarding.BrazeApiException;

import java.util.LinkedList;
import java.util.List;

public class TestBrazeClient extends BrazeApiClient {

    private List<JsonNode> attributes = new LinkedList<>();

    public TestBrazeClient() {
        super();
    }

    @Override
    public BrazeTrackingResponse uploadEvents(List<JsonNode> attributes, List<JsonNode> events, List<JsonNode> purchases) throws BrazeApiException {
        this.attributes.addAll(attributes);
        return null;
    }

    public List<JsonNode> getAttributes() {
        return attributes;
    }
}
