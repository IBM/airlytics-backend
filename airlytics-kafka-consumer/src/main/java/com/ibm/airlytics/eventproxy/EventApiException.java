package com.ibm.airlytics.eventproxy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import okhttp3.Response;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

public class EventApiException extends Exception {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(EventApiException.class.getName());

    private static ObjectMapper MAPPER = new ObjectMapper();

    private Response response;

    String responseBody = null;

    public EventApiException(Response response) {
        super();
        this.response = response;

        if(response != null) {
            try {
                responseBody = response.body().string(); // can be read only once!
            }
            catch(Exception e) {
                LOGGER.error("Missing response body from the event API");
            }
        }
    }

    public EventApiException(String s) {
        super(s);
    }

    @Override
    public String getMessage() {

        if(isResponsePresent()) {
            return String.format("Event API returned HTTP response %d", response.code());
        }
        String msg = super.getMessage();

        if(msg == null) {
            return "Error calling Event API";
        } else {
            return "Error calling Event API: " + msg;
        }
    }

    public boolean isResponsePresent() {
        return response != null;
    }

    public int getResponseCode() {

        if(isResponsePresent()) {
            return response.code();
        }
        return 0;
    }

    public boolean is202Accepted() {
        return getResponseCode() == 202;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public List<String> getShouldRetryEventIds() {

        if(is202Accepted() && responseBody != null) {

            try {

                JsonNode responseJson = MAPPER.readTree(responseBody);

                if(responseJson != null && responseJson instanceof ArrayNode) {
                    ArrayNode events = (ArrayNode)responseJson;
                    List<String> retries = new LinkedList<>();

                    for(int i = 0; i < events.size(); i++) {
                        JsonNode event = events.get(i);

                        if(event.has("shouldRetry") && event.has("eventId")) {

                            if( "true".equalsIgnoreCase(event.get("shouldRetry").asText()) ) {
                                retries.add(event.get("eventId").textValue());
                            }
                        }
                    }
                    return retries;
                }
            }
            catch (Exception e) {
                LOGGER.error("Unrecognized response format from event API: " + responseBody);
            }
        }
        return null;
    }
}
