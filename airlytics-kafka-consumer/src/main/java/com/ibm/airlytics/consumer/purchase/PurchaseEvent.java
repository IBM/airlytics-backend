package com.ibm.airlytics.consumer.purchase;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Timestamp;

@SuppressWarnings("unused")
public class PurchaseEvent {
    private String name;
    private String userId;
    private String platform;
    private String product;
    private Timestamp eventTime;
    private JsonNode attributes;
    private String appVersion;
    private String eventId;
    private String productId;
    private String schemaVersion;

    public PurchaseEvent(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();
        // be able to process historical purchase event which were created with typo in the name attr.
        name = event.has("name") ? event.get("name").asText() : event.get("eventName").asText();
        userId = event.get("userId").asText();
        eventTime = new Timestamp(event.get("eventTime").asLong());
        platform = event.get("platform").asText();
        attributes = event.get("attributes");
        appVersion = event.get("appVersion") == null ? null : event.get("appVersion").asText();
        eventId = event.get("eventId").asText();
        schemaVersion = event.get("schemaVersion") == null ? null : event.get("schemaVersion").asText();
        productId = event.get("productId").asText();

        //TODO: fix according to configuration
        if (platform.equals("ios"))
            product = "iOS Product App";
        else
            product = "Android Product App";
    }


    public String getUserId() {
        return userId;
    }

    public String getPlatform() {
        return platform;
    }

    public String getProduct() {
        return product;
    }

    public Timestamp getEventTime() {
        return eventTime;
    }

    public JsonNode getAttributes() {
        return attributes;
    }

    public String getName() {
        return name;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getEventId() {
        return eventId;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public Object getProductId() {
        return productId;
    }
}
