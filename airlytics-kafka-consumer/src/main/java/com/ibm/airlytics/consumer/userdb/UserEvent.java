package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.utilities.Product;
import com.ibm.airlytics.utilities.Products;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Timestamp;
import java.util.Comparator;

public class UserEvent {
    private String eventName;
    private String userId;
    private String platform;
    private String product;
    private Timestamp eventTime;
    private JsonNode attributes;
    private String appVersion;
    private String eventId;
    private String productId;
    private String schemaVersion;
    private Long revenue = null;

    public UserEvent(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();
        eventName = event.get("name").asText();
        userId = event.get("userId").asText();
        eventTime = new Timestamp(event.get("eventTime").asLong());
        platform = event.get("platform").asText();
        attributes = event.get("attributes");
        appVersion = event.get("appVersion") == null ? null : event.get("appVersion").asText();
        eventId = event.get("eventId").asText();
        schemaVersion = event.get("schemaVersion").asText();
        productId = event.get("productId").asText();

        Product prod = Products.getProductById(productId);
        if (prod != null) {
            product = prod.getName();
        }

        if (eventName.equals("ad-impression") && attributes != null)
            revenue = attributes.get("revenue") == null ? null : attributes.get("revenue").asLong();
    }

    private static Comparator<String> nullSafeStringComparator = Comparator.nullsFirst(String::compareTo);
    private static Comparator<Timestamp> nullSafeTimestampComparator = Comparator.nullsFirst(Timestamp::compareTo);
    public static Comparator<UserEvent> userIdEventTimeComparator = Comparator.nullsFirst(Comparator
            .comparing(UserEvent::getUserId, nullSafeStringComparator)
            .thenComparing(UserEvent::getEventTime, nullSafeTimestampComparator));

    public static Comparator<ConsumerRecord<String, JsonNode>> eventRecordComparator =
            Comparator.nullsFirst(Comparator
            .comparing((ConsumerRecord<String, JsonNode> record) -> record.value().get("userId").asText(), nullSafeStringComparator)
            .thenComparingLong(record -> record.value().get("eventTime").asLong()));

    public String getUserId() {
        return userId;
    }

    public String getPlatform() {
        return platform;
    }

    public String getProduct() { return product; }

    public Timestamp getEventTime() {
        return eventTime;
    }

    public JsonNode getAttributes() {
        return attributes;
    }

    public String getEventName() {
        return eventName;
    }

    public String getAppVersion() { return appVersion; }

    public String getEventId() { return eventId; }

    public String getSchemaVersion() { return schemaVersion; }

    public Object getProductId() { return productId; }

    public Long getRevenue() { return revenue; }
}
