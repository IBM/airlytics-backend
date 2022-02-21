package com.ibm.airlytics.consumer.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Timestamp;
import java.util.Comparator;

public class TransformedUserEvent {
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

    public TransformedUserEvent(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();
        name = event.get("name").asText();
        userId = event.get("userId").asText();
        eventTime = new Timestamp(event.get("eventTime").asLong());
        platform = event.get("platform").asText();
        attributes = event.get("attributes");
        appVersion = event.get("appVersion") == null ? null : event.get("appVersion").asText();
        eventId = event.get("eventId").asText();
        schemaVersion = event.get("schemaVersion").asText();
        productId = event.get("productId") == null ? null : event.get("productId").asText();

        //TODO: fix according to configuration
        if (platform.equals("ios"))
            product = "iOS Product App";
        else
            product = "Android Product App";
    }

    private static Comparator<String> nullSafeStringComparator = Comparator.nullsFirst(String::compareTo);
    private static Comparator<Timestamp> nullSafeTimestampComparator = Comparator.nullsFirst(Timestamp::compareTo);
    public static Comparator<TransformedUserEvent> userIdEventTimeComparator = Comparator.nullsFirst(Comparator
            .comparing(TransformedUserEvent::getUserId, nullSafeStringComparator)
            .thenComparing(TransformedUserEvent::getEventTime, nullSafeTimestampComparator));

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
