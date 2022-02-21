package com.ibm.airlytics.consumer.purchase.inject;


import java.util.UUID;

public class InjectedIOSUserAttributeEvent {
    private String name = "user-attributes";
    private Attributes attributes;
    private Long eventTime;
    private String eventId;
    private String platform;
    private String userId;
    private String productId;
    private String schemaVersion;


    private class Attributes {
        private String encodedReceipt;

        public Attributes(String encodedReceipt) {
            this.encodedReceipt = encodedReceipt;
        }

        public String getEncodedReceipt() {
            return encodedReceipt;
        }

        public void setEncodedReceipt(String encodedReceipt) {
            this.encodedReceipt = encodedReceipt;
        }
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public InjectedIOSUserAttributeEvent(String encodedReceipt, String productId, String platform, String userId) {
        this.attributes = new Attributes(encodedReceipt);
        this.userId = userId;
        this.productId = productId;
        this.eventTime = System.currentTimeMillis();
        this.platform = platform;
        this.eventId = UUID.randomUUID().toString();
        this.schemaVersion = "1.0";
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }
}
