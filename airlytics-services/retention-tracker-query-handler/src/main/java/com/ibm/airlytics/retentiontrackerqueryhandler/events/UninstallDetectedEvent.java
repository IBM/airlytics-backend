package com.ibm.airlytics.retentiontrackerqueryhandler.events;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class UninstallDetectedEvent {
    public String getEventId() {
        return eventId;
    }

    public String getProductId() {
        return productId;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getName() {
        return name;
    }

    public long getEventTime() {
        return eventTime;
    }

    public String getPlatform() {
        return platform;
    }

    String eventId;
    String productId;
    String schemaVersion;

    public Map<String,String> getAttributes() {
        return attributes;
    }

    Map<String,String> attributes = new HashMap<>();
    final String name = "uninstall-detected";

    public String getUserId() {
        return userId;
    }

    String userId;
    long eventTime;
    String platform;

    public UninstallDetectedEvent() {

    }

    public void setEventId(String eventId) {
        this.eventId=eventId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }
}
