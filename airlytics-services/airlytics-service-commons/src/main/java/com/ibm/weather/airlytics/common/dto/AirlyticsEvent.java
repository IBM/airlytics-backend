package com.ibm.weather.airlytics.common.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AirlyticsEvent {

    public static final String USER_ATTRIBUTES_EVENT = "user-attributes";

    private String eventId;

    private String name;

    private String userId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String registeredUserId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String sessionId;

    private String productId;

    private String platform;

    private Long eventTime;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long sessionStartTime;

    private String schemaVersion;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String appVersion;

    private Map<String, Object> attributes;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Map<String, Object> customDimensions;

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getRegisteredUserId() {
        return registeredUserId;
    }

    public void setRegisteredUserId(String registeredUserId) {
        this.registeredUserId = registeredUserId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getSessionStartTime() { return sessionStartTime; }

    public void setSessionStartTime(Long sessionStartTime) { this.sessionStartTime = sessionStartTime; }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Object> getCustomDimensions() {
        return customDimensions;
    }

    public void setCustomDimensions(Map<String, Object> customDimensions) {
        this.customDimensions = customDimensions;
    }
}
