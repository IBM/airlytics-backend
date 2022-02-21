package com.ibm.airlytics.consumer.purchase.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

import javax.annotation.Nullable;
import java.util.UUID;

public abstract class BaseSubscriptionEvent<T> {
    protected String name;
    protected String userId;
    protected String eventId;
    protected String platform;
    protected long eventTime;
    protected String schemaVersion;
    protected T attributes;
    protected String productId;
    protected static final ObjectMapper oMapper = new ObjectMapper();


    @Nullable
    protected AirlyticsPurchase currentAirlyticsPurchase;

    protected BaseSubscriptionEvent() {

    }

    protected BaseSubscriptionEvent(AirlyticsPurchase currentAirlyticsPurchase, AirlyticsPurchase updatedAirlyticsPurchase,
                                    String airlockProductId, Long eventTime, String originalEventId, String platform,
                                    String name) {
        if (updatedAirlyticsPurchase.getUserId() == null) {
            throw new IllegalArgumentException("Can't create event, user Id is missing");
        }
        this.platform = platform;
        this.name = name;
        this.eventTime = eventTime;
        this.eventId = UUID.nameUUIDFromBytes((originalEventId + updatedAirlyticsPurchase.getId() + name).getBytes()).toString();
        this.productId = airlockProductId;
        this.userId = updatedAirlyticsPurchase.getUserId();
        this.currentAirlyticsPurchase = currentAirlyticsPurchase;
        this.attributes = createAttributes(updatedAirlyticsPurchase);
    }

    public abstract T createAttributes(AirlyticsPurchase airlyticsPurchase);

    public String toJson() throws JsonProcessingException {
        return oMapper.writeValueAsString(this);
    }

    @JsonIgnore
    public String getPremiumProductId() {
        return ((Attributes) attributes).premiumProductId;
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

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }


    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public T getAttributes() {
        return attributes;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public void setAttributes(T attributes) {
        this.attributes = attributes;
    }
}
