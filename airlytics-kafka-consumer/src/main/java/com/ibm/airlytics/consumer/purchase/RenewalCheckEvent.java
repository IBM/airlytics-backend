package com.ibm.airlytics.consumer.purchase;

import java.util.UUID;

public class RenewalCheckEvent {
    private String name = "renewal-status-check";
    private String purchaseToken;
    private String subscriptionId;
    private String transactionId;
    private Long eventTime;
    private String eventId;
    private String platform;

    public RenewalCheckEvent(String purchaseToken, String subscriptionId, String transactionId, Long eventTime, String platform) {
        this.purchaseToken = purchaseToken;
        this.subscriptionId = subscriptionId;
        this.transactionId = transactionId;
        this.eventTime = eventTime;
        this.platform = platform;
        this.eventId = UUID.randomUUID().toString();
    }

    public String getPurchaseToken() {
        return purchaseToken;
    }

    public void setPurchaseToken(String purchaseToken) {
        this.purchaseToken = purchaseToken;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
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
