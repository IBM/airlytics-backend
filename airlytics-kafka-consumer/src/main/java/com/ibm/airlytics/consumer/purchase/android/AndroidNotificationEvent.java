package com.ibm.airlytics.consumer.purchase.android;


import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "version",
        "packageName",
        "eventTimeMillis",
        "subscriptionNotification"
})
public class AndroidNotificationEvent {

    @JsonProperty("version")
    private String version;
    @JsonProperty("packageName")
    private String packageName;
    @JsonProperty("eventTimeMillis")
    private long eventTimeMillis;
    @JsonProperty("subscriptionNotification")
    private SubscriptionNotification subscriptionNotification;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("version")
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty("packageName")
    public String getPackageName() {
        return packageName;
    }

    @JsonProperty("packageName")
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    @JsonProperty("eventTimeMillis")
    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    @JsonProperty("eventTimeMillis")
    public void setEventTimeMillis(long eventTimeMillis) {
        this.eventTimeMillis = eventTimeMillis;
    }

    @JsonProperty("subscriptionNotification")
    public SubscriptionNotification getSubscriptionNotification() {
        return subscriptionNotification;
    }

    @JsonProperty("subscriptionNotification")
    public void setSubscriptionNotification(SubscriptionNotification subscriptionNotification) {
        this.subscriptionNotification = subscriptionNotification;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({
            "version",
            "notificationType",
            "purchaseToken",
            "subscriptionId"
    })
    public class SubscriptionNotification {

        @JsonProperty("version")
        private String version;
        @JsonProperty("notificationType")
        private Integer notificationType;
        @JsonProperty("purchaseToken")
        private String purchaseToken;
        @JsonProperty("subscriptionId")
        private String subscriptionId;
        @JsonIgnore
        private Map<String, Object> additionalProperties = new HashMap<String, Object>();

        @JsonProperty("version")
        public String getVersion() {
            return version;
        }

        @JsonProperty("version")
        public void setVersion(String version) {
            this.version = version;
        }

        @JsonProperty("notificationType")
        public Integer getNotificationType() {
            return notificationType;
        }

        @JsonProperty("notificationType")
        public void setNotificationType(Integer notificationType) {
            this.notificationType = notificationType;
        }

        @JsonProperty("purchaseToken")
        public String getPurchaseToken() {
            return purchaseToken;
        }

        @JsonProperty("purchaseToken")
        public void setPurchaseToken(String purchaseToken) {
            this.purchaseToken = purchaseToken;
        }

        @JsonProperty("subscriptionId")
        public String getSubscriptionId() {
            return subscriptionId;
        }

        @JsonProperty("subscriptionId")
        public void setSubscriptionId(String subscriptionId) {
            this.subscriptionId = subscriptionId;
        }

        @JsonAnyGetter
        public Map<String, Object> getAdditionalProperties() {
            return this.additionalProperties;
        }

        @JsonAnySetter
        public void setAdditionalProperty(String name, Object value) {
            this.additionalProperties.put(name, value);
        }

    }
}