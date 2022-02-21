
package com.ibm.airlytics.consumer.purchase.ios;

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
        "latest_receipt",
        "latest_receipt_info",
        "environment",
        "auto_renew_status",
        "unified_receipt",
        "bvrs",
        "bid",
        "password",
        "auto_renew_product_id",
        "notification_type"
})
public class IOSNotificationEvent {

    @JsonProperty("latest_receipt")
    private String latestReceipt;
    @JsonProperty("latest_receipt_info")
    private RootLatestReceiptInfo latestReceiptInfo;
    @JsonProperty("environment")
    private String environment;
    @JsonProperty("auto_renew_status")
    private boolean autoRenewStatus;
    @JsonProperty("unified_receipt")
    private UnifiedReceipt unifiedReceipt;
    @JsonProperty("bvrs")
    private String bvrs;
    @JsonProperty("bid")
    private String bid;
    @JsonProperty("password")
    private String password;
    @JsonProperty("auto_renew_product_id")
    private String autoRenewProductId;
    @JsonProperty("notification_type")
    private String notificationType;
    @JsonProperty("auto_renew_status_change_date_pst")
    private String autoRenewStatusChangeDatePst;
    @JsonProperty("auto_renew_status_change_date_ms")
    private Long autoRenewStatusChangeDateMs;
    @JsonProperty("cancellation_date_ms")
    private Long cancellationDateMs;
    @JsonProperty("auto_renew_status_change_date")
    private String autoRenewStatusChangeDate;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("latest_receipt")
    public String getLatestReceipt() {
        return latestReceipt;
    }

    @JsonProperty("latest_receipt")
    public void setLatestReceipt(String latestReceipt) {
        this.latestReceipt = latestReceipt;
    }

    @JsonProperty("latest_receipt_info")
    public RootLatestReceiptInfo getLatestReceiptInfo() {
        return latestReceiptInfo;
    }

    @JsonProperty("cancellation_date_ms")
    public Long getCancellationDateMs() {
        return cancellationDateMs;
    }

    @JsonProperty("cancellation_date_ms")
    public void setCancellationDateMs(Long cancellationDateMs) {
        this.cancellationDateMs = cancellationDateMs;
    }

    @JsonProperty("latest_receipt_info")
    public void setLatestReceiptInfo(RootLatestReceiptInfo latestReceiptInfo) {
        this.latestReceiptInfo = latestReceiptInfo;
    }

    @JsonProperty("environment")
    public String getEnvironment() {
        return environment;
    }

    @JsonProperty("environment")
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    @JsonProperty("auto_renew_status")
    public boolean getAutoRenewStatus() {
        return autoRenewStatus;
    }

    @JsonProperty("auto_renew_status")
    public void setAutoRenewStatus(boolean autoRenewStatus) {
        this.autoRenewStatus = autoRenewStatus;
    }

    @JsonProperty("unified_receipt")
    public UnifiedReceipt getUnifiedReceipt() {
        return unifiedReceipt;
    }

    @JsonProperty("unified_receipt")
    public void setUnifiedReceipt(UnifiedReceipt unifiedReceipt) {
        this.unifiedReceipt = unifiedReceipt;
    }

    @JsonProperty("bvrs")
    public String getBvrs() {
        return bvrs;
    }

    @JsonProperty("bvrs")
    public void setBvrs(String bvrs) {
        this.bvrs = bvrs;
    }

    @JsonProperty("bid")
    public String getBid() {
        return bid;
    }

    @JsonProperty("bid")
    public void setBid(String bid) {
        this.bid = bid;
    }

    @JsonProperty("password")
    public String getPassword() {
        return password;
    }

    @JsonProperty("password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonProperty("auto_renew_product_id")
    public String getAutoRenewProductId() {
        return autoRenewProductId;
    }

    @JsonProperty("auto_renew_product_id")
    public void setAutoRenewProductId(String autoRenewProductId) {
        this.autoRenewProductId = autoRenewProductId;
    }

    @JsonProperty("notification_type")
    public String getNotificationType() {
        return notificationType;
    }

    @JsonProperty("notification_type")
    public void setNotificationType(String notificationType) {
        this.notificationType = notificationType;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @JsonProperty("auto_renew_status_change_date_pst")
    public String getAutoRenewStatusChangeDatePst() {
        return autoRenewStatusChangeDatePst;
    }

    @JsonProperty("auto_renew_status_change_date_pst")
    public void setAutoRenewStatusChangeDatePst(String autoRenewStatusChangeDatePst) {
        this.autoRenewStatusChangeDatePst = autoRenewStatusChangeDatePst;
    }

    @JsonProperty("auto_renew_status_change_date_ms")
    public Long getAutoRenewStatusChangeDateMs() {
        return autoRenewStatusChangeDateMs;
    }

    @JsonProperty("auto_renew_status_change_date_ms")
    public void setAutoRenewStatusChangeDateMs(Long autoRenewStatusChangeDateMs) {
        this.autoRenewStatusChangeDateMs = autoRenewStatusChangeDateMs;
    }

    @JsonProperty("auto_renew_status_change_date")
    public String getAutoRenewStatusChangeDate() {
        return autoRenewStatusChangeDate;
    }

    @JsonProperty("auto_renew_status_change_date")
    public void setAutoRenewStatusChangeDate(String autoRenewStatusChangeDate) {
        this.autoRenewStatusChangeDate = autoRenewStatusChangeDate;
    }
}


