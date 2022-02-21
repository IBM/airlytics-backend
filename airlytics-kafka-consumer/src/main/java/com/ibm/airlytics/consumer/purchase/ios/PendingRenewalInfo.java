package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "original_transaction_id",
        "product_id",
        "auto_renew_status",
        "auto_renew_product_id",
        "expiration_intent",
        "grace_period_expires_date_ms"
})
public class PendingRenewalInfo {

    @JsonProperty("original_transaction_id")
    private String originalTransactionId;
    @JsonProperty("product_id")
    private String productId;
    @JsonProperty("auto_renew_status")
    private String autoRenewStatus;
    @JsonProperty("auto_renew_product_id")
    private String autoRenewProductId;
    @JsonProperty("expiration_intent")
    private Integer expirationIntent;
    @JsonProperty("grace_period_expires_date_ms")
    private Long gracePeriodExpiresDateMs;
    @JsonProperty("is_in_billing_retry_period")
    private Long isInBillingRetryPeriod;


    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("original_transaction_id")
    public String getOriginalTransactionId() {
        return originalTransactionId;
    }

    @JsonProperty("original_transaction_id")
    public void setOriginalTransactionId(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
    }

    @JsonProperty("is_in_billing_retry_period")
    public Long getIsInBillingRetryPeriod() {
        return isInBillingRetryPeriod;
    }

    @JsonProperty("is_in_billing_retry_period")
    public void setIsInBillingRetryPeriod(Long isInBillingRetryPeriod) {
        this.isInBillingRetryPeriod = isInBillingRetryPeriod;
    }

    @JsonProperty("product_id")
    public String getProductId() {
        return productId;
    }

    @JsonProperty("product_id")
    public void setProductId(String productId) {
        this.productId = productId;
    }

    @JsonProperty("auto_renew_status")
    public String getAutoRenewStatus() {
        return autoRenewStatus;
    }

    @JsonProperty("auto_renew_status")
    public void setAutoRenewStatus(String autoRenewStatus) {
        this.autoRenewStatus = autoRenewStatus;
    }

    @JsonProperty("auto_renew_product_id")
    public String getAutoRenewProductId() {
        return autoRenewProductId;
    }

    @JsonProperty("auto_renew_product_id")
    public void setAutoRenewProductId(String autoRenewProductId) {
        this.autoRenewProductId = autoRenewProductId;
    }

    @JsonProperty("auto_renew_status")
    public boolean isAutoRenewStatus() {
        return autoRenewStatus != null && autoRenewStatus.equals("1");
    }
    @JsonProperty("expiration_intent")
    public Integer getExpirationIntent() {
        return expirationIntent;
    }
    @JsonProperty("expiration_intent")
    public void setExpirationIntent(Integer expirationIntent) {
        this.expirationIntent = expirationIntent;
    }

    @JsonProperty("grace_period_expires_date_ms")
    public Long getGracePeriodExpiresDateMs() {
        return gracePeriodExpiresDateMs;
    }
    @JsonProperty("grace_period_expires_date_ms")
    public void setGracePeriodExpiresDateMs(Long gracePeriodExpiresDateMs) {
        this.gracePeriodExpiresDateMs = gracePeriodExpiresDateMs;
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
