
package com.ibm.airlytics.consumer.purchase.ios;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchaseEvent;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "status",
        "environment",
        "receipt",
        "latest_receipt_info",
        "latest_receipt",
        "pending_renewal_info"
})
@JsonIgnoreProperties(value = {"objectMapper"})
public class IOSubscriptionPurchase {

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    @JsonProperty("status")
    private Integer status;
    @JsonProperty("environment")
    private String environment;
    @JsonProperty("receipt")
    private Receipt receipt;
    @JsonProperty("latest_receipt_info")
    private List<LatestReceiptInfo> latestReceiptInfo = null;
    @JsonProperty("latest_receipt")
    private String latestReceipt;
    @JsonProperty("pending_renewal_info")
    private List<PendingRenewalInfo> pendingRenewalInfo = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("status")
    public Integer getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(Integer status) {
        this.status = status;
    }

    @JsonProperty("environment")
    public String getEnvironment() {
        return environment;
    }

    @JsonProperty("environment")
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    @JsonProperty("receipt")
    public Receipt getReceipt() {
        return receipt;
    }

    @JsonProperty("receipt")
    public void setReceipt(Receipt receipt) {
        this.receipt = receipt;
    }

    @JsonProperty("latest_receipt_info")
    public List<LatestReceiptInfo> getLatestReceiptInfo() {
        return latestReceiptInfo;
    }

    @JsonProperty("latest_receipt_info")
    public void setLatestReceiptInfo(List<LatestReceiptInfo> latestReceiptInfo) {
        this.latestReceiptInfo = latestReceiptInfo;
    }

    @JsonProperty("latest_receipt")
    public String getLatestReceipt() {
        return latestReceipt;
    }

    @JsonProperty("latest_receipt")
    public void setLatestReceipt(String latestReceipt) {
        this.latestReceipt = latestReceipt;
    }

    @JsonProperty("pending_renewal_info")
    public List<PendingRenewalInfo> getPendingRenewalInfo() {
        return pendingRenewalInfo;
    }

    @JsonProperty("pending_renewal_info")
    public void setPendingRenewalInfo(List<PendingRenewalInfo> pendingRenewalInfo) {
        this.pendingRenewalInfo = pendingRenewalInfo;
    }

    public IOSubscriptionPurchase clone() {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(this), IOSubscriptionPurchase.class);
        } catch (JsonProcessingException e) {
            // ignore
        }
        return null;
    }

    public String toJSON() {
        try {
            return objectMapper.writeValueAsString(objectMapper.writeValueAsString(this));
        } catch (JsonProcessingException e) {
            // ignore
        }
        return "{}";
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


