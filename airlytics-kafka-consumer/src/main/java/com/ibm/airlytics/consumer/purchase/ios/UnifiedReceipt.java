package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "latest_receipt",
        "pending_renewal_info",
        "environment",
        "status",
        "latest_receipt_info"
})
public class UnifiedReceipt {

    @JsonProperty("latest_receipt")
    private String latestReceipt;
    @JsonProperty("pending_renewal_info")
    private List<PendingRenewalInfo> pendingRenewalInfo = null;
    @JsonProperty("environment")
    private String environment;
    @JsonProperty("status")
    private Integer status;
    @JsonProperty("latest_receipt_info")
    private List<LatestReceiptInfo> latestReceiptInfo = null;
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

    @JsonProperty("pending_renewal_info")
    public List<PendingRenewalInfo> getPendingRenewalInfo() {
        return pendingRenewalInfo;
    }

    @JsonProperty("pending_renewal_info")
    public void setPendingRenewalInfo(List<PendingRenewalInfo> pendingRenewalInfo) {
        this.pendingRenewalInfo = pendingRenewalInfo;
    }

    @JsonProperty("environment")
    public String getEnvironment() {
        return environment;
    }

    @JsonProperty("environment")
    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    @JsonProperty("status")
    public Integer getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(Integer status) {
        this.status = status;
    }

    @JsonProperty("latest_receipt_info")
    public List<LatestReceiptInfo> getLatestReceiptInfo() {
        return latestReceiptInfo;
    }

    @JsonProperty("latest_receipt_info")
    public void setLatestReceiptInfo(List<LatestReceiptInfo> latestReceiptInfo) {
        this.latestReceiptInfo = latestReceiptInfo;
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
