package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "receipt_type",
        "adam_id",
        "app_item_id",
        "bundle_id",
        "application_version",
        "download_id",
        "version_external_identifier",
        "receipt_creation_date",
        "receipt_creation_date_ms",
        "receipt_creation_date_pst",
        "request_date",
        "request_date_ms",
        "request_date_pst",
        "original_purchase_date",
        "original_purchase_date_ms",
        "original_purchase_date_pst",
        "in_app"
})
public class Receipt {

    @JsonProperty("receipt_type")
    private String receiptType;
    @JsonProperty("adam_id")
    private Integer adamId;
    @JsonProperty("app_item_id")
    private Integer appItemId;
    @JsonProperty("bundle_id")
    private String bundleId;
    @JsonProperty("application_version")
    private String applicationVersion;
    @JsonProperty("download_id")
    private Object downloadId;
    @JsonProperty("version_external_identifier")
    private Integer versionExternalIdentifier;
    @JsonProperty("receipt_creation_date")
    private String receiptCreationDate;
    @JsonProperty("receipt_creation_date_ms")
    private String receiptCreationDateMs;
    @JsonProperty("receipt_creation_date_pst")
    private String receiptCreationDatePst;
    @JsonProperty("request_date")
    private String requestDate;
    @JsonProperty("request_date_ms")
    private String requestDateMs;
    @JsonProperty("request_date_pst")
    private String requestDatePst;
    @JsonProperty("original_purchase_date")
    private String originalPurchaseDate;
    @JsonProperty("original_purchase_date_ms")
    private String originalPurchaseDateMs;
    @JsonProperty("original_purchase_date_pst")
    private String originalPurchaseDatePst;
    @JsonProperty("in_app")
    private List<InApp> inApp = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("receipt_type")
    public String getReceiptType() {
        return receiptType;
    }

    @JsonProperty("receipt_type")
    public void setReceiptType(String receiptType) {
        this.receiptType = receiptType;
    }

    @JsonProperty("adam_id")
    public Integer getAdamId() {
        return adamId;
    }

    @JsonProperty("adam_id")
    public void setAdamId(Integer adamId) {
        this.adamId = adamId;
    }

    @JsonProperty("app_item_id")
    public Integer getAppItemId() {
        return appItemId;
    }

    @JsonProperty("app_item_id")
    public void setAppItemId(Integer appItemId) {
        this.appItemId = appItemId;
    }

    @JsonProperty("bundle_id")
    public String getBundleId() {
        return bundleId;
    }

    @JsonProperty("bundle_id")
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    @JsonProperty("application_version")
    public String getApplicationVersion() {
        return applicationVersion;
    }

    @JsonProperty("application_version")
    public void setApplicationVersion(String applicationVersion) {
        this.applicationVersion = applicationVersion;
    }

    @JsonProperty("download_id")
    public Object getDownloadId() {
        return downloadId;
    }

    @JsonProperty("download_id")
    public void setDownloadId(Object downloadId) {
        this.downloadId = downloadId;
    }

    @JsonProperty("version_external_identifier")
    public Integer getVersionExternalIdentifier() {
        return versionExternalIdentifier;
    }

    @JsonProperty("version_external_identifier")
    public void setVersionExternalIdentifier(Integer versionExternalIdentifier) {
        this.versionExternalIdentifier = versionExternalIdentifier;
    }

    @JsonProperty("receipt_creation_date")
    public String getReceiptCreationDate() {
        return receiptCreationDate;
    }

    @JsonProperty("receipt_creation_date")
    public void setReceiptCreationDate(String receiptCreationDate) {
        this.receiptCreationDate = receiptCreationDate;
    }

    @JsonProperty("receipt_creation_date_ms")
    public String getReceiptCreationDateMs() {
        return receiptCreationDateMs;
    }

    @JsonProperty("receipt_creation_date_ms")
    public void setReceiptCreationDateMs(String receiptCreationDateMs) {
        this.receiptCreationDateMs = receiptCreationDateMs;
    }

    @JsonProperty("receipt_creation_date_pst")
    public String getReceiptCreationDatePst() {
        return receiptCreationDatePst;
    }

    @JsonProperty("receipt_creation_date_pst")
    public void setReceiptCreationDatePst(String receiptCreationDatePst) {
        this.receiptCreationDatePst = receiptCreationDatePst;
    }

    @JsonProperty("request_date")
    public String getRequestDate() {
        return requestDate;
    }

    @JsonProperty("request_date")
    public void setRequestDate(String requestDate) {
        this.requestDate = requestDate;
    }

    @JsonProperty("request_date_ms")
    public String getRequestDateMs() {
        return requestDateMs;
    }

    @JsonProperty("request_date_ms")
    public void setRequestDateMs(String requestDateMs) {
        this.requestDateMs = requestDateMs;
    }

    @JsonProperty("request_date_pst")
    public String getRequestDatePst() {
        return requestDatePst;
    }

    @JsonProperty("request_date_pst")
    public void setRequestDatePst(String requestDatePst) {
        this.requestDatePst = requestDatePst;
    }

    @JsonProperty("original_purchase_date")
    public String getOriginalPurchaseDate() {
        return originalPurchaseDate;
    }

    @JsonProperty("original_purchase_date")
    public void setOriginalPurchaseDate(String originalPurchaseDate) {
        this.originalPurchaseDate = originalPurchaseDate;
    }

    @JsonProperty("original_purchase_date_ms")
    public String getOriginalPurchaseDateMs() {
        return originalPurchaseDateMs;
    }

    @JsonProperty("original_purchase_date_ms")
    public void setOriginalPurchaseDateMs(String originalPurchaseDateMs) {
        this.originalPurchaseDateMs = originalPurchaseDateMs;
    }

    @JsonProperty("original_purchase_date_pst")
    public String getOriginalPurchaseDatePst() {
        return originalPurchaseDatePst;
    }

    @JsonProperty("original_purchase_date_pst")
    public void setOriginalPurchaseDatePst(String originalPurchaseDatePst) {
        this.originalPurchaseDatePst = originalPurchaseDatePst;
    }

    @JsonProperty("in_app")
    public List<InApp> getInApp() {
        return inApp;
    }

    @JsonProperty("in_app")
    public void setInApp(List<InApp> inApp) {
        this.inApp = inApp;
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
