package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "quantity",
        "product_id",
        "transaction_id",
        "original_transaction_id",
        "purchase_date",
        "purchase_date_ms",
        "purchase_date_pst",
        "original_purchase_date",
        "original_purchase_date_ms",
        "original_purchase_date_pst",
        "is_trial_period",
        "expires_date",
        "expires_date_ms",
        "expires_date_pst",
        "web_order_line_item_id",
        "is_in_intro_offer_period"
})
public class InApp {

    @JsonProperty("quantity")
    private String quantity;
    @JsonProperty("product_id")
    private String productId;
    @JsonProperty("transaction_id")
    private String transactionId;
    @JsonProperty("original_transaction_id")
    private String originalTransactionId;
    @JsonProperty("purchase_date")
    private String purchaseDate;
    @JsonProperty("purchase_date_ms")
    private String purchaseDateMs;
    @JsonProperty("purchase_date_pst")
    private String purchaseDatePst;
    @JsonProperty("original_purchase_date")
    private String originalPurchaseDate;
    @JsonProperty("original_purchase_date_ms")
    private String originalPurchaseDateMs;
    @JsonProperty("original_purchase_date_pst")
    private String originalPurchaseDatePst;
    @JsonProperty("is_trial_period")
    private String isTrialPeriod;
    @JsonProperty("expires_date")
    private String expiresDate;
    @JsonProperty("expires_date_ms")
    private String expiresDateMs;
    @JsonProperty("expires_date_pst")
    private String expiresDatePst;
    @JsonProperty("web_order_line_item_id")
    private String webOrderLineItemId;
    @JsonProperty("is_in_intro_offer_period")
    private String isInIntroOfferPeriod;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("quantity")
    public String getQuantity() {
        return quantity;
    }

    @JsonProperty("quantity")
    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    @JsonProperty("product_id")
    public String getProductId() {
        return productId;
    }

    @JsonProperty("product_id")
    public void setProductId(String productId) {
        this.productId = productId;
    }

    @JsonProperty("transaction_id")
    public String getTransactionId() {
        return transactionId;
    }

    @JsonProperty("transaction_id")
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @JsonProperty("original_transaction_id")
    public String getOriginalTransactionId() {
        return originalTransactionId;
    }

    @JsonProperty("original_transaction_id")
    public void setOriginalTransactionId(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
    }

    @JsonProperty("purchase_date")
    public String getPurchaseDate() {
        return purchaseDate;
    }

    @JsonProperty("purchase_date")
    public void setPurchaseDate(String purchaseDate) {
        this.purchaseDate = purchaseDate;
    }

    @JsonProperty("purchase_date_ms")
    public String getPurchaseDateMs() {
        return purchaseDateMs;
    }

    @JsonProperty("purchase_date_ms")
    public void setPurchaseDateMs(String purchaseDateMs) {
        this.purchaseDateMs = purchaseDateMs;
    }

    @JsonProperty("purchase_date_pst")
    public String getPurchaseDatePst() {
        return purchaseDatePst;
    }

    @JsonProperty("purchase_date_pst")
    public void setPurchaseDatePst(String purchaseDatePst) {
        this.purchaseDatePst = purchaseDatePst;
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

    @JsonProperty("is_trial_period")
    public String getIsTrialPeriod() {
        return isTrialPeriod;
    }

    @JsonProperty("is_trial_period")
    public void setIsTrialPeriod(String isTrialPeriod) {
        this.isTrialPeriod = isTrialPeriod;
    }

    @JsonProperty("expires_date")
    public String getExpiresDate() {
        return expiresDate;
    }

    @JsonProperty("expires_date")
    public void setExpiresDate(String expiresDate) {
        this.expiresDate = expiresDate;
    }

    @JsonProperty("expires_date_ms")
    public String getExpiresDateMs() {
        return expiresDateMs;
    }

    @JsonProperty("expires_date_ms")
    public void setExpiresDateMs(String expiresDateMs) {
        this.expiresDateMs = expiresDateMs;
    }

    @JsonProperty("expires_date_pst")
    public String getExpiresDatePst() {
        return expiresDatePst;
    }

    @JsonProperty("expires_date_pst")
    public void setExpiresDatePst(String expiresDatePst) {
        this.expiresDatePst = expiresDatePst;
    }

    @JsonProperty("web_order_line_item_id")
    public String getWebOrderLineItemId() {
        return webOrderLineItemId;
    }

    @JsonProperty("web_order_line_item_id")
    public void setWebOrderLineItemId(String webOrderLineItemId) {
        this.webOrderLineItemId = webOrderLineItemId;
    }

    @JsonProperty("is_in_intro_offer_period")
    public String getIsInIntroOfferPeriod() {
        return isInIntroOfferPeriod;
    }

    @JsonProperty("is_in_intro_offer_period")
    public void setIsInIntroOfferPeriod(String isInIntroOfferPeriod) {
        this.isInIntroOfferPeriod = isInIntroOfferPeriod;
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
