package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.annotation.*;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "original_purchase_date_pst",
        "quantity",
        "subscription_group_identifier",
        "unique_vendor_identifier",
        "original_purchase_date_ms",
        "expires_date_formatted",
        "is_in_intro_offer_period",
        "purchase_date_ms",
        "expires_date_formatted_pst",
        "is_trial_period",
        "item_id",
        "unique_identifier",
        "original_transaction_id",
        "expires_date",
        "app_item_id",
        "transaction_id",
        "bvrs",
        "web_order_line_item_id",
        "version_external_identifier",
        "bid",
        "product_id",
        "purchase_date",
        "purchase_date_pst",
        "original_purchase_date"
})
public class RootLatestReceiptInfo {

    @JsonProperty("original_purchase_date_pst")
    private String originalPurchaseDatePst;
    @JsonProperty("quantity")
    private String quantity;
    @JsonProperty("subscription_group_identifier")
    private String subscriptionGroupIdentifier;
    @JsonProperty("unique_vendor_identifier")
    private String uniqueVendorIdentifier;
    @JsonProperty("original_purchase_date_ms")
    private String originalPurchaseDateMs;
    @JsonProperty("expires_date_formatted")
    private String expiresDateFormatted;
    @JsonProperty("is_in_intro_offer_period")
    private String isInIntroOfferPeriod;
    @JsonProperty("purchase_date_ms")
    private String purchaseDateMs;
    @JsonProperty("expires_date_formatted_pst")
    private String expiresDateFormattedPst;
    @JsonProperty("is_trial_period")
    private String isTrialPeriod;
    @JsonProperty("item_id")
    private String itemId;
    @JsonProperty("unique_identifier")
    private String uniqueIdentifier;
    @JsonProperty("original_transaction_id")
    private String originalTransactionId;
    @JsonProperty("expires_date")
    private String expiresDate;
    @JsonProperty("app_item_id")
    private String appItemId;
    @JsonProperty("transaction_id")
    private String transactionId;
    @JsonProperty("bvrs")
    private String bvrs;
    @JsonProperty("web_order_line_item_id")
    private String webOrderLineItemId;
    @JsonProperty("version_external_identifier")
    private String versionExternalIdentifier;
    @JsonProperty("bid")
    private String bid;
    @JsonProperty("product_id")
    private String productId;
    @JsonProperty("purchase_date")
    private String purchaseDate;
    @JsonProperty("purchase_date_pst")
    private String purchaseDatePst;
    @JsonProperty("original_purchase_date")
    private String originalPurchaseDate;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("original_purchase_date_pst")
    public String getOriginalPurchaseDatePst() {
        return originalPurchaseDatePst;
    }

    @JsonProperty("original_purchase_date_pst")
    public void setOriginalPurchaseDatePst(String originalPurchaseDatePst) {
        this.originalPurchaseDatePst = originalPurchaseDatePst;
    }

    @JsonProperty("quantity")
    public String getQuantity() {
        return quantity;
    }

    @JsonProperty("quantity")
    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    @JsonProperty("subscription_group_identifier")
    public String getSubscriptionGroupIdentifier() {
        return subscriptionGroupIdentifier;
    }

    @JsonProperty("subscription_group_identifier")
    public void setSubscriptionGroupIdentifier(String subscriptionGroupIdentifier) {
        this.subscriptionGroupIdentifier = subscriptionGroupIdentifier;
    }

    @JsonProperty("unique_vendor_identifier")
    public String getUniqueVendorIdentifier() {
        return uniqueVendorIdentifier;
    }

    @JsonProperty("unique_vendor_identifier")
    public void setUniqueVendorIdentifier(String uniqueVendorIdentifier) {
        this.uniqueVendorIdentifier = uniqueVendorIdentifier;
    }

    @JsonProperty("original_purchase_date_ms")
    public String getOriginalPurchaseDateMs() {
        return originalPurchaseDateMs;
    }

    @JsonProperty("original_purchase_date_ms")
    public void setOriginalPurchaseDateMs(String originalPurchaseDateMs) {
        this.originalPurchaseDateMs = originalPurchaseDateMs;
    }

    @JsonProperty("expires_date_formatted")
    public String getExpiresDateFormatted() {
        return expiresDateFormatted;
    }

    @JsonProperty("expires_date_formatted")
    public void setExpiresDateFormatted(String expiresDateFormatted) {
        this.expiresDateFormatted = expiresDateFormatted;
    }

    @JsonProperty("is_in_intro_offer_period")
    public String getIsInIntroOfferPeriod() {
        return isInIntroOfferPeriod;
    }

    @JsonProperty("is_in_intro_offer_period")
    public void setIsInIntroOfferPeriod(String isInIntroOfferPeriod) {
        this.isInIntroOfferPeriod = isInIntroOfferPeriod;
    }

    @JsonProperty("purchase_date_ms")
    public String getPurchaseDateMs() {
        return purchaseDateMs;
    }

    @JsonProperty("purchase_date_ms")
    public void setPurchaseDateMs(String purchaseDateMs) {
        this.purchaseDateMs = purchaseDateMs;
    }

    @JsonProperty("expires_date_formatted_pst")
    public String getExpiresDateFormattedPst() {
        return expiresDateFormattedPst;
    }

    @JsonProperty("expires_date_formatted_pst")
    public void setExpiresDateFormattedPst(String expiresDateFormattedPst) {
        this.expiresDateFormattedPst = expiresDateFormattedPst;
    }

    @JsonProperty("is_trial_period")
    public String getIsTrialPeriod() {
        return isTrialPeriod;
    }

    @JsonProperty("is_trial_period")
    public void setIsTrialPeriod(String isTrialPeriod) {
        this.isTrialPeriod = isTrialPeriod;
    }

    @JsonProperty("item_id")
    public String getItemId() {
        return itemId;
    }

    @JsonProperty("item_id")
    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @JsonProperty("unique_identifier")
    public String getUniqueIdentifier() {
        return uniqueIdentifier;
    }

    @JsonProperty("unique_identifier")
    public void setUniqueIdentifier(String uniqueIdentifier) {
        this.uniqueIdentifier = uniqueIdentifier;
    }

    @JsonProperty("original_transaction_id")
    public String getOriginalTransactionId() {
        return originalTransactionId;
    }

    @JsonProperty("original_transaction_id")
    public void setOriginalTransactionId(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
    }

    @JsonProperty("expires_date")
    public String getExpiresDate() {
        return expiresDate;
    }

    @JsonProperty("expires_date")
    public void setExpiresDate(String expiresDate) {
        this.expiresDate = expiresDate;
    }

    @JsonProperty("app_item_id")
    public String getAppItemId() {
        return appItemId;
    }

    @JsonProperty("app_item_id")
    public void setAppItemId(String appItemId) {
        this.appItemId = appItemId;
    }

    @JsonProperty("transaction_id")
    public String getTransactionId() {
        return transactionId;
    }

    @JsonProperty("transaction_id")
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @JsonProperty("bvrs")
    public String getBvrs() {
        return bvrs;
    }

    @JsonProperty("bvrs")
    public void setBvrs(String bvrs) {
        this.bvrs = bvrs;
    }

    @JsonProperty("web_order_line_item_id")
    public String getWebOrderLineItemId() {
        return webOrderLineItemId;
    }

    @JsonProperty("web_order_line_item_id")
    public void setWebOrderLineItemId(String webOrderLineItemId) {
        this.webOrderLineItemId = webOrderLineItemId;
    }

    @JsonProperty("version_external_identifier")
    public String getVersionExternalIdentifier() {
        return versionExternalIdentifier;
    }

    @JsonProperty("version_external_identifier")
    public void setVersionExternalIdentifier(String versionExternalIdentifier) {
        this.versionExternalIdentifier = versionExternalIdentifier;
    }

    @JsonProperty("bid")
    public String getBid() {
        return bid;
    }

    @JsonProperty("bid")
    public void setBid(String bid) {
        this.bid = bid;
    }

    @JsonProperty("product_id")
    public String getProductId() {
        return productId;
    }

    @JsonProperty("product_id")
    public void setProductId(String productId) {
        this.productId = productId;
    }

    @JsonProperty("purchase_date")
    public String getPurchaseDate() {
        return purchaseDate;
    }

    @JsonProperty("purchase_date")
    public void setPurchaseDate(String purchaseDate) {
        this.purchaseDate = purchaseDate;
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
