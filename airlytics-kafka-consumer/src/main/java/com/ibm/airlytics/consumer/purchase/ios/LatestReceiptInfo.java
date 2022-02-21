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
        "expires_date",
        "expires_date_ms",
        "expires_date_pst",
        "web_order_line_item_id",
        "is_trial_period",
        "is_in_intro_offer_period",
        "subscription_group_identifier",
        "is_upgraded",
        "cancellation_date_ms",
        "cancellation_date",
        "cancellation_reason"

})
public class LatestReceiptInfo {

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
    private Long purchaseDateMs;
    @JsonProperty("purchase_date_pst")
    private String purchaseDatePst;
    @JsonProperty("original_purchase_date")
    private String originalPurchaseDate;
    @JsonProperty("original_purchase_date_ms")
    private Long originalPurchaseDateMs;
    @JsonProperty("original_purchase_date_pst")
    private String originalPurchaseDatePst;
    @JsonProperty("expires_date")
    private String expiresDate;
    @JsonProperty("expires_date_ms")
    private Long expiresDateMs;
    @JsonProperty("expires_date_pst")
    private String expiresDatePst;
    @JsonProperty("web_order_line_item_id")
    private String webOrderLineItemId;
    @JsonProperty("is_trial_period")
    private Boolean isTrialPeriod;
    @JsonProperty("is_in_intro_offer_period")
    private Boolean isInIntroOfferPeriod;
    @JsonProperty("subscription_group_identifier")
    private String subscriptionGroupIdentifier;
    @JsonProperty("cancellation_reason")
    private Integer cancellationReason;
    @JsonProperty("cancellation_date")
    private String cancellationDate;
    @JsonProperty("cancellation_date_ms")
    private Long cancellationDateMs;
    @JsonProperty("is_upgraded")
    private boolean isUpgraded;
    @JsonProperty("start_period_date")



    @JsonIgnore
    // holds the value of start period date
    // if an user purchased a product after trial period
    private Long startPeriodDateAfterTrial;

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
    public Long getPurchaseDateMs() {
        return purchaseDateMs;
    }

    @JsonProperty("purchase_date_ms")
    public void setPurchaseDateMs(Long purchaseDateMs) {
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
    public Long getOriginalPurchaseDateMs() {
        return originalPurchaseDateMs;
    }

    @JsonProperty("original_purchase_date_ms")
    public void setOriginalPurchaseDateMs(Long originalPurchaseDateMs) {
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

    @JsonProperty("expires_date")
    public String getExpiresDate() {
        return expiresDate;
    }

    @JsonProperty("expires_date")
    public void setExpiresDate(String expiresDate) {
        this.expiresDate = expiresDate;
    }

    @JsonProperty("expires_date_ms")
    public Long getExpiresDateMs() {
        return expiresDateMs;
    }

    @JsonProperty("expires_date_ms")
    public void setExpiresDateMs(Long expiresDateMs) {
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

    @JsonProperty("is_trial_period")
    public Boolean getIsTrialPeriod() {
        return isTrialPeriod;
    }

    @JsonProperty("is_trial_period")
    public void setIsTrialPeriod(Boolean isTrialPeriod) {
        this.isTrialPeriod = isTrialPeriod;
    }

    @JsonProperty("is_in_intro_offer_period")
    public Boolean getIsInIntroOfferPeriod() {
        return isInIntroOfferPeriod;
    }

    @JsonProperty("is_in_intro_offer_period")
    public void setIsInIntroOfferPeriod(Boolean isInIntroOfferPeriod) {
        this.isInIntroOfferPeriod = isInIntroOfferPeriod;
    }

    @JsonProperty("subscription_group_identifier")
    public String getSubscriptionGroupIdentifier() {
        return subscriptionGroupIdentifier;
    }

    @JsonProperty("subscription_group_identifier")
    public void setSubscriptionGroupIdentifier(String subscriptionGroupIdentifier) {
        this.subscriptionGroupIdentifier = subscriptionGroupIdentifier;
    }

    public Long getStartPeriodDateAfterTrial() {
        return startPeriodDateAfterTrial;
    }

    public void setStartPeriodDateAfterTrial(Long startPeriodDateAfterTrial) {
        this.startPeriodDateAfterTrial = startPeriodDateAfterTrial;
    }

    @JsonProperty("cancellation_reason")
    public Integer getCancellationReason() {
        return cancellationReason;
    }
    @JsonProperty("cancellation_reason")
    public void setCancellationReason(Integer cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    @JsonProperty("cancellation_date")
    public String getCancellationDate() {
        return cancellationDate;
    }

    @JsonProperty("cancellation_date")
    public void setCancellationDate(String cancellationDate) {
        this.cancellationDate = cancellationDate;
    }
    @JsonProperty("cancellation_date_ms")
    public Long getCancellationDateMs() {
        return cancellationDateMs;
    }
    @JsonProperty("cancellation_date_ms")
    public void setCancellationDateMs(Long cancellationDateMs) {
        this.cancellationDateMs = cancellationDateMs;
    }
    @JsonProperty("is_upgraded")
    public boolean isUpgraded() {
        return isUpgraded;
    }
    @JsonProperty("is_upgraded")
    public void setUpgraded(boolean upgraded) {
        isUpgraded = upgraded;
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
