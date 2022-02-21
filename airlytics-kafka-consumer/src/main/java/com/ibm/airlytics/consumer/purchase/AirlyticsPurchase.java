package com.ibm.airlytics.consumer.purchase;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.purchase.ios.LatestReceiptInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

@JsonIgnoreProperties(value = {"renewalsHistory", "objectMapper"})
public class AirlyticsPurchase implements Comparable<AirlyticsPurchase>, Cloneable {

    @SuppressWarnings("unused")
    public enum Field {
        ID("id"),
        USER_ID("user_id"),
        PRODUCT("product"),
        AUTO_RENEW_STATUS("auto_renew_status"),
        EXPIRATION_DATE("expiration_date"),
        PERIODS("periods"),
        DURATION("duration"),
        START_DATE("start_date"),
        GRACE("grace"),
        ACTIVE("active"),
        TRIAL("trial"),
        INTRO_PRICING("intro_pricing"),
        PERIOD_START_DATE("period_start_date"),
        REVENUE_USD_MICROS("revenue_usd_micros"),
        PAYMENT_STATE("payment_state"),
        CANCELLATION_REASON("cancellation_reason"),
        SURVEY_RESPONSE("survey_response"),
        AUTO_RENEW_STATUS_CHANGE_DATE("auto_renew_status_change_date"),
        IS_UPGRADED("is_upgraded"),
        ACTUAL_END_DATE("actual_end_date"),
        CANCELLATION_DATE("cancellation_date"),
        PLATFORM("platform"),
        LINKED_PURCHASE_TOKEN("linked_purchase_token"),
        RECEIPT("receipt"),
        ORIGINAL_TRANSACTION_ID("original_transaction_id"),
        LAST_MODIFIED_DATE("last_modified_date"),
        MODIFIER_SOURCE("modifier_source"),
        EXPIRATION_REASON("expiration_reason"),
        LAST_PAYMENT_ACTION_DATE("last_payment_action_date"),
        STORE_RESPONSE("store_response"),
        TRIAL_START_DATE("trial_start_date"),
        TRIAL_END_DATE("trial_end_date"),
        UPGRADED_FROM("upgraded_from"),
        UPGRADED_TO("upgraded_to"),
        SUBSCRIPTION_STATUS("subscription_status");// semicolon needed when fields / methods follow;// semicolon needed when fields / methods follow

        @SuppressWarnings("FieldCanBeLocal")
        private final String name;

        Field(String name) {
            this.name = name;
        }
    }


    private String userId;
    private String id;
    private String product;
    private Boolean autoRenewStatus;
    private Long expirationDate;
    private Double periods = 0.0;
    private String duration;
    private Long startDate;
    private Boolean grace;
    private Boolean active;
    private Boolean trial;
    private Boolean introPricing;
    private Long periodStartDate;
    private Long revenueUsd = 0L;
    private String paymentState;
    private String expirationReason;
    private String cancellationReason;
    private String surveyResponse;
    private Long autoRenewStatusChangeDate;
    private Boolean isUpgraded;
    private Long actualEndDate;
    private Long cancellationDate;
    private String platform;
    private String linkedPurchaseToken;
    private String receipt;
    private String originalTransactionId;
    private Long lastModifiedDate;
    private String modifierSource;
    private Long lastPaymentActionDate;
    private String storeResponse;
    private Long trialStartDate;
    private Long trialEndDate;
    private Long priceUsdMicros;
    private String purchaseIdUpgradedFrom;
    private LatestReceiptInfo upgradedToProduct;
    private LatestReceiptInfo upgradedFromProduct;
    private Boolean upgradedToProductActive;
    private String subscriptionStatus;

    protected static final ObjectMapper objectMapper = new ObjectMapper();
    private final ArrayList<AirlyticsPurchase> renewalsHistory = new ArrayList<>();

    public void addRenewalToHistory(AirlyticsPurchase airlyticsPurchase) {
        renewalsHistory.add(airlyticsPurchase);
    }

    public LatestReceiptInfo getUpgradedToProduct() {
        return upgradedToProduct;
    }

    public void setUpgradedToProduct(LatestReceiptInfo upgradedToProduct) {
        this.upgradedToProduct = upgradedToProduct;
    }

    public String getSubscriptionStatus() {
        return subscriptionStatus;
    }

    public void setSubscriptionStatus(String subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }

    public Collection<AirlyticsPurchase> getRenewalsHistory() {
        return Collections.unmodifiableList(renewalsHistory);
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public Long getCancellationDate() {
        return cancellationDate;
    }

    public void setCancellationDate(Long cancellationDate) {
        this.cancellationDate = cancellationDate;
    }

    public String getLinkedPurchaseToken() {
        return linkedPurchaseToken;
    }

    public void setLinkedPurchaseToken(String linkedPurchaseToken) {
        this.linkedPurchaseToken = linkedPurchaseToken;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Boolean getAutoRenewStatus() {
        return autoRenewStatus;
    }

    public void setAutoRenewStatus(Boolean autoRenewStatus) {
        this.autoRenewStatus = autoRenewStatus;
    }

    public Long getExpirationDate() {
        return expirationDate;
    }

    public void setExpirationDate(Long expirationDate) {
        this.expirationDate = expirationDate;
    }

    public Double getPeriods() {
        return periods;
    }

    public void setPeriods(Double periods) {
        this.periods = periods;
    }

    public Long getTrialEndDate() {
        return trialEndDate;
    }

    public void setTrialEndDate(Long trialEndDate) {
        this.trialEndDate = trialEndDate;
    }

    public Long getStartDate() {
        return startDate;
    }

    public void setStartDate(Long startDate) {
        this.startDate = startDate;
    }

    public Boolean getGrace() {
        return grace;
    }

    public void setGrace(Boolean grace) {
        this.grace = grace;
    }

    public Boolean getActive() {
        return active == null ? false : active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    public Boolean getTrial() {
        return trial;
    }

    public void setTrial(Boolean trial) {
        this.trial = trial;
    }

    public Boolean getIntroPricing() {
        return introPricing;
    }

    public void setIntroPricing(Boolean introPricing) {
        this.introPricing = introPricing;
    }

    public Long getPeriodStartDate() {
        return periodStartDate;
    }

    public void setPeriodStartDate(Long periodStartDate) {
        this.periodStartDate = periodStartDate;
    }

    public Long getRevenueUsd() {
        return revenueUsd;
    }

    public void setRevenueUsd(Long revenueUsd) {
        this.revenueUsd = revenueUsd;
    }

    public String getPaymentState() {
        return paymentState;
    }

    public String getExpirationReason() {
        return expirationReason;
    }

    public void setExpirationReason(String expirationReason) {
        this.expirationReason = expirationReason;
    }

    public void setPaymentState(String paymentState) {
        this.paymentState = paymentState;
    }


    public String getCancellationReason() {
        return cancellationReason;
    }

    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public String getSurveyResponse() {
        return surveyResponse;
    }

    public void setSurveyResponse(String surveyResponse) {
        this.surveyResponse = surveyResponse;
    }

    public Long getAutoRenewStatusChangeDate() {
        return autoRenewStatusChangeDate;
    }

    public void setAutoRenewStatusChangeDate(Long autoRenewStatusChangeDate) {
        this.autoRenewStatusChangeDate = autoRenewStatusChangeDate;
    }

    public Boolean getUpgradedToProductActive() {
        return upgradedToProductActive == null ? false : upgradedToProductActive;
    }

    public void setUpgradedToProductActive(Boolean upgradedToProductActive) {
        this.upgradedToProductActive = upgradedToProductActive;
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public Boolean getUpgraded() {
        return isUpgraded;
    }

    public void setUpgraded(Boolean upgraded) {
        isUpgraded = upgraded;
    }

    public Long getActualEndDate() {
        return actualEndDate;
    }

    public void setActualEndDate(Long actualEndDate) {
        this.actualEndDate = actualEndDate;
    }

    public String getReceipt() {
        return receipt;
    }

    public String getOriginalTransactionId() {
        return originalTransactionId;
    }

    public void setOriginalTransactionId(String originalTransactionId) {
        this.originalTransactionId = originalTransactionId;
    }

    public Long getTrialStartDate() {
        return trialStartDate;
    }

    public void setTrialStartDate(Long trialStartDate) {
        this.trialStartDate = trialStartDate;
    }

    public void setReceipt(String receipt) {
        this.receipt = receipt;
    }

    public Long getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(Long lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public String getModifierSource() {
        return modifierSource;
    }

    public void setModifierSource(String modifierSource) {
        this.modifierSource = modifierSource;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public Long getLastPaymentActionDate() {
        return lastPaymentActionDate;
    }

    public void setLastPaymentActionDate(Long lastPaymentActionDate) {
        this.lastPaymentActionDate = lastPaymentActionDate;
    }

    public LatestReceiptInfo getUpgradedFromProduct() {
        return upgradedFromProduct;
    }

    public void setUpgradedFromProduct(LatestReceiptInfo upgradedFromProduct) {
        this.upgradedFromProduct = upgradedFromProduct;
    }

    public String getPurchaseIdUpgradedFrom() {
        return purchaseIdUpgradedFrom;
    }

    public void setPurchaseIdUpgradedFrom(String purchaseIdUpgradedFrom) {
        this.purchaseIdUpgradedFrom = purchaseIdUpgradedFrom;
    }

    public Long getPriceUsdMicros() {
        return priceUsdMicros;
    }

    public void setPriceUsdMicros(Long priceUsdMicros) {
        this.priceUsdMicros = priceUsdMicros;
    }

    public String getStoreResponse() {
        return storeResponse;
    }

    public void setStoreResponse(String storeResponse) {
        this.storeResponse = storeResponse;
    }

    @Override
    public int compareTo(AirlyticsPurchase o) {
        return (int) (o.startDate - this.startDate);
    }


    public boolean stateEquals(AirlyticsPurchase updatedAirlyticsPurchase) {
        if (!getActive().equals(updatedAirlyticsPurchase.getActive()) ||
                !getExpirationDate().equals(updatedAirlyticsPurchase.getExpirationDate())
                || !Objects.equals(getCancellationDate(), updatedAirlyticsPurchase.getCancellationDate())
                || !getAutoRenewStatus().equals(updatedAirlyticsPurchase.getAutoRenewStatus())
                || !getTrial().equals(updatedAirlyticsPurchase.getTrial())
        ) {
            return false;
        }
        return true;
    }

    /**
     * The sub considered expired is it's no grace (in the pending due to payment issue)
     * and the expiration date arrived.
     *
     * @param updatedAirlyticsPurchase
     * @return
     */
    public boolean isExpiredNow(AirlyticsPurchase updatedAirlyticsPurchase) {
        return active && !updatedAirlyticsPurchase.active
                && updatedAirlyticsPurchase.cancellationDate == null &&
                (updatedAirlyticsPurchase.grace == null || !updatedAirlyticsPurchase.grace);
    }

    // a subscription is considered as renewed if it's current renewal period
    // is bigger than 1 and is increased by 1 plus it has to be an active subscription
    // if periods is zero and was increased by 1, it was a trail before (is not renewed)
    public boolean isRenewedNow(AirlyticsPurchase updatedAirlyticsPurchase) {
        return updatedAirlyticsPurchase.active && periods < updatedAirlyticsPurchase.getPeriods();
    }

    public boolean isCancelledNow(AirlyticsPurchase updatedAirlyticsPurchase) {
        return getCancellationDate() == null && updatedAirlyticsPurchase.getCancellationDate() != null &&
                !(updatedAirlyticsPurchase.getUpgraded() == null ? false : updatedAirlyticsPurchase.getUpgraded());
    }

    public boolean isRenewalStatusChangedNow(AirlyticsPurchase updatedAirlyticsPurchase) {
        return updatedAirlyticsPurchase.getAutoRenewStatus() != null && updatedAirlyticsPurchase.active &&
                !getAutoRenewStatus().equals(updatedAirlyticsPurchase.getAutoRenewStatus());
    }

    public boolean isUpgradedNow(AirlyticsPurchase updatedAirlyticsPurchase) {
        if (getUpgraded() != null && updatedAirlyticsPurchase.getUpgraded() != null) {
            return !getUpgraded() && updatedAirlyticsPurchase.getUpgraded();
        }
        return false;
    }

    public AirlyticsPurchase clone() {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(this), AirlyticsPurchase.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
