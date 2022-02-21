package com.ibm.airlytics.consumer.purchase;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;


@JsonIgnoreProperties(value = {"objectMapper"})
public class AirlyticsPurchaseEvent implements Cloneable {


    @SuppressWarnings("unused")
    public enum Field {
        PURCHASE_ID("purchase_id"),
        PRODUCT("product"),
        EVENT_TIME("event_time"),
        AUTO_RENEW_STATUS("auto_renew_status"),
        EXPIRATION_DATE("expiration_date"),
        INTRO_PRICING("intro_pricing"),
        REVENUE_USD_MICROS("revenue_usd_micros"),
        PAYMENT_STATE("payment_state"),
        CANCELLATION_REASON("cancellation_reason"),
        SURVEY_RESPONSE("survey_response"),
        PLATFORM("platform"),
        EXPIRATION_REASON("expiration_reason"),
        UPGRADED_FROM("upgraded_from"),
        UPGRADED_TO("upgraded_to"),
        NAME("name"),
        RECEIPT("receipt");

        @SuppressWarnings("FieldCanBeLocal")
        private final String name;

        Field(String name) {
            this.name = name;
        }
    }


    private String purchaseId;
    private String product;
    private Long eventTime;
    private String platform;
    private Boolean autoRenewStatus;
    private Long expirationDate;
    private Boolean introPricing;
    private Long revenueUsd = 0L;
    private String paymentState;
    private String cancellationReason;
    private String surveyResponse;
    private String expirationReason;
    private String name;
    private String originalNotificationName;
    private String decodedReceipt;
    private String upgradedFrom;
    private String upgradedTo;
    private String receipt;

    public String getUpgradedTo() {
        return upgradedTo;
    }

    public void setUpgradedTo(String upgradedTo) {
        this.upgradedTo = upgradedTo;
    }

    public String getPurchaseId() {
        return purchaseId;
    }

    public void setPurchaseId(String purchaseId) {
        this.purchaseId = purchaseId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
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

    public Boolean getIntroPricing() {
        return introPricing == null ? false : introPricing;
    }

    public void setIntroPricing(Boolean introPricing) {
        this.introPricing = introPricing;
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

    public String getExpirationReason() {
        return expirationReason;
    }

    public void setExpirationReason(String expirationReason) {
        this.expirationReason = expirationReason;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUpgradedFrom() {
        return upgradedFrom;
    }

    public void setUpgradedFrom(String upgradedFrom) {
        this.upgradedFrom = upgradedFrom;
    }

    public String getOriginalNotificationName() {
        return originalNotificationName;
    }

    public void setOriginalNotificationName(String originalNotificationName) {
        this.originalNotificationName = originalNotificationName;
    }

    public String getReceipt() {
        return receipt;
    }

    public void setReceipt(String receipt) {
        this.receipt = receipt;
    }

    public String getDecodedReceipt() {
        return decodedReceipt;
    }

    public void setDecodedReceipt(String decodedReceipt) {
        this.decodedReceipt = decodedReceipt;
    }

    protected static final ObjectMapper objectMapper = new ObjectMapper();


    public int hashCode() {
        return Arrays.hashCode( new Object[] { name, eventTime,purchaseId } );
    }


    public boolean equals(Object obj){
        if (obj instanceof AirlyticsPurchaseEvent) {
            AirlyticsPurchaseEvent pp = (AirlyticsPurchaseEvent) obj;
            return (pp.name.equals(this.name) && pp.eventTime.equals(this.eventTime) && pp.purchaseId.equals(purchaseId));
        } else {
            return false;
        }
    }

    public AirlyticsPurchaseEvent clone() {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsString(this), AirlyticsPurchaseEvent.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public String toJSON() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
