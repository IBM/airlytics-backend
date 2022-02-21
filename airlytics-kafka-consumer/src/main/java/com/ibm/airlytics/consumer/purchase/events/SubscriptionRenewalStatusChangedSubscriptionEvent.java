package com.ibm.airlytics.consumer.purchase.events;

import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class SubscriptionRenewalStatusChangedSubscriptionEvent extends BaseSubscriptionEvent<SubscriptionRenewalStatusChangedSubscriptionEvent.SubscriptionRenewalStatusChangeAttributes> {

    public static final String EVENT_NAME = "subscription-renewal-status-changed";

    public SubscriptionRenewalStatusChangedSubscriptionEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                                             AirlyticsPurchase updatedAirlyticsPurchase,
                                                             String airlockProductId, String originalEventId, String platform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase, airlockProductId,
                updatedAirlyticsPurchase.getAutoRenewStatusChangeDate(), originalEventId, platform, EVENT_NAME);
        this.schemaVersion = "1.0";
    }


    public SubscriptionRenewalStatusChangeAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionRenewalStatusChangeAttributes(airlyticsPurchase);
    }

    public class SubscriptionRenewalStatusChangeAttributes extends Attributes {
        public boolean status;
        public boolean previousStatus;
        public String surveyResponse;


        public SubscriptionRenewalStatusChangeAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                this.status = airlyticsPurchase.getAutoRenewStatus();
                this.surveyResponse = airlyticsPurchase.getSurveyResponse();
                this.previousStatus = currentAirlyticsPurchase.getAutoRenewStatus();
            }
        }

        public boolean isStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public boolean isPreviousStatus() {
            return previousStatus;
        }

        public void setPreviousStatus(boolean previousStatus) {
            this.previousStatus = previousStatus;
        }

        public String getSurveyResponse() {
            return surveyResponse;
        }

        public void setSurveyResponse(String surveyResponse) {
            this.surveyResponse = surveyResponse;
        }

        public String getPremiumProductId() {
            return premiumProductId;
        }

        public void setPremiumProductId(String premiumProductId) {
            this.premiumProductId = premiumProductId;
        }
    }
}
