package com.ibm.airlytics.consumer.purchase.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class SubscriptionExpiredEvent extends BaseSubscriptionEvent<SubscriptionExpiredEvent.SubscriptionExpiredAttributes> {

    public static final String EVENT_NAME = "subscription-expired";

    public SubscriptionExpiredEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                    AirlyticsPurchase updatedAirlyticsPurchase,
                                    String airlockProductId, String originalEventId, String plaform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase,
                airlockProductId, updatedAirlyticsPurchase.getActualEndDate(),
                originalEventId, plaform, EVENT_NAME);
        this.schemaVersion = "1.0";
    }

    public SubscriptionExpiredAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionExpiredAttributes(airlyticsPurchase);
    }

    public class SubscriptionExpiredAttributes extends Attributes {
        private String expirationReason;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private String source;

        public SubscriptionExpiredAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                this.expirationReason = airlyticsPurchase.getExpirationReason();
                this.source = airlyticsPurchase.getCancellationDate() != null ? "cancellation" : null;
            }
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getExpirationReason() {
            return expirationReason;
        }

        public void setExpirationReason(String expirationReason) {
            this.expirationReason = expirationReason;
        }

        public String getPremiumProductId() {
            return premiumProductId;
        }

        public void setPremiumProductId(String premiumProductId) {
            this.premiumProductId = premiumProductId;
        }
    }
}
