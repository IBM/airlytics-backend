package com.ibm.airlytics.consumer.purchase.events;

import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class SubscriptionPurchasedEvent extends BaseSubscriptionEvent<SubscriptionPurchasedEvent.SubscriptionPurchasedEventAttributes> {

    public static final String EVENT_NAME = "subscription-purchased";

    public SubscriptionPurchasedEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                      AirlyticsPurchase updatedAirlyticsPurchase,
                                      String airlockProductId, String originalEventId, String platform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase, airlockProductId,
                updatedAirlyticsPurchase.getStartDate(), originalEventId, platform, EVENT_NAME);
        this.schemaVersion = "1.0";
    }


    public SubscriptionPurchasedEventAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionPurchasedEventAttributes(airlyticsPurchase);
    }

    public class SubscriptionPurchasedEventAttributes extends Attributes {
        private boolean trial;
        private Long expirationDate;
        private Long revenueUsdMicros;
        private String duration;
        private boolean introPricing;

        public SubscriptionPurchasedEventAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                this.trial = airlyticsPurchase.getTrial();
                this.duration = airlyticsPurchase.getDuration();
                this.introPricing = airlyticsPurchase.getIntroPricing();
                this.expirationDate = airlyticsPurchase.getExpirationDate();
                this.revenueUsdMicros = airlyticsPurchase.getRevenueUsd();
            }
        }

        public boolean isTrial() {
            return trial;
        }

        public void setTrial(boolean trial) {
            this.trial = trial;
        }

        public Long getExpirationDate() {
            return expirationDate;
        }

        public void setExpirationDate(Long expirationDate) {
            this.expirationDate = expirationDate;
        }

        public Long getRevenueUsdMicros() {
            return revenueUsdMicros;
        }

        public void setRevenueUsdMicros(Long revenueUsdMicros) {
            this.revenueUsdMicros = revenueUsdMicros;
        }

        public String getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            this.duration = duration;
        }

        public boolean isIntroPricing() {
            return introPricing;
        }

        public void setIntroPricing(boolean introPricing) {
            this.introPricing = introPricing;
        }

        public String getPremiumProductId() {
            return premiumProductId;
        }

        public void setPremiumProductId(String premiumProductId) {
            this.premiumProductId = premiumProductId;
        }
    }
}
