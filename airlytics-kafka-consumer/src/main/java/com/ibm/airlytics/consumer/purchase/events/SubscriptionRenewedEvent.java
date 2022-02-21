package com.ibm.airlytics.consumer.purchase.events;

import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class SubscriptionRenewedEvent extends BaseSubscriptionEvent<SubscriptionRenewedEvent.SubscriptionRenewedAttributes> {

    public static final String EVENT_NAME = "subscription-renewed";


    public SubscriptionRenewedEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                    AirlyticsPurchase updatedAirlyticsPurchase,
                                    String airlockProductId, String originalEventId, String platform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase, airlockProductId,
                updatedAirlyticsPurchase.getPeriodStartDate(), originalEventId, platform, EVENT_NAME);
        this.schemaVersion = "1.0";
    }

    @Override
    public SubscriptionRenewedAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionRenewedAttributes(airlyticsPurchase);
    }

    public class SubscriptionRenewedAttributes extends Attributes {

        private Double subscriptionPeriod;
        private Long expirationDate;
        private Long previousExpirationDate;
        private Long revenueUsdMicros;


        public SubscriptionRenewedAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                subscriptionPeriod = airlyticsPurchase.getPeriods();
                expirationDate = airlyticsPurchase.getExpirationDate();
                previousExpirationDate = currentAirlyticsPurchase.getExpirationDate();
                revenueUsdMicros = airlyticsPurchase.getPriceUsdMicros();
            }
        }

        public Double getSubscriptionPeriod() {
            return subscriptionPeriod;
        }

        public void setSubscriptionPeriod(Double subscriptionPeriod) {
            this.subscriptionPeriod = subscriptionPeriod;
        }

        public Long getExpirationDate() {
            return expirationDate;
        }

        public void setExpirationDate(Long expirationDate) {
            this.expirationDate = expirationDate;
        }

        public Long getPreviousExpirationDate() {
            return previousExpirationDate;
        }

        public void setPreviousExpirationDate(Long previousExpirationDate) {
            this.previousExpirationDate = previousExpirationDate;
        }

        public Long getRevenueUsdMicros() {
            return revenueUsdMicros;
        }

        public void setRevenueUsdMicros(Long revenueUsdMicros) {
            this.revenueUsdMicros = revenueUsdMicros;
        }

        public String getPremiumProductId() {
            return premiumProductId;
        }

        public void setPremiumProductId(String premiumProductId) {
            this.premiumProductId = premiumProductId;
        }
    }

}
