package com.ibm.airlytics.consumer.purchase.events;

import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class SubscriptionCancelledEvent extends BaseSubscriptionEvent<SubscriptionCancelledEvent.SubscriptionCancelledAttributes> {

    public static final String EVENT_NAME = "subscription-cancelled";


    public SubscriptionCancelledEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                      AirlyticsPurchase updatedAirlyticsPurchase,
                                      String airlockProductId, String originalEventId, String platform) {
        super(currentAirlyticsPurchase, updatedAirlyticsPurchase,
                airlockProductId, updatedAirlyticsPurchase.getCancellationDate(),
                originalEventId, platform, EVENT_NAME);
        this.schemaVersion = "1.0";
    }


    public SubscriptionCancelledAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionCancelledAttributes(airlyticsPurchase);
    }

    public class SubscriptionCancelledAttributes extends Attributes {

        private String cancellationReason;
        private Long revenueUsdMicros;


        public SubscriptionCancelledAttributes(AirlyticsPurchase airlyticsPurchase) {
            super(airlyticsPurchase);
            if (airlyticsPurchase != null) {
                cancellationReason = airlyticsPurchase.getCancellationReason();
                revenueUsdMicros = -airlyticsPurchase.getPriceUsdMicros();
            }
        }

        public String getCancellationReason() {
            return cancellationReason;
        }

        public void setCancellationReason(String cancellationReason) {
            this.cancellationReason = cancellationReason;
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
