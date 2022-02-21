package com.ibm.airlytics.consumer.purchase.events;

import com.ibm.airlytics.consumer.purchase.AirlyticsInAppProduct;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;
import com.ibm.airlytics.utilities.Duration;
import com.ibm.airlytics.utilities.MathUtils;

import java.util.Map;
import java.util.UUID;

public class SubscriptionUpgradedEvent extends BaseSubscriptionEvent<SubscriptionUpgradedEvent.SubscriptionUpgradedAttributes> {

    public static final String EVENT_NAME = "subscription-upgraded";
    private Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts;


    public SubscriptionUpgradedEvent(AirlyticsPurchase currentAirlyticsPurchase,
                                     AirlyticsPurchase updatedAirlyticsPurchase,
                                     String airlockProductId, Map<String,
            AirlyticsInAppProduct> inAppAirlyticsProducts, String originalEventId, String platform) throws IllegalSubscriptionEventArguments {

        try {
            this.name = EVENT_NAME;
            this.inAppAirlyticsProducts = inAppAirlyticsProducts;
            this.platform = platform;
            this.eventTime = updatedAirlyticsPurchase.getCancellationDate();
            this.eventId = UUID.nameUUIDFromBytes((originalEventId + updatedAirlyticsPurchase.getId() + this.name).getBytes()).toString();
            this.productId = airlockProductId;
            this.userId = updatedAirlyticsPurchase.getUserId();
            this.currentAirlyticsPurchase = currentAirlyticsPurchase;
            this.attributes = createAttributes(updatedAirlyticsPurchase);
            this.schemaVersion = "1.0";
        } catch (Throwable e) {
            throw new IllegalSubscriptionEventArguments(name, e);
        }
    }

    @Override
    public SubscriptionUpgradedAttributes createAttributes(AirlyticsPurchase airlyticsPurchase) {
        return new SubscriptionUpgradedAttributes(airlyticsPurchase);
    }

    public String getPremiumProductId() {
        return attributes.newProductId;
    }

    public class SubscriptionUpgradedAttributes {
        private String newProductId;
        private String previousProductId;
        private boolean trial;
        private Long expirationDate;
        private Long revenueUsdMicros;
        private String duration;
        private boolean introPricing;


        public SubscriptionUpgradedAttributes(AirlyticsPurchase airlyticsPurchase) {

            if (airlyticsPurchase != null) {

                if (airlyticsPurchase.getUpgradedToProduct() != null) {
                    newProductId = airlyticsPurchase.getUpgradedToProduct().getProductId();
                    trial = airlyticsPurchase.getUpgradedToProduct().getIsTrialPeriod();
                    expirationDate = airlyticsPurchase.getUpgradedToProduct().getExpiresDateMs();

                    duration = Duration.Periodicity.getNameByType(
                            inAppAirlyticsProducts.get(airlyticsPurchase.getUpgradedToProduct().getProductId()).getSubscriptionPeriod());

                    introPricing = airlyticsPurchase.getUpgradedToProduct().getIsInIntroOfferPeriod() == null
                            ? false : airlyticsPurchase.getUpgradedToProduct().getIsInIntroOfferPeriod();

                    // calculate the partial refund after upgrade, always is negative
                    Long partialRefund = (long) (1 - (airlyticsPurchase.getPeriods() - Math.floor(airlyticsPurchase.getPeriods()))
                            * airlyticsPurchase.getPriceUsdMicros());
                    // get the price of the new  product, the current was upgraded to
                    Long upgradedToProductPrice = inAppAirlyticsProducts.get(airlyticsPurchase.getUpgradedToProduct().getProductId()).getPriceUSDMicros();

                    revenueUsdMicros = upgradedToProductPrice + partialRefund;
                }
                previousProductId = airlyticsPurchase.getProduct();
            }
        }

        public String getNewProductId() {
            return newProductId;
        }

        public void setNewProductId(String newProductId) {
            this.newProductId = newProductId;
        }

        public String getPreviousProductId() {
            return previousProductId;
        }

        public void setPreviousProductId(String previousProductId) {
            this.previousProductId = previousProductId;
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
    }
}
