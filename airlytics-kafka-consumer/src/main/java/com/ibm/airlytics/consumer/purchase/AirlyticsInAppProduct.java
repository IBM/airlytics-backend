package com.ibm.airlytics.consumer.purchase;

@SuppressWarnings("unused")
public class AirlyticsInAppProduct {
    private Long priceUSDMicros;
    private String gracePeriod;
    private String productId;
    private String trialPeriod;
    private String subscriptionPeriod;
    private boolean autoRenewing;
    private Long introPriceUSDMicros;

    public Long getPriceUSDMicros() {
        return priceUSDMicros;
    }

    public void setPriceUSDMicros(Long priceUSDMicros) {
        this.priceUSDMicros = priceUSDMicros;
    }

    public String getGracePeriod() {
        return gracePeriod;
    }

    public void setGracePeriod(String gracePeriod) {
        this.gracePeriod = gracePeriod;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getTrialPeriod() {
        return trialPeriod;
    }

    public void setTrialPeriod(String trialPeriod) {
        this.trialPeriod = trialPeriod;
    }

    public String getSubscriptionPeriod() {
        return subscriptionPeriod;
    }

    public void setSubscriptionPeriod(String subscriptionPeriod) {
        this.subscriptionPeriod = subscriptionPeriod;
    }

    public boolean isAutoRenewing() {
        return autoRenewing;
    }

    public void setAutoRenewing(boolean autoRenewing) {
        this.autoRenewing = autoRenewing;
    }

    public Long getIntroPriceUSDMicros() {
        return introPriceUSDMicros;
    }

    public void setIntroPriceUSDMicros(Long introPriceUSDMicros) {
        this.introPriceUSDMicros = introPriceUSDMicros;
    }
}
