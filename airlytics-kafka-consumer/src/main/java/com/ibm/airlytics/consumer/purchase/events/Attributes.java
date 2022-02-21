package com.ibm.airlytics.consumer.purchase.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.ibm.airlytics.consumer.purchase.AirlyticsPurchase;

public class Attributes {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String premiumProductId;

    public Attributes() {

    }
    public Attributes(AirlyticsPurchase airlyticsPurchase) {
        if (airlyticsPurchase != null) {
            this.premiumProductId = airlyticsPurchase.getProduct();
        }
    }

    public String getPremiumProductId() {
        return premiumProductId;
    }

    public void setPremiumProductId(String premiumProductId) {
        this.premiumProductId = premiumProductId;
    }
}