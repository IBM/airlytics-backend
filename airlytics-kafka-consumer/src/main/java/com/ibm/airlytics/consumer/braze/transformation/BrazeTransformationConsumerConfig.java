package com.ibm.airlytics.consumer.braze.transformation;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventMappingConsumerConfig;

import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

public class BrazeTransformationConsumerConfig extends GenericEventMappingConsumerConfig {

    private String brazeAppId;

    private List<String> purchaseEvents;

    private boolean ignoreNegativePurchases = true;

    private boolean ignoreTrialPurchases = false;

    public String getBrazeAppId() {
        return brazeAppId;
    }

    public void setBrazeAppId(String brazeAppId) {
        this.brazeAppId = brazeAppId;
    }

    public List<String> getPurchaseEvents() {
        return purchaseEvents;
    }

    public void setPurchaseEvents(List<String> purchaseEvents) {
        this.purchaseEvents = purchaseEvents;
    }

    public boolean isIgnoreNegativePurchases() {
        return ignoreNegativePurchases;
    }

    public void setIgnoreNegativePurchases(boolean ignoreNegativePurchases) {
        this.ignoreNegativePurchases = ignoreNegativePurchases;
    }

    public boolean isIgnoreTrialPurchases() {
        return ignoreTrialPurchases;
    }

    public void setIgnoreTrialPurchases(boolean ignoreTrialPurchases) {
        this.ignoreTrialPurchases = ignoreTrialPurchases;
    }

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.BRAZE_TRANSFORMATION_CONSUMER);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BrazeTransformationConsumerConfig.class.getSimpleName() + "[", "]")
                .add("brazeAppId='" + brazeAppId + "'")
                .add("purchaseEvents=" + purchaseEvents)
                .add("ignoreNegativePurchases=" + ignoreNegativePurchases)
                .add("ignoreTrialPurchases=" + ignoreTrialPurchases)
                .add("destinationTopic='" + destinationTopic + "'")
                .add("jsonObjectAttributeAccepted=" + jsonObjectAttributeAccepted)
                .add("jsonArrayAttributeAccepted=" + jsonArrayAttributeAccepted)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("eventMappings=" + eventMappings)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BrazeTransformationConsumerConfig that = (BrazeTransformationConsumerConfig) o;

        if (ignoreNegativePurchases != that.ignoreNegativePurchases) return false;
        if (ignoreTrialPurchases != that.ignoreTrialPurchases) return false;
        if (brazeAppId != null ? !brazeAppId.equals(that.brazeAppId) : that.brazeAppId != null) return false;
        return purchaseEvents != null ? purchaseEvents.equals(that.purchaseEvents) : that.purchaseEvents == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (brazeAppId != null ? brazeAppId.hashCode() : 0);
        result = 31 * result + (purchaseEvents != null ? purchaseEvents.hashCode() : 0);
        result = 31 * result + (ignoreNegativePurchases ? 1 : 0);
        result = 31 * result + (ignoreTrialPurchases ? 1 : 0);
        return result;
    }
}
