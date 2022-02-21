package com.ibm.airlytics.consumer.mparticle.transformation;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.integrations.mappings.GenericEventMappingConsumerConfig;

import java.io.IOException;
import java.util.StringJoiner;

public class MparticleTransformationConsumerConfig extends GenericEventMappingConsumerConfig {

    private boolean ignoreNegativePurchases = false;

    private boolean ignoreTrialPurchases = false;

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
        readFromAirlockFeature(AirlockConstants.Consumers.MPARTICLE_TRANSFORMATION_CONSUMER);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        MparticleTransformationConsumerConfig that = (MparticleTransformationConsumerConfig) o;

        if (ignoreNegativePurchases != that.ignoreNegativePurchases) return false;
        return ignoreTrialPurchases == that.ignoreTrialPurchases;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (ignoreNegativePurchases ? 1 : 0);
        result = 31 * result + (ignoreTrialPurchases ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MparticleTransformationConsumerConfig.class.getSimpleName() + "[", "]")
                .add("maxPollRecords=" + maxPollRecords)
                .add("destinationTopic='" + destinationTopic + "'")
                .add("jsonObjectAttributeAccepted=" + jsonObjectAttributeAccepted)
                .add("jsonArrayAttributeAccepted=" + jsonArrayAttributeAccepted)
                .add("customDimsAcceptedByDefault=" + customDimsAcceptedByDefault)
                .add("ignoreEventTypes=" + ignoreEventTypes)
                .add("ignoreEventAttributes=" + ignoreEventAttributes)
                .add("ignoreCustomDimensions=" + ignoreCustomDimensions)
                .add("includeEventTypes=" + includeEventTypes)
                .add("includeEventAttributes=" + includeEventAttributes)
                .add("includeCustomDimensions=" + includeCustomDimensions)
                .add("eventMappings=" + eventMappings)
                .add("ignoreNegativePurchases=" + ignoreNegativePurchases)
                .add("ignoreTrialPurchases=" + ignoreTrialPurchases)
                .toString();
    }
}
