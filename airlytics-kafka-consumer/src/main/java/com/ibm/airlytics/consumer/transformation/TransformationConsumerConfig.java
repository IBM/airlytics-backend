package com.ibm.airlytics.consumer.transformation;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransformationConsumerConfig extends AirlyticsConsumerConfig {

    private List<TransformationConfig> transformations = new ArrayList<>();

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.TRANSFORMATION_CONSUMER);
    }

    public List<TransformationConfig> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<TransformationConfig> transformationConfigs) {
        this.transformations = transformationConfigs;
    }
}
