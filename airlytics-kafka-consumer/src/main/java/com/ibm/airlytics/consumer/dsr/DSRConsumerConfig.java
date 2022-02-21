package com.ibm.airlytics.consumer.dsr;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;

public class DSRConsumerConfig extends AirlyticsConsumerConfig {

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.DSR_CONSUMER);
    }
}