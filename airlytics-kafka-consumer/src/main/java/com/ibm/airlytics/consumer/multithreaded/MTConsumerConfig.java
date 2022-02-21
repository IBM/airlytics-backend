package com.ibm.airlytics.consumer.multithreaded;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;
import com.ibm.airlytics.consumer.userdb.DynamicAttributeConfig;

import java.io.IOException;
import java.util.HashMap;

public class MTConsumerConfig extends AirlyticsConsumerConfig {
    private int threads;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.MULTITHREADED_CONSUMER);
    }

    public int getThreads() { return threads; }

    public void setThreads(int threads) { this.threads = threads; }
}
