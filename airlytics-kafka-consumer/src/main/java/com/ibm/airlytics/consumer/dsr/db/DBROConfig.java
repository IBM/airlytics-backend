package com.ibm.airlytics.consumer.dsr.db;

import com.ibm.airlytics.airlock.AirlockConstants;

import java.io.IOException;

public class DBROConfig extends DBConfig {
    @Override
    public void initWithAirlock() throws IOException {
        readFromAirlockFeature(AirlockConstants.Consumers.DB_READ_ONLY_CONFIG);
    }
}
