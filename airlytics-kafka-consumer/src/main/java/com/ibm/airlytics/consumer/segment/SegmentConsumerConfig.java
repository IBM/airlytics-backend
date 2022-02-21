package com.ibm.airlytics.consumer.segment;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;


public class SegmentConsumerConfig extends AirlyticsConsumerConfig {
    private static final String SEGMENT_WRITE_KEY_ENV_VARIABLE = "SEGMENT_WRITE_KEY";

    private String writeKey;
    private String segmentEndpoint;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.SEGMENT_CONSUMER);

        String writeKeyFromEnv = System.getenv(SEGMENT_WRITE_KEY_ENV_VARIABLE);
        if (writeKeyFromEnv != null) {
            writeKey = writeKeyFromEnv;
        }
    }

    public String getWriteKey() {
        return writeKey;
    }

    public void setWriteKey(String writeKey) {
        this.writeKey = writeKey;
    }

    public String getSegmentEndpoint() {
        return segmentEndpoint;
    }

    public void setSegmentEndpoint(String segmentEndpoint) {
        this.segmentEndpoint = segmentEndpoint;
    }
}
