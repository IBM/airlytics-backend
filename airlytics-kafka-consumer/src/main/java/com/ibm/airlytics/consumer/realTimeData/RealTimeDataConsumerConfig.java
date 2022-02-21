package com.ibm.airlytics.consumer.realTimeData;

import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;

import java.io.IOException;

public class RealTimeDataConsumerConfig extends AirlyticsConsumerConfig {
    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.REAL_TIME_DATA_CONSUMER);
    }
    private RealTimePrometheusConfig eventsConfig;
    private Long staleThresholdSeconds;
    private Long periodicalPruneTimeMinutes;

    public RealTimePrometheusConfig getEventsConfig() {
        return eventsConfig;
    }

    public void setEventsConfig(RealTimePrometheusConfig eventsConfig) {
        this.eventsConfig = eventsConfig;
    }

    public Long getStaleThresholdSeconds() {
        return staleThresholdSeconds;
    }

    public void setStaleThresholdSeconds(Long staleThresholdSeconds) {
        this.staleThresholdSeconds = staleThresholdSeconds;
    }

    public Long getPeriodicalPruneTimeMinutes() {
        return periodicalPruneTimeMinutes;
    }

    public void setPeriodicalPruneTimeMinutes(Long periodicalPruneTimeMinutes) {
        this.periodicalPruneTimeMinutes = periodicalPruneTimeMinutes;
    }
}
