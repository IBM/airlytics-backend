package com.ibm.weather.airlytics.jobs.aggregate;

import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import com.ibm.weather.airlytics.jobs.aggregate.services.EventAggregatorConfigService;

import java.util.Optional;

public class TestEventAggregatorConfigService extends EventAggregatorConfigService {

    public TestEventAggregatorConfigService() {
        super(null);
    }

    @Override
    public void init() {
    }

    @Override
    public void refreshConfiguration() {
    }

    @Override
    public Optional<EventAggregatorAirlockConfig> getCurrentConfig() {
        EventAggregatorAirlockConfig config = new EventAggregatorAirlockConfig();
        
        return Optional.of(config);
    }
}
