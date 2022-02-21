package com.ibm.weather.airlytics.jobs.eventspatch;

import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import com.ibm.weather.airlytics.jobs.eventspatch.services.EventsPatchConfigService;

import java.util.Optional;

public class TestEventsPatchConfigService extends EventsPatchConfigService {

    public TestEventsPatchConfigService() {
        super(null);
    }

    @Override
    public void init() {
    }

    @Override
    public void refreshConfiguration() {
    }

    @Override
    public Optional<EventsPatchAirlockConfig> getCurrentConfig() {
        EventsPatchAirlockConfig config = new EventsPatchAirlockConfig();
        
        return Optional.of(config);
    }
}
