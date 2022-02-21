package com.ibm.weather.airlytics.jobs.eventspatch.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockFeatureConfigManager;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class EventsPatchFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(EventsPatchFeatureConfigManager.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public EventsPatchAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();
        logger.debug("Got Feature config: {}", configuration);
        return objectMapper.readValue(configuration, EventsPatchAirlockConfig.class);
    }
}
