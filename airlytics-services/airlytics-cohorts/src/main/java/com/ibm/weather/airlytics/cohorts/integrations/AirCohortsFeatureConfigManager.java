package com.ibm.weather.airlytics.cohorts.integrations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.cohorts.dto.AirCohortsAirlockConfig;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockFeatureConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class AirCohortsFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(AirCohortsFeatureConfigManager.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public AirCohortsAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();
        logger.debug("Got Feature config: {}", configuration);
        return objectMapper.readValue(configuration, AirCohortsAirlockConfig.class);
    }
}
