package com.ibm.weather.airlytics.jobs.dsr.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockFeatureConfigManager;
import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DsrJobFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(DsrJobFeatureConfigManager.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public DsrJobAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();
        logger.debug("Got Feature config: {}", configuration);
        return objectMapper.readValue(configuration, DsrJobAirlockConfig.class);
    }
}
