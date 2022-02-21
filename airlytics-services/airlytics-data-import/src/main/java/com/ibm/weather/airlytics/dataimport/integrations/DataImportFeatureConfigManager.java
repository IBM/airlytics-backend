package com.ibm.weather.airlytics.dataimport.integrations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockFeatureConfigManager;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class DataImportFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(DataImportFeatureConfigManager.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String lastConfig = null;

    public DataImportAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();

        if(lastConfig == null || !configuration.equalsIgnoreCase(lastConfig)) {
            logger.info("Got Feature config: {}", configuration);
            lastConfig = configuration;
        }
        return objectMapper.readValue(configuration, DataImportAirlockConfig.class);
    }
}
