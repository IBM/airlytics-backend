package com.ibm.weather.airlytics.jobs.aggregate.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockFeatureConfigManager;
import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class EventAggregatorFeatureConfigManager extends BaseAirlockFeatureConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(EventAggregatorFeatureConfigManager.class);

    @Value("${airlock.airlytics.aggregation}")
    private String airlyticsAggregation;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public EventAggregatorAirlockConfig readFromAirlockFeature() throws IOException {
        String configuration = getAirlockConfiguration();
        logger.debug("Got Feature config: {}", configuration);
        return objectMapper.readValue(configuration, EventAggregatorAirlockConfig.class);
    }

    @Override
    protected JSONObject createCalculationContext() {
        JSONObject context = super.createCalculationContext();
        context.put("aggregation", airlyticsAggregation);

        logger.debug("context = " + context.toString());
        return context;
    }
}
