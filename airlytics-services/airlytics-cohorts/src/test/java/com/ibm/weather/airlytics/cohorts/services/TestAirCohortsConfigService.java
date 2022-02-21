package com.ibm.weather.airlytics.cohorts.services;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.cohorts.dto.AirCohortsAirlockConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class TestAirCohortsConfigService extends AirCohortsConfigService {

    public TestAirCohortsConfigService() {
        super(null);
    }

    @Override
    public Optional<AirCohortsAirlockConfig> getCurrentConfig() {
        AirCohortsAirlockConfig config = new AirCohortsAirlockConfig();
        config.setAirlockApiBaseUrl("http://airlock-test.ibm.com/");
        config.setProductIds(Collections.singletonList("unittest_product_1"));
        config.setAdditionalTables(Arrays.asList("user_features"));
        System.out.println(new Gson().toJson(config));
        return Optional.of(config);
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void refreshConfiguration() {
        // do nothing
    }
}
