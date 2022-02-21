package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;

import java.util.Arrays;
import java.util.Optional;

public class TestDataImportConfigService extends DataImportConfigService {

    public TestDataImportConfigService() {
        super(null);
    }

    @Override
    public void refreshConfiguration() {
    }

    @Override
    public Optional<DataImportAirlockConfig> getCurrentConfig() {
        DataImportAirlockConfig config = new DataImportAirlockConfig();
        config.setFeatureTables(Arrays.asList("user_features_test", "user_features_video"));
        config.setPiFeatureTables(Arrays.asList("user_features_test_pi"));
        config.setAthenaImportTables(Arrays.asList("ad_impressions"));
        return Optional.of(config);
    }

    @Override
    protected void updateCurrentConfig() {
    }
}
