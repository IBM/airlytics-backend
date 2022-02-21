package com.ibm.weather.airlytics.jobs.dsr;

import com.ibm.weather.airlytics.jobs.dsr.dto.DsrJobAirlockConfig;
import com.ibm.weather.airlytics.jobs.dsr.services.DsrConfigService;

import java.util.Optional;

public class TestDsrConfigService extends DsrConfigService {

    public TestDsrConfigService() {
        super(null);
    }

    @Override
    public void init() {
    }

    @Override
    public void refreshConfiguration() {
    }

    @Override
    public Optional<DsrJobAirlockConfig> getCurrentConfig() {
        DsrJobAirlockConfig config = new DsrJobAirlockConfig();
        
        return Optional.of(config);
    }
}
