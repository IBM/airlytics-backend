package com.ibm.airlytics.retentiontrackerqueryhandler.utils;

import com.ibm.airlytics.retentiontracker.utils.BaseConfigurationManager;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component("ConfigurationManager")
@DependsOn("Airlock")
public class ConfigurationManager extends BaseConfigurationManager {
    public ConfigurationManager(Environment env) {
        super();
    }

    @PostConstruct
    private void setSecrets() {
        populateDbCreds();
        populateDbROCreds();
        populateRabbitCreds();
        populateAthenaConfigs();
        populateDbExporterConfigs();
        populatePollsConfigs();
        populateAirlockConfigs();
        populateAirlockApiCreds();
    }
}
