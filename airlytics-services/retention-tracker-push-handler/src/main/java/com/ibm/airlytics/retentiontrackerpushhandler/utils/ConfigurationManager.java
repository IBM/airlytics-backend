package com.ibm.airlytics.retentiontrackerpushhandler.utils;

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
        populateRabbitCreds();
        populateApnsCreds();
        populateFcmCreds();
    }
}
