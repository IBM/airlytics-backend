package com.ibm.airlytics.consumer.cohorts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockClient;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockCohortsConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;

import java.util.Arrays;
import java.util.Optional;

public class AbstractCohortsExporter {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AbstractCohortsExporter.class.getName());

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected static final Counter usersCounter =
            Counter.build()
                    .name("airlytics_cohorts_users_exported")
                    .help("Counter for user-cohort pairs sent to the 3rd party")
                    .labelNames("target")
                    .register();

    protected AirlockClient airlockClient;

    public AbstractCohortsExporter(AirlockCohortsConsumerConfig featureConfig) {
        this.airlockClient = new AirlockClient(featureConfig);
    }

    // for unit-testing
    public AbstractCohortsExporter() {}

    public Optional<UserCohortExport> getUserCohortExport(UserCohort userCohort, String key) {
        return userCohort.getEnabledExports()
                .stream()
                .filter(e -> key.equalsIgnoreCase(e.getExportType()))
                .findAny();
    }

    protected Object convertIntValue(UserCohort uc) {

        try {
            return Integer.valueOf(uc.getCohortValue());
        } catch(NumberFormatException e) {
            LOGGER.error("Invalid INT " + uc.getCohortValue() + " value in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }
        return uc.getCohortValue();
    }

    protected Object convertFloatValue(UserCohort uc) {

        try {
            return Double.valueOf(uc.getCohortValue());
        } catch(NumberFormatException e) {
            LOGGER.error("Invalid FLOAT value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }
        return uc.getCohortValue();
    }

    protected Object convertBoolValue(UserCohort uc) {

        if(!"true".equalsIgnoreCase(uc.getCohortValue()) &&
                !"false".equalsIgnoreCase(uc.getCohortValue()) &&
                !"1".equalsIgnoreCase(uc.getCohortValue()) &&
                !"0".equalsIgnoreCase(uc.getCohortValue())) {
            LOGGER.error("Invalid BOOL value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
            return uc.getCohortValue();
        } else {
            return "true".equalsIgnoreCase(uc.getCohortValue()) || "1".equalsIgnoreCase(uc.getCohortValue());
        }
    }

    protected Object convertArrayValue(UserCohort uc) {
        String sValue = uc.getCohortValue();

        if(sValue.startsWith("[") || sValue.startsWith("{")) {
            sValue = sValue.substring(1);
        } else {
            LOGGER.error("Invalid ARRAY value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }

        if(sValue.endsWith("]") || sValue.endsWith("}")) {
            sValue = sValue.substring(0, sValue.length() - 1);
        } else {
            LOGGER.error("Invalid ARRAY value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }
        String[] a = sValue.split("\\,\\s?");
        return Arrays.asList(a);
    }

    protected Object convertJsonValue(UserCohort uc) {

        if(!uc.getCohortValue().startsWith("{") || !uc.getCohortValue().endsWith("}")) {
            LOGGER.error("Invalid JSON value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }

        try {
            return MAPPER.readTree(uc.getCohortValue());
        } catch (JsonProcessingException e) {
            LOGGER.error("Invalid JSON value " + uc.getCohortValue() + " in cohort " + uc.getCohortId() + " for user " + uc.getUserId());
        }
        return uc.getCohortValue();
    }

}
