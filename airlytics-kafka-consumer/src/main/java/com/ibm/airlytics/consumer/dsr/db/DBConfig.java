package com.ibm.airlytics.consumer.dsr.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import java.io.IOException;

public class DBConfig {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(DBConfig.class.getName());

    private String dbPasswordVar;
    private String dbUrl;
    private String dbUsername;

    protected static ObjectMapper objectMapper = new ObjectMapper();

    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getAirlock().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON ("+ feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config.toString();
    }

    protected void readFromAirlockFeature(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        objectMapper.readerForUpdating(this).readValue(configuration);
    }

    public void initWithAirlock() throws IOException {
        readFromAirlockFeature(AirlockConstants.Consumers.DB_CONFIG);
    }

    @CheckForNull
    public String getDbPassword() {
        return System.getenv(getDbPasswordVar());
    }

    @Override
    public String toString() {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            LOGGER.error("Error serializing consumer config object: " + e.getMessage(), e);
            return "Error serializing consumer config object";
        }
    }

    public String getDbPasswordVar() {
        return dbPasswordVar;
    }

    public void setDbPasswordVar(String dbPasswordVar) {
        this.dbPasswordVar = dbPasswordVar;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }
}
