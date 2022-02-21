package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.consumer.AirlyticsConsumerConfig;


import javax.annotation.CheckForNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDBConsumerConfig extends AirlyticsConsumerConfig {
    private static final String USERDB_PASSWORD_ENV_VARIABLE = "USERDB_PASSWORD";

    private String dbUrl;
    private String dbUsername;
    private String userSchema;
    private String userTable;
    private String pollAnswersTable;
    private String pollAnswersPITable;
    private List<String> additionalUserTables;
    private boolean useSSL;
    private int numberOfShards = 1000;
    private List<String> sqlStatesContinuationPrefixes = new ArrayList<>();
    // per event name, per attribute name
    private HashMap<String, HashMap<String, DynamicAttributeConfig>> dynamicEventConfigMap;

    private int userCacheSizeInRecords = 10000;

    @Override
    public void initWithAirlock() throws IOException {
        super.initWithAirlock();
        readFromAirlockFeature(AirlockConstants.Consumers.USERDB_CONSUMER);

        // Read stream field configuration from Airlock
        String dynamicFieldConfig = getAirlockConfiguration(AirlockConstants.Consumers.DYNAMIC_EVENTS);
        TypeReference<HashMap<String, HashMap<String, DynamicAttributeConfig>>> typeRef
                = new TypeReference<HashMap<String, HashMap<String, DynamicAttributeConfig>>>() {};
        dynamicEventConfigMap = objectMapper.readValue(dynamicFieldConfig, typeRef);

        // initialize the schema for the tables, and the default table to be the user table
        dynamicEventConfigMap.forEach((eventName, attributes) ->
                attributes.forEach((attributeName, config) -> {
                        if (config.getDbTable() == null)
                            config.setDbTable(getUserTable());
                } ));
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
    @CheckForNull
    public String getDbPassword() {
        return System.getenv(USERDB_PASSWORD_ENV_VARIABLE);
    }

    public String getUserTable() {
        return userTable;
    }
    public void setUserTable(String userTable) { this.userTable = userTable; }

    public String getPollAnswersTable() {
        return pollAnswersTable;
    }
    public void setPollAnswersTable(String pollAnswersTable) { this.pollAnswersTable = pollAnswersTable; }

    public String getPollAnswersPITable() {
        return pollAnswersPITable;
    }
    public void setPollAnswersPITable(String pollAnswersPITable) { this.pollAnswersPITable = pollAnswersPITable; }

    public int getNumberOfShards() { return numberOfShards; }
    public void setNumberOfShards(int numberOfShards) { this.numberOfShards = numberOfShards; }

    public Map<String, HashMap<String, DynamicAttributeConfig>> getDynamicEventConfigMap() { return dynamicEventConfigMap; }

    public boolean isUseSSL() { return useSSL; }
    public void setUseSSL(boolean useSSL) { this.useSSL = useSSL; }

    public List<String> getSqlStatesContinuationPrefixes() { return sqlStatesContinuationPrefixes; }
    public void setSqlStatesContinuationPrefixes(List<String> sqlStatesContinuationPrefixes) {
        this.sqlStatesContinuationPrefixes = sqlStatesContinuationPrefixes;
    }

    public int getUserCacheSizeInRecords() { return userCacheSizeInRecords; }
    public void setUserCacheSizeInRecords(int userCacheSizeInRecords) { this.userCacheSizeInRecords = userCacheSizeInRecords; }

    public List<String> getAdditionalUserTables() { return additionalUserTables; }
    public void setAdditionalUserTables(List<String> additionalUserTables) { this.additionalUserTables = additionalUserTables; }

    public String getUserSchema() { return userSchema; }
    public void setUserSchema(String dbUserSchema) { this.userSchema = dbUserSchema; }
}
