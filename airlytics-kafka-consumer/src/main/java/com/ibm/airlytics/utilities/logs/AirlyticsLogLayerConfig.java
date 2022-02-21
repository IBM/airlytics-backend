package com.ibm.airlytics.utilities.logs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockManager;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import java.io.IOException;

public class AirlyticsLogLayerConfig {
    private String logLevel = "1";
    private String className = null;
    private String messageTerm = "";
    private String matchType = "contains"; //"contains"/"startsWith"

    protected static final ObjectMapper objectMapper = new ObjectMapper();

    public void initWithAirlock(String featureName) throws IOException {
        String configuration = getAirlockConfiguration(featureName);
        if (configuration != null) {
            objectMapper.readerForUpdating(this).readValue(configuration);
        }
    }

    @CheckForNull
    protected String getAirlockConfiguration(String featureName) throws IOException {
        Feature feature = AirlockManager.getAirlock().getFeature(featureName);
        if (!feature.isOn())
            throw new IOException("Airlock feature " + featureName + " is not ON (" + feature.getSource()
                    + ", " + feature.getTraceInfo() + ")");

        JSONObject config = feature.getConfiguration();
        return config == null ? null : config.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getMessageTerm() {
        return messageTerm;
    }

    public void setMessageTerm(String messageTerm) {
        this.messageTerm = messageTerm;
    }

    public String getMatchType() {
        return matchType;
    }

    public void setMatchType(String matchType) {
        this.matchType = matchType;
    }

    public boolean hasMatch(String className, String message) {
        boolean classNameMatched = getClassName()==null || getClassName().equalsIgnoreCase(className);
        if (!classNameMatched) return false;
        boolean messageMatched = getMessageTerm()==null || matchMessage(message, getMessageTerm(), getMatchType());
        return messageMatched;
    }

    private boolean matchMessage(String message, String messageTerm, String matchType) {
        if (matchType.equals("startsWith")) {
            return message.startsWith(messageTerm);
        } else if (matchType.equals("contains")) {
            return message.contains(messageTerm);
        } else {
            //unsupported
            return false;
        }
    }
}
