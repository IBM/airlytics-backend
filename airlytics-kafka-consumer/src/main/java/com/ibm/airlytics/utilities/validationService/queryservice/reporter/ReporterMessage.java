package com.ibm.airlytics.utilities.validationService.queryservice.reporter;

public class ReporterMessage {

    private final static String FIELD_SEPARATOR = ";";

    public enum Levels {
        error,
        test,
        info
    }

    private Levels level;
    private String platformAndMode;
    private String appVersion;
    private String eventName;
    private long count;
    private long expectedRate;
    private String alertStr;

    public ReporterMessage(Levels level, String platformAndMode,String appVersion, String eventName, long count, long expectedRate, String alertStr) {
        this.level = level;
        this.platformAndMode = platformAndMode;
        this.appVersion = appVersion;
        this.eventName = eventName;
        this.count = count;
        this.expectedRate = expectedRate;
        this.alertStr = alertStr;
        this.platformAndMode = platformAndMode;
    }

    public ReporterMessage(Levels level, String platformAndMode, String eventName, long count, Long expectedRate, String alertStr) {
        this.level = level;
        this.platformAndMode = platformAndMode;
        this.appVersion = "All";
        this.eventName = eventName;
        this.count = count;
        if (expectedRate != null){
            this.expectedRate = expectedRate;
        }else{
            this.expectedRate = -1;
        }
        this.alertStr = alertStr;
    }

    public ReporterMessage(Levels level, String platformAndMode, String appVersion, String eventName, long count, String alertStr) {
        this.level = level;
        this.platformAndMode = platformAndMode;
        this.appVersion = appVersion;
        this.eventName = eventName;
        this.count = count;
        this.expectedRate = -1;
        this.alertStr = alertStr;
    }

    public void setLevel(Levels level) {
        this.level = level;
    }

    public void setAlertStr(String alertStr) {
        this.alertStr = alertStr;
    }

    public void appendAlertStr(String appendedString) {
        this.alertStr = this.alertStr + "\n\n" + appendedString;
    }


    public String getEventName() {
        return eventName;
    }

    public String getPlatformAndMode() {
        return platformAndMode;
    }

    public String toString(){
        String normalizedAlertStr = alertStr.replaceAll("'", "\\\\'").replaceAll("\n","\\\\n");
        String expectedRateStr = "";
        if(expectedRate > -1){
            expectedRateStr = String.format("%,d", expectedRate) + FIELD_SEPARATOR;
        }
        return level.name() + FIELD_SEPARATOR + platformAndMode + FIELD_SEPARATOR + appVersion + FIELD_SEPARATOR + eventName + FIELD_SEPARATOR + String.format("%,d", count) + FIELD_SEPARATOR + expectedRateStr + normalizedAlertStr;
    }
}
