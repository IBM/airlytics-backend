package com.ibm.airlytics.consumer.userdb;

import java.sql.Timestamp;

public class UserBasicRow {
    private Timestamp firstSession;
    private Timestamp lastSession;
    private String appVersion;

    public void setFirstSession(Timestamp firstSession) {
        this.firstSession = firstSession;
    }

    public void setLastSession(Timestamp lastSession) {
        this.lastSession = lastSession;
    }

    public UserBasicRow(Timestamp firstSession, Timestamp lastSession, String appVersion) {
        this.firstSession = firstSession;
        this.lastSession = lastSession;
        this.appVersion = appVersion;
    }

    public Timestamp getFirstSession() { return firstSession; }
    public Timestamp getLastSession() {
        return lastSession;
    }
    public String getAppVersion() {return appVersion; }
}
