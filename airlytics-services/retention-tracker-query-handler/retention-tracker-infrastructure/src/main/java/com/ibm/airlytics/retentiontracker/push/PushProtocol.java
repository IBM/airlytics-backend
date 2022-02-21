package com.ibm.airlytics.retentiontracker.push;

import java.sql.SQLException;
import java.util.List;

public abstract class PushProtocol {
    public abstract boolean notifyUser(String userID) throws ClassNotFoundException, SQLException;
    public abstract boolean notifyUser(String token, String message);
    public abstract boolean notifyUsers(List<String> tokens);
}
