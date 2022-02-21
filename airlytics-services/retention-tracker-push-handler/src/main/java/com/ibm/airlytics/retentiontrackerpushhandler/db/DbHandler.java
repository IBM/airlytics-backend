package com.ibm.airlytics.retentiontrackerpushhandler.db;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.model.User;
import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;

@Component
@DependsOn("ConfigurationManager")
public class DbHandler implements HealthCheckable {

    Connection conn;
    String dburl;
    String userName;
    final ConfigurationManager configurationManager;

    String healthMessage = HEALTY;

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(DbHandler.class.getName());

    public DbHandler(ConfigurationManager configurationManager) throws KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException, URISyntaxException, ClassNotFoundException, SQLException {
        dburl = configurationManager.getDbUrl();
        userName = configurationManager.getDbUser();
        this.configurationManager = configurationManager;
//        validateConnection();
    }

    private void validateConnection() throws SQLException, ClassNotFoundException {
        if (conn == null || !conn.isValid(3)) {
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.setProperty("user", userName);
            props.setProperty("password", configurationManager.getDbPass());
            props.setProperty("ssl", "true");
            props.setProperty("sslmode", "verify-full");
            conn = DriverManager.getConnection(dburl, props);
        }
    }
    public User getUser(String userID) throws SQLException, ClassNotFoundException {
        logger.debug("getUser() for userId " + userID);
        Class.forName("org.postgresql.Driver");
        validateConnection();
        Feature getIdFeature = AirlockManager.getInstance().getFeature(AirlockConstants.retention.GET_USER_BY_ID);
        String select = getIdFeature.getConfiguration().getString("query");

        PreparedStatement stmt = conn.prepareStatement(select);
        stmt.setString(1, userID);
        ResultSet rs = stmt.executeQuery(select);
        if (rs.next()) {
            logger.debug("user with userId " + userID + " was found in DB");
            String pushToken = rs.getString("push_token");
            Timestamp lastPushSent = rs.getTimestamp("last_push_sent");
            String deviceType = rs.getString("platform");
            logger.info("got user with id:" + userID + ", push token:" + pushToken + ", last push sent:" + lastPushSent + ", platform:" + deviceType);
            String productId = configurationManager.getProductId(deviceType);
            User user = new User(userID, pushToken, deviceType,productId);
            return user;
        } else {
            return null;
        }
    }

    @Override
    public boolean isHealthy() {
        if (conn == null) {
            healthMessage = "Connection not started yet";
            return false;
        }
        try {
            if (!conn.isValid(3)) {
                healthMessage = "DB Connection Invalid";
                return false;
            }
        } catch (SQLException e) {
            logger.info("error checking if the connection is valid:"+e.getMessage());
            healthMessage = e.getMessage();
            return false;
        }

        healthMessage = HEALTY;
        return true;
    }

    @PreDestroy
    public void onDestroy() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

}