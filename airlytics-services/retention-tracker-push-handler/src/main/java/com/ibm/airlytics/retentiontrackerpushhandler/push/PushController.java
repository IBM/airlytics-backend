package com.ibm.airlytics.retentiontrackerpushhandler.push;

import com.ibm.airlytics.retentiontrackerpushhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.model.User;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontracker.push.PushProtocol;
import io.prometheus.client.Counter;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class PushController extends PushProtocol {
    static final Counter pushNotificationsCounter = Counter.build()
            .name("retention_tracker_push_notifications_sent_total")
            .help("Total number of push notifications sent.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();
    public static final Counter bouncedTokensCounter = Counter.build()
            .name("retention_tracker_bounced_tokens_total")
            .help("Total number of bounced tokens.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();
    public static final Counter pushErrorsCounter = Counter.build()
            .name("retention_tracker_push_errors_total")
            .help("Total number of errors in push.")
            .labelNames(Constants.ERROR_TYPE, Constants.ERROR_MESSAGE, Constants.PLATFORM, Constants.ENV)
            .register();
    static final Counter receivedPushMessageCounter = Counter.build()
            .name("retention_tracker_push_messages_total")
            .help("Total number of to-push messages received from queue.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(PushController.class.getName());

    private final APNSPushAdapter apnsAdapter;
    private final FCMPushAdapter fcmAdapter;
    private final QueuePublisher queuePublisher;
    private final DbHandler dbHandler;
    private final long batchSize;
    private final ConfigurationManager configurationManager;
    private int numRetries = 0;
    @Autowired
    public PushController(APNSPushAdapter apnsAdapter, FCMPushAdapter fcmAdapter,
                          QueuePublisher queuePublisher, DbHandler dbHandler, ConfigurationManager configurationManager) throws IOException, KeystoreException {
        this.apnsAdapter = apnsAdapter;
        this.fcmAdapter = fcmAdapter;
        this.queuePublisher = queuePublisher;
        this.dbHandler = dbHandler;
        this.configurationManager = configurationManager;
        batchSize = configurationManager.getBatchSize();
    }

    public void initPushAdapters() throws IOException, KeystoreException, NoSuchAlgorithmException, InvalidKeyException {
        if (AirlockManager.getAirlock().getPlatform()=="ios") {
            this.apnsAdapter.initAdapter();
        } else if (AirlockManager.getAirlock().getPlatform()=="android") {
            this.fcmAdapter.initAdapter();
        }
    }

    public boolean notifyUser(String userID) throws ClassNotFoundException, SQLException {
        logger.debug("notify user with id "+userID);
        User user = dbHandler.getUser(userID);
        if (user != null) {
            return notifyUser(user.getPushToken(), null);
        }
        logger.error("did not find user with id "+userID);
        return false;
    }
    public boolean notifyUser(String token, String message) {
        logger.debug("notifyUser called. token:"+token+", message:"+message);
        String type = AirlockManager.getAirlock().getPlatform();
        String product = AirlockManager.getAirlock().getProduct();
        switch (type) {
            case "ios":
                receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc();
                apnsAdapter.push(token);
                pushNotificationsCounter.labels(product, ConfigurationManager.getEnvVar()).inc();
                return true;
            case "android":
                receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc();
                fcmAdapter.push(token, message);
                pushNotificationsCounter.labels(product, ConfigurationManager.getEnvVar()).inc();
                return true;
        }
        receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc();
        logger.warn("notifyUser called with wrong DeviceType: deviceType:"+type);
        return false;
    }

    public boolean notifyUsers(List<String> tokens) {
        logger.debug("notifyUsers called. tokens:"+tokens);
        String type = AirlockManager.getAirlock().getPlatform();
        String product = AirlockManager.getAirlock().getProduct();
        switch (type) {
            case "ios":
                receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc(tokens.size());
                apnsAdapter.push(tokens);
                pushNotificationsCounter.labels(product, ConfigurationManager.getEnvVar()).inc(tokens.size());
                return true;
            case "android":
                receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc(tokens.size());
                fcmAdapter.push(tokens);
                pushNotificationsCounter.labels(product, ConfigurationManager.getEnvVar()).inc(tokens.size());
                return true;
        }
        receivedPushMessageCounter.labels(product, ConfigurationManager.getEnvVar()).inc(tokens.size());
        logger.warn("notifyUser called with wrong DeviceType: deviceType:"+type);
        return false;
    }

    public List<String> getBouncedTokens() throws CommunicationException, KeystoreException {
        return apnsAdapter.requestExpiredTokens();
    }

    private void queryForBouncedTokens() {
        try {
            List<String> tokens = apnsAdapter.requestExpiredTokens();
            for (String token : tokens) {
                queuePublisher.publishMessage(token);
            }
        } catch (CommunicationException | KeystoreException | IOException e) {
            logger.error("error querying for bounced tokens:"+e.getMessage());
        }
    }
    public void listenForBouncedTokens() {
        logger.info("connecting to feedback service");
        try {
            apnsAdapter.initFeedbackService();
        } catch (Exception e) {
            logger.error("error connecting to feedback service:"+e.getMessage());
            e.printStackTrace();
            return;
        }
        int interval = configurationManager.getFeedbackServiceInterval();
        logger.info("feedback service call time interval:"+interval);
        if (interval <= 0) {
            interval = 60;
        }
        long maxRetries = configurationManager.getFeedbackServiceNumRetries();
        logger.info("feedback service number of retries:"+maxRetries);
        if (maxRetries <= 0) {
            maxRetries = 12;
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        long finalMaxRetries = maxRetries;
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    List<String> tokens = apnsAdapter.requestExpiredTokens();
                    bouncedTokensCounter.labels(AirlockManager.getAirlock().getProduct(),ConfigurationManager.getEnvVar()).inc(tokens.size());
                    ArrayList<String> batch = new ArrayList<>();
                    for (String token : tokens) {
                        batch.add(token);
                        if (batch.size() >= batchSize) {
                            queuePublisher.publishMessage(batch.toString());
                            batch.clear();
                        }
                    }
                    if (batch.size() > 0) {
                        queuePublisher.publishMessage(batch.toString());
                    }
                    numRetries=0;
                } catch (Exception e) {
                    numRetries++;
                    if (numRetries > finalMaxRetries) {
                        logger.error("error connecting to apple feedback service:"+e.getMessage());
                        numRetries = 0;
                    } else {
                        logger.warn("error connecting to apple feedback service:"+e.getMessage());
                    }

                }
            }
        },0, interval, TimeUnit.MINUTES);
    }


}
