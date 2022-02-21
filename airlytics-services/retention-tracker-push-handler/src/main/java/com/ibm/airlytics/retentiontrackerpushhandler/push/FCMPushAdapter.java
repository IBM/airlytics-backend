package com.ibm.airlytics.retentiontrackerpushhandler.push;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;

@Component
@DependsOn("ConfigurationManager")
public class FCMPushAdapter implements HealthCheckable {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(FCMPushAdapter.class.getName());

    private final QueuePublisher queuePublisher;
    private final ConfigurationManager configurationManager;
    private static String UNREGISTERED = "registration-token-not-registered";
    private String healthMessage = HEALTY;
    private ThreadPoolExecutor executor;
    private boolean isInitialized = false;

    @Autowired
    public FCMPushAdapter(QueuePublisher queuePublisher, ConfigurationManager configurationManager) throws IOException {
        this.configurationManager = configurationManager;
        this.queuePublisher = queuePublisher;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(configurationManager.getPushThreads());
    }

    public void initAdapter() throws IOException {
        String configStr = configurationManager.getFcmConfig();
        String dbUrl = configurationManager.getFcmDbUrl();
        InputStream serviceAccount = new ByteArrayInputStream(configStr.getBytes());

        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                .setDatabaseUrl(dbUrl)
                .build();

        FirebaseApp.initializeApp(options);
        this.isInitialized = true;
    }

    public void push(List<String> tokens) {
        ArrayList<Future> pushes = new ArrayList<Future>();
        for (String tokenStr : tokens) {
            FCMPushTask task = new FCMPushTask(tokenStr);
            Future f = executor.submit(task);
            pushes.add(f);
        }
    }

    public void push(String token, String message) {
        try {
            sendAdminPushNotification(token);
        } catch (FirebaseMessagingException e) {

            String errorCode = e.getErrorCode();
            if (errorCode != null && errorCode.equals(FCMPushAdapter.UNREGISTERED)) {
                //device has bounced
                logger.info("android device with token:"+token+" has bounced");
                PushController.bouncedTokensCounter.labels(AirlockManager.getAirlock().getProduct(),ConfigurationManager.getEnvVar()).inc();
                try {
                    queuePublisher.publishMessage("["+token+"]");
                } catch (IOException ex) {
                    logger.error("failed sending token "+token+" to bounced queue");
                }
            } else {
                logger.error("Failed to send notification to FCM for token:"+token+". error:"+e.getErrorCode());
            }
        }
    }

    private void sendAdminPushNotification(String deviceToken) throws FirebaseMessagingException {
        logger.debug("sending android silent push to token:"+deviceToken);
        // This registration token comes from the client FCM SDKs.
        String registrationToken = deviceToken;

        Notification notif = Notification.builder().setTitle("איתן").build();
        // See documentation on defining a message payload.
        Message message = Message.builder()
                .setToken(registrationToken)//.setNotification(notif)
                .build();

        // Send a message to the device corresponding to the provided
        // registration token.

        String response = FirebaseMessaging.getInstance().send(message);
        // Response is a message ID string.
        logger.debug("Successfully sent message: " + response);
    }

    @Override
    public boolean isHealthy() {
        if (!isInitialized) {
            return true;
        }
        if (FirebaseMessaging.getInstance() == null) {
            healthMessage = "Error getting FirebaseMessaging instance";
            return false;
        }

        healthMessage = HEALTY;
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

    class FCMPushTask implements Runnable {

        private String token;

        public FCMPushTask(String tokenStr) {
            this.token=tokenStr;
        }
        @Override
        public void run() {
            try {
                sendAdminPushNotification(token);
            } catch (FirebaseMessagingException e) {

                String errorCode = e.getErrorCode();
                if (errorCode != null && errorCode.equals(FCMPushAdapter.UNREGISTERED)) {
                    logger.info("found bounced token:"+token);
                    PushController.bouncedTokensCounter.labels(AirlockManager.getAirlock().getProduct(),ConfigurationManager.getEnvVar()).inc();
                    //device has bounced
                    try {
                        queuePublisher.publishMessage("["+token+"]");
                    } catch (IOException ex) {
                        logger.error("error publishing message:"+ex.getMessage());
                    }
                } else {
                    logger.warn("Failed to send notification to FCM for token:"+token+". error:"+e.getErrorCode());
                    PushController.pushErrorsCounter.labels("not accepted",e.getErrorCode(),"android",ConfigurationManager.getEnvVar()).inc();
                }
            }
        }
    }
}
