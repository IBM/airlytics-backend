package com.ibm.airlytics.retentiontrackerpushhandler.push;
import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.turo.pushy.apns.*;
import com.turo.pushy.apns.auth.ApnsSigningKey;
import com.turo.pushy.apns.util.ApnsPayloadBuilder;
import com.turo.pushy.apns.util.SimpleApnsPushNotification;
import com.turo.pushy.apns.util.TokenUtil;
import com.turo.pushy.apns.util.concurrent.PushNotificationFuture;
import com.turo.pushy.apns.util.concurrent.PushNotificationResponseListener;
import javapns.communication.exceptions.CommunicationException;
import javapns.communication.exceptions.KeystoreException;
import javapns.devices.Device;
import javapns.feedback.AppleFeedbackServer;
import javapns.feedback.AppleFeedbackServerBasicImpl;
import javapns.feedback.FeedbackServiceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;


@Component
@DependsOn("ConfigurationManager")
public class APNSPushAdapter  implements HealthCheckable {

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(APNSPushAdapter.class.getName());

    private final Environment env;
    private ApnsClient apnsClient;
    private final ConfigurationManager configurationManager;
    private final QueuePublisher queuePublisher;
    private ThreadPoolExecutor executor;
    private String healthMessage = HEALTY;
    private boolean isInitialized = false;

    FeedbackServiceManager feedbackManager;
    AppleFeedbackServer server;

    @Autowired
    public APNSPushAdapter(Environment env, ConfigurationManager configurationManager, QueuePublisher queuePublisher) throws IOException, KeystoreException {
        super();

        this.env = env;
        this.configurationManager = configurationManager;
        this.queuePublisher = queuePublisher;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(configurationManager.getPushThreads());
    }

    public void initAdapter() throws IOException, KeystoreException, NoSuchAlgorithmException, InvalidKeyException {
        String key = configurationManager.getApnsKey();
        InputStream targetStream = new ByteArrayInputStream(key.getBytes());
//        String cert = configurationManager.getApnsCert();
//        byte[] bytes = toByteArray(cert);
        boolean useProd = configurationManager.isUseAPNSProdServer();
        logger.info("using APNS prod server:"+useProd);
        String apnsEnvironment = useProd==true ? ApnsClientBuilder.PRODUCTION_APNS_HOST : ApnsClientBuilder.DEVELOPMENT_APNS_HOST;
        apnsClient = new ApnsClientBuilder()
                .setApnsServer(apnsEnvironment)
                .setSigningKey(ApnsSigningKey.loadFromInputStream(targetStream, configurationManager.getApnsTeamId(), configurationManager.getApnsKeyId()))
                .build();
        connectToFeedbackServer();

        this.isInitialized = true;
    }

    public void initFeedbackService() throws KeystoreException, IOException, NoSuchAlgorithmException, InvalidKeyException {
        connectToFeedbackServer();
    }


    private void connectToFeedbackServer() throws KeystoreException, IOException, NoSuchAlgorithmException, InvalidKeyException {
        String cert = configurationManager.getApnsCert();
        byte[] bytes = toByteArray(cert);

        InputStream targetStream = new ByteArrayInputStream(bytes);
        boolean useProd = configurationManager.isUseAPNSProdServer();
        logger.info("using APNS prod server for feedback service:"+useProd);
        feedbackManager = new FeedbackServiceManager();
        server = new AppleFeedbackServerBasicImpl(targetStream, configurationManager.getApnsCertPass(), useProd);
    }
    private static byte[] toByteArray(String s) {
        return DatatypeConverter.parseHexBinary(s);
    }

    public void push(List<String> tokens) {
        ArrayList<Future> pushes = new ArrayList<Future>();
        for (String tokenStr : tokens) {
            APNSPushTask task = new APNSPushTask(tokenStr);
            Future f = executor.submit(task);
            pushes.add(f);
        }
    }

    public void push(String tokenStr) {
        logger.debug("sending push to apple. token:"+tokenStr);
        final String appId = configurationManager.getApnsProductId();
        final ApnsPayloadBuilder payloadBuilder = new ApnsPayloadBuilder();
        payloadBuilder.setContentAvailable(true);
        final String payload = payloadBuilder.buildWithDefaultMaximumLength();
        final String token = TokenUtil.sanitizeTokenString(tokenStr);
        SimpleApnsPushNotification pushNotification = new SimpleApnsPushNotification(token, appId, payload, null, DeliveryPriority.IMMEDIATE, PushType.ALERT);
        PushNotificationFuture<SimpleApnsPushNotification, PushNotificationResponse<SimpleApnsPushNotification>> future = apnsClient.sendNotification(pushNotification);
        future.addListener(new PushNotificationResponseListener<SimpleApnsPushNotification>() {

            @Override
            public void operationComplete(final PushNotificationFuture<SimpleApnsPushNotification, PushNotificationResponse<SimpleApnsPushNotification>> notifFuture) throws Exception {
                // When using a listener, callers should check for a failure to send a
                // notification by checking whether the future itself was successful
                // since an exception will not be thrown.
                if (notifFuture.isSuccess()) {
                    final PushNotificationResponse<SimpleApnsPushNotification> pushNotificationResponse =
                            future.get();

                    // Handle the push notification response as before from here.
                    if (pushNotificationResponse.isAccepted()) {

                        logger.debug("Push notification accepted by APNs gateway. token: "+tokenStr);

                    } else {
                        if (pushNotificationResponse.getRejectionReason() != null && pushNotificationResponse.getRejectionReason().equals("Unregistered")) {
                            //token has bounced
                            PushController.bouncedTokensCounter.labels(AirlockManager.getAirlock().getProduct(),ConfigurationManager.getEnvVar()).inc();
                            queuePublisher.publishMessage(tokenStr);
                        } else {
                            logger.warn("Notification for token "+ tokenStr +" was rejected by the APNs gateway: " +
                                    pushNotificationResponse.getRejectionReason());

                            if (pushNotificationResponse.getTokenInvalidationTimestamp() != null) {
                                logger.warn("\t…and the token is invalid as of " +
                                        pushNotificationResponse.getTokenInvalidationTimestamp());
                            }
                            PushController.pushErrorsCounter.labels("not accepted",pushNotificationResponse.getRejectionReason(),"ios",ConfigurationManager.getEnvVar()).inc();
                        }
                    }
                } else {
                    // Something went wrong when trying to send the notification to the
                    // APNs gateway. We can find the exception that caused the failure
                    // by getting future.cause().
                    logger.error("failed in sending notification for token: "+tokenStr+". "+notifFuture.cause().getMessage());
                    PushController.pushErrorsCounter.labels("error in sending","","ios",ConfigurationManager.getEnvVar()).inc();
                }
            }
        });

    }

    public List<String> requestExpiredTokens2() {
        String cert = configurationManager.getApnsCert();
        byte[] bytes = toByteArray(cert);
        List<String> tokens = new ArrayList<>();
        InputStream targetStream = new ByteArrayInputStream(bytes);
        ApnsService service =
                APNS.newService()
                        .withCert(targetStream, configurationManager.getApnsCertPass())
                        .withProductionDestination()
                        .build();
        Map<String, Date> inactiveDevices = service.getInactiveDevices();
        for (String deviceToken : inactiveDevices.keySet()) {
            Date inactiveAsOf = inactiveDevices.get(deviceToken);
            tokens.add(deviceToken);
        }
        logger.info("expiredToken:"+tokens.toString());
        return tokens;
    }


    public List<String> requestExpiredTokens() throws CommunicationException, KeystoreException {
        logger.debug("requestExpiredTokens() called");
        List<Device> devices= feedbackManager.getDevices(server);
        logger.info("retrieved "+devices.size()+" devices");
        List<String> tokens = new ArrayList<>();
        for (Device device : devices) {
            tokens.add(device.getToken());
        }
        return tokens;
    }

    @Override
    public boolean isHealthy() {
        if (!isInitialized) {
            return true;
        }
        if (apnsClient == null) {
            healthMessage = "Error getting APNS client instance";
            return false;
        }
        healthMessage = HEALTY;
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

    class APNSPushTask implements Runnable {

        private String tokenStr;
        public APNSPushTask(String token) {
            this.tokenStr=token;
        }

        @Override
        public void run() {
            logger.debug("sending push to apple. token:"+tokenStr);
            final String appId = configurationManager.getApnsProductId();
            final ApnsPayloadBuilder payloadBuilder = new ApnsPayloadBuilder();
            payloadBuilder.setContentAvailable(true);
            final String payload = payloadBuilder.buildWithDefaultMaximumLength();
            final String token = TokenUtil.sanitizeTokenString(tokenStr);
            SimpleApnsPushNotification pushNotification = new SimpleApnsPushNotification(token, appId, payload, null, DeliveryPriority.CONSERVE_POWER, PushType.BACKGROUND);
            PushNotificationFuture<SimpleApnsPushNotification, PushNotificationResponse<SimpleApnsPushNotification>> future = apnsClient.sendNotification(pushNotification);
            future.addListener(new PushNotificationResponseListener<SimpleApnsPushNotification>() {

                @Override
                public void operationComplete(final PushNotificationFuture<SimpleApnsPushNotification, PushNotificationResponse<SimpleApnsPushNotification>> notifFuture) throws Exception {
                    // When using a listener, callers should check for a failure to send a
                    // notification by checking whether the future itself was successful
                    // since an exception will not be thrown.
                    if (notifFuture.isSuccess()) {
                        final PushNotificationResponse<SimpleApnsPushNotification> pushNotificationResponse =
                                future.getNow();

                        // Handle the push notification response as before from here.
                        if (pushNotificationResponse.isAccepted()) {

                            logger.debug("Push notification accepted by APNs gateway. token: "+tokenStr);

                        } else {
                            if (pushNotificationResponse.getRejectionReason() != null && pushNotificationResponse.getRejectionReason().equals("Unregistered")) {
                                //token has bounced
                                logger.info("token was rejected due to bounce:"+tokenStr);
                                PushController.bouncedTokensCounter.labels(AirlockManager.getAirlock().getProduct(),ConfigurationManager.getEnvVar()).inc();
                                queuePublisher.publishMessage("["+tokenStr+"]");
                            } else {
                                logger.warn("Notification for token "+ tokenStr +" was rejected by the APNs gateway: " +
                                        pushNotificationResponse.getRejectionReason());

                                if (pushNotificationResponse.getTokenInvalidationTimestamp() != null) {
                                    logger.warn("\t…and the token is invalid as of " +
                                            pushNotificationResponse.getTokenInvalidationTimestamp());
                                }
                                PushController.pushErrorsCounter.labels("not accepted",pushNotificationResponse.getRejectionReason(),"ios",ConfigurationManager.getEnvVar()).inc();
                            }
                        }
                    } else {
                        // Something went wrong when trying to send the notification to the
                        // APNs gateway. We can find the exception that caused the failure
                        // by getting future.cause().
                        logger.error("failed in sending notification for token: "+tokenStr+". "+notifFuture.cause().getMessage());
                    }
                }
            });
        }
    }
}

