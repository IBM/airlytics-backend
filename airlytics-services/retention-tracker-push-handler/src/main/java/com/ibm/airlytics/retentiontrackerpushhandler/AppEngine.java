package com.ibm.airlytics.retentiontrackerpushhandler;

import com.ibm.airlytics.retentiontracker.exception.TrackerInitializationException;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerpushhandler.consumer.QueueListener;
import com.ibm.airlytics.retentiontrackerpushhandler.push.PushController;
import javapns.communication.exceptions.KeystoreException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

@Component
public class AppEngine {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(AppEngine.class.getName());
    public final static String TYPE_VARIABLE = "USERDB_PROCESS_TYPE";
    public final static String TYPE_IOS_HANDLER = "ios-handler";
    public final static String TYPE_ANDROID_HANDLER = "android-handler";
    public final static String TYPE_IOS_BOUNCE = "ios-bounce-listener";

    private final Environment env;
    @Autowired
    private QueueListener queueListener;

    @Autowired
    private PushController pushController;

    public AppEngine(Environment env) {
        this.env = env;
    }

    @PostConstruct
    public void process() throws IOException, TrackerInitializationException, URISyntaxException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, KeystoreException, InvalidKeyException {
        String appType = env.getProperty(TYPE_VARIABLE, TYPE_IOS_HANDLER);
        logger.info("appType:"+env.getProperty(TYPE_VARIABLE));
        if (appType.equals(TYPE_IOS_HANDLER) || appType.equals(TYPE_ANDROID_HANDLER)) {
            logger.info("Running as "+appType);
            pushController.initPushAdapters();
            queueListener.registerForWork(pushController);
        } else if (appType.equals(TYPE_IOS_BOUNCE)) {
            logger.info("Running as "+appType);
            pushController.listenForBouncedTokens();
        }
    }
}
