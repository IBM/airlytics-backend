package com.ibm.airlytics.retentiontrackerpushhandler.publisher;

import com.ibm.airlytics.retentiontracker.exception.TrackerInitializationException;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

@Component
public class PushQueuePublisher extends QueuePublisher {
    public PushQueuePublisher(ConfigurationManager configurationManager) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException, TrackerInitializationException {
        super(configurationManager);
        connectToRabbitMQ();
    }
}
