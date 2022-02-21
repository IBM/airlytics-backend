package com.ibm.airlytics.retentiontrackerqueryhandler.publisher;

import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class QueryQueuePublisher extends QueuePublisher {

    public QueryQueuePublisher(ConfigurationManager configurationManager) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super(configurationManager);
    }
}
