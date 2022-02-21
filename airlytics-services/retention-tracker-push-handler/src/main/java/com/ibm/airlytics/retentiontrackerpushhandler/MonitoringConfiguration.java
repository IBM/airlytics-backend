package com.ibm.airlytics.retentiontrackerpushhandler;

import com.ibm.airlytics.retentiontrackerpushhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerpushhandler.push.FCMPushAdapter;
import com.ibm.airlytics.retentiontracker.config.MonitoringServer;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontrackerpushhandler.consumer.QueueListener;
import com.ibm.airlytics.retentiontrackerpushhandler.push.APNSPushAdapter;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class MonitoringConfiguration extends MonitoringServer {

    private final List<HealthCheckable> healthComponents;

    private final Environment env;

    public MonitoringConfiguration(QueuePublisher queuePublisher, QueueListener queueListener, DbHandler dbHandler, APNSPushAdapter apnsPushAdapter, FCMPushAdapter fcmPushAdapter, Environment environment) {

        healthComponents = new ArrayList<HealthCheckable>();
        healthComponents.add(queuePublisher);
        healthComponents.add(queueListener);
        healthComponents.add(dbHandler);
        healthComponents.add(apnsPushAdapter);
        healthComponents.add(fcmPushAdapter);
        this.env = environment;
    }

    @Override
    protected List<HealthCheckable> getHealthComponents() {
        return healthComponents;
    }

    @PostConstruct
    public void registerCollectors() throws ServletException {
        int port = env.getProperty("MONITORING_PORT", Integer.class, 8084);
        super.startServer(port);
    }
}