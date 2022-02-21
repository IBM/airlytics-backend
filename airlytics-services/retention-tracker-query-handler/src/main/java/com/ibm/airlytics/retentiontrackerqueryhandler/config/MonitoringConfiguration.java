package com.ibm.airlytics.retentiontrackerqueryhandler.config;

import com.ibm.airlytics.retentiontracker.config.MonitoringServer;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontrackerqueryhandler.consumer.QueueListener;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableScheduling
public class MonitoringConfiguration extends MonitoringServer {

    private final List<HealthCheckable> healthComponents;

    private final Environment env;

    public MonitoringConfiguration(QueuePublisher queuePublisher, QueueListener queueListener, DbHandler dbHandler, Environment environment) {

        healthComponents = new ArrayList<HealthCheckable>();
        healthComponents.add(queuePublisher);
        healthComponents.add(queueListener);
        healthComponents.add(dbHandler);
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