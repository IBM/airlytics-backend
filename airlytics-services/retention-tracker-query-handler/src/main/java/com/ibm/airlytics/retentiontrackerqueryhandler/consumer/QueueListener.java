package com.ibm.airlytics.retentiontrackerqueryhandler.consumer;

import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.inactivation.InactivationController;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.consumer.RabbitMQueueListener;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.push.DeviceType;
import com.ibm.airlytics.retentiontracker.push.PushProtocol;
import com.ibm.airlytics.retentiontracker.exception.TrackerInitializationException;
import com.rabbitmq.client.*;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;

@Component
@DependsOn({"ConfigurationManager","Airlock"})
public class QueueListener implements HealthCheckable {

    private final String bouncedQueueName;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(QueueListener.class.getName());
    private RabbitMQueueListener bouncedConsumer;
    private Channel channelBounced;
    private Connection conn;
    private final ConfigurationManager configurationManager;
    private final DbHandler dbHandler;
    private final InactivationController inactivationController;
    private String healthMessage = HEALTY;

    public QueueListener(ConfigurationManager configurationManager, DbHandler dbHandler, Environment env, InactivationController inactivationController) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super();
        this.configurationManager = configurationManager;
        bouncedQueueName = configurationManager.getListenerQueueName();
        this.dbHandler = dbHandler;
        this.inactivationController = inactivationController;
    }

    private ShutdownListener bouncedShutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            logger.warn("SHUTDOWN!!!!"+cause.getMessage());
            try {
                channelBounced.basicRecover();
            } catch (IOException e) {
                logger.error("failure in recovering connection to rabbitMQ:"+e.getMessage());
            }
        }
    };
    private void connectToRabbitMQ() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        logger.info("QueueListener:Connecting To RabbitMQ");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setRequestedHeartbeat(5); // keeps an idle connection alive
        String uri = configurationManager.getRabbitmqUri();
        factory.setUri(uri);

        conn = factory.newConnection();
        channelBounced = conn.createChannel();
        channelBounced.basicQos(1);
        channelBounced.addShutdownListener(bouncedShutdownListener);

        channelBounced.queueDeclareNoWait(bouncedQueueName, true, false, false, null);
        logger.debug("declaring "+bouncedQueueName);
        bouncedConsumer = new BouncedQueueListener(channelBounced, dbHandler, inactivationController);
        bouncedConsumer.setQueueName(bouncedQueueName);
        bouncedConsumer.setType(DeviceType.BOUNCED);
    }

    @PreDestroy
    public void onDestroy() throws Exception {
        disconnectFromRabbitMQ();
    }

    private void disconnectFromRabbitMQ() {
        if (channelBounced != null) {
            try {
                channelBounced.removeShutdownListener(bouncedShutdownListener);
                channelBounced.close();
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException | TimeoutException e) {
                logger.debug("errors disconnecting from rabbitMQ."+e.getMessage());
            }
        }
    }


    public void registerForWork(PushProtocol pushController) throws IOException, TrackerInitializationException {
        logger.info("registerForWork");
        try {
            connectToRabbitMQ();
        } catch (TimeoutException | NoSuchAlgorithmException | KeyManagementException | URISyntaxException e) {
            logger.error("error connecting to rabbitMQ:"+e.getMessage());
            throw new TrackerInitializationException(e.getMessage());
        }
        bouncedConsumer.registerForMessages(pushController);
    }

    @Override
    public boolean isHealthy() {
        if (conn != null && !conn.isOpen()) {
            healthMessage = "RabbitMQ connection is closed";
            return false;
        }
        if (channelBounced != null && !channelBounced.isOpen()) {
            healthMessage = "RabbitMQ Channel for listening to bounced tokens is closed";
            return false;
        }
        healthMessage = HEALTY;
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }
}
