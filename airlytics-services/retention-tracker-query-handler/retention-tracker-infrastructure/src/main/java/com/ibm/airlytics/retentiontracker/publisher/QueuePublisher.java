package com.ibm.airlytics.retentiontracker.publisher;

import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.utils.BaseConfigurationManager;
import com.ibm.airlytics.retentiontracker.exception.TrackerInitializationException;
import com.rabbitmq.client.*;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class QueuePublisher implements HealthCheckable {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(QueuePublisher.class.getName());
    private Channel channel;
    private Connection conn;
    private final String queueName;
    private final String exchangeName;
    private final BaseConfigurationManager configurationManager;

    private String healthMessage = Constants.HEALTY;

    private ShutdownListener shutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            logger.warn("SHUTDOWN!!!! "+cause.getMessage());
            try {
                channel.basicRecover();
            } catch (IOException e) {
                logger.error("failure in recovering connection to rabbitMQ:"+e.getMessage());
            }
        }
    };
    public QueuePublisher(BaseConfigurationManager configurationManager) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super();
        this.configurationManager = configurationManager;
        queueName = configurationManager.getPublisherQueueName();
        exchangeName = configurationManager.getExchangeName();
//        connectToRabbitMQ();
    }

    public void connectToRabbitMQ() throws TrackerInitializationException {
        logger.info("QueuePublisher:connectToRabbitMQ");
        try {
            ConnectionFactory factory = new ConnectionFactory();
            String uri = configurationManager.getRabbitmqUri();
            factory.setUri(uri);

            conn = factory.newConnection();
            channel = conn.createChannel();
            channel.addShutdownListener(shutdownListener);

            channel.queueDeclare(queueName, true, false, false, null);
            channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT);
            channel.queueBind(queueName, exchangeName,queueName, null);
        } catch (IOException | NoSuchAlgorithmException | URISyntaxException | TimeoutException | KeyManagementException e) {
            throw new TrackerInitializationException(e.getMessage());
        }


    }

    public void publishMessage(String message) throws IOException {
        byte[] messageBodyBytes = message.getBytes();
        String routingKey = queueName;
        logger.debug("publishing message "+message+ " to exhange "+exchangeName+" with routingKey "+routingKey);
        channel.basicPublish(exchangeName,routingKey,null, messageBodyBytes);
    }

    @PreDestroy
    public void onDestroy() throws Exception {
        disconnectFromRabbitMQ();
    }

    private void disconnectFromRabbitMQ() {
        logger.debug("QueuePublisher:disconnectFromRabbitMQ");
        if (channel != null) {
            try {
                channel.removeShutdownListener(shutdownListener);
                channel.close();
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException | TimeoutException e) {
                logger.debug("errors disconnecting from rabbitMQ."+e.getMessage());
            }
        }
    }

    @Override
    public boolean isHealthy() {

        if (conn != null && !conn.isOpen()) {
            healthMessage = "RabbitMQ connection is closed";
            return false;
        }
        if (channel != null && !channel.isOpen()) {
            healthMessage = "RabbitMQ Channel for publishing users to push notifications to is closed";
            return false;
        }
        healthMessage = Constants.HEALTY;
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }
}
