package com.ibm.airlytics.retentiontrackerpushhandler.consumer;

import com.ibm.airlytics.retentiontrackerpushhandler.utils.ConfigurationManager;
import com.ibm.airlytics.retentiontracker.consumer.RabbitMQueueListener;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.push.DeviceType;
import com.ibm.airlytics.retentiontrackerpushhandler.push.PushController;
import com.rabbitmq.client.*;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;

@Component
@DependsOn("ConfigurationManager")
public class QueueListener implements HealthCheckable {

    private final String queueName;
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(QueueListener.class.getName());
    private RabbitMQueueListener consumer;
    private Channel channel;
    private Connection conn;
    private final ConfigurationManager configurationManager;

    private String healthMessage = HEALTY;

    public QueueListener(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        queueName = configurationManager.getListenerQueueName();
    }

    public void registerForWork() throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        connectToRabbitMQ();
    }
    private ShutdownListener shutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            logger.error("SHUTDOWN!!!!"+cause.getMessage());
            try {
                boolean isOpen = conn.isOpen();
                channel.basicRecover();
            } catch (IOException e) {
                logger.error("failure in recovering connection to rabbitMQ:"+e.getMessage());
            }
        }
    };

    private void connectToRabbitMQ() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        logger.debug("Connecting To RabbitMQ");
        ConnectionFactory factory = new ConnectionFactory();
        String uri = configurationManager.getRabbitmqUri();
        factory.setUri(uri);

        conn = factory.newConnection();
        channel = conn.createChannel();

        channel.addShutdownListener(shutdownListener);

        channel.queueDeclareNoWait(queueName, true, false, false, null);
        logger.debug("declaring "+ queueName);
        consumer = new PushQueueListener(channel);
        consumer.setQueueName(queueName);
        consumer.setType(DeviceType.APPLICATION);
    }

    @PreDestroy
    public void onDestroy() throws Exception {
//        disconnectFromRabbitMQ();
    }

    private void disconnectFromRabbitMQ() {

        try {
            channel.removeShutdownListener(shutdownListener);
            channel.close();
            conn.close();
        } catch (IOException | TimeoutException e) {
            logger.error("error disconnecting from rabbitMQ:"+e.getMessage());
        }
    }

    public void waitForWork() throws IOException {
        logger.info("waitForWork");
        boolean didGet = consumer.pullMessage();

    }

    public void registerForWork(PushController pushController) throws IOException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, URISyntaxException {
        logger.info("registerForWork");
        this.registerForWork();
        consumer.registerForMessages(pushController);
    }

    public boolean isHealthy() {
        if (conn != null && !conn.isOpen()) {
            healthMessage = "RabbitMQ connection is closed";
            return false;
        }
        if (channel != null && !channel.isOpen()) {
            healthMessage = "RabbitMQ Channel for pushing notifications is closed";
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
