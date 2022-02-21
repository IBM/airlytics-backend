package com.ibm.airlytics.retentiontrackerpushhandler.consumer;

import com.ibm.airlytics.retentiontracker.consumer.RabbitMQueueListener;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class PushQueueListener extends RabbitMQueueListener {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(PushQueueListener.class.getName());

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public PushQueueListener(Channel channel) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super(channel);
    }

    @Override
    protected void handleMessage(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        logger.debug("message received by push from queue "+queueName);
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        // (process the message components here ...)
        String payload = new String(body);
        logger.info(payload);
        List<String> tokens = getStringsArray(payload);

        pushController.notifyUsers(tokens);

    }

}
