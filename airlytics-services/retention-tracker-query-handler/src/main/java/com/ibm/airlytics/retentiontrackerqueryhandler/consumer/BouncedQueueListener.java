package com.ibm.airlytics.retentiontrackerqueryhandler.consumer;

import com.ibm.airlytics.retentiontrackerqueryhandler.db.DbHandler;
import com.ibm.airlytics.retentiontrackerqueryhandler.inactivation.InactivationController;
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

public class BouncedQueueListener extends RabbitMQueueListener {
    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(BouncedQueueListener.class.getName());
    private final DbHandler dbHandler;
    private final InactivationController inactivationController;
    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     * @param inactivationController
     */
    public BouncedQueueListener(Channel channel, DbHandler dbHandler, InactivationController inactivationController) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super(channel);
        this.autoAck = false;
        this.dbHandler = dbHandler;
        this.inactivationController = inactivationController;
    }

    @Override
    protected void handleMessage(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        logger.info("message received by push from queue "+queueName);
        String routingKey = envelope.getRoutingKey();
        String contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();
        // (process the message components here ...)
        String tokensStr = new String(body);
        List<String> tokens = getStringsArray(tokensStr);
        logger.info("got message:"+tokensStr);
        inactivationController.startUserInactivation(tokens);
        logger.info("message received to BouncedQueueListener by push");
    }
}
