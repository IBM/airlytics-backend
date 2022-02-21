package com.ibm.airlytics.retentiontracker.consumer;

import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.push.DeviceType;
import com.ibm.airlytics.retentiontracker.push.PushProtocol;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.ibm.airlytics.retentiontracker.Constants.RABBIT_RETRY;

public abstract class RabbitMQueueListener extends DefaultConsumer {

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(RabbitMQueueListener.class.getName());
    protected PushProtocol pushController;

    protected DeviceType type;
    protected String queueName;
    private Channel channel;
    protected boolean autoAck = true;
    /**
     * Constructs a new instance and records its association to the passed-in channel.
     *
     * @param channel the channel to which this consumer is attached
     */
    public RabbitMQueueListener(Channel channel) throws URISyntaxException, KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException {
        super(channel);
        this.channel = channel;
    }

    public void registerForMessages(PushProtocol pushController) throws IOException {
        this.setPushController(pushController);
        channel.basicConsume(queueName, autoAck, "myConsumerTag",
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body)
                            throws IOException
                    {
                        handleMessage(channel, consumerTag, envelope, properties, body);
                        if (!autoAck) {
                            performAck(envelope);
                        }
                    }
                });
    }

    protected void performAck(Envelope envelope) {
        long deliveryTag = envelope.getDeliveryTag();
        int retryCounter = 0;
        while (retryCounter < RABBIT_RETRY) {
            try {
                channel.basicAck(deliveryTag, false);
                logger.debug("message received by push - Ack");
                return;
            } catch (IOException e) {
                logger.error("failed performing ack "+e.getMessage());
                retryCounter++;
            }
        }
        logger.error("failed performing ack "+RABBIT_RETRY+ " times");
    }
    protected abstract void handleMessage(Channel channel,
                                        String consumerTag,
                                        Envelope envelope,
                                        AMQP.BasicProperties properties,
                                        byte[] body) throws IOException;

    public boolean pullMessage() throws IOException {
        boolean autoAck = false;
        GetResponse response = channel.basicGet(queueName, autoAck);
        if (response == null) {
            // No message retrieved.
            return false;
        } else {
            AMQP.BasicProperties props = response.getProps();
            byte[] body = response.getBody();
            String message = new String(body);
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            channel.basicAck(deliveryTag, false); // acknowledge receipt of the message
            logger.debug("received message by pull from queue "+queueName+". message:"+message);
            return true;
        }
    }

    protected List<String> getStringsArray(String str) {
        if (str != null && !str.isEmpty()) {
            String arrStr = str.substring(1, str.length()-1);
            String[] arr = arrStr.split("\\s*,\\s*");
            return Arrays.asList(arr);
        }
        return new ArrayList<String>();
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setType(DeviceType type) {
        this.type = type;
    }

    public void setPushController(PushProtocol pushController) {
        this.pushController = pushController;
    }
}
