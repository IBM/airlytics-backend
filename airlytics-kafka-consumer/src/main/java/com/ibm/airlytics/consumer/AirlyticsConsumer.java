package com.ibm.airlytics.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Summary;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.*;


public abstract class AirlyticsConsumer implements AutoCloseable {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AirlyticsConsumer.class.getName());

    protected Consumer<String, JsonNode> consumer;
    protected Producer errorsProducer;
    private ConsumerRecords<String, JsonNode> currentRecords;
    private boolean running = true;
    private String consumerType = "null";

    static final Summary batchSizeSummary = Summary.build()
            .name("airlytics_consumer_batch_size")
            .help("Kafka batches processed by the consumer and the number of records in them")
            .labelNames(AirlyticsConsumerConstants.CONSUMER, AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    public void stop() {
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

    public AirlyticsConsumer() {

    }

    public AirlyticsConsumer(AirlyticsConsumerConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");

        if (config.getSecurityProtocol() != null && !config.getSecurityProtocol().equalsIgnoreCase("NONE")) {
            props.put("security.protocol", config.getSecurityProtocol());
        }
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset","earliest");
        props.put("max.poll.records", config.getMaxPollRecords());
        props.put("max.poll.interval.ms", config.getMaxPollIntervalMs()); //is the interval between polls is greater - the consumer is consider non-responsive. (this is another consumer keep alive)
        props.put("request.timeout.ms", config.getMaxPollIntervalMs()+1000);  //should be greater than max.poll.interval.ms
        props.put("fetch.min.bytes", config.getFetchMinBytes());
        props.put("fetch.max.wait.ms", config.getFetchMaxWaitMs());
        props.put("session.timeout.ms", config.getSessionTimeoutMS());

        // Create the consumer using props, and subscribe to the topic
        this.consumer = new KafkaConsumer<>(props);

        if (supportsMultipleTopics() == false && config.getTopics().size() > 1)
            throw new IllegalArgumentException("Consumer configuration is set to multiple topics, but the consumer only supports one.\n" +
                    "(In order to support multiple topics in a consumer you must override supportsMultipleTopics()");

        consumer.subscribe(config.getTopics());

        if (config.getErrorsTopic()!=null) {
            errorsProducer = new Producer(
                    config.getErrorsTopic(),
                    config.getBootstrapServers(),
                    config.getSecurityProtocol(),
                    config.getProducerCompressionType(),
                    config.getLingerMs());
        }
    }

    protected boolean supportsMultipleTopics() {
        return false;
    }

    protected int getNumberOfPartitions(String topic) {
        return consumer.partitionsFor(topic).size();
    }

    public void runConsumer() {
        try {
            while (running) {
                currentRecords = consumer.poll(Duration.ofSeconds(1));

                batchSizeSummary.labels(getConsumerType(), AirlockManager.getEnvVar(), AirlockManager.getProduct())
                        .observe(currentRecords.count());

                processRecords(currentRecords);
            }
            currentRecords = null;
        }

        catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
        finally {
            System.out.println("Closing consumer");
            consumer.close();
        }

        System.out.println("DONE");
    }

    protected abstract int processRecords(ConsumerRecords<String, JsonNode> records);

    protected void commit() {
        if(errorsProducer != null) {
            errorsProducer.flush();
        }
        consumer.commitSync();
    }

    protected void rollback() {
        if(errorsProducer != null) {
            errorsProducer.flush();
        }

        for (TopicPartition partition : currentRecords.partitions()) {
            Iterator<ConsumerRecord<String, JsonNode>> iterator = currentRecords.records(partition).iterator();
            if (iterator.hasNext())
                consumer.seek(partition, iterator.next().offset());
        }
    }

    protected Consumer<String, JsonNode> getKafkaConsumer() { return consumer; }

    @Override
    public void close() throws Exception {
        if(errorsProducer != null) {
            errorsProducer.close();
        }
    }

    public boolean isHealthy() { return true; }
    public String getHealthMessage() { return "Healthy!"; }

    public abstract void newConfigurationAvailable();

    public String getConsumerType() { return consumerType; }
    public void setConsumerType(String consumerType) { this.consumerType = consumerType; }
}
