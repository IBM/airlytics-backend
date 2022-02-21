package com.ibm.weather.airlytics.common.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private String bootstrapServers;
    private String securityProtocol;
    private String topic;
    private AtomicInteger errorCounter = new AtomicInteger(0);

    private org.apache.kafka.clients.producer.Producer<String, String> producer;
    private Hashing hashing;

    public KafkaProducer(String topic, String bootstrapServers, String securityProtocol) {
        this(topic, bootstrapServers, securityProtocol, null);
    }

    public KafkaProducer(String topic, String bootstrapServers, String securityProtocol, Map<String, Object> additionalProperties) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;

        Properties props = createProducerProperties(bootstrapServers, securityProtocol);

        if(additionalProperties != null) {
            props.putAll(additionalProperties);
        }

        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        this.hashing = new Hashing(producer.partitionsFor(topic).size());
    }

    public void resetErrorCounter() {
        errorCounter.set(0);
    }

    public int getErrorsCount() {
        return errorCounter.get();
    }

    public Future<RecordMetadata> sendRecord(String idForPartitioning, String recordKey, String record) {
        int partition = hashing.hashMurmur2(idForPartitioning);
        Future<RecordMetadata> future =
                producer.send(
                        new ProducerRecord<>(topic, partition, System.currentTimeMillis(), recordKey, record),
                        ((metadata, exception) -> {

                            if(exception != null) {
                                int cnt = errorCounter.incrementAndGet();
                                logger.error(cnt + " errors sending to Kafka: " + record, exception);
                            }
                        }));
        return future;
    }

    public void close() {
        flush();
        producer.close();
    }

    public void flush() {
        producer.flush();
    }

    public boolean isChanged(String configTopic, String configBootstrapServers, String configSecurityProtocol) {
        if (bootstrapServers != null ? !bootstrapServers.equals(configBootstrapServers) : configBootstrapServers != null)
            return false;
        if (securityProtocol != null ? !securityProtocol.equals(configSecurityProtocol) : configSecurityProtocol != null)
            return false;
        return topic != null ? topic.equals(configTopic) : configTopic == null;
    }

    private Properties createProducerProperties(String bootstrapServers, String securityProtocol) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());

        if (securityProtocol != null && !securityProtocol.equalsIgnoreCase("NONE")) {
            props.put("security.protocol", securityProtocol);
        }
        // if you set retry > 0, then you should also set max.in.flight.requests.per.connection to 1,
        // or there is the possibility that a re-tried message could be delivered out of order.
        // - max.in.flight.requests.per.connection (default 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        //Set the number of retries - retries
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        //Request timeout - request.timeout.ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        //Only retry after 1 second.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        return props;
    }
}