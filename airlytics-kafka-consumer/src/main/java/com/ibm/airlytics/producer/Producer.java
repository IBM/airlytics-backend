package com.ibm.airlytics.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.utilities.Hashing;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.concurrent.Future;

public class Producer implements AutoCloseable {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(Producer.class.getName());

    //TODO: add separate configuration?

    private final org.apache.kafka.clients.producer.Producer<String, JsonNode> producer;
    private final String topic;
    private final Hashing hashing;

    public Producer(String topic, String bootstrapServers, String securityProtocol, @Nullable String compressionType, int lingerMs) {
        this(topic, bootstrapServers, securityProtocol, compressionType, lingerMs, 0);
    }

    public Producer(String topic, String bootstrapServers, String securityProtocol, @Nullable String compressionType, int lingerMs, int batchSize) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

        // Valid values are "none", "gzip", "snappy", "lz4", or "zstd".
        // If you need compression and don't have a preference, use snappy for speed + compatibility.
        if(compressionType !=null){
            props.put("compression.type", compressionType);
        }

        if (!"none".equals(compressionType)) {
            LOGGER.info("Kafka producer created with compression " + compressionType);
        }

        if (lingerMs > 0) {
            props.put("linger.ms", lingerMs);
        }

        if(batchSize > 0) {
            props.put("batch.size", batchSize);
        }

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

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.hashing = new Hashing(producer.partitionsFor(topic).size());
    }

    /**
     * @deprecated we use userId for partitioning and eventId for Kafka record key.
     */
    @Deprecated
    public Future<RecordMetadata> sendRecord(String key, JsonNode record, Long receivedTimestamp) {
        return sendRecord(key, key, record, receivedTimestamp);
    }

    public Future<RecordMetadata> sendRecord(String idForPartitioning, String recordKey, JsonNode record, Long receivedTimestamp) {
        int partition = hashing.hashMurmur2(idForPartitioning);
        return sendRecord(partition, recordKey, record, receivedTimestamp);
    }

    public Future<RecordMetadata> sendRecord(int partition, String recordKey, JsonNode record, Long receivedTimestamp) {
        return producer.send(new ProducerRecord<>(topic, partition, receivedTimestamp, recordKey, record));
    }

    public void close() {
        flush();
        producer.close();
    }

    public void flush() {
        producer.flush();
    }
}
