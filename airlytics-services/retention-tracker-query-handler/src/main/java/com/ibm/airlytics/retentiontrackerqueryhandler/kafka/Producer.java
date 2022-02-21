package com.ibm.airlytics.retentiontrackerqueryhandler.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.Hashing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {

    //TODO: add separate configuration?

    private org.apache.kafka.clients.producer.Producer<String, JsonNode> producer;

    public String getTopic() {
        return topic;
    }

    private String topic;
    private Hashing hashing;

    public Producer(String topic, String bootstrapServers, String securityProtocol) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
//        props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        if (securityProtocol != null && !securityProtocol.equalsIgnoreCase("NONE")) {
            props.put("security.protocol", securityProtocol);
        }

        this.producer = new KafkaProducer<String, JsonNode>(props);
        this.topic = topic;
        this.hashing = new Hashing(producer.partitionsFor(topic).size());
    }

    //TODO: send sync/async?
    public Future<RecordMetadata> sendRecord(String key, JsonNode record) {
        int partition = hashing.hashMurmur2(key);
        return producer.send(new ProducerRecord<>(topic, partition, key, record));
    }
}
