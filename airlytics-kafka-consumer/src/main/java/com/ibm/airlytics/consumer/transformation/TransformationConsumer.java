package com.ibm.airlytics.consumer.transformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.transformation.transformations.PurchaseAttributesTransformation;
import com.ibm.airlytics.consumer.transformation.transformations.Transformation;
import com.ibm.airlytics.producer.Producer;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TransformationConsumer extends AirlyticsConsumer {

    private static HashMap<String, Transformation> transformersMap;

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_transformation_records_processed_total")
            .help("Total records processed by the transformation consumer.")
            .labelNames("result", "topic", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    //Number of write trials. Includes both fail and success writes.
    static final Counter transformedRecordCounter = Counter.build()
            .name("airlytics_transformation_records_transformed_total")
            .help("Total records transformed by the transformation consumer.")
            .labelNames("topic", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static {
        transformersMap = new HashMap<>();
        transformersMap.put(PurchaseAttributesTransformation.transformationId(), new PurchaseAttributesTransformation());
    }

    private static final Logger LOGGER = Logger.getLogger(TransformationConsumer.class.getName());

    private HashMap<String, Producer> producers;
    private TransformationConsumerConfig config;
    private TransformationConsumerConfig newConfig = null;
    final private ObjectMapper mapper = new ObjectMapper();

    public TransformationConsumer(TransformationConsumerConfig config) {
        super(config);
        this.config = config;
        this.producers = new HashMap<>();
        this.updateProducers();
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {

        if (records.isEmpty()) {
            return 0;
        }

        updateToNewConfigIfExists();

        int recordsProcessed = 0;
        Map<String, Future<RecordMetadata>> futures = new HashMap<>();

        for (ConsumerRecord<String, JsonNode> record : records) {

            String result = "success";
            try {
                TransformedUserEvent event = new TransformedUserEvent(record);

                for (TransformationConfig transformationConfig : config.getTransformations()) {

                    if (record.partition() >= transformationConfig.getPercentageOfEvents()) {
                        continue;
                    }

                    Transformation transformation = TransformationConsumer.transformersMap.get(transformationConfig.getTransformationId());
                    if (transformation != null) {

                        Optional<JsonNode> transformationResult = transformation.transform(event, mapper);

                        if (transformationResult.isPresent()) {
                            for (String destinationTopic : transformationConfig.getDestinationTopics()) {
                                Producer producer = this.producers.get(destinationTopic);
                                if (producer != null) {
                                    futures.put(destinationTopic, producer.sendRecord(record.partition(), record.key(), transformationResult.get(), record.timestamp()));
                                    transformedRecordCounter.labels(record.topic(), AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                String eventId = (record.value().get("eventId") == null ? "unknown" : record.value().get("eventId").asText());
                errorsProducer.sendRecord(record.partition(), record.key(), record.value(), record.timestamp());
                LOGGER.error("Error happened while event processing eventId:" + eventId, e);
                result = "error";
            } finally {
                recordsProcessedCounter.labels(result, record.topic(), AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                ++recordsProcessed;
            }
        }

        for (Map.Entry<String, Future<RecordMetadata>> futureEntry : futures.entrySet()) {
            try {
                futureEntry.getValue().get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Exception in the thread sending events to Kafka topic: " + futureEntry.getKey(), e);
                stop();
                return 0;
            }
        }

        commit();
        return recordsProcessed;
    }

    @Override
    protected void commit() {
        for (Producer producer : this.producers.values()) {
            producer.flush();
        }
        super.commit();
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        TransformationConsumerConfig config = new TransformationConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error happened while updating airlock config");
        }
    }

    private synchronized void updateToNewConfigIfExists() {
        if (newConfig != null) {
            config = newConfig;
            newConfig = null;
            updateProducers();
        }
    }

    @Override
    public void close() {

    }

    private void updateProducers() {
        for (TransformationConfig tc : config.getTransformations()) {
            for (String topic : tc.getDestinationTopics()) {
                if (producers.get(topic) == null) {
                    producers.put(topic, new Producer(topic,
                            config.getBootstrapServers(),
                            config.getSecurityProtocol(),
                            config.getProducerCompressionType(),
                            config.getLingerMs()));
                }
            }
        }
    }
}
