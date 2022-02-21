package com.ibm.airlytics.consumer.kafkacloning;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.Environment;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.EVENT_CUSTOM_DIMENSIONS_FIELD;

public class KafkaCloningConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(KafkaCloningConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_kafkacloning_records_processed_total")
            .help("Total records processed by the Kafka cloning consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, AirlyticsConsumerConstants.SOURCE)
            .register();

    private KafkaCloningConsumerConfig config;

    private KafkaCloningConsumerConfig newConfig;

    private Producer kafkaProducer;

    private String rawDataSource;

    public KafkaCloningConsumer() {}

    public KafkaCloningConsumer(KafkaCloningConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        rawDataSource = Environment.getAlphanumericEnv(AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM, false);
        if (!"ERROR".equals(rawDataSource) && !"SUCCESS".equals(rawDataSource)  && !"NOTIFICATIONS".equals(rawDataSource)) {
            throw new IllegalArgumentException("Illegal value for " + AirlockManager.AIRLYTICS_RAWDATA_SOURCE_PARAM + " environment parameter. Can be one of the following: ERROR, SUCCESS, NOTIFICATIONS");
        }
        LOGGER.info("Kafka cloning consumer created with configuration:" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if (consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();

        final List<Future<RecordMetadata>> futures = new LinkedList<>();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {

            if( !isRunning() ) {
                break;
            }
            processRecord(record).ifPresent(r -> futures.add(submitToTarget(r)));

            if(config.getTargetKafkaMaxMessages() > 0 && (futures.size() % config.getTargetKafkaMaxMessages()) == 0) {
                kafkaProducer.flush();
            }
        }

        if( isRunning() ) {
            kafkaProducer.flush();

            for(Future<RecordMetadata> future : futures) {

                try {
                    if( isRunning() ) {
                        future.get();
                    } else {
                        future.cancel(true);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Exception in the thread sending events to Kafka topic " + config.getTargetKafkaTopic(), e);
                    stop();
                }
            }
        }

        if( !isRunning() ) {
            recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(futures.size());
            return 0;
        }
        commit();

        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(futures.size());

        return consumerRecords.count();
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        KafkaCloningConsumerConfig config = new KafkaCloningConsumerConfig();

        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Kafka Cloning Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Kafka cloning consumer updated with configuration: " + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping Kafka cloning consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    // used for unit-testing
    void setConfig(KafkaCloningConsumerConfig config) {

        if(config.getPercentageUsersCloned() < 0 || config.getPercentageUsersCloned() > 100 || config.getPercentageUsersCloned() == 0) {
            throw new IllegalArgumentException("Invalid percentageUsersCloned " + config.getPercentageUsersCloned());
        }
        this.config = config;
    }

    void init() {

        if(this.kafkaProducer != null) {
            kafkaProducer.close();
        }
        this.kafkaProducer = new Producer(
                this.config.getTargetKafkaTopic(),
                this.config.getTargetKafkaBootstrapServers(),
                this.config.getTargetKafkaSecurityProtocol(),
                this.config.getTargetKafkaCompressionType(),
                this.config.getTargetKafkaLingerMs(),
                this.config.getTargetKafkaBatchSize()
        );
    }

    private Optional<ConsumerRecord<String, JsonNode>> processRecord(ConsumerRecord<String, JsonNode> record) {

        if(isAcceptedMessage(record)) {
            JsonNode event = record.value();

            applyAttributeFilters(
                    event,
                    EVENT_ATTRIBUTES_FIELD,
                    config.getIgnoreEventAttributes(),
                    config.getIncludeEventAttributes());

            applyAttributeFilters(
                    event,
                    EVENT_CUSTOM_DIMENSIONS_FIELD,
                    config.getIgnoreCustomDimensions(),
                    config.getIncludeCustomDimensions());

            return Optional.of(record);
        }
        return Optional.empty();
    }

    private boolean isAcceptedMessage(ConsumerRecord<String, JsonNode> record) {
        JsonNode event = record.value();

        if (!event.has(USER_ID_FIELD)) {
            LOGGER.error("Ignoring illegal event without '" + USER_ID_FIELD + "': " + event.toString());
            return false;
        }

        if (!event.has(EVENT_NAME_FIELD)) {
            LOGGER.error("Ignoring illegal event without '" + EVENT_NAME_FIELD + "': " + event.toString());
            return false;
        }
        String eventName = event.get(EVENT_NAME_FIELD).textValue();

        if(config.getIgnoreEventTypes() != null && config.getIgnoreEventTypes().contains(eventName)) {
            return false;
        }

        if(config.getIncludeEventTypes() != null && !config.getIncludeEventTypes().contains(eventName)) {
            return false;
        }

        if(config.getPercentageUsersCloned() == 100) {
            return true;
        }

        if(config.getPercentageUsersCloned() > 0) {
            return record.partition() < config.getPercentageUsersCloned();
        }
        return false;
    }

    private void applyAttributeFilters(JsonNode event, String parentField, List<String> ignored, List<String> included) {
        ObjectNode eventAttributes = (ObjectNode)event.findValue(parentField);

        if ( eventAttributes != null &&
                (ignored != null || included != null) ) {
            Iterator<String> eventFieldsIter = eventAttributes.fieldNames();
            List<String> attributeNames = new LinkedList<>();

            while (eventFieldsIter.hasNext()) {
                attributeNames.add(eventFieldsIter.next());
            }

            for (String attribute : attributeNames) {

                if(ignored != null && ignored.contains(attribute)) {
                    eventAttributes.remove(attribute);
                } else if(included != null && !included.contains(attribute)) {
                    eventAttributes.remove(attribute);
                }
            }
        }
    }

    private Future<RecordMetadata> submitToTarget(ConsumerRecord<String, JsonNode> originalRecord) {

        return kafkaProducer.sendRecord(
                originalRecord.partition(),
                originalRecord.key(),
                originalRecord.value(),
                originalRecord.timestamp());
    }
}
