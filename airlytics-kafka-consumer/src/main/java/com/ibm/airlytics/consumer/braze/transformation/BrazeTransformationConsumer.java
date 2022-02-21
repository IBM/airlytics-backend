package com.ibm.airlytics.consumer.braze.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.braze.dto.BrazeEntity;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class BrazeTransformationConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeTransformationConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_braze_transform_records_processed_total")
            .help("Total records processed by the braze transformation consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    final private ObjectMapper mapper = new ObjectMapper();

    private BrazeTransformationConsumerConfig config;

    private BrazeTransformationConsumerConfig newConfig;

    private Producer brazeEventsProducer;

    private BrazeEventTransformer eventTransformer;

    public BrazeTransformationConsumer() {}

    public BrazeTransformationConsumer(BrazeTransformationConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        LOGGER.info("Braze transformation consumer created with configuration: " + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if (consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();

        Map<BrazeEntity, ConsumerRecord<String, JsonNode>> brazeEvents = new LinkedHashMap<>();
        List<Future<RecordMetadata>> futures = new LinkedList<>();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            Optional<BrazeEntity> optEvent = processRecord(record);

            if( !isRunning() ) {
                break;
            }
            optEvent.ifPresent(e -> brazeEvents.put(e, record));
        }

        if( isRunning() ) {
            brazeEvents.forEach((e, record) -> {
                // ignore empty user attribute updates
                if(e.getKind() != BrazeEntity.Kind.USER || MapUtils.isNotEmpty(e.getProperties())) {
                    futures.add(submitTransformed(e, record));
                }
            });

            for(Future<RecordMetadata> future : futures) {

                try {
                    if( isRunning() ) {
                        future.get();
                    } else {
                        future.cancel(true);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Exception in the thread sending events to Kafka topic " + config.getDestinationTopic(), e);
                    stop();
                }
            }
        }

        if( !isRunning() ) {
            recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(brazeEvents.size());
            return 0;
        }
        commit();

        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(brazeEvents.size());

        return consumerRecords.count();
    }

    @Override
    protected void commit() {
        brazeEventsProducer.flush();
        consumer.commitSync();
    }

    @Override
    public void close() throws Exception {
        this.brazeEventsProducer.close();
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        BrazeTransformationConsumerConfig latest = new BrazeTransformationConsumerConfig();

        try {
            latest.initWithAirlock();

            if(!latest.equals(this.config) ||
                    !latest.getProducerCompressionType().equals(this.config.getProducerCompressionType()) ||
                    latest.getLingerMs() != this.config.getLingerMs()
            ) {
                this.newConfig = latest;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Braze Transformation Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (this.newConfig != null) {

            try {
                setConfig(this.newConfig);
                this.newConfig = null;
                // re-init
                init();
                LOGGER.info("Braze transformation consumer updated with configuration: " + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping braze transformation consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(BrazeTransformationConsumerConfig config) {
        this.config = config;
    }

    void init() {

        if(this.brazeEventsProducer != null) {
            this.brazeEventsProducer.close();
        }
        this.brazeEventsProducer = new Producer(
                this.config.getDestinationTopic(),
                this.config.getBootstrapServers(),
                this.config.getSecurityProtocol(),
                this.config.getProducerCompressionType(),
                this.config.getLingerMs());

        this.eventTransformer = new BrazeEventTransformer(this.config);
    }

    private Optional<BrazeEntity> processRecord(ConsumerRecord<String, JsonNode> record) {
        JsonNode eventJson = record.value();
        try {
            AirlyticsEvent event = mapper.treeToValue(eventJson, AirlyticsEvent.class);
            return eventTransformer.transform(event);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing Localytics event " + eventJson.toString(), e);
            stop();
        }
        return Optional.empty();
    }

    private Future<RecordMetadata> submitTransformed(BrazeEntity event, ConsumerRecord<String, JsonNode> originalRecord) {
        JsonNode json = mapper.valueToTree(event.toMap());

        return brazeEventsProducer.sendRecord(
                originalRecord.partition(),
                originalRecord.key(),
                json,
                originalRecord.timestamp());
    }
}
