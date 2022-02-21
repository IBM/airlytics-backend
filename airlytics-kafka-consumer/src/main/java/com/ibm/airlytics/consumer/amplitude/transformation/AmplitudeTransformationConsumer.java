package com.ibm.airlytics.consumer.amplitude.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class AmplitudeTransformationConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AmplitudeTransformationConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_amplitude_transform_records_processed_total")
            .help("Total records processed by the amplitude transformation consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    final private ObjectMapper mapper = new ObjectMapper();

    private AmplitudeTransformationConsumerConfig config;

    private AmplitudeTransformationConsumerConfig newConfig;

    private Producer amplitudeEventsProducer;

    private AmplitudeEventTransformer eventTransformer;

    public AmplitudeTransformationConsumer() {}

    public AmplitudeTransformationConsumer(AmplitudeTransformationConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        LOGGER.info("Amplitude transformation consumer created with configuration:\n" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if (consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();

        Map<AmplitudeEvent, ConsumerRecord<String, JsonNode>> amplitudeEvents = new LinkedHashMap<>();
        List<Future<RecordMetadata>> futures = new LinkedList<>();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            Optional<AmplitudeEvent> optEvent = processRecord(record);

            if( !isRunning() ) {
                break;
            }
            optEvent.ifPresent(e -> amplitudeEvents.put(e, record));
        }

        if( isRunning() ) {

            amplitudeEvents.forEach((e, record) -> {
                futures.add(submitTransformed(e, record));
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
            recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(amplitudeEvents.size());
            return 0;
        }
        commit();

        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(amplitudeEvents.size());

        return consumerRecords.count();
    }

    @Override
    protected void commit() {
        amplitudeEventsProducer.flush();
        consumer.commitSync();
    }

    @Override
    public void close() throws Exception {
        this.amplitudeEventsProducer.close();
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        AmplitudeTransformationConsumerConfig latest = new AmplitudeTransformationConsumerConfig();

        try {
            latest.initWithAirlock();

            if(!latest.equals(this.config) ||
                    !latest.getProducerCompressionType().equals(this.config.getProducerCompressionType()) ||
                    latest.getLingerMs() != this.config.getLingerMs()
            ) {
                this.newConfig = latest;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Amplitude Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (this.newConfig != null) {

            try {
                setConfig(this.newConfig);
                this.newConfig = null;
                // re-init
                init();
                LOGGER.info("Amplitude transformation consumer updated with configuration:\n" + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping amplitude transformation consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(AmplitudeTransformationConsumerConfig config) {
        this.config = config;
    }

    void init() {

        if(this.amplitudeEventsProducer != null) {
            this.amplitudeEventsProducer.close();
        }
        this.amplitudeEventsProducer = new Producer(
                this.config.getDestinationTopic(),
                this.config.getBootstrapServers(),
                this.config.getSecurityProtocol(),
                this.config.getProducerCompressionType(),
                this.config.getLingerMs());

        this.eventTransformer = new AmplitudeEventTransformer(this.config);
    }

    private Optional<AmplitudeEvent> processRecord(ConsumerRecord<String, JsonNode> record) {
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

    private Future<RecordMetadata> submitTransformed(AmplitudeEvent event, ConsumerRecord<String, JsonNode> originalRecord) {
        JsonNode json = mapper.valueToTree(event);

        return amplitudeEventsProducer.sendRecord(
                originalRecord.partition(),
                originalRecord.key(),
                json,
                originalRecord.timestamp());
    }

}
