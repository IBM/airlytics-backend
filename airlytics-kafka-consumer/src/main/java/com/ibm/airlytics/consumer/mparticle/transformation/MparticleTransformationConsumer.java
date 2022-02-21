package com.ibm.airlytics.consumer.mparticle.transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.integrations.dto.AirlyticsEvent;
import com.ibm.airlytics.consumer.mparticle.MparticleJson;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.mparticle.model.Batch;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_ERROR;
import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class MparticleTransformationConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(MparticleTransformationConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_mparticle_transform_records_processed_total")
            .help("Total records processed by the mparticle transformation consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    private static final Counter eventReceivedCounter = Counter.build()
            .name("airlytics_mparticle_transform_events_received_total")
            .help("Total events produced by the mparticle transformation consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    private static final Counter eventTransformedCounter = Counter.build()
            .name("airlytics_mparticle_transform_events_total")
            .help("Total events produced by the mparticle transformation consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "eventtype").register();

    final private ObjectMapper mapper = new ObjectMapper();

    final private MparticleJson mpJson = new MparticleJson();

    private MparticleTransformationConsumerConfig config;

    private MparticleTransformationConsumerConfig newConfig;

    private Producer mparticleEventsProducer;

    private MparticleEventTransformer eventTransformer;

    public MparticleTransformationConsumer() {}

    public MparticleTransformationConsumer(MparticleTransformationConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        LOGGER.info("Mparticle transformation consumer created with configuration: " + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if (consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();

        List<Triple<AirlyticsEvent, Batch, ConsumerRecord<String, JsonNode>>> mparticleEvents = new LinkedList<>();
        List<Future<RecordMetadata>> futures = new LinkedList<>();
        Map<String, Long> eventsReceivedPerType = new HashMap<>();
        Map<String, Long> eventsPerType = new HashMap<>();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
            Optional<Pair<AirlyticsEvent, Batch>> optEvent = processRecord(record, eventsReceivedPerType);

            if( !isRunning() ) {
                break;
            }
            optEvent.ifPresent(e -> mparticleEvents.add(Triple.of(e.getLeft(), e.getRight(), record)));
        }

        if( isRunning() ) {
            mparticleEvents.forEach((triple) -> {
                submitTransformed(triple.getLeft(), triple.getMiddle(), triple.getRight(), eventsPerType)
                        .ifPresent(futures::add);
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
            recordsProcessedCounter.labels(RESULT_ERROR, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(mparticleEvents.size());
            return 0;
        }
        commit();

        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(mparticleEvents.size());
        report(eventReceivedCounter, eventsReceivedPerType);
        report(eventTransformedCounter, eventsPerType);

        return consumerRecords.count();
    }

    @Override
    protected void commit() {
        mparticleEventsProducer.flush();
        consumer.commitSync();
    }

    @Override
    public void close() throws Exception {
        this.mparticleEventsProducer.close();
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        MparticleTransformationConsumerConfig latest = new MparticleTransformationConsumerConfig();

        try {
            latest.initWithAirlock();

            if(!latest.equals(this.config) ||
                    !latest.getProducerCompressionType().equals(this.config.getProducerCompressionType()) ||
                    latest.getLingerMs() != this.config.getLingerMs()
            ) {
                this.newConfig = latest;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Mparticle Transformation Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (this.newConfig != null) {

            try {
                setConfig(this.newConfig);
                this.newConfig = null;
                // re-init
                init();
                LOGGER.info("Mparticle transformation consumer updated with configuration: " + config.toString());
            } catch (Exception e) {
                LOGGER.error("stopping mparticle transformation consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(MparticleTransformationConsumerConfig config) {
        this.config = config;
    }

    void init() {

        if(this.mparticleEventsProducer != null) {
            this.mparticleEventsProducer.close();
        }
        this.mparticleEventsProducer = new Producer(
                this.config.getDestinationTopic(),
                this.config.getBootstrapServers(),
                this.config.getSecurityProtocol(),
                this.config.getProducerCompressionType(),
                this.config.getLingerMs());

        this.eventTransformer = new MparticleEventTransformer(this.config);
    }

    private Optional<Pair<AirlyticsEvent, Batch>> processRecord(ConsumerRecord<String, JsonNode> record, Map<String, Long> eventsReceivedPerType) {
        JsonNode eventJson = record.value();
        try {
            AirlyticsEvent event = mapper.treeToValue(eventJson, AirlyticsEvent.class);
            Long cnt = eventsReceivedPerType.get(event.getName());

            if(cnt == null) {
                cnt = 1L;
            } else {
                cnt = cnt.longValue() + 1L;
            }
            eventsReceivedPerType.put(event.getName(), cnt);
            Optional<Batch>  mpEvent = eventTransformer.transform(event);

            if(mpEvent.isPresent()) {
                return Optional.of(Pair.of(event, mpEvent.get()));
            }
            return Optional.empty();
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing Airlytics event " + eventJson.toString(), e);
            stop();
        }
        return Optional.empty();
    }

    private Optional<Future<RecordMetadata>> submitTransformed(AirlyticsEvent airEvent, Batch mpEvent, ConsumerRecord<String, JsonNode> originalRecord, Map<String, Long> eventsProducedPerType) {
        String batchJson = mpJson.getGson().toJson(mpEvent);

        try {
            JsonNode node = mapper.readTree(batchJson);
            Long cnt = eventsProducedPerType.get(airEvent.getName());

            if(cnt == null) {
                cnt = 1L;
            } else {
                cnt = cnt.longValue() + 1L;
            }
            eventsProducedPerType.put(airEvent.getName(), cnt);

            return Optional.of(mparticleEventsProducer.sendRecord(
                    originalRecord.partition(),
                    originalRecord.key(),
                    node,
                    originalRecord.timestamp()));
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing mParticle batch " + batchJson, e);
            stop();
        }
        return Optional.empty();
    }

    private void report(Counter counter, Map<String, Long> eventsPerType) {
        eventsPerType.forEach((k, v) -> report(counter, k, v.intValue()));
    }

    private void report(Counter counter, String eventType, int size) {
        counter.labels(
                AirlockManager.getEnvVar(), AirlockManager.getProduct(), eventType).inc(size);
    }
}
