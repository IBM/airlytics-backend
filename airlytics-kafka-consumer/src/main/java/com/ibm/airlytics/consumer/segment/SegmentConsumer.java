package com.ibm.airlytics.consumer.segment;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.segment.analytics.Analytics;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class SegmentConsumer extends AirlyticsConsumer {
    private static final Logger LOGGER = Logger.getLogger(SegmentConsumer.class.getName());

    private SegmentConsumerConfig config;
    private Analytics segment;

    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_segment_records_processed_total")
            .help("Total records processed by the segment consumer.")
            .labelNames("event_name", "result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    public SegmentConsumer(SegmentConsumerConfig config) {
        super(config);
        this.config = config;

        segment = Analytics.builder(config.getWriteKey()).endpoint(config.getSegmentEndpoint()).build();
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;

        int recordsProcessed = 0;
        Map<String, Object> eventProperties= new HashMap<>();

        for (ConsumerRecord<String, JsonNode> record : records) {
            String eventName = "";
            String result = "success";

            try {
                SegmentEvent event = new SegmentEvent(record);
                segment.enqueue(event.getSegmentMessageBuilder());

            } catch (NullPointerException e) {
                e.printStackTrace();
                errorsProducer.sendRecord(record.key(), record.value(), record.timestamp());
                result = "error";
            } finally {
                recordsProcessedCounter.labels(eventName, result,
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                ++recordsProcessed;
            }
        }

        commit();

        totalProgress += recordsProcessed;

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.info("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsProcessed;
    }

    @Override
    protected void commit() {
        segment.flush();
        super.commit();
    }

    @Override
    public void close() throws Exception {
        super.close();
        segment.shutdown();
    }

    // Does not support airlock configuration refresh
    @Override
    public void newConfigurationAvailable() {
    }
}
