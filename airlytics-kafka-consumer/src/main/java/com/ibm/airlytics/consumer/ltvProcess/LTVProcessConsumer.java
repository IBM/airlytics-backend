package com.ibm.airlytics.consumer.ltvProcess;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.integrations.dto.FileProcessingRequest;
import com.ibm.airlytics.consumer.ltvProcess.processor.LTVFileProcessor;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.utilities.datamarker.ReadProgressMarker;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class LTVProcessConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(com.ibm.airlytics.consumer.ltvProcess.LTVProcessConsumer.class.getName());

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_ltv_process_requests_processed_total")
            .help("Total LTV Process requests processed by the ltv process consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV).register();

    //count ltv events: labels: env, product, filled/unfilled
    static final Counter ltvEventsCounter = Counter.build()
            .name("airlytics_ltv_events_counter")
            .help("Total LTV events")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT, "filled").register();


    //count error ltv events: events withh ltv value but userId extraction error
    static final Counter ltvErrorEventsCounter = Counter.build()
            .name("airlytics_error_ltv_events_counter")
            .help("Total LTV error events")
            .labelNames(AirlyticsConsumerConstants.ENV).register();

    static final Summary revenueSummary = Summary.build()
            .name("airlytics_ltv_revenue")
            .help("LTV revenue")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Summary ltvFileProcessingSummary = Summary.build()
            .name("ltv_processor_file_latency_seconds")
            .help("LTV processor single file processing latency in seconds")
            .labelNames(AirlyticsConsumerConstants.ENV).register();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private LTVProcessConsumerConfig config;
    private LTVProcessConsumerConfig newConfig = null;
    private LTVFileProcessor fileProcessor;
    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();
    private Date startProcessDay;
    private ReadProgressMarker readProgressMarker;


    public LTVProcessConsumer(LTVProcessConsumerConfig config) throws Exception {
        super(config);
        setConfig(config);

        DataSerializer progressDataSerializer = new FSSerializer(config.getProgressFolder(), 3);
        this.readProgressMarker = new ReadProgressMarker(progressDataSerializer);
        init();

        LOGGER.info("LTV Processor Consumer updated with configuration:" + config.toString());
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;
        LOGGER.info("LTV Process consumer - process records:" + records.count());

        boolean success = true;
        int recordsProcessed = 0;
        updateToNewConfigIfExists();

        List<String> progressFilesToDelete = new ArrayList<>();
        for (ConsumerRecord<String, JsonNode> record : records) {

            //act on the request
            try {
                FileProcessingRequest request = new FileProcessingRequest(record);
                LOGGER.info("record is:"+record.value().toString());
                String filePath = request.getFileName();
                LOGGER.info("filePath:"+filePath);
                int dayPos = filePath.indexOf("imprsn_date=");
                String dayStr = filePath.substring(dayPos + 12, dayPos + 22);
                Date fileDay;
                try {
                    fileDay = formatter.parse(dayStr);
                } catch (Exception e) {
                    LOGGER.error("parse error, skipping record:"+e.getMessage());
                    continue;
                }
                fileDay = formatter.parse(dayStr);

                if (fileDay.before(startProcessDay)) {
                    LOGGER.info("Do not process file: "+filePath + " . Earlier than startProcessDay: " + startProcessDay.toString());
                }
                else {
                    LOGGER.info("processing record:" + record.toString());
                    progressFilesToDelete = fileProcessor.processFile(request, ltvEventsCounter, ltvErrorEventsCounter, revenueSummary);
                }
                recordsProcessedCounter.labels(AirlockManager.getEnvVar()).inc();
                ++recordsProcessed;
            } catch (Exception e) {
                LOGGER.error("error processing request", e);
                stop();
                success=false;
                break;
            }
        }

        if (success) {
            totalProgress += recordsProcessed;

            Instant now = Instant.now();
            Duration timePassed = Duration.between(lastRecordProcessed, now);
            if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
                LOGGER.info("Processed " + totalProgress + " records. Current rate: " +
                        ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
                lastRecordProcessed = now;
                lastProgress = totalProgress;
            }

            //finish up
            commit();
            LOGGER.info("***** commit");

            //delete progress files
            try {
                readProgressMarker.deleteFiles(progressFilesToDelete);
                LOGGER.info("***** deleteFiles: number of files = " + progressFilesToDelete.size());
            } catch (IOException e) {
                LOGGER.warn("Fail deleting progress files: " + e.getMessage());
            }
        }

        return recordsProcessed;
    }

    @Override
    public void newConfigurationAvailable() {
        LTVProcessConsumerConfig latest = new LTVProcessConsumerConfig();

        try {
            latest.initWithAirlock();

            if(!latest.equals(this.config) ||
                    !latest.getProducerCompressionType().equals(this.config.getProducerCompressionType()) ||
                    latest.getLingerMs() != this.config.getLingerMs()
            ) {
                this.newConfig = latest;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for LTV Processor Consumer", e);
        }
    }

    // Note that this will NOT update Kafka URLs etc. If those change the consumer must be restarted.
    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("LTV Processor Consumer updated with configuration:" + config.toString());
            } catch (Exception e) {
                LOGGER.error("Stopping LTV Processor Consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(LTVProcessConsumerConfig config) {

        if(config.getProducts() == null || config.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Products are not configured for LTV Processor Consumer");
        }

        if (config.getStartProcessDay() == null || config.getStartProcessDay().isEmpty()) {
            throw new IllegalArgumentException("Missing 'startProcessDay' configuration.");
        }
        this.config = config;
    }

    private void init() throws Exception {
        this.fileProcessor = new LTVFileProcessor(this.config, this.readProgressMarker);
        this.startProcessDay = formatter.parse(config.getStartProcessDay());
    }
}
