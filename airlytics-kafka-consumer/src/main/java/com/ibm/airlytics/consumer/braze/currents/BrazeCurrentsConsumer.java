package com.ibm.airlytics.consumer.braze.currents;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.integrations.dto.FileProcessingRequest;
import com.ibm.airlytics.serialize.data.DataSerializer;
import com.ibm.airlytics.serialize.data.FSSerializer;
import com.ibm.airlytics.utilities.datamarker.ReadProgressMarker;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BrazeCurrentsConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeCurrentsConsumer.class.getName());

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_braze_currents_files_processed_total")
            .help("Total Braze Currents files processed.")
            .register();

    private BrazeCurrentsConsumerConfig config;

    private BrazeCurrentsConsumerConfig newConfig;

    private BrazeCurrentsFileProcessor fileProcessor;

    private ReadProgressMarker readProgressMarker;

    public BrazeCurrentsConsumer() {}

    public BrazeCurrentsConsumer(BrazeCurrentsConsumerConfig config) {
        super(config);

        setConfig(config);
        init();

        LOGGER.info("Braze Currents Consumer created with configuration:" + config.toString());
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;
        LOGGER.info("Braze Currents consumer - process records:" + records.count());

        boolean success = true;
        int recordsProcessed = 0;
        updateToNewConfigIfExists();

        List<String> progressFilesToDelete = new ArrayList<>();
        FileProcessingRequest request = null;

        for (ConsumerRecord<String, JsonNode> record : records) {

            //act on the request
            try {
                request = new FileProcessingRequest(record);
                Collection<String> progressFiles = this.fileProcessor.processFile(request);
                progressFilesToDelete.addAll(progressFiles);
                recordsProcessedCounter.inc();
                ++recordsProcessed;
            } catch (Exception e) {

                if(request != null) {
                    LOGGER.error("error processing request: " + request.getBucketName() + "/" + request.getFileName(), e);
                } else {
                    LOGGER.error("error parsing request: " + record.toString(), e);
                }
                stop();
                success = false;
                break;
            }
        }

        if (success) {
            //finish up
            commit();

            //delete progress files
            try {
                LOGGER.info("Deleting progress files: " + progressFilesToDelete);
                readProgressMarker.deleteFiles(progressFilesToDelete);
            } catch (IOException e) {
                LOGGER.warn("Failed deleting progress files: ", e);
            }
        }
        return recordsProcessed;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        BrazeCurrentsConsumerConfig latest = new BrazeCurrentsConsumerConfig();

        try {
            latest.initWithAirlock();

            if(!latest.equals(this.config) ||
                    !latest.getProducerCompressionType().equals(this.config.getProducerCompressionType()) ||
                    latest.getLingerMs() != this.config.getLingerMs()
            ) {
                this.newConfig = latest;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Braze Currents Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Braze Currents Consumer updated with configuration:" + config.toString());
            } catch (Exception e) {
                LOGGER.error("Stopping Braze Currents Consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(BrazeCurrentsConsumerConfig config) {

        if(config.getProducts() == null || config.getProducts().isEmpty()) {
            throw new IllegalArgumentException("Products are not configured for Braze Currents Consumer");
        }
        this.config = config;
    }

    void init() {
        if(this.fileProcessor != null) {
            this.fileProcessor.shutdown();
        }
        DataSerializer progressDataSerializer = new FSSerializer(config.getProgressFolder(), 3);
        this.readProgressMarker = new ReadProgressMarker(progressDataSerializer);
        this.fileProcessor = new BrazeCurrentsFileProcessor(this.config, this.readProgressMarker);
    }
}
