package com.ibm.airlytics.consumer.cohorts.braze;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.RESULT_SUCCESS;

public class BrazeCohortsConsumer extends AirlyticsConsumer {

    private static final Logger LOGGER = Logger.getLogger(BrazeCohortsConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_braze_cohorts_records_processed_total")
            .help("Total records processed by the Braze cohorts consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    final private ObjectMapper mapper = new ObjectMapper();

    private BrazeCohortsConsumerConfig config;
    private BrazeCohortsConsumerConfig newConfig;

    private BrazeExporter brazeExporter;

    public BrazeCohortsConsumer(BrazeCohortsConsumerConfig config) {
        super(config);
        setConfig(config);
        init();
        LOGGER.info("Ups cohorts consumer created with configuration:\n" + config.toString());
    }

    @Override
    public int processRecords(ConsumerRecords<String, JsonNode> consumerRecords) {

        if(consumerRecords.isEmpty()) {
            return 0;
        }
        updateToNewConfigIfExists();
        int recordsCount = 0;
        List<UserCohort> batch = new LinkedList<>();

        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {

            if(!isRunning()) {
                break;
            }
            processRecord(record).ifPresent(batch::add);
            recordsCount++;
        }
        boolean success = brazeExporter.sendExportedDeltasBatch(batch);

        if(!success) {
            LOGGER.error("Stopping Braze cohorts consumer due to errors calling Braze API");
            stop();
            return 0;
        }
        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(batch.size());
        commit();
        return recordsCount;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        BrazeCohortsConsumerConfig config = new BrazeCohortsConsumerConfig();
        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Braze Cohorts Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Braze cohorts consumer updated with configuration: " + config.toString());
            } catch (Exception e) {
                LOGGER.error("Stopping Braze cohorts consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(BrazeCohortsConsumerConfig config) {
        this.config = config;
    }

    void init() {

        if(this.brazeExporter != null) {
            this.brazeExporter.close();
        }
        this.brazeExporter = new BrazeExporter(this.config);

        if(!this.config.isBrazeApiEnabled()) {
            LOGGER.warn("Braze integration is disabled!");
        }
    }

    private Optional<UserCohort> processRecord(ConsumerRecord<String, JsonNode> record) {
        JsonNode eventJson = record.value();

        try {
            UserCohort userCohort = mapper.treeToValue(eventJson, UserCohort.class);

            if(StringUtils.isNotBlank(userCohort.getUserId()) && userCohort.getEnabledExports() != null) {
                Optional<UserCohortExport> exportConfig = brazeExporter.getUserCohortExport(userCohort, BrazeExporter.BRAZE_EXPORT_KEY);

                if(exportConfig.isPresent()) {
                    // This user has a user ID, and Braze export is enabled for this cohort
                    return Optional.of(userCohort);
                }
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing UserCohort event " + eventJson.toString() + ". Braze cohorts consumer will be stopped.", e);
            stop();
        }
        return Optional.empty();
    }
}
