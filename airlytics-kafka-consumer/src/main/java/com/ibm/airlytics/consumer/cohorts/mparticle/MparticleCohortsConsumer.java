package com.ibm.airlytics.consumer.cohorts.mparticle;

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

public class MparticleCohortsConsumer extends AirlyticsConsumer {

    private static final Logger LOGGER = Logger.getLogger(MparticleCohortsConsumer.class.getName());

    private static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_mparticle_cohorts_records_processed_total")
            .help("Total records processed by the Mparticle cohorts consumer.")
            .labelNames("result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    final private ObjectMapper mapper = new ObjectMapper();

    private MparticleCohortsConsumerConfig config;
    private MparticleCohortsConsumerConfig newConfig;

    private MparticleExporter mparticleExporter;

    public MparticleCohortsConsumer(MparticleCohortsConsumerConfig config) {
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
        boolean success = mparticleExporter.sendExportedDeltasBatch(batch);

        if(!success) {
            LOGGER.error("Stopping Mparticle cohorts consumer due to errors calling Mparticle API");
            stop();
            return 0;
        }
        recordsProcessedCounter.labels(RESULT_SUCCESS, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(batch.size());
        commit();
        return recordsCount;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        MparticleCohortsConsumerConfig config = new MparticleCohortsConsumerConfig();
        try {
            config.initWithAirlock();

            if(!config.equals(this.config)) {
                newConfig = config;
            }
        } catch (IOException e) {
            LOGGER.error("Could not read Airlocks config for Mparticle Cohorts Consumer", e);
        }
    }

    private synchronized void updateToNewConfigIfExists() {

        if (newConfig != null) {

            try {
                setConfig(newConfig);
                newConfig = null;
                // re-init
                init();
                LOGGER.info("Mparticle cohorts consumer updated with configuration: " + config.toString());
            } catch (Exception e) {
                LOGGER.error("Stopping Mparticle cohorts consumer due to invalid configuration", e);
                stop();
            }
        }
    }

    void setConfig(MparticleCohortsConsumerConfig config) {
        this.config = config;
    }

    void init() {

        if(this.mparticleExporter != null) {
            this.mparticleExporter.close();
        }
        this.mparticleExporter = new MparticleExporter(this.config);

        if(!this.config.isMparticleIntegrationEnabled()) {
            LOGGER.warn("Mparticle integration is disabled!");
        }
    }

    private Optional<UserCohort> processRecord(ConsumerRecord<String, JsonNode> record) {
        JsonNode eventJson = record.value();

        try {
            UserCohort userCohort = mapper.treeToValue(eventJson, UserCohort.class);

            if(StringUtils.isNotBlank(userCohort.getUserId()) && userCohort.getEnabledExports() != null) {
                Optional<UserCohortExport> exportConfig = mparticleExporter.getUserCohortExport(userCohort, MparticleExporter.MPARTICLE_EXPORT_KEY);

                if(exportConfig.isPresent()) {
                    // This user has a user ID, and Mparticle export is enabled for this cohort
                    return Optional.of(userCohort);
                }
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Error parsing UserCohort event " + eventJson.toString() + ". Mparticle cohorts consumer will be stopped.", e);
            stop();
        }
        return Optional.empty();
    }
}
