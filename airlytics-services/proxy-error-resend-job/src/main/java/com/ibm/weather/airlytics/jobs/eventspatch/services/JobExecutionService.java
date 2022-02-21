package com.ibm.weather.airlytics.jobs.eventspatch.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.common.athena.AthenaDriver;
import com.ibm.weather.airlytics.common.athena.QueryBuilder;
import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;
import com.ibm.weather.airlytics.jobs.eventspatch.db.AthenaDao;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import com.ibm.weather.airlytics.jobs.eventspatch.eventproxy.EventBatchSubmitter;
import com.ibm.weather.airlytics.jobs.eventspatch.patcher.AbstractPatch;
import com.ibm.weather.airlytics.jobs.eventspatch.patcher.AirlyticsEventPatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class JobExecutionService implements ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(JobExecutionService.class);

    private static final int PARTITIONS_NUMBER = 100;

    private static final Object lock = new Object();

    final private ObjectMapper mapper = new ObjectMapper();

    private ApplicationContext context;

    private EventsPatchConfigService featureConfigService;

    private AthenaDriver athena;

    private AthenaDao athenaDao;

    @Autowired
    public JobExecutionService(
            EventsPatchConfigService featureConfigService,
            AthenaDriver athena,
            AthenaDao athenaDao) {
        this.featureConfigService = featureConfigService;
        this.athena = athena;
        this.athenaDao = athenaDao;
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.context = ctx;
    }

    @Scheduled(initialDelay = 10_000L, fixedDelay = Long.MAX_VALUE)
    public void runJob() {
        logger.info("Starting Events Patch job");
        long startTs = System.currentTimeMillis();

        try {
            EventsPatchAirlockConfig featureConfig = featureConfigService.getAirlockConfig();
            ExecutorService executorService = Executors.newFixedThreadPool(featureConfig.getAthenaParallelThreads());
            Class<?> patchClass = Class.forName("com.ibm.weather.airlytics.jobs.eventspatch.patcher." + featureConfig.getPatchName());
            Constructor<?> ctr = patchClass.getConstructor(EventsPatchAirlockConfig.class);
            AbstractPatch patch = (AbstractPatch)ctr.newInstance(featureConfig);
            AthenaClient athenaClient = athena.getAthenaClient(featureConfig);
            EventBatchSubmitter batchSubmitter = new EventBatchSubmitter(featureConfig);
            logger.info("Applying {}", patch.getClass().getName());
            boolean success = true;

            for(int partition = featureConfig.getPatchStartPartition(); partition < PARTITIONS_NUMBER; partition++) {
                final List<AirlyticsEvent> batch = new LinkedList<>();
                final List<Future<Boolean>> pendingQueries = new LinkedList<>();
                final List<Future<Boolean>> pendingPosts = new LinkedList<>();

                for(LocalDate day : patch.getDays()) {
                    pendingQueries.add(
                            submitProcessQuery(
                                    featureConfig,
                                    executorService,
                                    patch,
                                    athenaClient,
                                    batchSubmitter,
                                    batch,
                                    pendingPosts,
                                    partition,
                                    day));
                }

                for(Future<Boolean> future : pendingQueries) {

                    try {

                        if(!success) {// failed at a previous iteration
                            future.cancel(true);
                        } else {
                            success = future.get();
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        logger.error("Exception in a thread extracting events from Athena", e);
                        success = false;
                    }
                }

                if(success) {

                    if (!batch.isEmpty()) {
                        pendingPosts.add(
                                batchSubmitter.submitBatchToProxy(
                                        new ArrayList<>(batch), patch.isFailOnValidationError()));
                        batch.clear();
                    }

                    for(Future<Boolean> future : pendingPosts) {

                        try {

                            if(!success) {// failed at a previous iteration
                                future.cancel(true);
                            } else {
                                success = future.get();
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            logger.error("Exception in a thread sending events to the eventas API", e);
                            success = false;
                        }
                    }
                }

                if(success) {
                    logger.info("Submitted all days from partition {}", partition);
                } else {
                    logger.warn("Failed at partition {}", partition);
                    break;
                }
            }

            if(success) {
                logger.info(
                        "Events Patch job {} finished. It took {}ms, sent {} events",
                        featureConfig.getPatchName(),
                        (System.currentTimeMillis() - startTs),
                        EventBatchSubmitter.eventCount.get());
                // shutdown
                int exitCode = SpringApplication.exit(this.context, () -> 0);
                System.exit(exitCode);
            } else {
                int exitCode = SpringApplication.exit(this.context, () -> 1);
                System.exit(exitCode);
            }
        } catch (Exception e) {
            logger.error("Error performing Events Patch job", e);
            int exitCode = SpringApplication.exit(this.context, () -> 1);
            System.exit(exitCode);
            //throw new RuntimeException("Error performing DSR Cleanup job");
        }
    }

    private Future<Boolean> submitProcessQuery(
            EventsPatchAirlockConfig featureConfig,
            ExecutorService executorService,
            AbstractPatch patch,
            AthenaClient athenaClient,
            EventBatchSubmitter batchSubmitter,
            List<AirlyticsEvent> batch,
            List<Future<Boolean>> pendingPosts,
            int partition,
            LocalDate day) {
        return executorService.submit(() ->
                processPartitionDay(featureConfig, patch, athenaClient, batchSubmitter, batch, pendingPosts, partition, day));
    }

    private boolean processPartitionDay(
            EventsPatchAirlockConfig featureConfig,
            AbstractPatch patch,
            AthenaClient athenaClient,
            EventBatchSubmitter batchSubmitter,
            List<AirlyticsEvent> batch,
            List<Future<Boolean>> pendingPosts,
            int partition,
            LocalDate day)
            throws Exception {

        try {
            final QueryBuilder qb = patch.getPartiotionDayQueryBuilder(partition, day);
            final AirlyticsEventPatcher patcher = patch.getEventPatcher();
            final Instant now = Instant.now();
            athenaDao.processEvents(athenaClient, featureConfig, qb.build(), (event -> {
                Instant eventTime = Instant.ofEpochMilli(event.getEventTime());

                if(eventTime.isBefore(now)) {

                    synchronized (lock) {
                        patcher.patch(event);
                        batch.add(event);

                        if (batch.size() >= featureConfig.getEventProxyIntegrationConfig().getEventApiBatchSize()) {
                            pendingPosts.add(
                                    batchSubmitter.submitBatchToProxy(
                                            new ArrayList<>(batch), patch.isFailOnValidationError()));
                            batch.clear();
                        }
                    }
                } else {

                    try {
                        logger.warn("Ignoring future event {}, {} is after {}", mapper.writeValueAsString(event), eventTime, now);
                    } catch (JsonProcessingException e) {
                        logger.warn("Ignoring future event {}", event.getEventId());
                    }
                }
            }));
            logger.info("Queued events from partition {}, day {}", partition, day);
            return true;
        } catch (Exception e) {
            logger.error("Error extracting data from Athena: " + e.getMessage(), e);
        }
        return false;
    }
}
