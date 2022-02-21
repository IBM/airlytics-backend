package com.ibm.weather.airlytics.amplitude.service;

import com.ibm.weather.airlytics.amplitude.db.AmplitudeExportDao;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Service
public class AmplitudeHistoryExtractionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeHistoryExtractionService.class);

    private static final int SHARDS_NUMBER = 1000;

    private AtomicInteger usersCount = new AtomicInteger(0);
    private AtomicInteger subscriptionsCount = new AtomicInteger(0);
    private AtomicInteger eventsCount = new AtomicInteger(0);

    @Value("${export.users.platform:ios}")
    private String platform;

    @Value("${amplitude.batch.size:100}")
    private int amplitudeBatchSize;

    @Value("${users.start.shard:0}")
    private int usersStart;

    @Value("${subscriptions.start.shard:0}")
    private int subscriptionsStart;

    @Value("${events.start.shard:0}")
    private int eventsStart;

    private AmplitudeExportDao dao;

    private AmplitudeHistoryUploadService amplitudeService;

    private S3FileService s3FileService;

    @Autowired
    public AmplitudeHistoryExtractionService(AmplitudeExportDao dao, AmplitudeHistoryUploadService amplitudeService, S3FileService s3FileService) {
        this.dao = dao;
        this.amplitudeService = amplitudeService;
        this.s3FileService = s3FileService;
    }

    @Scheduled(initialDelay = 10_000, fixedDelay = Long.MAX_VALUE)
    public void exportHistoryToAmplitude() {
        exportUsers();
        exportSubscriptions();
        exportDummyEvents();
        s3FileService.sendCountyLanguage();

        if(this.amplitudeService.isError()) {
            LOGGER.error("Export has failed");
        } else {
            try {
                Thread.sleep(120000L);
            } catch (InterruptedException ie) {
            }
            LOGGER.info(
                    "Export completed, submitted {} users, {} subscriptions, {} events, {} countries.",
                    this.usersCount.get(),
                    this.subscriptionsCount.get(),
                    this.eventsCount.get(),
                    this.s3FileService.getProcessedCount());
        }
    }

    private void exportUsers() {
        if(this.amplitudeService.isError()) return;

        if(usersStart >= 999) return;

        LOGGER.info("Uploading users");

        BatchingConsumer consumer = new BatchingConsumer(this.amplitudeService, this.usersCount, this.amplitudeBatchSize);

        IntStream.range(usersStart, SHARDS_NUMBER).forEach(shard -> dao.sendUserData(platform, shard, consumer));
        consumer.sendBatch();
    }

    private void exportSubscriptions() {
        if(this.amplitudeService.isError()) return;

        if(subscriptionsStart >= 999) return;

        LOGGER.info("Uploading subscriptions");

        BatchingConsumer consumer = new BatchingConsumer(this.amplitudeService, this.subscriptionsCount, this.amplitudeBatchSize);

        IntStream.range(subscriptionsStart, SHARDS_NUMBER).forEach(shard -> dao.sendPurchases(platform, shard, consumer));
        consumer.sendBatch();
    }

    private void exportDummyEvents() {
        if(this.amplitudeService.isError()) return;

        if(eventsStart >= 999) return;

        LOGGER.info("Uploading dummy events");

        BatchingConsumer consumer = new BatchingConsumer(this.amplitudeService, this.eventsCount, this.amplitudeBatchSize);

        IntStream.range(eventsStart, SHARDS_NUMBER).forEach(shard -> dao.sendDummyEvents(platform, shard, consumer));
        consumer.sendBatch();
    }
}
