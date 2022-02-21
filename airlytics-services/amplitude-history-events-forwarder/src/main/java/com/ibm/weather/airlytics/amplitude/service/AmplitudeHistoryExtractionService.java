package com.ibm.weather.airlytics.amplitude.service;

import com.ibm.weather.airlytics.amplitude.db.AthenaDao;
import com.ibm.weather.airlytics.amplitude.db.AthenaDriver;
import com.ibm.weather.airlytics.amplitude.transform.AmplitudeEventTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.AthenaClient;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AmplitudeHistoryExtractionService {

    private static final Logger logger = LoggerFactory.getLogger(AmplitudeHistoryExtractionService.class);

    private AtomicInteger eventsCount = new AtomicInteger(0);

    @Value("${amplitude.batch.size:100}")
    private int amplitudeBatchSize;

    @Value("${history.start}")
    private String startDateString;

    @Value("${history.end}")
    private long endTime;

    @Value("${history.events}")
    private String eventsString;

    @Value("${history.maxAppversion}")
    private String maxAppversion;

    private AthenaDriver athena;

    private AthenaDao athenaDao;

    private AmplitudeEventTransformer transformer;

    private AmplitudeHistoryUploadService amplitudeService;

    @Autowired
    public AmplitudeHistoryExtractionService(
            AthenaDriver athena,
            AthenaDao athenaDao,
            AmplitudeEventTransformer transformer,
            AmplitudeHistoryUploadService amplitudeService) {
        this.athena = athena;
        this.athenaDao = athenaDao;
        this.transformer = transformer;
        this.amplitudeService = amplitudeService;
    }

    @Scheduled(initialDelay = 60_000L, fixedDelay = Long.MAX_VALUE)
    public void runJob() {
        logger.info("Starting Amplitude history upload job");
        LocalDate startDate = LocalDate.parse(startDateString);
        LocalDate endDate = Instant.ofEpochMilli(endTime).atZone(ZoneOffset.UTC).toLocalDate();
        List<String> events = Arrays.asList(eventsString.split(","));
        BatchingAmplitudeEventConsumer consumer = new BatchingAmplitudeEventConsumer(amplitudeService, eventsCount, amplitudeBatchSize);
        boolean isError = false;

        try {
            AthenaClient athenaClient = athena.getAthenaClient();
            final SortedSet<LocalDate> days = new TreeSet<>();
            LocalDate next = startDate;

            while (next.isBefore(endDate)) {
                days.add(next);
                next = next.plusDays(1L);
            }
            days.add(endDate);

            for (LocalDate day : days) {

                try {
                    athenaDao.processEvents(athenaClient, day, endTime, maxAppversion, events, air -> {
                        transformer.transform(air).ifPresent(consumer::accept);
                    });
                } catch (Exception e) {
                    logger.error("Error processing day " + day + ". Stopping the job", e);
                    isError = true;
                    break;
                }
                logger.info("Processed day {}", day);
            }
        } catch(Exception e) {
            logger.error("Error creating Athena client. Stopping the job", e);
            isError = true;
        }

        if(isError || this.amplitudeService.isError()) {
            logger.error("Export has failed");
        } else {
            //send last batch
            consumer.sendBatch();

            try {
                Thread.sleep(120000L);
            } catch (InterruptedException ie) {
            }
            logger.info(
                    "Export completed, submitted {} events.",
                    this.eventsCount.get());
        }
    }
}
