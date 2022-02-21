package com.ibm.weather.airlytics.amplitude.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.amplitude.client.AmplitudeApiClient;
import com.ibm.weather.airlytics.amplitude.client.AmplitudeApiException;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class AmplitudeHistoryUploadService {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeHistoryUploadService.class);

    final private ObjectMapper mapper = new ObjectMapper();

    private AmplitudeApiClient amplitudeApi;

    private AtomicBoolean error = new AtomicBoolean(false);

    @Autowired
    public AmplitudeHistoryUploadService(AmplitudeApiClient amplitudeApi) {
        this.amplitudeApi = amplitudeApi;
    }

    @Async
    public void asyncSendBatch(List<AmplitudeEvent> entities, AtomicInteger counter) {

        if(error.get()) {
            return;
        }

        if(CollectionUtils.isEmpty(entities)) {
            return;
        }

        try {

            if(!error.get()) {
                amplitudeApi.uploadEvents(entities);
            }
            counter.addAndGet(entities.size());

            if(counter.get() % 1000 < 75) {
                LOGGER.info("Uploaded {}", counter.get());
            }
        } catch (AmplitudeApiException e) {

            try {
                LOGGER.warn("Error uploading Amplitude history: " + e.getMessage() + ", re-trying...", e);
                LOGGER.info("Giving Amplitude a break. Sleep for {}ms", 10000L);

                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException ie) {
                }

                if(!error.get()) {
                    amplitudeApi.uploadEvents(entities);
                }
                counter.addAndGet(entities.size());

                if(counter.get() % 1000 < 75) {
                    LOGGER.info("Uploaded {}", counter.get());
                }
            } catch (AmplitudeApiException e1) {
                error.set(true);
                LOGGER.error("Error sending to Amplitude", e1);
            }
        }
    }

    public boolean isError() {
        return error.get();
    }
}
