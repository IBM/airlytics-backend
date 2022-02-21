package com.ibm.weather.airlytics.braze.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.braze.client.BrazeApiClient;
import com.ibm.weather.airlytics.braze.client.BrazeApiException;
import com.ibm.weather.airlytics.braze.dto.BrazeEntity;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class BrazeHistoryUploadService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrazeHistoryUploadService.class);

    final private ObjectMapper mapper = new ObjectMapper();

    private BrazeApiClient brazeApi;

    private AtomicBoolean error = new AtomicBoolean(false);

    @Autowired
    public BrazeHistoryUploadService(BrazeApiClient brazeApi) {
        this.brazeApi = brazeApi;
    }

    @Async
    public void asyncSendBatch(List<BrazeEntity> entities, AtomicInteger counter) {

        if(error.get()) {
            return;
        }

        if(CollectionUtils.isEmpty(entities)) {
            return;
        }
        List<JsonNode> attributes = entities.stream()
                .filter(e -> (e.getKind() == BrazeEntity.Kind.USER))
                .map(a -> (JsonNode) mapper.valueToTree(a.toMap()))
                .collect(Collectors.toList());

        try {

            if(!error.get()) {
                brazeApi.uploadEvents(attributes, null, null);
            }
            counter.addAndGet(entities.size());

            if(counter.get() % 1000 < 75) {
                LOGGER.info("Uploaded {}", counter.get());
            }
        } catch (BrazeApiException e) {

            try {
                LOGGER.warn("Error uploading Braze history: " + e.getMessage() + ", re-trying...", e);
                LOGGER.info("Giving Braze a break. Sleep for {}ms", 10000L);

                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException ie) {
                }

                if(!error.get()) {
                    brazeApi.uploadEvents(attributes, null, null);
                }
                counter.addAndGet(entities.size());

                if(counter.get() % 1000 < 75) {
                    LOGGER.info("Uploaded {}", counter.get());
                }
            } catch (BrazeApiException e1) {
                error.set(true);
                LOGGER.error("Error sending to Braze", e1);
            }
        }
    }

    public boolean isError() {
        return error.get();
    }
}
