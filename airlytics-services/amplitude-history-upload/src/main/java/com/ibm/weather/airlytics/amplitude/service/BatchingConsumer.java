package com.ibm.weather.airlytics.amplitude.service;

import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class BatchingConsumer implements Consumer<AmplitudeEvent> {

    private List<AmplitudeEvent> batch;

    private AmplitudeHistoryUploadService amplitudeService;
    private AtomicInteger counter;
    private int amplitudeBatchSize;

    public BatchingConsumer(AmplitudeHistoryUploadService amplitudeService, AtomicInteger counter, int amplitudeBatchSize) {
        this.amplitudeBatchSize = amplitudeBatchSize;
        this.batch = new ArrayList<>(amplitudeBatchSize);
        this.amplitudeService = amplitudeService;
        this.counter = counter;
    }

    @Override
    public void accept(AmplitudeEvent event) {
        if(this.amplitudeService.isError()) return;

        batch.add(event);

        if(batch.size() == amplitudeBatchSize) {
            sendBatch();
        }
    }

    public void sendBatch() {
        if(this.amplitudeService.isError()) return;

        amplitudeService.asyncSendBatch(batch, counter);

        batch = new ArrayList<>(amplitudeBatchSize);
    }

}
