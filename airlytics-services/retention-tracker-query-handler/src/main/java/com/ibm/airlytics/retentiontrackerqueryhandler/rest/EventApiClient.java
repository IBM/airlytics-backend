package com.ibm.airlytics.retentiontrackerqueryhandler.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.RateLimiter;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EventApiClient {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private static final Logger LOGGER = Logger.getLogger(EventApiClient.class.getName());

    private OkHttpClient client = new OkHttpClient();

    private String eventApiBaseUrl;

    private String eventApiPath;

    private String eventApiKey;

    private RateLimiter rateLimiter;

    private boolean isClonedEvent;

    private int maxAttempts = 3;

    private int maxEvents = -1;

    public EventApiClient(String eventApiBaseUrl, String eventApiPath,
                          String eventApiKey, int eventApiRateLimit, boolean isClonedEvent, int maxAttempts, int maxEvents) {
        this(eventApiBaseUrl, eventApiPath, eventApiKey, eventApiRateLimit, isClonedEvent);
        this.maxAttempts = maxAttempts;
        this.maxEvents = maxEvents;
    }
    public EventApiClient(String eventApiBaseUrl, String eventApiPath,
                          String eventApiKey, int eventApiRateLimit, boolean isClonedEvent) {
        if (StringUtils.isNoneBlank(eventApiBaseUrl, eventApiPath)) {
            this.eventApiBaseUrl = eventApiBaseUrl;
            this.eventApiPath = eventApiPath;
            this.eventApiKey = eventApiKey;
            this.isClonedEvent = isClonedEvent;

            if (eventApiRateLimit > 0) {
                this.rateLimiter = RateLimiter.create(eventApiRateLimit);
            }
        } else {
            LOGGER.error("Invalid Proxy API configuration: eventApiBaseUrl = " + eventApiBaseUrl + ", eventApiPath = " + eventApiPath);
            throw new IllegalArgumentException("Event Proxy Api client requires the following configuration parameters: eventApiBaseUrl, eventApiPath");
        }
    }

    public void post(String json) throws IOException, EventApiException {
        RequestBody body = RequestBody.create(JSON, json);
        Request.Builder requestBuilder = new Request.Builder()
                .url(eventApiBaseUrl + eventApiPath)
                .post(body)
                .header("x-cloned-event", isClonedEvent ? "true" : "false");

        if(StringUtils.isNotBlank(this.eventApiKey)) {
            requestBuilder.header("x-api-key", eventApiKey);
        }
        Request request = requestBuilder.build();
        acquirePermit();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() || response.code() == 202) { // in case of 202, the caller should check for any necessary retries
                throw new EventApiException(response);
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void post(JsonNode json) throws IOException, EventApiException {
        post(json.toString());
    }

    private Collection<List<Object>> splitEvents(List<Object> events) {
        int batchSize = events.size();
        if (this.maxEvents > 0 && batchSize > this.maxEvents) {
            //split into batches
            final AtomicInteger counter = new AtomicInteger();
            final Collection<List<Object>> result = events.stream()
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / this.maxEvents))
                    .values();
            return result;
        } else {
            Collection<List<Object>> toRet = new ArrayList<>();
            toRet.add(events);
            return toRet;
        }
    }
    public boolean sendEventsBatchToEventProxy(List<Object> purchases, int attempt, IOException lastException) throws EventApiException {

        if (attempt == this.maxAttempts) {
            throw new EventApiException(lastException != null ? "reached maximum number of attempts to send the event:"+lastException.getMessage() : "reached maximum number of attempts to send the event");
        }

        Collection<List<Object>> splittedEvents = splitEvents(purchases);
        boolean result = true;
        for (List<Object> events : splittedEvents) {
            boolean currRes = sendEventsBatch(events, 0, null);
            if (!currRes) {
                result = false;
            }
        }
        return false;

    }

    private boolean sendEventsBatch(List<Object> purchases, int attempt, IOException lastException) throws EventApiException {
        JSONObject batch = new JSONObject();
        batch.put("events", purchases);

        try {
            this.post(batch.toString());
            // all succeeded
            return true;
        } catch (IOException e) {
            attempt++;
            try {
                Thread.sleep(attempt * 1000L);
            } catch (InterruptedException e1) {
                // it seems, the process is being stopped, so, stop retrying
            }
            return sendEventsBatch(purchases, attempt, e);
        } catch (EventApiException e) {

            if (e.is202Accepted()) {
                return true;// do not report the error
            } else { // log error only for responses other than 202
                throw new EventApiException("Error sending events:" + e.getMessage());
            }
        }
    }


    private void acquirePermit() {

        if (this.rateLimiter != null) {
            this.rateLimiter.acquire();
        }
    }


}
