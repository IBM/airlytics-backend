package com.ibm.weather.airlytics.jobs.eventspatch.eventproxy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.RateLimiter;
import com.ibm.weather.airlytics.common.dto.AirlyticsEvent;
import com.ibm.weather.airlytics.jobs.eventspatch.dto.EventsPatchAirlockConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class EventBatchSubmitter {

    private static final Logger logger = LoggerFactory.getLogger(EventBatchSubmitter.class);

    private static final AtomicInteger batchCount = new AtomicInteger(0);

    public static final AtomicInteger eventCount = new AtomicInteger(0);

    final private ObjectMapper mapper = new ObjectMapper();

    private EventsPatchAirlockConfig featureConfig;

    private EventApiClient proxyClient;

    private ExecutorService executor;

    private RateLimiter rateLimiter;

    public EventBatchSubmitter(EventsPatchAirlockConfig featureConfig) {
        this.featureConfig = featureConfig;
        this.executor = Executors.newFixedThreadPool(featureConfig.getEventProxyIntegrationConfig().getEventApiParallelThreads());

        if (featureConfig.getEventProxyIntegrationConfig().getEventApiRateLimit() > 0) {
            this.rateLimiter = RateLimiter.create(featureConfig.getEventProxyIntegrationConfig().getEventApiRateLimit());
        }

        if (featureConfig.getEventProxyIntegrationConfig().isEventApiEnabled()) {
            this.proxyClient = new EventApiClient(featureConfig.getEventApiClientConfig());
        }
    }

    public Future<Boolean> submitBatchToProxy(final List<AirlyticsEvent> currentBatch, boolean failOnValidationError) {
        eventCount.getAndAdd(currentBatch.size());
        return this.executor.submit(() -> sendCurrentBatchToProxy(currentBatch, false, failOnValidationError));
    }

    private boolean sendCurrentBatchToProxy(List<AirlyticsEvent> currentBatch, boolean is202Reply, boolean failOnValidationError) {

        if(currentBatch.isEmpty()) {
            return true;
        }
        String json = batchToJson(currentBatch);

        if(json == null) {
            return false;
        }
        int retryCounter = 0;
        IOException lastIoException = null;
        int cnt = batchCount.getAndIncrement();

        while(retryCounter < featureConfig.getEventProxyIntegrationConfig().getEventApiRetries()) {

            if(cnt % 1000 == 0) {
                logger.info("Sending " + currentBatch.size() + " events in batch " + cnt);
            }

            try {
                acquirePermit();

                if(proxyClient != null) {
                    proxyClient.post(json);

                    if(cnt % 1000 == 0) {
                        logger.info("We have sent " + currentBatch.size() + " events in batch " + cnt);
                    }
                } else {

                    if(cnt % 1000 == 0) {
                        logger.info("Event API is disabled - otherwise, we would send " + currentBatch.size() + " events in batch " + cnt);
                    }

                    if(cnt == 0) {
                        logger.info(json);
                    }
                }
                return true;
            } catch (IOException e) {
                retryCounter++;
                lastIoException = e;

                try {
                    Thread.sleep(retryCounter * 1000L);
                } catch (InterruptedException e1) {
                    // it seems, the process is being stopped, so, stop retrying
                }
            } catch (EventApiException e) {

                if (e.is202Accepted()) {

                    if (is202Reply || retryOn202(currentBatch, e, failOnValidationError)) {
                        return true;// do not fail on the error
                    }
                } else { // log error only for responses other than 202
                    logger.error("Error sending events: " + json + ":" + e.getMessage(), e);
                }
                retryCounter++;
            }
        }

        if (retryCounter >= featureConfig.getEventProxyIntegrationConfig().getEventApiRetries()) {

            if(lastIoException != null) {
                logger.error("I/O error sending events to proxy:" + lastIoException.getMessage(), lastIoException);
            } else {
                logger.error("Error sending events to proxy.");
            }
        }
        return false;
    }

    private boolean retryOn202(List<AirlyticsEvent> currentBatch, EventApiException e, boolean failOnError) {
        // some events may need to be retried
        if(e.is202Accepted() && e.getResponseBody() != null) {
            boolean foundErrors = false;

            try {
                String responseBody = e.getResponseBody();
                JsonNode responseJson = mapper.readTree(responseBody);

                if(responseJson instanceof ArrayNode) {
                    ArrayNode events = (ArrayNode)responseJson;
                    List<String> retries = new LinkedList<>();

                    for(int i = 0; i < events.size(); i++) {
                        JsonNode event = events.get(i);

                        if(event.has("eventId")) {

                            if (event.has("shouldRetry")) {

                                if ("true".equalsIgnoreCase(event.get("shouldRetry").asText())) {
                                    retries.add(event.get("eventId").textValue());
                                    continue;
                                }
                            }

                            if(event.has("error") && !foundErrors) {
                                foundErrors = true;

                                if(failOnError) {
                                    logger.warn("Event Proxy returned errors like: " + event.toString());
                                }
                            }
                        }
                    }

                    if(!retries.isEmpty()) {
                        List<AirlyticsEvent> newBatch =
                                currentBatch.stream()
                                        .filter(ev -> retries.contains(ev.getEventId()))
                                        .collect(Collectors.toList());
                        return sendCurrentBatchToProxy(newBatch, true, failOnError);
                    }
                    return !(failOnError && foundErrors);
                }
            }
            catch (Exception e1) {
                logger.error("Unrecognized response format from event API: " + e.getResponseBody());
            }
        }
        return false;
    }

    private String batchToJson(List<AirlyticsEvent> currentBatch) {
        ArrayNode events = mapper.createArrayNode();

        for(AirlyticsEvent e : currentBatch) {
            JsonNode node = mapper.valueToTree(e);
            events.add(node);
        }
        ObjectNode wrapper = mapper.createObjectNode();
        wrapper.set("events", events);

        try {
            return mapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            // safe to ignore
        }
        // generally unreachable
        return null;
    }

    private void acquirePermit() {

        if (this.rateLimiter != null) {
            this.rateLimiter.acquire();
        }
    }
}
