package com.ibm.weather.airlytics.jobs.eventspatch.eventproxy;

import com.fasterxml.jackson.databind.JsonNode;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EventApiClient {
    private static final Logger logger = LoggerFactory.getLogger(EventApiClient.class);

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private static final List<String> basicHeaders =
            Collections.unmodifiableList(
                    Arrays.asList("x-cloned-event", "content-type", "connection", "x-api-key")
            );

    private static final int DEFAULT_CONNECT_TIMEOUT = 10;

    private static final int DEFAULT_READ_TIMEOUT = 30;

    private OkHttpClient client;

    private String eventApiBaseUrl;

    private String eventApiPath;

    private String eventApiKey;

    private boolean isClonedEvent;

    public EventApiClient(EventApiClientConfig config, String eventApiKey) {
        this(
                config.getEventApiBaseUrl(),
                config.getEventApiPath(),
                eventApiKey,
                config.getConnectTimeoutSeconds(),
                config.getReadTimeoutSeconds(),
                false);
    }

    public EventApiClient(EventApiClientConfig config) {
        this(
                config.getEventApiBaseUrl(),
                config.getEventApiPath(),
                config.getEventApiKey(),
                config.getConnectTimeoutSeconds(),
                config.getReadTimeoutSeconds(),
                false);
    }

    public EventApiClient(EventApiClientConfig config, boolean isClonedEvent) {
        this(
                config.getEventApiBaseUrl(),
                config.getEventApiPath(),
                config.getEventApiKey(),
                config.getConnectTimeoutSeconds(),
                config.getReadTimeoutSeconds(),
                isClonedEvent);
    }

    public EventApiClient(String eventApiBaseUrl, String eventApiPath, String eventApiKey) {
        this(
                eventApiBaseUrl,
                eventApiPath,
                eventApiKey,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_READ_TIMEOUT,
                false);
    }

    public EventApiClient(String eventApiBaseUrl, String eventApiPath, String eventApiKey, boolean isClonedEvent) {
        this(
                eventApiBaseUrl,
                eventApiPath,
                eventApiKey,
                DEFAULT_CONNECT_TIMEOUT,
                DEFAULT_READ_TIMEOUT,
                isClonedEvent);
    }

    public EventApiClient(String eventApiBaseUrl, String eventApiPath,
                          String eventApiKey, int connectTimeoutSeconds, int readTimeoutSeconds,
                          boolean isClonedEvent) {

        if (StringUtils.isNoneBlank(eventApiBaseUrl, eventApiPath)) {
            this.eventApiBaseUrl = eventApiBaseUrl;
            this.eventApiPath = eventApiPath;
            this.eventApiKey = eventApiKey;
            this.isClonedEvent = isClonedEvent;

            this.client = new OkHttpClient.Builder()
                    .connectTimeout(connectTimeoutSeconds, TimeUnit.SECONDS)    // establishing a connection timeout
                    .readTimeout(readTimeoutSeconds, TimeUnit.SECONDS) // A read timeout is applied from the moment the connection between
                    // a client and a target host has been successfully established.
                    .build();
        } else {
            logger.error("Invalid Proxy API configuration: eventApiBaseUrl = " + eventApiBaseUrl + ", eventApiPath = " + eventApiPath);
            throw new IllegalArgumentException("Event Proxy Api client requires the following configuration parameters: eventApiBaseUrl, eventApiPath");
        }
    }

    public void post(String json) throws IOException, EventApiException {
        Request.Builder requestBuilder = prebuildBasicRequest(json);

        doPost(requestBuilder);
    }

    public void post(JsonNode json) throws IOException, EventApiException {
        post(json.toString());
    }

    private Request.Builder prebuildBasicRequest(String json) {
        RequestBody body = RequestBody.create(JSON, json);
        Request.Builder requestBuilder = new Request.Builder()
                .url(eventApiBaseUrl + eventApiPath)
                .post(body)
                .header("x-cloned-event", isClonedEvent ? "true" : "false")
                .header("content-type","application/json")
                .header("Connection","close");

        if (StringUtils.isNotBlank(this.eventApiKey)) {
            requestBuilder.header("x-api-key", eventApiKey);
        }
        return requestBuilder;
    }

    private void doPost(Request.Builder requestBuilder) throws EventApiException, IOException {
        Request request = requestBuilder.build();

        try (Response response = client.newCall(request).execute()) {

            if (!response.isSuccessful() || response.code() == 202) { // in case of 202, the caller should check for any necessary retries
                throw new EventApiException(response);
            }
        } catch (IOException e) {
            throw e;
        }
    }
}
