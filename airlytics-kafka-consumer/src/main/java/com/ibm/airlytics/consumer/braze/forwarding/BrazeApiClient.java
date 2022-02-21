package com.ibm.airlytics.consumer.braze.forwarding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.braze.dto.BrazeTrackingRequest;
import com.ibm.airlytics.consumer.braze.dto.BrazeTrackingResponse;
import com.ibm.airlytics.consumer.cohorts.braze.BrazeCohortsConsumerConfig;
import com.ibm.airlytics.utilities.HttpClientUtils;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Histogram;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;

public class BrazeApiClient {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeApiClient.class.getName());

    private static final MediaType JSON = MediaType.parse("application/json");
    private static final String AUTHENTICATION_HEADER_NAME = "Authorization";

    private static final Histogram proxyResponseTimes = Histogram.build()
            .name("airlytics_braze_response_time_seconds")
            .help("Braze events API response times in seconds.").register();

    final private ObjectMapper mapper = new ObjectMapper();

    private boolean enabled;

    private OkHttpClient http;

    private String apiBaseUrl;

    private String apiPath;

    private String apiKey;

    public BrazeApiClient(String apiBaseUrl, String apiPath, String apiKey, boolean enabled, int httpMaxConnections, int httpKeepAlive, int httpTimeout) {

        if(enabled && StringUtils.isAnyBlank(apiBaseUrl, apiPath, apiKey)) {
            throw new IllegalArgumentException(
                    "Braze consumer requires the following configuration parameters: " +
                            "brazeApiBaseUrl, brazeApiPath, brazeApiKey");
        }
        this.enabled = enabled;
        this.apiBaseUrl = apiBaseUrl;
        this.apiPath = apiPath;
        this.apiKey = apiKey;
        this.http = HttpClientUtils.buildHttpClient(httpMaxConnections, httpKeepAlive, httpTimeout);
        LOGGER.info("API key: " + this.apiKey.substring(0,1) + "..." + this.apiKey.substring(this.apiKey.length() - 3) );
    }

    public BrazeApiClient(BrazeForwardingConsumerConfig config) {
        this(
                config.getBrazeApiBaseUrl(),
                config.getBrazeApiPath(),
                config.getBrazeKey(),
                config.isBrazeIntegrationEnabled(),
                config.getHttpMaxConnections(),
                config.getHttpKeepAlive(),
                config.getHttpTimeout() );
    }

    public BrazeApiClient(BrazeCohortsConsumerConfig config, String brazeApiKey) {
        this(
                config.getApiBaseUrl(),
                config.getApiPath(),
                brazeApiKey,
                config.isBrazeApiEnabled(),
                config.getHttpMaxConnections(),
                config.getHttpKeepAlive(),
                config.getHttpTimeout() );
    }

    // for unit-testing
    public BrazeApiClient() {
        this.enabled = false;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public BrazeTrackingResponse uploadEvents(List<JsonNode> attributes, List<JsonNode> events, List<JsonNode> purchases) throws BrazeApiException {

        try {
            BrazeTrackingRequest bodyObj = new BrazeTrackingRequest(attributes, events, purchases);
            String json = mapper.writeValueAsString(bodyObj);

            if(enabled) {
                Request.Builder builder =
                        new Request.Builder()
                                .url(apiBaseUrl + apiPath);

                builder.addHeader(AUTHENTICATION_HEADER_NAME, "Bearer " + apiKey);
                RequestBody body = RequestBody.create(JSON, json);
                builder.post(body);
                Request request = builder.build();
                Histogram.Timer timer = proxyResponseTimes.startTimer();

                try (Response response = http.newCall(request).execute()) {

                    if (!response.isSuccessful()) {
                        throw new BrazeApiException(json, response);
                    }
                    String responseBody = response.body().string();

                    try {
                        return mapper.readValue(responseBody, BrazeTrackingResponse.class);
                    } catch (Exception e) {
                        LOGGER.warn("Error parsing Braze response " + responseBody, e);
                        // nevertheless, the operation was successful
                        return new BrazeTrackingResponse(response.code());
                    }
                } catch (IOException e) {
                    throw new BrazeApiException("I/O error sending events: " + json + ":" + e.getMessage(), e);
                } finally {
                    timer.observeDuration();
                }
            } else {
                LOGGER.debug("If Braze integration was enabled the following JSON would be submitted to " + apiPath + ": " + json);
                return new BrazeTrackingResponse(200);
            }
        } catch (JsonProcessingException e) {
            throw new BrazeApiException("Error producing request JSON", e);
        }
    }
}
