package com.ibm.weather.airlytics.braze.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.braze.dto.BrazeTrackingRequest;
import com.ibm.weather.airlytics.braze.dto.BrazeTrackingResponse;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
public class BrazeApiClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrazeApiClient.class);

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final int MAX_THREADS = 100;

    private static final MediaType JSON = MediaType.parse("application/json");
    private static final String AUTHENTICATION_HEADER_NAME = "Authorization";

    final private ObjectMapper mapper = new ObjectMapper();

    @Value("${braze.api.enabled:false}")
    private boolean enabled;

    @Value("${braze.api.base:}")
    private String apiBaseUrl;

    @Value("${braze.api.path:}")
    private String apiPath;

    @Value("${braze.api.key:}")
    private String apiKey;

    private OkHttpClient http;

    @PostConstruct
    public void init() {

        if(enabled && StringUtils.isAnyBlank(apiBaseUrl, apiPath, apiKey)) {
            throw new IllegalArgumentException(
                    "Braze consumer requires the following configuration parameters: " +
                            "brazeApiBaseUrl, brazeApiPath, brazeApiKey");
        }
        this.http = HttpClientUtils.buildHttpClient(HTTP_MAX_IDLE, HTTP_KEEP_ALIVE, HTTP_CONNECTION_TIMEOUT);
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
