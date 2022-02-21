package com.ibm.airlytics.consumer.amplitude.forwarding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeUploadRequestBody;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeUploadResponseBody;
import com.ibm.airlytics.utilities.HttpClientUtils;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Histogram;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;

public class AmplitudeApiClient {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AmplitudeApiClient.class.getName());

    private static final MediaType JSON = MediaType.parse("application/json");

    private static final Histogram proxyResponseTimes = Histogram.build()
            .name("airlytics_amplitude_response_time_seconds")
            .help("Amplitude events API response times in seconds.").register();

    final private ObjectMapper mapper = new ObjectMapper();

    private boolean enabled;

    private OkHttpClient http;

    private String apiBaseUrl;

    private String apiPath;

    private String apiKey;

    public AmplitudeApiClient(String apiBaseUrl, String apiPath, String apiKey, boolean enabled, int httpMaxConnections, int httpKeepAlive, int httpTimeout) {
        if(StringUtils.isNoneBlank(apiBaseUrl, apiPath, apiKey)) {
            this.enabled = enabled;
            this.apiBaseUrl = apiBaseUrl;
            this.apiPath = apiPath;
            this.apiKey = apiKey;
            this.http = HttpClientUtils.buildHttpClient(httpMaxConnections, httpKeepAlive, httpTimeout);
            LOGGER.info("API key: " + this.apiKey.substring(0,1) + "..." + this.apiKey.substring(this.apiKey.length() - 3) );
        }
        else {
            throw new IllegalArgumentException(
                    "Amplitude consumer requires the following configuration parameters: " +
                            "amplitudeApiBaseUrl, amplitudeEventApiPath, amplitudeBatchApiPath, amplitudeApiKey");
        }
    }

    public AmplitudeApiClient(AmplitudeForwardingConsumerConfig config, boolean isForceBatchApi) {
        this(
                config.getAmplitudeApiBaseUrl(),
                isForceBatchApi ?
                        config.getAmplitudeBatchApiPath() : config.getAmplitudeEventApiPath(),
                config.getAmplitudeKey(),
                config.isAmplitudeIntegrationEnabled(),
                config.getHttpMaxConnections(),
                config.getHttpKeepAlive(),
                config.getHttpTimeout() );
    }

    public AmplitudeUploadResponseBody uploadEvents(List<AmplitudeEvent> events) throws AmplitudeApiException {

        try {
            AmplitudeUploadRequestBody bodyObj = new AmplitudeUploadRequestBody(apiKey, events);
            String json = mapper.writeValueAsString(bodyObj);

            if(enabled) {
                RequestBody body = RequestBody.create(JSON, json);
                Request request = new Request.Builder()
                        .url(apiBaseUrl + apiPath)
                        .post(body)
                        .build();
                Histogram.Timer timer = proxyResponseTimes.startTimer();

                try (Response response = http.newCall(request).execute()) {

                    if (!response.isSuccessful()) {
                        String k = this.apiKey.substring(0,1) + "..." + this.apiKey.substring(this.apiKey.length() - 3);
                        bodyObj = new AmplitudeUploadRequestBody(k, events);
                        json = mapper.writeValueAsString(bodyObj);// hide key
                        throw new AmplitudeApiException(json, response);
                    }
                    String responseBody = response.body().string();

                    try {
                        return mapper.readValue(responseBody, AmplitudeUploadResponseBody.class);
                    } catch (Exception e) {
                        LOGGER.warn("Error parsing Amplitude response " + responseBody, e);
                        // nevertheless, the operation was successful
                        return new AmplitudeUploadResponseBody(response.code());
                    }
                } catch (IOException e) {
                    throw new AmplitudeApiException("I/O error sending events: " + json + ":" + e.getMessage(), e);
                } finally {
                    timer.observeDuration();
                }
            } else {
                LOGGER.debug("If Amplitude integration was enabled the following JSON would be submitted to " + apiPath + ": " + json);
                return new AmplitudeUploadResponseBody(200);
            }
        } catch (JsonProcessingException e) {
            throw new AmplitudeApiException("Error producing request JSON", e);
        }
    }
}
