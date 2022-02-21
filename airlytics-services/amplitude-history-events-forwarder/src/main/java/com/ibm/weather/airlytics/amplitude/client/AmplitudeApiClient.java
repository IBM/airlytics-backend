package com.ibm.weather.airlytics.amplitude.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeUploadRequestBody;
import com.ibm.weather.airlytics.amplitude.dto.AmplitudeUploadResponseBody;
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
public class AmplitudeApiClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeApiClient.class.getName());

    private static final int HTTP_MAX_IDLE = 50;// connections
    private static final int HTTP_KEEP_ALIVE = 30;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final MediaType JSON = MediaType.parse("application/json");

    final private ObjectMapper mapper = new ObjectMapper();

    @Value("${amplitude.api.enabled:false}")
    private boolean enabled;

    @Value("${amplitude.api.base:}")
    private String apiBaseUrl;

    @Value("${amplitude.api.path:}")
    private String apiPath;

    @Value("${amplitude.api.key:}")
    private String apiKey;

    private OkHttpClient http;

    @PostConstruct
    public void init() {

        if(enabled && StringUtils.isAnyBlank(apiBaseUrl, apiPath, apiKey)) {
            throw new IllegalArgumentException(
                    "Amplitude history upload tool requires the following configuration parameters: " +
                            "apiBaseUrl, apiPath, apiKey");
        }
        this.http = HttpClientUtils.buildHttpClient(HTTP_MAX_IDLE, HTTP_KEEP_ALIVE, HTTP_CONNECTION_TIMEOUT);
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

                try (Response response = http.newCall(request).execute()) {

                    if (!response.isSuccessful()) {
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
                    throw new AmplitudeApiException("I/O error sending events:" + e.getMessage(), e);
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
