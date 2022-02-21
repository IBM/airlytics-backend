package com.ibm.weather.airlytics.amplitude.client;

import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AmplitudeApiException extends Exception {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeApiException.class.getName());

    private Response response;

    private String requestBody = null;

    private String responseBody = null;

    public AmplitudeApiException(String requestBody, Response response) {
        super();
        this.requestBody = requestBody;
        this.response = response;

        if(response != null) {

            try {
                responseBody = response.body().string(); // can be read only once!
            }
            catch(Exception e) {
                LOGGER.error("Missing response body from the Amplitude API");
            }
        }
    }

    public AmplitudeApiException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getMessage() {

        if(isResponsePresent()) {
            return String.format("Amplitude API returned HTTP response %d", response.code());
        }

        if(StringUtils.isNotBlank(super.getMessage())) {
            return super.getMessage();
        }
        return "Error calling Amplitude API";
    }

    public boolean isResponsePresent() {
        return response != null;
    }

    public int getResponseCode() {

        if(isResponsePresent()) {
            return response.code();
        }
        return 0;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public String getRequestBody() {
        return requestBody;
    }

    public boolean isIOException() {
        return getCause() != null && getCause() instanceof IOException;
    }
}
