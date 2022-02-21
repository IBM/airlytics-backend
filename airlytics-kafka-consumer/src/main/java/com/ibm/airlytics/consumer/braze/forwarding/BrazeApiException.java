package com.ibm.airlytics.consumer.braze.forwarding;

import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class BrazeApiException extends Exception {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(BrazeApiException.class.getName());

    private Response response;

    private String requestBody = null;

    private String responseBody = null;

    public BrazeApiException(String requestBody, Response response) {
        super();
        this.requestBody = requestBody;
        this.response = response;

        if(response != null) {

            try {
                responseBody = response.body().string(); // can be read only once!
            }
            catch(Exception e) {
                LOGGER.error("Missing response body from the Braze API");
            }
        }
    }

    public BrazeApiException(String message, Throwable cause) {
        super(message, cause);
    }

    public BrazeApiException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {

        if(isResponsePresent()) {
            return String.format("Braze API returned HTTP response %d", response.code());
        }

        if(StringUtils.isNotBlank(super.getMessage())) {
            return super.getMessage();
        }
        return "Error calling Braze API";
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
