package com.ibm.airlytics.consumer.cohorts.airlock;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.airlytics.consumer.cohorts.dto.ExportJobStatusReport;
import com.ibm.airlytics.utilities.HttpClientUtils;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.jcraft.jsch.Logger;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Optional;

public class AirlockClient {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AirlockClient.class.getName());

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private static final int MAX_RETRIES = 3;
    private static final int HTTP_MAX_IDLE = 10;// connections
    private static final int HTTP_KEEP_ALIVE = 10;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds
    private static final long TOKEN_EXPIRATION_TIME = 60*60*1000L;//60 minutes
    private static final long TOKEN_RENEWAL_TIME = 50*60*1000L;//50 minutes

    private static final String AUTHENTICATION_PATH = "/admin/authentication/startSessionFromKey";
    private static final String EXTEND_PATH = "/admin/authentication/extend";
    private static final String COHORT_STATUS_PATH = "/admin/airlytics/cohorts/{cohort-id}/status";
    private static final String AUTHENTICATION_HEADER_NAME = "sessionToken";

    private AirlockCohortsConsumerConfig featureConfig;

    private OkHttpClient client;

    private String lastToken = null;
    private long lastTokenTime = 0L;

    public AirlockClient(AirlockCohortsConsumerConfig featureConfig) {
        this.featureConfig = featureConfig;
        this.client = HttpClientUtils.buildHttpClient(HTTP_MAX_IDLE, HTTP_KEEP_ALIVE, HTTP_CONNECTION_TIMEOUT);
    }

    public void updateExportJobStatus(String cohortId, ExportJobStatusReport status) throws AirlockException {
        String content = (new Gson()).toJson(status);

        if(featureConfig.isAirlockApiEnabled()) {
            callAirlockApi(cohortId, content, false);
        } else {
            LOGGER.info("Airlock reporting is disabled. Report: " + content);
        }
    }

    private void callAirlockApi(String cohortId, String content, boolean enforceAuth) throws AirlockException {
        String url = featureConfig.getAirlockApiBaseUrl() + COHORT_STATUS_PATH.replace("{cohort-id}", cohortId);
        String token = startSessionGetToken(enforceAuth);
        Request request = createPutRequestEntityWithAuth(
                url,
                content,
                token);
        LOGGER.info("PUT report to " + url + " : " + content);

        int retryCounter = 0;
        AirlockException lastException = null;

        while(retryCounter < MAX_RETRIES) {
            lastException = null;

            try (Response response = client.newCall(request).execute()) {

                if(response.code() == 401 && !enforceAuth) {
                    callAirlockApi(cohortId, content, true);
                } else if(response.code() == 400) {
                    LOGGER.warn("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": response " + response.code() + " from Airlock API");
                    break;// must be deletion
                } else if (!response.isSuccessful()) {
                    LOGGER.error("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": response " + response.code() + " from Airlock API");
                    // retry
                } else {
                    break;// success
                }
            } catch (Exception e) {
                lastException = new AirlockException("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);

                try {
                    Thread.sleep(retryCounter * 1000L);
                } catch (InterruptedException e1) {
                    // it seems, the process is being stopped, so, stop retrying
                }
            }
            retryCounter++;
        }

        if(lastException != null) {
            LOGGER.error("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", lastException.getCause());
            throw lastException;
        }
    }

    private String startSessionGetToken(boolean enforce) throws AirlockException {
        String token = null;
        String airlockKey = featureConfig.getAirlockKey();
        String airlockPassword = featureConfig.getAirlockPassword();

        if (StringUtils.isNoneBlank(airlockKey, airlockPassword)) {
            // auth required
            token = getAuthToken(enforce).orElseThrow(() -> new AirlockException("Airlock authentication failed"));
        } else if (!StringUtils.isAllBlank(airlockKey, airlockPassword)) {
            throw new AirlockException("Airlock authentication is mis-configured");
        }
        return token;
    }

    private Request createGetRequestEntityWithAuth(String url, String token) {
        Request.Builder builder = new Request.Builder().url(url);

        if(StringUtils.isNotBlank(token)) {
            builder.addHeader(AUTHENTICATION_HEADER_NAME, token);
        }

        return builder.build();
    }

    private Request createPutRequestEntityWithAuth(String url, String json, String token) {
        Request.Builder builder = new Request.Builder().url(url);

        if(StringUtils.isNotBlank(token)) {
            builder.addHeader(AUTHENTICATION_HEADER_NAME, token);
        }
        RequestBody body = RequestBody.create(JSON, json);
        builder.put(body);

        return builder.build();
    }

    private Optional<String> getAuthToken(boolean enforce) throws AirlockException {

        if(!enforce && lastToken != null && (System.currentTimeMillis() - lastTokenTime) < TOKEN_RENEWAL_TIME) {
            return Optional.of(lastToken);
        }
        Request request = null;

        if(!enforce && lastToken != null && (System.currentTimeMillis() - lastTokenTime) < TOKEN_EXPIRATION_TIME) {
            Request.Builder builder = new Request.Builder().url(featureConfig.getAirlockApiBaseUrl() + EXTEND_PATH);
            builder.addHeader(AUTHENTICATION_HEADER_NAME, lastToken);
            request = builder.build();
        } else {
            String airlockKey = featureConfig.getAirlockKey();
            String airlockPassword = featureConfig.getAirlockPassword();

            JsonObject content = new JsonObject();
            content.addProperty("key", airlockKey);
            content.addProperty("keyPassword", airlockPassword);

            RequestBody body = RequestBody.create(JSON, content.toString());
            request = new Request.Builder()
                    .url(featureConfig.getAirlockApiBaseUrl() + AUTHENTICATION_PATH)
                    .post(body)
                    .build();
        }

        try (Response response = client.newCall(request).execute()) {

            if(response.code() == 401 && !enforce) {
                return getAuthToken(true);
            } else if (response.isSuccessful()) {
                lastToken = response.body().string();
                lastTokenTime = System.currentTimeMillis();
                return Optional.ofNullable(lastToken);
            } else {
                LOGGER.error("Authentication failed: response " + response.code() + " from Airlock API");
            }
        } catch(Exception e) {
            LOGGER.error("Authentication failed: error calling Airlock API", e);
            throw new AirlockException("Authentication failed: error calling Airlock API", e);
        }
        return Optional.empty();
    }
}
