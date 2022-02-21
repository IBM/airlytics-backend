package com.ibm.airlytics.retentiontrackerqueryhandler.airlock;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import okhttp3.*;
import org.json.JSONObject;
import org.springframework.context.annotation.DependsOn;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilderFactory;

import java.io.IOException;
import java.net.URI;

@DependsOn({"ConfigurationManager","Airlock"})
public class AirlockClient {

    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(AirlockClient.class.getName());
    private static final String AUTHENTICATION_PATH = "/airlock/api/admin/authentication/startSessionFromKey";
    private static final String POLL_RESULTS_PATH_FORMAT = "/airlock/api/products/%s/polls/pollresults";
    private static final String AUTHENTICATION_HEADER_NAME = "sessionToken";

    private final ConfigurationManager configurationManager;
    private UriBuilderFactory uriBuilderFactory;
    private Boolean authenticationServer;
    private String token;

    static private OkHttpClient client = new OkHttpClient();

    public AirlockClient(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
        this.uriBuilderFactory = new DefaultUriBuilderFactory(configurationManager.getAirlockBaseURL());
        this.authenticationServer = configurationManager.getAirLockServerAuthentication();
        this.token = null;
    }

    void initToken() {
        String apiKey = configurationManager.getAirlockApiKey();
        String apiPassword = configurationManager.getAirlockApiPassword();
        JSONObject o = new JSONObject();
        o.put("key", apiKey);
        o.put("keyPassword", apiPassword);
        String jsonStr = o.toString();

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(jsonStr, JSON);
        URI startSessionFromKeyUrl = uriBuilderFactory.builder().path(AUTHENTICATION_PATH).build();

        Request request = new Request.Builder()
                .url(startSessionFromKeyUrl.toString())
                .post(body)
                .build();

        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                logger.debug("Success to getAirlockToken");
                token = response.body().string();
            } else {
                logger.error("Error in getAirlockToken, return code:" + response.code());
            }
        } catch (IOException e) {
            logger.error("Error in getAirlockToken:" + e.getMessage());
        } finally {
             if (response != null) {
                 response.close();
             }
        }
    }

    public void setPollResults(String resultsJSON) {
        if (configurationManager.getAirLockServerAuthentication()) {
            initToken();
        }

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(resultsJSON,JSON);
        String putResultsURL = getPutPollResultsURL();
        Request.Builder builder = new Request.Builder();
        if (token != null) {
            builder.addHeader(AUTHENTICATION_HEADER_NAME,token);
        }

        Request request = builder
                .url(putResultsURL)
                .put(body)
                .build();

        Response response = null;
        try {
            response = client.newCall(request).execute();
            if (response.isSuccessful()) {
                logger.debug("Success to setAirlockPollsResults");
            } else {
                logger.error("Error in setAirlockPollsResults, return code:" + response.code() + ", message: " + response.body().string());
            }
        } catch (IOException e) {
            logger.error("Error in setAirlockPollsResults:" + e.getMessage());
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    String getPutPollResultsURL() {
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.query.POLLS);
        String stage = feature.getConfiguration().getString("stage");
        String productId = configurationManager.getAirLockInternalProductId();
        String pollResultsPath = String.format(POLL_RESULTS_PATH_FORMAT,productId);
        URI pollResultsUrl = uriBuilderFactory.builder().path(pollResultsPath).queryParam("stage",stage).build();
        return pollResultsUrl.toString();
    }
}
