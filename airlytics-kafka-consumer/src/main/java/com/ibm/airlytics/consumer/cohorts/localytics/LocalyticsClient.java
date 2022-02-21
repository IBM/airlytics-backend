package com.ibm.airlytics.consumer.cohorts.localytics;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.airlytics.consumer.cohorts.dto.ExportJobStatusReport;
import com.ibm.airlytics.consumer.cohorts.dto.JobStatusDetails;
import com.ibm.airlytics.utilities.HttpClientUtils;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Optional;

public class LocalyticsClient {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(LocalyticsClient.class.getName());

    public static final String LOCALYTICS_EXPORT = "Localytics";
    public static final String RESPONSE_ACTIVITY_ID = "activity_id";
    private static final String IMPORT_PROFILES_PATH = "/imports/profiles/upload";
    private static final String IMPORT_PROFILES_STATUS_PATH = "/imports/profiles/status/";

    private static final int HTTP_MAX_IDLE = 1;// connections
    private static final int HTTP_KEEP_ALIVE = 10;// seconds
    private static final int HTTP_CONNECTION_TIMEOUT = 30;// seconds

    private static final long MAX_FILE_SIZE = 100_000_000L;

    private final LocalyticsCohortsConsumerConfig featureConfig;

    private final OkHttpClient okHttp;

    public LocalyticsClient(LocalyticsCohortsConsumerConfig featureConfig) {
        this.featureConfig = featureConfig;
        this.okHttp = HttpClientUtils.buildHttpClient(HTTP_MAX_IDLE, HTTP_KEEP_ALIVE, HTTP_CONNECTION_TIMEOUT);
        LOGGER.info("API key: " + featureConfig.getLocalyticsKey().substring(0,1) + "..." + featureConfig.getLocalyticsKey().substring(featureConfig.getLocalyticsKey().length() - 3) );
        LOGGER.info("API secret: " + featureConfig.getLocalyticsSecret().substring(0,1) + "..." + featureConfig.getLocalyticsSecret().substring(featureConfig.getLocalyticsSecret().length() - 3) );
    }

    public boolean isApiEnabled() {
        return featureConfig.isLocalyticsApiEnabled();
    }

    /**
     * Uploads profiles batch and returns Localytics activity ID
     *
     * @param profileFileName
     * @param profilesFilePath
     * @param productId
     * @return Localytics activity ID
     * @throws LocalyticsException
     */
    public Optional<String> uploadProfiles(String profileFileName, String profilesFilePath, String productId) throws LocalyticsException {

        try {
            MultipartBody.Builder bodyBuilder = new MultipartBody.Builder()
                    .setType(MultipartBody.FORM);

            String localyticsAppId = StringUtils.isBlank(productId) ? null : featureConfig.getLocalyticsAppIds().get(productId);

            if (StringUtils.isNotBlank(localyticsAppId)) {
                bodyBuilder.addFormDataPart("app_id", localyticsAppId);
            } else {
                bodyBuilder.addFormDataPart("org_id", featureConfig.getLocalyticsOrgId());
            }

            if (profilesFilePath.endsWith(".csv")) {
                bodyBuilder.addFormDataPart("format", "csv");
            } else {
                bodyBuilder.addFormDataPart("format", "json");
            }
            File f = new File(profilesFilePath);
            long fileSize = f.length();

            if(MAX_FILE_SIZE < fileSize) {
                LOGGER.error("Uploaded file "+profileFileName+" exceeds Localytics size limit");
                //throw ?
            }

            bodyBuilder.addFormDataPart(
                    "file",
                    profileFileName,
                    RequestBody.create(
                            MediaType.parse("application/octet-stream"),
                            f));
            RequestBody requestBody = bodyBuilder.build();

            Request request = new Request.Builder()
                    .url(featureConfig.getApiBaseUrl() + IMPORT_PROFILES_PATH)
                    .addHeader("Authorization", Credentials.basic(featureConfig.getLocalyticsKey(), featureConfig.getLocalyticsSecret()))
                    .post(requestBody)
                    .build();
            Call call = okHttp.newCall(request);

            LOGGER.info("Uploading " + profileFileName + ", size " + fileSize + "B to Localytics");

            try (Response response = call.execute()) {

                if (response != null && response.isSuccessful()) {
                    String body = response.body().string();

                    if (StringUtils.isNotBlank(body)) {
                        JsonObject json = new Gson().fromJson(body, JsonObject.class);

                        if (json.has(RESPONSE_ACTIVITY_ID)) {
                            return Optional.ofNullable(json.get(RESPONSE_ACTIVITY_ID).getAsString());
                        }
                    } else {
                        LOGGER.error("Authentication failed: response "+response.code()+" from Localytics API");
                        throw new LocalyticsException("Authentication failed: response " + response.code() + " from Localytics API");
                    }
                }
            }
        } catch (LocalyticsException e) {
            throw e;
        } catch (Exception e) {
            LOGGER.error("Uploading profiles to Localytics failed for file " + profilesFilePath + ": error calling Localytics API", e);
            throw new LocalyticsException("Uploading profiles to Localytics failed for file " + profilesFilePath + ": error calling Localytics API", e);
        }
        return Optional.empty();

    }

    public Optional<ExportJobStatusReport> getJobStatus(String activityId) throws LocalyticsException {

        try {
            String url = featureConfig.getApiBaseUrl() + IMPORT_PROFILES_STATUS_PATH + activityId;
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", Credentials.basic(featureConfig.getLocalyticsKey(), featureConfig.getLocalyticsSecret()))
                    .build();
            Call call = okHttp.newCall(request);

            try (Response response = call.execute()) {

                if (response.isSuccessful()) {
                    String body = response.body().string();

                    if (StringUtils.isNotBlank(body)) {
                        LOGGER.info("Upload status from Localytics: " + body);
                        JsonObject json = new Gson().fromJson(body, JsonObject.class);

                        if (json.has("status")) {
                            String s = json.get("status").getAsString();
                            ExportJobStatusReport report = new ExportJobStatusReport();
                            report.setExportKey(LOCALYTICS_EXPORT);
                            report.setStatus(ExportJobStatusReport.JobStatus.RUNNING);
                            report.setStatusMessage(s);
                            JobStatusDetails details = new JobStatusDetails(activityId);
                            details.setStatus(ExportJobStatusReport.JobStatus.valueOf(s.toUpperCase()));

                            if (report.getStatus() == ExportJobStatusReport.JobStatus.COMPLETED && json.has("upload_statistics")) {
                                JsonObject stats = json.getAsJsonObject("upload_statistics");

                                if (stats.has("successful_imports")) {
                                    details.setSuccessfulImports(stats.get("successful_imports").getAsInt());
                                }
                                if (stats.has("parsed_imports")) {
                                    details.setParsedImports(stats.get("parsed_imports").getAsInt());
                                }
                                if (stats.has("failed_imports")) {
                                    details.setFailedImports(stats.get("failed_imports").getAsInt());
                                }
                                if (stats.has("total_number_of_imports")) {
                                    details.setTotalImports(stats.get("total_number_of_imports").getAsInt());
                                }
                            }
                            report.setThirdPartyStatusDetails(details);

                            return Optional.of(report);
                        }
                    }
                } else {
                    LOGGER.error("Authentication failed: response " + response.code() + " from Airlock API");
                }
            }

        } catch (Exception e) {
            LOGGER.error("Failed to obtain Localytics profile import status for activity " + activityId + ": error calling Localytics API", e);
            throw new LocalyticsException("Failed to obtain Localytics profile import status for activity " + activityId + ": error calling Localytics API", e);
        }
        return Optional.empty();
    }

    private int responseCount(Response response) {
        int result = 1;
        while ((response = response.priorResponse()) != null) {
            result++;
        }
        return result;
    }
}
