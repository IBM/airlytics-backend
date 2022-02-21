package com.ibm.weather.airlytics.dataimport.integrations;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockClient;
import com.ibm.weather.airlytics.common.rest.RetryableRestTemplate;
import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.GetDataImportsResponse;
import com.ibm.weather.airlytics.dataimport.dto.JobStatusReport;
import com.ibm.weather.airlytics.dataimport.services.DataImportConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.net.URI;
import java.util.Optional;

@Component
public class AirlockDataImportClient extends BaseAirlockClient {

    private static final Logger logger = LoggerFactory.getLogger(AirlockDataImportClient.class);

    private static final String JOBS_PATH = "/admin/airlytics/products/{product-id}/dataimport";

    private static final String JOB_STATUS_PATH = "/admin/airlytics/products/{product-id}/dataimport/{job-id}/status";

    private DataImportConfigService featureConfigService;

    @Autowired
    public AirlockDataImportClient(DataImportConfigService featureConfigService, RetryableRestTemplate restTemplate) {
        super(restTemplate);
        this.featureConfigService = featureConfigService;
    }

    @Override
    public void initBaseUrl() throws AirlockException {
        DataImportAirlockConfig config = getAirlockConfig();
        this.airlockBaseUrl = config.getAirlockApiBaseUrl();
    }

    public Optional<GetDataImportsResponse> getJobs(String productId) throws AirlockException {
        return getJobs(productId, false);
    }

    public Optional<GetDataImportsResponse> getJobs(String productId, boolean enforceAuth) throws AirlockException {
        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createEmptyRequestEntityWithAuth(token, null);

            URI url = uriBuilderFactory.builder().path(JOBS_PATH).build(productId);

            ResponseEntity<GetDataImportsResponse> responseEntity = restTemplate.execute(url, HttpMethod.GET, request, GetDataImportsResponse.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                return getJobs(productId, true);
            } else if (responseEntity.getStatusCode().is2xxSuccessful()) {
                return Optional.ofNullable(responseEntity.getBody());
            } else {
                logger.error("Fetching Airlock config failed for product {}: response {} from Airlock API", productId, responseEntity.getStatusCodeValue());
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                return getJobs(productId, true);
            } else {
                logger.error("Fetching Airlock config failed for product " + productId + ": error calling Airlock API", e);
                throw new AirlockException("Fetching Airlock config failed for product " + productId + ": error calling Airlock API", e);
            }
        } catch(Exception e) {
            logger.error("Fetching Airlock config failed for product " + productId + ": error calling Airlock API", e);
            throw new AirlockException("Fetching Airlock config failed for product " + productId + ": error calling Airlock API", e);
        }
        return Optional.empty();
    }

    public void updateJobStatus(String productId, String jobId, JobStatusReport status) throws AirlockException {
        updateJobStatus(productId, jobId, status, false);
    }

    public void updateJobStatus(String productId, String jobId, JobStatusReport status, boolean enforceAuth) throws AirlockException {
        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createRequestEntityWithAuth((new Gson()).toJson(status), token, null);

            URI url = uriBuilderFactory.builder().path(JOB_STATUS_PATH).build(productId, jobId);

            ResponseEntity<Void> responseEntity = restTemplate.execute(url, HttpMethod.PUT, request, Void.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                updateJobStatus(productId, jobId, status, true);
            } else if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                logger.error(
                        "Updating user features import status in Airlock failed for product {} job {}: response {} from Airlock API",
                        productId, jobId, responseEntity.getStatusCodeValue());
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                updateJobStatus(productId, jobId, status, true);
            } else {
                logger.error("Updating user features import status in Airlock failed for job " + jobId + ": error calling Airlock API", e);
                throw new AirlockException("Updating user features import status in Airlock failed for job " + jobId + ": error calling Airlock API", e);
            }
        } catch(Exception e) {
            logger.error("Updating user features import status in Airlock failed for job " + jobId + ": error calling Airlock API", e);
            throw new AirlockException("Updating user features import status in Airlock failed for job " + jobId + ": error calling Airlock API", e);
        }
    }

    @Async
    public void asyncUpdateJobStatus(String productId, String jobId, JobStatusReport status) {
        try {
            updateJobStatus(productId, jobId, status);
        } catch(Exception e) {
            logger.error("Updating user features import status in Airlock failed for job " + jobId + ": error calling Airlock API", e);
        }
    }

    private DataImportAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<DataImportAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }
}
