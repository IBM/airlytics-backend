package com.ibm.weather.airlytics.cohorts.integrations;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.services.AirCohortsConfigService;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockClient;
import com.ibm.weather.airlytics.common.rest.RetryableRestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.net.URI;
import java.util.Optional;

@Component
public class AirlockCohortsClient extends BaseAirlockClient {

    private static final Logger logger = LoggerFactory.getLogger(AirlockCohortsClient.class);

    private static final String PRODUCT_COHORTS_PATH = "/admin/airlytics/products/{product-id}/cohorts";

    private static final String COHORT_PATH = "/admin/airlytics/cohorts/{cohort-id}";

    private static final String COHORT_STATUS_PATH = "/admin/airlytics/cohorts/{cohort-id}/status";

    private AirCohortsConfigService featureConfigService;

    @Autowired
    public AirlockCohortsClient(AirCohortsConfigService featureConfigService, RetryableRestTemplate restTemplate) {
        super(restTemplate);
        this.featureConfigService = featureConfigService;
    }

    @Override
    public void initBaseUrl() throws AirlockException {
        AirCohortsAirlockConfig config = getAirlockConfig();
        this.airlockBaseUrl = config.getAirlockApiBaseUrl();
    }

    public Optional<ProductCohortsConfig> fetchAirlockProductConfig(String productId) throws AirlockException {
        return fetchAirlockProductConfig(productId, false);
    }

    public Optional<ProductCohortsConfig> fetchAirlockProductConfig(String productId, boolean enforceAuth) throws AirlockException {

        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createEmptyRequestEntityWithAuth(token, null);

            URI url = uriBuilderFactory.builder().path(PRODUCT_COHORTS_PATH).build(productId);

            ResponseEntity<ProductCohortsConfig> responseEntity = restTemplate.execute(url, HttpMethod.GET, request, ProductCohortsConfig.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                return fetchAirlockProductConfig(productId, true);
            } else if (responseEntity.getStatusCode().is2xxSuccessful()) {
                return Optional.ofNullable(responseEntity.getBody());
            } else {
                logger.error("Fetching Airlock config failed for product {}: response {} from Airlock API", productId, responseEntity.getStatusCodeValue());
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                return fetchAirlockProductConfig(productId, true);
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

    public Optional<CohortConfig> fetchAirlockCohortConfig(String cohortId) throws AirlockException {
        return fetchAirlockCohortConfig(cohortId, false);
    }

    public Optional<CohortConfig> fetchAirlockCohortConfig(String cohortId, boolean enforceAuth) throws AirlockException {

        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createEmptyRequestEntityWithAuth(token, null);

            URI url = uriBuilderFactory.builder().path(COHORT_PATH).build(cohortId);

            ResponseEntity<CohortConfig> responseEntity = restTemplate.execute(url, HttpMethod.GET, request, CohortConfig.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                return fetchAirlockCohortConfig(cohortId, true);
            } else if (responseEntity.getStatusCode().is2xxSuccessful()) {
                return Optional.ofNullable(responseEntity.getBody());
            } else {
                logger.error("Fetching Airlock config failed for cohort {}: response {} from Airlock API", cohortId, responseEntity.getStatusCodeValue());
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                return fetchAirlockCohortConfig(cohortId, true);
            } else {
                logger.error("Fetching Airlock config failed for cohort " + cohortId + ": error calling Airlock API", e);
                throw new AirlockException("Fetching Airlock config failed for cohort " + cohortId + ": error calling Airlock API", e);
            }
        } catch(Exception e) {
            logger.error("Fetching Airlock config failed for cohort " + cohortId + ": error calling Airlock API", e);
            throw new AirlockException("Fetching Airlock config failed for cohort " + cohortId + ": error calling Airlock API", e);
        }
        return Optional.empty();
    }

    public void updateJobStatus(String cohortId, BasicJobStatusReport status) throws AirlockException {
        updateJobStatus(cohortId, status, false);
    }

    public void updateJobStatus(String cohortId, BasicJobStatusReport status, boolean enforceAuth) throws AirlockException {

        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createRequestEntityWithAuth((new Gson()).toJson(status), token, null);

            URI url = uriBuilderFactory.builder().path(COHORT_STATUS_PATH).build(cohortId);

            ResponseEntity<Void> responseEntity = restTemplate.execute(url, HttpMethod.PUT, request, Void.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                updateJobStatus(cohortId, status, true);
            } else if(responseEntity.getStatusCodeValue() == 400) {
                logger.warn("Updating Cohort Export status in Airlock failed for cohort {}: response {} from Airlock API", cohortId, responseEntity.getStatusCodeValue());
            }  else if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                logger.error("Updating Cohort Export status in Airlock failed for cohort {}: response {} from Airlock API", cohortId, responseEntity.getStatusCodeValue());
            } else {
                logger.info("Cohort Export status in Airlock updated to {} for cohort {}", status.getStatus(), cohortId);
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                updateJobStatus(cohortId, status, true);
            } else if(e.getStatusCode().value() == 400) {
                logger.warn("Updating Cohort Export status in Airlock failed for cohort {}: response {} from Airlock API", cohortId, e.getStatusCode().value());
            } else {
                logger.error("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);
                throw new AirlockException("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);
            }
        } catch(Exception e) {
            logger.error("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);
            throw new AirlockException("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);
        }
    }

    @Async("airlockUpdateAsyncExecutor")
    public void asyncUpdateJobStatus(String cohortId, BasicJobStatusReport status) {
        try {
            updateJobStatus(cohortId, status);
        } catch(Exception e) {
            logger.error("Updating Cohort Export status in Airlock failed for cohort " + cohortId + ": error calling Airlock API", e);
        }
    }

    private AirCohortsAirlockConfig getAirlockConfig() throws AirlockException {
        Optional<AirCohortsAirlockConfig> optConfig = featureConfigService.getCurrentConfig();

        if(!optConfig.isPresent()) {
            throw new AirlockException("Error reading Airlock feature config");
        }
        return optConfig.get();
    }
}
