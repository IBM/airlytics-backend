package com.ibm.weather.airlytics.jobs.aggregate.services;

import com.google.gson.Gson;
import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.common.airlock.BaseAirlockClient;
import com.ibm.weather.airlytics.common.rest.RetryableRestTemplate;
import com.ibm.weather.airlytics.jobs.aggregate.dto.DataImportConfig;
import com.ibm.weather.airlytics.jobs.aggregate.dto.EventAggregatorAirlockConfig;
import com.ibm.weather.airlytics.jobs.aggregate.dto.SecondaryAggregationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;

@Component
public class AirlockDataImportClient extends BaseAirlockClient {

    private static final Logger logger = LoggerFactory.getLogger(AirlockDataImportClient.class);

    private static final String JOBS_PATH = "/admin/airlytics/products/{product-id}/dataimport";

    @Value("${airlock.airlytics.product}")
    private String airlockProduct;

    @Value("${airlock.airlytics.env}")
    private String airlockEnv;

    @Value("${airlock.airlytics.deployment}")
    private String airlockDeplyment;

    @Value("${airlock.airlytics.aggregation}")
    private String airlockAggregation;

    private EventAggregatorConfigService featureConfigService;

    @Autowired
    public AirlockDataImportClient(EventAggregatorConfigService featureConfigService, RetryableRestTemplate restTemplate) {
        super(restTemplate);
        this.featureConfigService = featureConfigService;
    }

    @Override
    public void initBaseUrl() throws AirlockException {
        EventAggregatorAirlockConfig featureConfig = featureConfigService.getAirlockConfig();
        this.airlockBaseUrl = featureConfig.getAirlockApiBaseUrl();
    }

    public void createDataImport(EventAggregatorAirlockConfig featureConfig, String s3Path) throws AirlockException {
        DataImportConfig job = new DataImportConfig();
        job.setName(createJobName());
        job.setCreator(airlockAggregation + " Aggregator");
        job.setS3File(s3Path);
        job.setTargetTable(featureConfig.getImportTargetTable());
        job.setDevUsers(featureConfig.isDevUsers());
        job.setAffectedColumns(getColumns(featureConfig));
        String jobJson = (new Gson()).toJson(job);

        createDataImport(jobJson, featureConfig.getAirlockProductId(), false);
    }

    public void createDataImport(String jobJson, String productId, boolean enforceAuth) throws AirlockException {

        try {
            String token = startSessionGetToken(enforceAuth);
            HttpEntity request = createRequestEntityWithAuth(jobJson, token, null);

            URI url = uriBuilderFactory.builder().path(JOBS_PATH).build(productId);
            logger.info("Submitting import job {}", jobJson);

            ResponseEntity<Void> responseEntity = restTemplate.execute(url, HttpMethod.POST, request, Void.class);

            if (responseEntity.getStatusCode().is4xxClientError() && !enforceAuth) {
                createDataImport(jobJson, productId, true);
            } else if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                logger.error(
                        "Creating import job in Airlock failed for product {} job {}: response {} from Airlock API",
                        productId, jobJson, responseEntity.getStatusCodeValue());
            }
        } catch(HttpClientErrorException e) {

            if(e.getStatusCode().is4xxClientError() && !enforceAuth) {
                createDataImport(jobJson, productId, true);
            } else {
                logger.error("Creating import job in Airlock failed for job " + jobJson + ": error calling Airlock API", e);
                throw new AirlockException("Creating import job in Airlock failed for job " + jobJson + ": error calling Airlock API", e);
            }
        } catch(Exception e) {
            logger.error("Creating import job in Airlock failed for job " + jobJson + ": error calling Airlock API", e);
            throw new AirlockException("Creating import job in Airlock failed for job " + jobJson + ": error calling Airlock API", e);
        }
    }

    private String createJobName() {
        return airlockAggregation + " " + airlockProduct + " " + airlockEnv + " " + LocalDate.now().toString().replaceAll("\\-", "");
    }

    private List<String> getColumns(EventAggregatorAirlockConfig featureConfig) {
        List<String> result = new LinkedList<>();
        result.add("shard");
        result.add(featureConfig.getTargetColumn() + "_d1");
        result.add("cnt_d1");

        for (SecondaryAggregationConfig aggregationConfig : featureConfig.getSecondaryAggregations()) {
            if(aggregationConfig.getAggregationWindowDays() > 3650) {
                result.add(featureConfig.getTargetColumn() + "_total");
                result.add("cnt_total");
            } else {
                result.add(featureConfig.getTargetColumn() + "_d" + aggregationConfig.getAggregationWindowDays());
                result.add("cnt_d" + aggregationConfig.getAggregationWindowDays());
            }
        }
        return result;
    }

}
