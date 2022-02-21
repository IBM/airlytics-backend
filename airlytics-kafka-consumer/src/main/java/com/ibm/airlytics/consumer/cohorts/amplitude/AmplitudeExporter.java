package com.ibm.airlytics.consumer.cohorts.amplitude;

import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlytics.consumer.amplitude.dto.AmplitudeEvent;
import com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeApiClient;
import com.ibm.airlytics.consumer.amplitude.forwarding.AmplitudeApiException;
import com.ibm.airlytics.consumer.amplitude.transformation.AmplitudeEventTransformer;
import com.ibm.airlytics.consumer.cohorts.AbstractCohortsExporter;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockException;
import com.ibm.airlytics.consumer.cohorts.amplitude.AmplitudeCohortsConsumerConfig;
import com.ibm.airlytics.consumer.cohorts.dto.ExportJobStatusReport;
import com.ibm.airlytics.consumer.cohorts.dto.JobStatusDetails;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import com.ibm.airlytics.utilities.SimpleRateLimiter;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class AmplitudeExporter extends AbstractCohortsExporter {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AmplitudeExporter.class.getName());

    public static final String AMPLITUDE_EXPORT_KEY = "Amplitude";

    private static final SimpleRateLimiter RATE_LIMITER = new SimpleRateLimiter(50_000, TimeUnit.MINUTES);

    private Map<String, AmplitudeApiClient> amplitudeClientPerProduct;

    private ExecutorService executor;

    private AtomicBoolean error = new AtomicBoolean(false);

    private int apiBatchSize;

    public AmplitudeExporter(AmplitudeCohortsConsumerConfig featureConfig) {
        super(featureConfig);

        if(featureConfig.isAmplitudeApiEnabled() && featureConfig.getAmplitudeApiKeys() == null) {
            throw new IllegalArgumentException("Invalid configuration: API key per product is missing");
        }
        amplitudeClientPerProduct = new HashMap<>();

        featureConfig.getAmplitudeApiKeys().forEach((productId, envVar) -> {

            if(StringUtils.isNotBlank(envVar) && System.getenv().containsKey(envVar)) {
                amplitudeClientPerProduct.put(
                        productId.toLowerCase(),
                        new AmplitudeApiClient(
                                featureConfig.getApiBaseUrl(),
                                featureConfig.getApiPath(),
                                System.getenv(envVar),
                                featureConfig.isAmplitudeApiEnabled(),
                                featureConfig.getHttpMaxConnections(),
                                featureConfig.getHttpKeepAlive(),
                                featureConfig.getHttpTimeout()));
            }
        });
        this.executor = Executors.newFixedThreadPool(featureConfig.getApiParallelThreads());
        this.apiBatchSize = featureConfig.getApiBatchSize();
    }

    public boolean sendExportedDeltasBatch(List<UserCohort> deltas) {

        if(deltas.isEmpty()) {
            return true;
        }
        long start = System.currentTimeMillis();

        List<Future<Boolean>> submitted = new LinkedList<>();

        Set<String> cohortNames = new LinkedHashSet<>();
        Map<String, Integer> progress = new HashMap<>();
        int total = 0;
        Map<String, List<UserCohort>> cohortsPerProduct = deltas.stream().collect(Collectors.groupingBy(UserCohort::getProductId));

        for(String productId : cohortsPerProduct.keySet()) {
            List<UserCohort> ucList = cohortsPerProduct.get(productId);
            Map<String, List<UserCohort>> cohortsPerUser =
                    ucList.stream().collect(Collectors.groupingBy(UserCohort::getUserId));
            List<AmplitudeEvent> batch = new ArrayList<>(apiBatchSize);

            for (String userId : cohortsPerUser.keySet()) {
                List<UserCohort> userCohorts = cohortsPerUser.get(userId);
                Map<String, Object> properties = new HashMap<>();

                for (UserCohort uc : userCohorts) {
                    Optional<UserCohortExport> export = getUserCohortExport(uc, AMPLITUDE_EXPORT_KEY);

                    if (export.isPresent()) {

                        if (StringUtils.isNotBlank(export.get().getOldFieldName())) {
                            properties.put(export.get().getOldFieldName(), null);
                            cohortNames.add(export.get().getOldFieldName());
                        }
                        properties.put(export.get().getExportFieldName(), uc.isPendingDeletion() ? null : getConvertedValue(uc));
                        total++;

                        if (!progress.containsKey(uc.getCohortId())) {
                            progress.put(uc.getCohortId(), 1);
                        } else {
                            int cnt = progress.get(uc.getCohortId());
                            progress.put(uc.getCohortId(), cnt + 1);
                        }
                    }
                }

                if (!properties.isEmpty()) {
                    batch.add(convert(userId, properties));
                }

                if (batch.size() == apiBatchSize) {
                    submitted.add(submitToAmplitude(batch, productId));
                    batch = new ArrayList<>(apiBatchSize);
                }
            }

            if (batch.size() > 0) {
                submitted.add(submitToAmplitude(batch, productId));
            }
        }

        if(!submitted.isEmpty()) {

            for(Future<Boolean> future : submitted) {

                try {
                    boolean success = future.get();

                    if(!success) {
                        error.set(true);
                    }
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                    error.set(true);
                    LOGGER.error("Error calling Amplitude API", e.getCause());
                }
            }
        }

        if(!error.get()) {
            reportAirlyticsSuccess(progress);
            LOGGER.info("Export for cohorts " + cohortNames + " took " + (System.currentTimeMillis() - start) + "ms, exporting " + total + " user-cohorts");
            usersCounter.labels(AMPLITUDE_EXPORT_KEY).inc(total);
        } else {
            close();
        }
        return !error.get();
    }

    public void close() {
        this.executor.shutdown();

        try {

            if (!this.executor.awaitTermination(1, TimeUnit.SECONDS)) {
                this.executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            this.executor.shutdownNow();
        }
    }

    private void reportAirlyticsSuccess(Map<String, Integer> progress) {

        if(MapUtils.isNotEmpty(progress)) {
            progress.forEach((cohortId, cnt) ->
                    this.executor.submit(() ->
                            reportAirlyticsProgress(cohortId, cnt, ExportJobStatusReport.JobStatus.COMPLETED)));
        }
    }

    private void reportAirlyticsProgress(String cohortId, int cnt, ExportJobStatusReport.JobStatus detailsStatus) {
        ExportJobStatusReport report = new ExportJobStatusReport();
        report.setExportKey(AMPLITUDE_EXPORT_KEY);
        report.setStatus(
                detailsStatus == ExportJobStatusReport.JobStatus.FAILED ?
                        ExportJobStatusReport.JobStatus.FAILED : ExportJobStatusReport.JobStatus.RUNNING);
        report.setStatusMessage("Batch sent to Amplitude");
        JobStatusDetails details = new JobStatusDetails();
        details.setStatus(detailsStatus);
        if (detailsStatus != ExportJobStatusReport.JobStatus.FAILED) {
            details.setSuccessfulImports(cnt);
        } else {
            details.setFailedImports(cnt);
        }
        report.setAirlyticsStatusDetails(details);

        if(this.airlockClient != null) {

            for (int i = 0; i < 3; i++) {

                try {
                    this.airlockClient.updateExportJobStatus(cohortId, report);
                    break;
                } catch (AirlockException e) {

                    if (i == 2) {
                        LOGGER.error("Error reporting Airlytics job status to Airlock, cohort " + cohortId, e);
                    } else {
                        LOGGER.warn("Error reporting Airlytics job status to Airlock, cohort " + cohortId + ". Retrying", e);

                        try {
                            Thread.sleep(10_000L);
                        } catch (InterruptedException interruptedException) {
                        }
                    }
                }
            }
        }
    }

    private AmplitudeEvent convert(String userId, Map<String, Object> properties) {
        AmplitudeEvent result = new AmplitudeEvent();
        //common properties
        result.setEvent_type(AmplitudeEventTransformer.AMP_TYPE_TRIGGER);
        result.setUser_id(userId);
        result.setDevice_id(userId);
        //user properties
        result.setUser_properties(properties);
        //event ptoperties
        result.setEvent_properties(Collections.emptyMap());
        result.setTime(Instant.now().toEpochMilli());
        result.setSession_id(-1L);

        return result;
    }

    private Future<Boolean> submitToAmplitude(List<AmplitudeEvent> batch, String productId) {

        return executor.submit(() -> callAmplitude(batch, productId));
    }

    private Boolean callAmplitude(List<AmplitudeEvent> batch, String productId)  throws AmplitudeApiException {

        AmplitudeApiClient client = amplitudeClientPerProduct.get(productId.toLowerCase());

        if(error.get()) {
            return false;
        }

        if(client == null) {
            LOGGER.warn("Client not configured for product " + productId);
            return false;
        }

        if(!RATE_LIMITER.tryAcquire()) {
            LOGGER.error("Amplitude upload rate limit exceeded");
            throw new AmplitudeApiException("Amplitude upload rate limit exceeded");
        }

        try {
            client.uploadEvents(batch);
        } catch (AmplitudeApiException e) {

            try {
                Thread.sleep(10_000L);
            } catch (InterruptedException interruptedException) {
            }

            // retry
            try {
                client.uploadEvents(batch);
            } catch (AmplitudeApiException amplitudeApiException) {
                error.set(true);
                throw amplitudeApiException;
            }
        }
        return true;
    }

    // for unit-testing
    public AmplitudeExporter() {
        this.executor = Executors.newFixedThreadPool(2);
    }

    // for unit-testing
    public void setAmplitudeClientPerProduct(Map<String, AmplitudeApiClient> amplitudeClientPerProduct) {
        this.amplitudeClientPerProduct = amplitudeClientPerProduct;
    }

    private Object getConvertedValue(UserCohort uc) {

        if(uc != null && uc.getCohortValue() != null) {
            Object value = uc.getCohortValue();

            if(uc.getValueType() != null) {

                switch(uc.getValueType()) {
                    case INT: value = convertIntValue(uc); break;
                    case FLOAT: value = convertFloatValue(uc); break;
                    case BOOL: value = convertBoolValue(uc); break;
                    case ARRAY: value = convertArrayValue(uc); break;
                }
            }
            return value;
        }
        return null;
    }
}
