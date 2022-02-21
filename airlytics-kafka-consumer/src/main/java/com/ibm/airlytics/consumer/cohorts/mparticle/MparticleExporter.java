package com.ibm.airlytics.consumer.cohorts.mparticle;

import com.google.common.util.concurrent.RateLimiter;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.cohorts.AbstractCohortsExporter;
import com.ibm.airlytics.consumer.cohorts.airlock.AirlockException;
import com.ibm.airlytics.consumer.cohorts.dto.ExportJobStatusReport;
import com.ibm.airlytics.consumer.cohorts.dto.JobStatusDetails;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohort;
import com.ibm.airlytics.consumer.cohorts.dto.UserCohortExport;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.mparticle.ApiClient;
import com.mparticle.client.EventsApi;
import com.mparticle.model.Batch;
import com.mparticle.model.CustomEvent;
import com.mparticle.model.CustomEventData;
import com.mparticle.model.UserIdentities;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Call;
import retrofit2.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MparticleExporter extends AbstractCohortsExporter {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(MparticleExporter.class.getName());

    public static final String MPARTICLE_EXPORT_KEY = "mParticle";

    private static final String USER_ATTRIBUTES_EVENT = "user-cohorts-updated";

    private static final int BULK_SIZE = 100;

    private MparticleCohortsConsumerConfig featureConfig;

    private ExecutorService executor;

    private Map<String, EventsApi> mparticleClientPerProduct;

    private Batch.Environment environment;

    private RateLimiter rateLimiter;

    private AtomicBoolean error = new AtomicBoolean(false);

    public MparticleExporter(MparticleCohortsConsumerConfig featureConfig) {
        super(featureConfig);

        if(featureConfig.isMparticleIntegrationEnabled() && featureConfig.getMparticleApiKeys() == null) {
            throw new IllegalArgumentException("Invalid configuration: API key per product is missing");
        }
        this.featureConfig = featureConfig;
        this.mparticleClientPerProduct = new HashMap<>();

        featureConfig.getMparticleApiKeys().forEach((productId, envVar) -> {

            if(StringUtils.isNotBlank(envVar) && System.getenv().containsKey(envVar+"_KEY") && System.getenv().containsKey(envVar+"_SECRET")) {
                String key = System.getenv(envVar+"_KEY");
                String secret = System.getenv(envVar+"_SECRET");
                ApiClient client = new ApiClient(key, secret);
                client.getAdapterBuilder().baseUrl(featureConfig.getMparticleApiBaseUrl());
                EventsApi mparticleClient = client.createService(EventsApi.class);

                mparticleClientPerProduct.put(
                        productId.toLowerCase(),
                        mparticleClient);
            }
        });

        if(featureConfig.isMparticleIntegrationEnabled()) {
            this.rateLimiter = RateLimiter.create(featureConfig.getApiRateLimit());
        } else {
            this.rateLimiter = RateLimiter.create(Integer.MAX_VALUE);
        }

        try {
            String deployment = AirlockManager.getDeployment();
            this.environment =
                    "EXTERNAL".equalsIgnoreCase(deployment) ?
                            Batch.Environment.PRODUCTION :
                            Batch.Environment.DEVELOPMENT;
            LOGGER.info("mParticle Environment: " + this.environment.getValue());
        } catch (IllegalArgumentException e) {
            this.environment = Batch.Environment.DEVELOPMENT;
        }
        this.executor = Executors.newFixedThreadPool(featureConfig.getApiParallelThreads());
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
            List<Batch> bulk = new ArrayList<>(BULK_SIZE);

            for (String userId : cohortsPerUser.keySet()) {
                List<UserCohort> userCohorts = cohortsPerUser.get(userId);
                Map<String, Object> properties = new HashMap<>();

                for (UserCohort uc : userCohorts) {
                    Optional<UserCohortExport> export = getUserCohortExport(uc, MPARTICLE_EXPORT_KEY);

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
                    bulk.add(convert(userId, properties));
                }

                if (bulk.size() == BULK_SIZE) {
                    submitted.add(submitBulk(bulk, productId));
                    bulk = new ArrayList<>(BULK_SIZE);
                }
            }

            if (bulk.size() > 0) {
                submitted.add(submitBulk(bulk, productId));
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
            usersCounter.labels(MPARTICLE_EXPORT_KEY).inc(total);
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
        report.setExportKey(MPARTICLE_EXPORT_KEY);
        report.setStatus(
                detailsStatus == ExportJobStatusReport.JobStatus.FAILED ?
                        ExportJobStatusReport.JobStatus.FAILED : ExportJobStatusReport.JobStatus.RUNNING);
        report.setStatusMessage("Batch sent to mPparticle");
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

    private Future<Boolean> submitBulk(final List<Batch> bulk, String productId) {
        return executor.submit(() -> callMparticle(bulk, productId));
    }

    private boolean callMparticle(final List<Batch> bulk, String productId) {

        if (error.get()) {
            return false;
        }

        if (bulk.isEmpty()) {
            return true;
        }

        EventsApi mparticleClient = mparticleClientPerProduct.get(productId.toLowerCase());
        int nRetries = 4;
        int retryCounter = 0;
        IOException lastIoException = null;

        while (retryCounter < nRetries) {

            try {
                rateLimiter.acquire();
                Response<Void> bulkResponse = null;

                if (featureConfig.isMparticleIntegrationEnabled()) {
                    Call<Void> bulkResult = mparticleClient.bulkUploadEvents(bulk);
                    bulkResponse = bulkResult.execute();
                }

                if (bulkResponse == null || bulkResponse.isSuccessful()) {
                    return true;
                } else if (bulkResponse.code() == 429) {// throttling
                    LOGGER.warn("Mparticle API throttling response: " + bulkResponse);
                    retryCounter++;

                    int secs = retryCounter;
                    String sSecs = bulkResponse.headers().get("Retry-After");

                    if (sSecs != null) {

                        try {
                            secs = Integer.parseInt(sSecs);
                            LOGGER.info("Retry-After " + secs);
                        } catch (Exception nfe) {
                            // unparsable - do notyhing
                        }
                    }

                    try {
                        Thread.sleep(secs * 1000L);
                    } catch (InterruptedException e1) {
                        // it seems, the process is being stopped, so, stop retrying
                    }
                    continue;
                } else if (bulkResponse.code() > 500) {// server failure
                    LOGGER.error("Mparticle API " + bulkResponse.code() + " response: " + bulkResponse);
                    retryCounter++;

                    try {
                        Thread.sleep(retryCounter * 1000L);
                    } catch (InterruptedException e1) {
                        // it seems, the process is being stopped, so, stop retrying
                    }
                    continue;
                } else {
                    LOGGER.error("Mparticle API error response: " + bulkResponse);
                    break;
                }
            } catch (IOException e) {

                retryCounter++;
                lastIoException = e;

                try {
                    Thread.sleep(retryCounter * 1000L);
                } catch (InterruptedException e1) {
                    // it seems, the process is being stopped, so, stop retrying
                }
                continue;
            }
        }

        if (retryCounter >= nRetries) {

            if(lastIoException != null) {
                LOGGER.error(lastIoException.getMessage(), lastIoException.getCause());
            } else {
                LOGGER.error("Error sending events.");
            }
        }
        return false;
    }

    private Batch convert(String userId, Map<String, Object> properties) {
        Batch batch = new Batch();
        batch.environment(this.environment);
        batch.userIdentities(new UserIdentities().customerId(userId));
        batch.userAttributes(properties);

        CustomEvent mpEvent = new CustomEvent().data(new CustomEventData());
        mpEvent.getData().timestampUnixtimeMs(Instant.now().toEpochMilli());
        mpEvent.getData().eventName(USER_ATTRIBUTES_EVENT);
        mpEvent.getData().customEventType(CustomEventData.CustomEventType.OTHER);

        batch.addEventsItem(mpEvent);

        return batch;
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
