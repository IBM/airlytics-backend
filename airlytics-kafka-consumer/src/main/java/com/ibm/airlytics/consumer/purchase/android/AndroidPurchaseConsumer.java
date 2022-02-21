package com.ibm.airlytics.consumer.purchase.android;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisherScopes;
import com.google.api.services.androidpublisher.model.InappproductsListResponse;
import com.google.api.services.androidpublisher.model.SubscriptionPurchase;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.purchase.*;
import com.ibm.airlytics.consumer.purchase.events.BaseSubscriptionEvent;
import com.ibm.airlytics.consumer.purchase.inject.AndroidReceiptsInjector;
import com.ibm.airlytics.utilities.Hashing;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class AndroidPurchaseConsumer extends PurchaseConsumer {
    protected static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(AndroidPurchaseConsumer.class.getName());
    protected static final String APP_PACKAGE_NAME_ENV_VARIABLE = "APP_PACKAGE";


    public enum GoogleAPIErrorResponseReason {
        SUBSCRIPTION_PURCHASE_NO_LONGER_AVAILABLE("subscriptionPurchaseNoLongerAvailable"),
        PURCHASE_TOKEN_NO_LONGER_VALID("purchaseTokenNoLongerValid"),
        UNKNOWN("unknown");


        private String reason;

        GoogleAPIErrorResponseReason(String reason) {
            this.reason = reason;
        }


        public static AndroidPurchaseConsumer.GoogleAPIErrorResponseReason getValueByReason(String reason) {
            if (reason == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.GoogleAPIErrorResponseReason e : AndroidPurchaseConsumer.GoogleAPIErrorResponseReason.values()) {
                if (reason.equals(e.reason)) return e;
            }
            return null;
        }
    }

    @SuppressWarnings("unused")
    public enum PurchaseType {
        TEST(0),
        PROMO(1),
        ;

        private final int type;

        PurchaseType(int type) {
            this.type = type;
        }

        public static String getNameByType(Integer type) {
            if (type == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.PurchaseType e : AndroidPurchaseConsumer.PurchaseType.values()) {
                if (type == e.type) return e.name();
            }
            return null;
        }

        public static AndroidPurchaseConsumer.PurchaseType getValueByType(Integer type) {
            if (type == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.PurchaseType e : AndroidPurchaseConsumer.PurchaseType.values()) {
                if (type == e.type) return e;
            }
            return null;
        }
    }

    @SuppressWarnings("unused")
    public enum NotificationType {
        RECOVERED(1),
        RENEWED(2),
        CANCELED(3),
        PURCHASED(4),
        ON_HOLD(5),
        IN_GRACE_PERIOD(6),
        RESTARTED(7),
        PRICE_CHANGE_CONFIRMED(8),
        DEFERRED(9),
        PAUSED(10),
        PAUSE_SCHEDULE_CHANGED(11),
        REVOKED(12),
        EXPIRED(13),
        ; // semicolon needed when fields / methods follow

        private final int type;

        NotificationType(int type) {
            this.type = type;
        }

        public static String getNameByType(Integer type) {
            if (type == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.NotificationType e : AndroidPurchaseConsumer.NotificationType.values()) {
                if (type == e.type) return e.name();
            }
            return null;
        }

        public static AndroidPurchaseConsumer.NotificationType getValueByType(Integer type) {
            if (type == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.NotificationType e : AndroidPurchaseConsumer.NotificationType.values()) {
                if (type == e.type) return e;
            }
            return null;
        }
    }

    public enum CancelSurveyReason {
        OTHER(0),  // other
        LACK_OF_USAGE(1),  //   I don't use this service enough
        TECHNICAL_ISSUE(2),   // Technical issues
        COST(3), // Cost-related reasons
        FOUND_BETTER_APP(4),  //I found a better app
        ; // semicolon needed when fields / methods follow

        private final int reason;

        CancelSurveyReason(int reason) {
            this.reason = reason;
        }

        static String getNameByReason(Integer reason) {
            if (reason == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.CancelSurveyReason e : AndroidPurchaseConsumer.CancelSurveyReason.values()) {
                if (reason == e.reason) return e.name();
            }
            return null;
        }
    }

    public enum CancelReason {
        USER_CANCELED(0),  // User canceled the subscription
        SYSTEM_CANCELED(1),  // Subscription was canceled by the system, for example because of a billing problem
        REPLACED_WITH_SUB(2),   // Subscription was replaced with a new subscription
        DEVELOPER_CANCELED(3) // Subscription was canceled by the developer
        ; // semicolon needed when fields / methods follow

        private final int reason;

        CancelReason(int reason) {
            this.reason = reason;
        }

        static String getNameByReason(Integer reason) {
            if (reason == null) {
                return null;
            }
            for (AndroidPurchaseConsumer.CancelReason e : AndroidPurchaseConsumer.CancelReason.values()) {
                if (reason == e.reason) return e.name();
            }
            return null;
        }

        static AndroidPurchaseConsumer.CancelReason getValueByReason(Integer reason) {
            if (reason == null) {
                return null;
            }
            for (CancelReason e : CancelReason.values()) {
                if (reason == e.reason) return e;
            }
            return null;
        }
    }

    public enum PaymentState {
        PENDING(0),  //    Payment pending
        PAYMENT_RECEIVED(1),  //   Payment received
        FREE_TRIAL(2),   // Free trial
        PENDING_DEFERRED(3), //   Pending deferred upgrade/downgrade
        ; // semicolon needed when fields / methods follow

        private final int state;

        PaymentState(int state) {
            this.state = state;
        }

        static String getNameByState(Integer state) {
            if (state == null) {
                return null;
            }
            for (PaymentState e : PaymentState.values()) {
                if (state == e.state) return e.name();
            }
            return null;
        }

        static PaymentState getValueByState(Integer state) {
            if (state == null) {
                return null;
            }
            for (PaymentState e : PaymentState.values()) {
                if (state == e.state) return e;
            }
            return null;
        }
    }


    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, AtomicInteger> recordTypeCountersInBatch = new Hashtable<>();
    private AndroidPublisher androidPublisher;
    private final int purchasesBatchSize = 10;
    private String appPackage;
    private static final ScheduledThreadPoolExecutor inAppProductsListRefreshScheduler = new ScheduledThreadPoolExecutor(1);
    private static final ScheduledThreadPoolExecutor renewalRefreshScheduler = new ScheduledThreadPoolExecutor(1);

    public AndroidPurchaseConsumer(PurchaseConsumerConfig config) {
        super(config);

        appPackage = System.getenv(APP_PACKAGE_NAME_ENV_VARIABLE);
        if (appPackage == null) {
            logException(new IllegalAccessException(), "Error while init PurchaseAttributeConsumer (Google AndroidPublisher init problem), details: App package not specified");
            stop();
        }

        try {
            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    new FileInputStream("keys" + File.separator + "google-api-user-key.json")).createScoped(AndroidPublisherScopes.ANDROIDPUBLISHER);
            androidPublisher = new AndroidPublisher.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    JacksonFactory.getDefaultInstance(),
                    new HttpCredentialsAdapter(credentials)
            ).setApplicationName("Android[Product]").build();
        } catch (Exception e) {
            logException(e, "Error while init PurchaseAttributeConsumer (Google AndroidPublisher init problem), details: " + e.getMessage());
            stop();
        }


        try {
            fetchInAppsList(appPackage);
        } catch (Exception e) {
            logException(e, "Error in fetching InApp Products list");
            stop();
        }

        this.config.getTopics().forEach(topic -> this.recordTypeCountersInBatch.put(topic, new AtomicInteger(0)));

        // inject purchase tokens, if specified
        if (config.getInjectablePurchaseDataSource() != null &&
                config.getS3Bucket() != null && config.getS3region() != null) {
            try {
                AndroidReceiptsInjector androidReceiptsInjector = new AndroidReceiptsInjector(purchaseTopic, config, executionConn);
                androidReceiptsInjector.processEncodedReceipts(config.getS3Bucket(), config.getInjectablePurchaseDataSource());
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
    }


    public AndroidPurchaseConsumer(PurchaseConsumerConfig config, boolean isDataRepairer) {

        this.config = config;
        this.hashing = new Hashing(config.getShards());
        try {
            executionConn = initConnection(config);
            longReadQueryConn = initConnection(config);
            prepareStatements();
        } catch (ClassNotFoundException | SQLException e) {
            logException(e, "Error while init Purchase Consumer, details: " + e.getMessage());
            stop();
        }
        try {
            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    new FileInputStream("keys" + File.separator + "google-api-user-key.json")).createScoped(AndroidPublisherScopes.ANDROIDPUBLISHER);
            androidPublisher = new AndroidPublisher.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    JacksonFactory.getDefaultInstance(),
                    new HttpCredentialsAdapter(credentials)
            ).setApplicationName("Android[Product]").build();
        } catch (Exception e) {
            logException(e, "Error while init PurchaseAttributeConsumer (Google AndroidPublisher init problem), details: " + e.getMessage());
            stop();
        }

        try {
            fetchInAppsList(appPackage);
        } catch (Exception e) {
            logException(e, "Error in fetching InApp Products list");
            stop();
        }
    }


    protected void fetchInAppsList(String appPackage) throws IOException {
        InappproductsListResponse inAppProducts = androidPublisher.inappproducts().list(appPackage).execute();
        inAppProducts.getInappproduct().forEach(inAppProduct -> {
            AirlyticsInAppProduct airlyticsInAppProduct = new AirlyticsInAppProduct();
            airlyticsInAppProduct.setProductId(inAppProduct.getSku());
            airlyticsInAppProduct.setPriceUSDMicros(new Long(inAppProduct.getPrices().get("US").getPriceMicros()));
            airlyticsInAppProduct.setGracePeriod(inAppProduct.getGracePeriod());
            airlyticsInAppProduct.setSubscriptionPeriod(inAppProduct.getSubscriptionPeriod());
            airlyticsInAppProduct.setTrialPeriod(inAppProduct.getTrialPeriod());
            inAppAirlyticsProducts.put(inAppProduct.getSku(), airlyticsInAppProduct);
        });
    }

    protected void refreshInAppsList() {
        try {
            fetchInAppsList(appPackage);
        } catch (IOException e) {
            logException(e, "Error while refreshing InApp Products list");
        }
    }

    protected Pair<SubscriptionPurchase, StoreResponse> queryPurchaseDetails(String type, String purchaseToken, String subscriptionId, int currentAttempt, String appPackage) {
        try {
            SubscriptionPurchase purchase = androidPublisher.purchases().subscriptions().get(
                    appPackage,
                    subscriptionId,
                    purchaseToken
            ).execute();

            apiRequestCounter.labels("request",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            return new Pair(purchase, StoreResponse.OK);
        } catch (SocketTimeoutException e) {

            LOGGER.info("Got exception of Google API request attempt [" + currentAttempt + "]");
            apiRequestCounter.labels("retry",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

            apiResponseCounter.labels(StoreResponse.NETWORK_ERROR.name().toLowerCase(),
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            // wait for a while before the next retry
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                //do nothing
            }
            int maxGoogleAPICallRetry = 3;
            if (currentAttempt < maxGoogleAPICallRetry) {
                //if the any network error attempt to retrieve a few more times
                return queryPurchaseDetails(type, purchaseToken, subscriptionId, ++currentAttempt, appPackage);
            } else {
                return new Pair(null, StoreResponse.NETWORK_ERROR);
            }
        } catch (GoogleJsonResponseException e) {

            GoogleAPIErrorResponseReason googleAPIErrorResponseReason = getGoogleAPIErrorResponseReason(e.getContent());
            // The subscription purchase is no longer available for query because it has been expired for too long
            if (googleAPIErrorResponseReason == GoogleAPIErrorResponseReason.SUBSCRIPTION_PURCHASE_NO_LONGER_AVAILABLE) {
                processedPurchasesCounter.labels(type, StoreResponse.OLD.name().toLowerCase(),
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                apiResponseCounter.labels(StoreResponse.OLD.name().toLowerCase(),
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                return new Pair(null, StoreResponse.OLD);
            } else if (googleAPIErrorResponseReason == GoogleAPIErrorResponseReason.PURCHASE_TOKEN_NO_LONGER_VALID) {
                apiResponseCounter.labels(StoreResponse.NOT_VALID.name().toLowerCase(),
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                return new Pair(null, StoreResponse.NOT_VALID);
            } else {
                apiResponseCounter.labels(StoreResponse.NETWORK_ERROR.name().toLowerCase(),
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                return new Pair(null, StoreResponse.NETWORK_ERROR);
            }
        } catch (IOException e) {
            processedPurchasesCounter.labels(type, StoreResponse.NETWORK_ERROR.name().toLowerCase(),
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            logException(e, "Error in fetching purchase details: of purchaseToken: [" + purchaseToken + "] subscriptionId:[" + subscriptionId + "] " + e.getMessage());
            apiResponseCounter.labels(StoreResponse.NETWORK_ERROR.name().toLowerCase(),
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            return new Pair(null, StoreResponse.NETWORK_ERROR);
        }
    }


    private GoogleAPIErrorResponseReason getGoogleAPIErrorResponseReason(String error) {
        try {
            GoogleAPIResponseError googleAPIResponseError = objectMapper.readValue(error, GoogleAPIResponseError.class);
            if (googleAPIResponseError.errors.size() > 0) {
                return GoogleAPIErrorResponseReason.getValueByReason(googleAPIResponseError.errors.get(0).reason);
            }
        } catch (JsonProcessingException ex) {
            return GoogleAPIErrorResponseReason.UNKNOWN;
        }
        return GoogleAPIErrorResponseReason.UNKNOWN;
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty()) {
            return 0;
        }

        updateToNewConfigIfExists();

        int recordsProcessed = 0;
        List<PurchaseMetaData> purchasesBatch = new ArrayList<>();

        for (ConsumerRecord<String, JsonNode> record : records) {

            List<PurchaseMetaData> purchaseMetaDataArray = retrievePurchaseMetaData(record);

            purchaseMetaDataArray.forEach(purchaseMetaData -> {
                purchasesBatch.add(purchaseMetaData);

                if (purchasesBatch.size() == purchasesBatchSize) {
                    // process batch
                    this.recordTypeCountersInBatch.keySet().forEach(key -> this.recordTypeCountersInBatch.get(key).set(0));
                    totalProgress += processPurchasesBatch(purchasesBatch, submittedSubscriptionEvents);
                    purchasesBatch.clear();
                }
            });
        }

        // process not full batch
        // process batch
        if (!purchasesBatch.isEmpty()) {
            this.recordTypeCountersInBatch.keySet().forEach(key -> this.recordTypeCountersInBatch.get(key).set(0));
            totalProgress += processPurchasesBatch(purchasesBatch, submittedSubscriptionEvents);
            purchasesBatch.clear();
        }

        commit();

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.debug("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsProcessed;
    }


    private int processPurchasesBatch(List<PurchaseMetaData> purchasesBatch, Map<String, BaseSubscriptionEvent> submittedSubscriptionEvents) {

        int recordsProcessed = 0;
        final List<Pair<SubscriptionPurchase, PurchaseMetaData>> validaPurchasesBatch = Collections.synchronizedList(new ArrayList<>());
        final List<Pair<PurchaseMetaData, StoreResponse>> nonValidaPurchasesBatch = Collections.synchronizedList(new ArrayList<>());

        purchasesBatch.parallelStream().forEach(purchaseMetaData -> {
            try {
                if (purchaseMetaData.getPurchaseToken() == null || purchaseMetaData.getPurchaseToken().isEmpty()) {
                    validaPurchasesBatch.add(new Pair<>(null, purchaseMetaData));
                    processedPurchasesCounter.labels(purchaseMetaData.getType().name(), "empty token",
                            AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                } else {
                    Pair<SubscriptionPurchase, StoreResponse> subscriptionPurchase = queryPurchaseDetails(purchaseMetaData.getType().name(),
                            purchaseMetaData.getPurchaseToken(), purchaseMetaData.getProductId(), 0, appPackage);
                    if (subscriptionPurchase.getFirst() == null && subscriptionPurchase.getSecond() != null) {
                        nonValidaPurchasesBatch.add(new Pair(purchaseMetaData, subscriptionPurchase.getSecond()));
                    } else {
                        validaPurchasesBatch.add(new Pair(subscriptionPurchase.getFirst(), purchaseMetaData));
                    }
                }
            } catch (Exception e) {
                logException(e, "Error in processRecords, details: " + e.getMessage());
            }
        });


        // process valid purchases
        for (Pair<SubscriptionPurchase, PurchaseMetaData> tuple : validaPurchasesBatch) {
            // Insert new purchase details row into purchases table
            if (tuple.getFirst() != null) {
                String result = "success";

                try {
                    PurchaseMetaData purchaseMetaData = tuple.getSecond();
                    SubscriptionPurchase subscriptionPurchase = tuple.getFirst();

                    List<AirlyticsPurchaseEvent> newNotificationEvents;

                    // dev user
                    if (subscriptionPurchase.getPurchaseType() != null) {
                        newNotificationEvents = processNotificationEvents(subscriptionPurchase, purchaseMetaData,
                                purchaseEventDevUpdateStatement, purchaseEventDevInsertOnConflictDoNothingStatement,
                                getDevPurchaseTransactionById, getDevPurchaseEventsByID);
                    } else { // production user
                        newNotificationEvents = processNotificationEvents(subscriptionPurchase, purchaseMetaData, purchaseEventUpdateStatement,
                                purchaseEventInsertOnConflictDoNothingStatement, getPurchaseTransactionById, getPurchaseEventsByID);
                    }

                    AirlyticsPurchase updatedPurchase = processPurchase(tuple.getFirst(), tuple.getSecond(), newNotificationEvents);
                    // if getPurchaseType is not null means the purchase is not production
                    // we put it into dev table
                    if (purchaseMetaData.getUserId() != null) {
                        updatedPurchase.setUserId(purchaseMetaData.getUserId());
                    }

                    if (subscriptionPurchase.getPurchaseType() != null) {
                        sendSubscriptionEventsBatch(removeDuplications(getSubscriptionEventsByPurchaseTransaction(updatedPurchase,
                                getDevPurchaseTransactionByIdWithUserId, purchaseMetaData), submittedSubscriptionEvents),
                                devEventProducerProxyClient, 3);
                        PurchasesDAO.insertUpdatePurchase(devPurchaseStatement, updatedPurchase);
                        eventsBatchToEventProxy.add(updatedPurchase);
                    } else {
                        sendSubscriptionEventsBatch(removeDuplications(getSubscriptionEventsByPurchaseTransaction(updatedPurchase,
                                getPurchaseTransactionByIdWithUserId, purchaseMetaData), submittedSubscriptionEvents),
                                eventProducerProxyClient, 3);
                        PurchasesDAO.insertUpdatePurchase(purchaseStatement, updatedPurchase);
                        eventsBatchToEventProxy.add(updatedPurchase);
                    }

                    insertsInPurchaseBatch++;

                    if (purchaseMetaData.getUserId() != null) {
                        if (subscriptionPurchase.getPurchaseType() != null) {
                            PurchasesDAO.insertUpdatePurchaseUser(purchaseUserIdDevInsertStatement, purchaseMetaData.getPurchaseToken(),
                                    purchaseMetaData.getUserId(), hashing.hashMurmur2(purchaseMetaData.getUserId()));
                        } else {
                            PurchasesDAO.insertUpdatePurchaseUser(purchaseUserIdInsertStatement, purchaseMetaData.getPurchaseToken(),
                                    purchaseMetaData.getUserId(), hashing.hashMurmur2(purchaseMetaData.getUserId()));
                        }
                        insertsInPurchaseUsersBatch++;
                    }

                } catch (SQLException | JsonProcessingException e) {
                    result = "error";
                    logException(e, "Error in processRecords, details: " + e.getMessage());
                } finally {
                    processedPurchasesCounter.labels(tuple.getSecond().getType().name(), result,
                            AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    recordsProcessed++;
                }
            } else {
                recordsProcessed++;
            }
        }

        //process invalid purchases
        for (Pair<PurchaseMetaData, StoreResponse> tuple : nonValidaPurchasesBatch) {
            // Insert new purchase details row into purchases table
            if (tuple.getFirst() != null) {
                String result = "success";
                try {
                    PurchaseMetaData purchaseMetaData = tuple.getFirst();

                    PurchasesDAO.setPurchaseInActiveAndUpdateStoreResponse(purchaseStoreResponseUpdateStatement,
                            purchaseMetaData, tuple.getSecond());

                    // try to update the develop users table, if the id doesn't exist there it will be skipped.
                    PurchasesDAO.setPurchaseInActiveAndUpdateStoreResponse(devPurchaseStoreResponseUpdateStatement,
                            purchaseMetaData, tuple.getSecond());

                    insertsInPurchaseBatch++;
                } catch (SQLException e) {
                    result = "error";
                    logException(e, "Error in processRecords, details: " + e.getMessage());
                } finally {
                    processedPurchasesCounter.labels(tuple.getFirst().getType().name(), result,
                            AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    recordsProcessed++;
                }
            } else {
                recordsProcessed++;
            }
        }


        return recordsProcessed;
    }

    private List<AirlyticsPurchaseEvent> processNotificationEvents(SubscriptionPurchase subscriptionPurchase, PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                                                   PreparedStatement updateStatement, PreparedStatement insertStatement,
                                                                   PreparedStatement getPurchaseTransactionById, PreparedStatement getPurchaseEventsByID) throws JsonProcessingException {
        AirlyticsPurchase currentAirlyticsPurchaseState;
        final List<AirlyticsPurchaseEvent> airlyticsPurchaseEventsHistory;
        AirlyticsPurchaseEvent upgradedSub = null;
        List<AirlyticsPurchaseEvent> newNotificationEvents;
        Set<AirlyticsPurchaseEvent> uniqueAirlyticsPurchaseEventsHistory = new HashSet<>();
        try {
            currentAirlyticsPurchaseState = PurchasesDAO.getTransactionById(getPurchaseTransactionById,
                    dbCommandsSummary, purchaseMetaData.getPurchaseToken());

            airlyticsPurchaseEventsHistory =
                    PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID,
                            purchaseMetaData.getPurchaseToken());


            if (subscriptionPurchase.getLinkedPurchaseToken() != null) {
                Optional<AirlyticsPurchaseEvent> upgradedSubOptional = PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID,
                        subscriptionPurchase.getLinkedPurchaseToken()).
                        stream().filter(event -> event.getName().equals(PurchaseEventType.UPGRADED.name())).findFirst();

                upgradedSub = upgradedSubOptional.isPresent() ? upgradedSubOptional.get() : null;

                if (upgradedSub != null) {
                    upgradedSub.setUpgradedTo(purchaseMetaData.getPurchaseToken());
                } else {
                    LOGGER.warn("PurchaseToken:" + subscriptionPurchase.getLinkedPurchaseToken() + " has not UPGRADED event");
                }
            }

            AndroidAirlyticsPurchaseEvent airlyticsPurchaseEvent = new AndroidAirlyticsPurchaseEvent(
                    subscriptionPurchase,
                    purchaseMetaData,
                    inAppAirlyticsProducts.get(purchaseMetaData.getProductId()),
                    currentAirlyticsPurchaseState,
                    airlyticsPurchaseEventsHistory);

            newNotificationEvents =
                    airlyticsPurchaseEvent.getPurchaseEvents();

            if (upgradedSub != null) {
                newNotificationEvents.add(upgradedSub);
            }

            newNotificationEvents.forEach(subEvent -> {
                try {
                    if (purchaseMetaData.getNotificationType() != null) {
                        if (ifPurchaseEventExists(airlyticsPurchaseEventsHistory, subEvent) &&
                                !subEvent.getName().equals(PurchaseEventType.RECOVERED.name())) {
                            PurchasesDAO.updatePurchaseEvent(updateStatement, subEvent);
                        } else {
                            PurchasesDAO.insertPurchaseEvent(insertStatement, subEvent);
                        }
                    } else {
                        if (ifPurchaseEventExists(airlyticsPurchaseEventsHistory, subEvent)) {
                            PurchasesDAO.updatePurchaseEvent(updateStatement, subEvent);
                        } else {
                            // if a new event is PURCHASE and we have UPGRADE_PURCHASED or TRIAL_STARTED at
                            // the same time don't add it
                            if (subEvent.getName().equals(PurchaseEventType.PURCHASED.name())
                                    && !ifPurchaseEventExists(airlyticsPurchaseEventsHistory, subEvent.getPurchaseId(),
                                    PurchaseEventType.TRIAL_STARTED.name(), subEvent.getEventTime()) &&
                                    !ifPurchaseEventExists(airlyticsPurchaseEventsHistory, subEvent.getPurchaseId(),
                                            PurchaseEventType.UPGRADED.name(), subEvent.getEventTime())) {
                                PurchasesDAO.insertPurchaseEvent(insertStatement, subEvent);
                            } else if (!subEvent.getName().equals(PurchaseEventType.PURCHASED.name())) {
                                PurchasesDAO.insertPurchaseEvent(insertStatement, subEvent);
                            }
                        }
                    }
                } catch (SQLException sqlException) {
                    logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                }
            });

            uniqueAirlyticsPurchaseEventsHistory.addAll(airlyticsPurchaseEventsHistory);
            uniqueAirlyticsPurchaseEventsHistory.addAll(newNotificationEvents);

        } catch (SQLException sqlException) {
            logException(sqlException, "Error in getting purchase state, " + sqlException.getMessage());
        }

        return new ArrayList<>(uniqueAirlyticsPurchaseEventsHistory);
    }

    protected AirlyticsPurchase processPurchase(SubscriptionPurchase newSubscriptionPurchase,
                                                PurchaseMetaData metaData, List<AirlyticsPurchaseEvent> newNotificationEvents) {

        AirlyticsPurchase currentAirlyticsPurchaseState = null;

        try {
            currentAirlyticsPurchaseState = PurchasesDAO.getTransactionById(getPurchaseTransactionById,
                    dbCommandsSummary, metaData.getPurchaseToken());
        } catch (SQLException sqlException) {
            logException(sqlException, "Error in getting purchase state, " + sqlException.getMessage());
        }

        return new AndroidAirlyticsPurchaseState(newSubscriptionPurchase,
                metaData,
                this.inAppAirlyticsProducts.get(metaData.getProductId()), currentAirlyticsPurchaseState, newNotificationEvents).getUpdatedPurchase();

    }


    private List<PurchaseMetaData> retrievePurchaseMetaData(ConsumerRecord<String, JsonNode> record) {

        String result = "success";

        List<PurchaseMetaData> purchaseMetaDataArrayList = new ArrayList<>();
        if (record.topic().equals(this.config.getPurchasesTopic())) {

            if (record.value().get("purchaseToken") != null && record.value().get("subscriptionId") != null) {
                PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
                purchaseMetaData.setType(PurchaseMetaData.Type.RENEWAL_CHECK);
                purchaseMetaData.setPlatform(getPlatform());
                purchaseMetaData.setPurchaseToken(record.value().get("purchaseToken").asText());
                purchaseMetaData.setProductId(record.value().get("subscriptionId").asText());
                purchaseMetaDataArrayList.add(purchaseMetaData);

                // we need the original event id to use it as a input for generation back-end events.
                purchaseMetaData.setOriginalEventId(record.value().has("eventId") ?
                        record.value().get("eventId").asText() :
                        UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());

                return purchaseMetaDataArrayList;
            }

            PurchaseEvent purchaseEvent = new PurchaseEvent(record);
            Iterator<Map.Entry<String, JsonNode>> attributeIterator = purchaseEvent.getAttributes().fields();

            while (attributeIterator.hasNext()) {
                Map.Entry<String, JsonNode> attribute = attributeIterator.next();
                if (attribute.getKey().equals("purchases")) {
                    this.recordTypeCountersInBatch.get(record.topic()).incrementAndGet();
                    JsonNode purchases = attribute.getValue();
                    if (purchases.isArray()) {
                        for (final JsonNode purchase : purchases) {

                            String subscriptionId = purchase.get("subscriptionId").asText();

                            PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
                            purchaseMetaData.setPurchaseToken(purchase.get("purchaseToken").asText());
                            purchaseMetaData.setProductId(subscriptionId);
                            purchaseMetaData.setUserId(purchaseEvent.getUserId());
                            purchaseMetaData.setPlatform(getPlatform());
                            purchaseMetaData.setType(PurchaseMetaData.Type.APP);

                            // we need the original event id to use it as a input for generation back-end events.
                            purchaseMetaData.setOriginalEventId(record.value().has("eventId") ?
                                    record.value().get("eventId").asText() :
                                    UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());

                            purchaseMetaDataArrayList.add(purchaseMetaData);
                        }
                    }
                }
            }
        } else {
            try {
                this.recordTypeCountersInBatch.get(record.topic()).incrementAndGet();

                AndroidNotificationEvent notificationEvent = objectMapper.treeToValue(record.value(), AndroidNotificationEvent.class);

                PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
                purchaseMetaData.setType(PurchaseMetaData.Type.NOTIFICATION);
                purchaseMetaData.setPurchaseToken(notificationEvent.getSubscriptionNotification().getPurchaseToken());
                purchaseMetaData.setProductId(notificationEvent.getSubscriptionNotification().getSubscriptionId());
                purchaseMetaData.setNotificationType(NotificationType.
                        getNameByType(notificationEvent.getSubscriptionNotification().getNotificationType()));
                purchaseMetaData.setNotificationTime(notificationEvent.getEventTimeMillis());
                purchaseMetaData.setPlatform(getPlatform());

                // need the original event id to use it as a input for generation back-end events.
                // notification doesn't have its own eventId that's why we use the whole notification value as an input.
                purchaseMetaData.setOriginalEventId(UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());

                purchaseMetaDataArrayList.add(purchaseMetaData);
            } catch (JsonProcessingException e) {
                result = "error";
                logException(e, "Error in parsing android notification, details: " + e.getMessage());
            }
        }

        processedEventsCounter.labels(record.topic(), result,
                AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

        return purchaseMetaDataArrayList;
    }
}
