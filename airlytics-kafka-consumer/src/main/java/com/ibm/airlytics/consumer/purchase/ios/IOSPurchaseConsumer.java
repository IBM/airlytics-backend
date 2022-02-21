package com.ibm.airlytics.consumer.purchase.ios;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.purchase.*;
import com.ibm.airlytics.consumer.purchase.events.BaseSubscriptionEvent;
import com.ibm.airlytics.consumer.purchase.inject.IOSReceiptsInjector;
import com.ibm.airlytics.utilities.Hashing;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import javax.annotation.CheckForNull;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.ibm.airlytics.consumer.purchase.ios.IOSPurchaseApiClient.USER_ACCOUNT_NOT_FOUND;


@SuppressWarnings("DuplicatedCode")
public class IOSPurchaseConsumer extends PurchaseConsumer {


    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(IOSPurchaseConsumer.class.getName());
    protected static IOSPurchaseApiClient iosPurchaseApiClient;


    private static final ScheduledThreadPoolExecutor inAppProductsListRefreshScheduler = new ScheduledThreadPoolExecutor(1);

    public enum CancellationReason {
        OTHER_REASON(0),
        ISSUE_WITH_APP(1),
        ;

        public static final String SUBSCRIPTION_UPGRADED = "SUBSCRIPTION_UPGRADED";

        private final Integer type;

        CancellationReason(Integer type) {
            this.type = type;
        }


        public static String getNameByType(Integer type, boolean isUpgraded) {

            if (isUpgraded) {
                return SUBSCRIPTION_UPGRADED;
            }
            if (type == null) {
                return null;
            }
            for (CancellationReason e : CancellationReason.values()) {
                if (Objects.equals(type, e.type)) return e.name();
            }
            return null;
        }
    }

    public enum ExpirationReason {
        CANCELED(1),
        BILLING_ISSUE(2),
        REJECTED_PRICE_INCREASE(3),
        PRODUCT_NOT_AVAILABLE(4),
        UNKNOWN(5),
        ;

        private final Integer type;

        ExpirationReason(Integer type) {
            this.type = type;
        }

        public Integer getType() {
            return type;
        }

        public static String getNameByType(Integer type) {
            if (type == null) {
                return null;
            }
            for (ExpirationReason e : ExpirationReason.values()) {
                if (Objects.equals(type, e.type)) return e.name();
            }
            return null;
        }
    }


    @SuppressWarnings("unused")
    public enum NotificationType {
        CANCEL("CANCEL"),
        DID_CHANGE_RENEWAL_PREF("DID_CHANGE_RENEWAL_PREF"),
        DID_CHANGE_RENEWAL_STATUS("DID_CHANGE_RENEWAL_STATUS"),
        DID_FAIL_TO_RENEW("DID_FAIL_TO_RENEW"),
        DID_RECOVER("DID_RECOVER"),
        DID_RENEW("DID_RENEW"),
        INITIAL_BUY("INITIAL_BUY"),
        INTERACTIVE_RENEWAL("INTERACTIVE_RENEWAL"),
        RENEWAL("RENEWAL"),
        REFUND("REFUND"),
        CONSUMPTION_REQUEST("CONSUMPTION_REQUEST"),
        PRICE_INCREASE_CONSENT("PRICE_INCREASE_CONSENT");
        ; // semicolon needed when fields / methods follow

        private final String type;

        NotificationType(String type) {
            this.type = type;
        }

        public static String getNameByType(String type) {
            if (type == null) {
                return null;
            }
            for (IOSPurchaseConsumer.NotificationType e : IOSPurchaseConsumer.NotificationType.values()) {
                if (Objects.equals(type, e.type)) return e.name();
            }
            return null;
        }

        public static IOSPurchaseConsumer.NotificationType getValueByType(String type) {
            if (type == null) {
                return null;
            }
            for (IOSPurchaseConsumer.NotificationType e : IOSPurchaseConsumer.NotificationType.values()) {
                if (Objects.equals(type, e.type)) return e;
            }
            return null;
        }
    }


    @SuppressWarnings("unused")
    public enum Environment {
        SAND_BOX("Sandbox"),
        PRODUCTION("Production"),
        ;

        private final String name;

        Environment(String environment) {
            this.name = environment;
        }

    }


    public IOSPurchaseConsumer(PurchaseConsumerConfig config, boolean isDataRepairer) {
        iosPurchaseApiClient = new IOSPurchaseApiClient();
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

        submittedSubscriptionEvents =
                Collections.synchronizedMap(new LinkedHashMap<String, BaseSubscriptionEvent>(CACHE_SIZE) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, BaseSubscriptionEvent> entry) {
                        return size() > CACHE_SIZE;
                    }
                });
        inAppProductsListRefreshScheduler.scheduleAtFixedRate(this::refreshInAppsList,
                config.getRefreshInAppProductsLisInitDelay(), config.getRefreshInAppProductsListInterval(), TimeUnit.SECONDS);

    }

    public IOSPurchaseConsumer(PurchaseConsumerConfig config) {
        super(config);
        iosPurchaseApiClient = new IOSPurchaseApiClient();
        // inject encoded receipts, if specified
        if (config.getInjectablePurchaseDataSource() != null &&
                config.getS3Bucket() != null && config.getS3region() != null) {
            IOSReceiptsInjector iosReceiptsInjector = new IOSReceiptsInjector(purchaseTopic, config);
            iosReceiptsInjector.processEncodedReceipts(config.getS3Bucket(), config.getInjectablePurchaseDataSource());
        }
    }

    protected void refreshInAppsList() {
        Feature iOSInAppProductsList = AirlockManager.getAirlock().getFeature(AirlockConstants.Consumers.IOS_PREMIUM_PRODUCTS);
        if (iOSInAppProductsList.isOn()) {
            for (Feature inAppProductAsFeature : iOSInAppProductsList.getChildren()) {
                if (inAppProductAsFeature.isOn() && inAppProductAsFeature.getConfiguration() != null) {
                    try {
                        AirlyticsInAppProduct airlyticsInAppProduct;
                        airlyticsInAppProduct = objectMapper.readValue(inAppProductAsFeature.getConfiguration().toString(), AirlyticsInAppProduct.class);
                        if (airlyticsInAppProduct != null) {
                            this.inAppAirlyticsProducts.put(airlyticsInAppProduct.getProductId(), airlyticsInAppProduct);
                        }
                    } catch (JsonProcessingException e) {
                        logException(e, "Error in parsing iOS in App Product definition, details: " + e.getMessage());
                    }
                }
            }
        }
    }


    @CheckForNull
    protected Pair<IOSubscriptionPurchase, StoreResponse> getPurchaseDetailsByIOSAPI(String purchaseToken) {
        return getPurchaseDetailsByIOSAPI(purchaseToken, 1);
    }

    @CheckForNull
    protected Pair<IOSubscriptionPurchase, StoreResponse> getPurchaseDetailsByIOSAPI(String purchaseToken, int attempt) {

        // exceeded the max of 3 attempts
        if (attempt == 3) {
            return new Pair<>(null, StoreResponse.NETWORK_ERROR);
        }

        try {
            apiRequestCounter.labels("request",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            String receipt = iosPurchaseApiClient.post(purchaseToken);
            if (receipt == null) {
                // invoke internal retry in the catch section
                throw new IOException();
            }
            IOSubscriptionPurchase ioSubscriptionPurchase = objectMapper.readValue(receipt, IOSubscriptionPurchase.class);
            return ioSubscriptionPurchase.getStatus() == USER_ACCOUNT_NOT_FOUND ? new Pair<>(null, StoreResponse.NOT_VALID) :
                    new Pair<>(ioSubscriptionPurchase, null);


        } catch (IOException e) {
            // wait for a while before the next retry
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                //do nothing
            }
            apiRequestCounter.labels("retry",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            return getPurchaseDetailsByIOSAPI(purchaseToken, ++attempt);
        }
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty()) {
            return 0;
        }

        updateToNewConfigIfExists();
        AtomicInteger recordsProcessed = new AtomicInteger(0);

        final ArrayList<ConsumerRecord<String, JsonNode>> recordsAsArray = new ArrayList<>();
        for (ConsumerRecord<String, JsonNode> record : records) {
            recordsAsArray.add(record);
        }

        final List<Pair<IOSubscriptionPurchase, PurchaseMetaData>> validPurchasesBatch = Collections.synchronizedList(new ArrayList<>());
        final List<Pair<PurchaseMetaData, IOSubscriptionPurchase>> notificationsBatch = Collections.synchronizedList(new ArrayList<>());
        final List<Pair<PurchaseMetaData, StoreResponse>> nonValidPurchasesBatch = Collections.synchronizedList(new ArrayList<>());


        final int parallelism = 200;
        ForkJoinPool forkJoinPool = null;
        try {
            // process the event batch in parallel, reduce multiple remote API calls latency time
            forkJoinPool = new ForkJoinPool(parallelism);
            forkJoinPool.submit(() ->
                    recordsAsArray.parallelStream().forEach(record -> {
                        try {
                            // determine which kind of event is being processed user-attribute event/notification
                            // and extract the meta-date into separate object
                            PurchaseMetaData purchaseMetaData = retrievePurchaseMetaData(record);

                            if (purchaseMetaData != null) {
                                LOGGER.debug("Got  encodedReceipt from producer type:[" + purchaseMetaData.getType().name() + "]");
                                Pair<IOSubscriptionPurchase, StoreResponse> subscriptionPurchase =
                                        getPurchaseDetailsByIOSAPI(purchaseMetaData.getPurchaseToken());
                                if (purchaseMetaData.getType() == PurchaseMetaData.Type.APP ||
                                        purchaseMetaData.getType() == PurchaseMetaData.Type.RENEWAL_CHECK) {

                                    // Insert new receipt details row into batch
                                    if (subscriptionPurchase.getFirst() != null) {
                                        if (subscriptionPurchase.getFirst().getLatestReceiptInfo() != null) {
                                            validPurchasesBatch.add(new Pair(subscriptionPurchase.getFirst(), purchaseMetaData));
                                        } else {
                                            processedEventsCounter.labels(record.topic(), "skipped",
                                                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                                        }
                                    } else {
                                        // purchase store status should be only updated
                                        nonValidPurchasesBatch.add(new Pair<>(purchaseMetaData, subscriptionPurchase.getSecond()));
                                    }
                                } else {
                                    LOGGER.debug("Got iOS server notification");
                                    // Insert new receipt details row into batch
                                    if (subscriptionPurchase.getFirst() != null) {
                                        if (subscriptionPurchase.getFirst().getLatestReceiptInfo() != null) {
                                            notificationsBatch.add(new Pair(purchaseMetaData, subscriptionPurchase.getFirst()));
                                        } else {
                                            processedEventsCounter.labels(record.topic(), "skipped",
                                                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                                        }
                                    } else {
                                        // purchase store status should not empty
                                        nonValidPurchasesBatch.add(new Pair<>(purchaseMetaData, subscriptionPurchase.getSecond()));
                                    }

                                }
                            } else {
                                processedEventsCounter.labels(record.topic(), "error",
                                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            }
                        } catch (Exception e) {
                            processedEventsCounter.labels(record.topic(), "error",
                                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            logException(e, "Error in processRecords, purchase event: " + record.value().toString());
                        } finally {
                            recordsProcessed.incrementAndGet();
                        }
                    })
            ).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }

        processNotificationsBatch(notificationsBatch, submittedSubscriptionEvents);

        for (Pair<IOSubscriptionPurchase, PurchaseMetaData> tuple : validPurchasesBatch) {
            String resultRecordProcess = "success";

            IOSAirlyticsPurchaseEvent airlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                    tuple.getFirst().clone(),
                    tuple.getSecond(),
                    inAppAirlyticsProducts
            );

            try {
                if (tuple.getFirst().getEnvironment().equals(Environment.SAND_BOX.name)) {

                    airlyticsPurchaseEvent.getPurchaseEvents().stream().forEach(event -> {
                        try {
                            PurchasesDAO.insertPurchaseEvent(
                                    purchaseEventDevInsertOnConflictDoNothingStatement, event);
                        } catch (SQLException sqlException) {
                            logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                        }
                    });

                    Collection<AirlyticsPurchase> updatedPurchase = processReceiptFromPurchase(tuple.getFirst().clone(), tuple.getSecond());
                    if (updatedPurchase != null) {

                        List<BaseSubscriptionEvent> subscriptionEvents = new ArrayList<>();
                        for (AirlyticsPurchase airlyticsPurchase : updatedPurchase) {
                            String result = "success";
                            try {

                                String userId = null;
                                if (tuple.getSecond().getType() == PurchaseMetaData.Type.APP) {
                                    userId = tuple.getSecond().getUserId();
                                    airlyticsPurchase.setUserId(userId);
                                }
                                subscriptionEvents.addAll(getSubscriptionEventsByPurchaseTransaction
                                        (airlyticsPurchase, getDevPurchaseTransactionByIdWithUserId, tuple.getSecond()));
                                PurchasesDAO.insertUpdatePurchase(devPurchaseStatement, airlyticsPurchase);
                                if (userId != null) {
                                    PurchasesDAO.insertUpdatePurchaseUser(purchaseUserIdDevInsertStatement, airlyticsPurchase.getId(),
                                            tuple.getSecond().getUserId(), hashing.hashMurmur2(tuple.getSecond().getUserId()));
                                }
                                insertsInPurchaseBatch++;
                            } catch (Exception e) {
                                logException(e, "Error in processRecords, " + e.getMessage());
                                result = "error";
                            } finally {
                                processedPurchasesCounter.labels(tuple.getSecond().getType().name(), result,
                                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            }
                        }

                        subscriptionEvents = removeDuplications(subscriptionEvents, submittedSubscriptionEvents);
                        // send all subscription events as one batch
                        List<BaseSubscriptionEvent> sortedSubscriptionEvents = subscriptionEvents.stream().
                                sorted(Comparator.comparingLong(BaseSubscriptionEvent::getEventTime)).collect(Collectors.toList());
                        sendSubscriptionEventsBatch(sortedSubscriptionEvents, devEventProducerProxyClient, 3);

                    } else {
                        resultRecordProcess = "error";
                    }
                } else {

                    airlyticsPurchaseEvent.getPurchaseEvents().stream().forEach(event -> {
                        try {
                            PurchasesDAO.insertPurchaseEvent(
                                    purchaseEventInsertOnConflictDoNothingStatement, event);
                        } catch (SQLException sqlException) {
                            logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                        }
                    });

                    Collection<AirlyticsPurchase> updatedPurchase = processReceiptFromPurchase(tuple.getFirst().clone(), tuple.getSecond());
                    if (updatedPurchase != null) {
                        List<BaseSubscriptionEvent> subscriptionEvents = new ArrayList<>();
                        for (AirlyticsPurchase airlyticsPurchase : updatedPurchase) {
                            String result = "success";
                            try {
                                String userId = null;
                                if (tuple.getSecond().getType() == PurchaseMetaData.Type.APP) {
                                    userId = tuple.getSecond().getUserId();
                                    airlyticsPurchase.setUserId(userId);
                                }

                                subscriptionEvents.addAll(getSubscriptionEventsByPurchaseTransaction
                                        (airlyticsPurchase, getPurchaseTransactionByIdWithUserId, tuple.getSecond()));
                                PurchasesDAO.insertUpdatePurchase(purchaseStatement, airlyticsPurchase);
                                if (userId != null) {
                                    PurchasesDAO.insertUpdatePurchaseUser(purchaseUserIdInsertStatement, airlyticsPurchase.getId(),
                                            tuple.getSecond().getUserId(), hashing.hashMurmur2(tuple.getSecond().getUserId()));
                                }
                                insertsInPurchaseBatch++;
                            } catch (Exception e) {
                                logException(e, "Error in processRecords, " + e.getMessage());
                                result = "error";
                            } finally {
                                processedPurchasesCounter.labels(tuple.getSecond().getType().name(), result,
                                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            }
                        }

                        subscriptionEvents = removeDuplications(subscriptionEvents, submittedSubscriptionEvents);
                        // send all subscription events as one batch
                        List<BaseSubscriptionEvent> sortedSubscriptionEvents = subscriptionEvents.stream().
                                sorted(Comparator.comparingLong(BaseSubscriptionEvent::getEventTime)).collect(Collectors.toList());
                        sendSubscriptionEventsBatch(sortedSubscriptionEvents, eventProducerProxyClient, 3);
                    } else {
                        resultRecordProcess = "error";
                    }
                }
            } catch (Exception e) {
                resultRecordProcess = "error";
                logException(e, "Error in processRecords, " + e.getMessage());
            } finally {
                processedEventsCounter.labels(config.getPurchasesTopic(), resultRecordProcess,
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }
        }

        for (Pair<PurchaseMetaData, StoreResponse> tuple : nonValidPurchasesBatch) {
            String resultRecordProcess = "success";
            try {

                PurchaseMetaData purchaseMetaData = tuple.getFirst();

                PurchasesDAO.setPurchaseInActiveAndUpdateStoreResponse(purchaseStoreResponseUpdateStatement,
                        purchaseMetaData, tuple.getSecond());

                // try to update the develop users table, if the id doesn't exist there it will be skipped.
                PurchasesDAO.setPurchaseInActiveAndUpdateStoreResponse(devPurchaseStoreResponseUpdateStatement,
                        purchaseMetaData, tuple.getSecond());

                insertsInPurchaseBatch++;

            } catch (Exception e) {
                resultRecordProcess = "error";
                logException(e, "Error in processRecords, " + e.getMessage());
            } finally {
                processedEventsCounter.labels(config.getPurchasesTopic(), resultRecordProcess,
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }
        }

        commit();

        totalProgress += recordsProcessed.get();

        Instant now = Instant.now();
        Duration timePassed = Duration.between(lastRecordProcessed, now);
        if (timePassed.compareTo(Duration.ofSeconds(5)) >= 0) {
            LOGGER.debug("Processed " + totalProgress + " records. Current rate: " +
                    ((double) (totalProgress - lastProgress)) / timePassed.toMillis() * 1000 + " records/sec");
            lastRecordProcessed = now;
            lastProgress = totalProgress;
        }

        return recordsProcessed.get();
    }

    private void processNotificationsBatch(final List<Pair<PurchaseMetaData, IOSubscriptionPurchase>> notificationsBatch,
                                           Map<String, BaseSubscriptionEvent> submittedSubscriptionEvents) {
        for (Pair<PurchaseMetaData, IOSubscriptionPurchase> notificationBatchPair : notificationsBatch) {
            PurchaseMetaData purchaseMetaData = notificationBatchPair.getFirst();
            IOSubscriptionPurchase notificationEvent = notificationBatchPair.getSecond();

            String result = "success";
            if (notificationEvent != null) {

                IOSAirlyticsPurchaseEvent airlyticsPurchaseEvent = new IOSAirlyticsPurchaseEvent(
                        notificationEvent.clone(),
                        purchaseMetaData,
                        inAppAirlyticsProducts
                );

                if (notificationEvent.getEnvironment().equals(Environment.SAND_BOX.name)) {

                    airlyticsPurchaseEvent.getPurchaseEvents().stream().forEach(event -> {
                        try {
                            final List<AirlyticsPurchaseEvent> airlyticsPurchaseEventsHistory =
                                    PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID,
                                            purchaseMetaData.getPurchaseToken());
                            if (ifPurchaseEventExists(airlyticsPurchaseEventsHistory, event) &&
                                    !event.getName().equals(PurchaseEventType.RECOVERED.name())) {
                                PurchasesDAO.updatePurchaseEvent(purchaseEventDevUpdateStatement, event);
                            } else {
                                PurchasesDAO.insertPurchaseEvent(purchaseEventDevInsertOnConflictDoNothingStatement, event);
                            }

                        } catch (SQLException sqlException) {
                            logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                        }
                    });

                    // process notification
                    LOGGER.debug("Got iOS Sand Box server notification");
                    Collection<AirlyticsPurchase> updatedPurchase = processReceiptFromNotification(notificationEvent.clone(), purchaseMetaData);

                    if (updatedPurchase != null) {

                        List<BaseSubscriptionEvent> subscriptionEvents = new ArrayList<>();

                        for (AirlyticsPurchase airlyticsPurchase : updatedPurchase) {
                            String purchaseProcessResult = "success";
                            try {
                                subscriptionEvents.addAll(getSubscriptionEventsByPurchaseTransaction(airlyticsPurchase,
                                        getDevPurchaseTransactionByIdWithUserId, purchaseMetaData));
                                PurchasesDAO.insertUpdatePurchase(devPurchaseStatement, airlyticsPurchase);
                            } catch (SQLException e) {
                                logException(e, "Error in processRecords, " + e.getMessage());
                                purchaseProcessResult = "error";
                            } finally {
                                processedPurchasesCounter.labels(PurchaseMetaData.Type.NOTIFICATION.name(), purchaseProcessResult,
                                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            }
                        }

                        subscriptionEvents = removeDuplications(subscriptionEvents, submittedSubscriptionEvents);
                        // send all subscription events as one batch
                        List<BaseSubscriptionEvent> sortedSubscriptionEvents = subscriptionEvents.stream().
                                sorted(Comparator.comparingLong(BaseSubscriptionEvent::getEventTime)).collect(Collectors.toList());
                        sendSubscriptionEventsBatch(sortedSubscriptionEvents, devEventProducerProxyClient, 3);

                    } else {
                        result = "error";
                    }
                } else {

                    airlyticsPurchaseEvent.getPurchaseEvents().stream().forEach(event -> {
                        try {

                            final List<AirlyticsPurchaseEvent> airlyticsPurchaseEventsHistory =
                                    PurchasesDAO.getPurchaseEventsByPurchaseId(getPurchaseEventsByID,
                                            purchaseMetaData.getPurchaseToken());
                            if (ifPurchaseEventExists(airlyticsPurchaseEventsHistory, event) &&
                                    !event.getName().equals(PurchaseEventType.RECOVERED.name())) {
                                PurchasesDAO.updatePurchaseEvent(purchaseEventUpdateStatement, event);
                            } else {
                                PurchasesDAO.insertPurchaseEvent(purchaseEventInsertOnConflictDoNothingStatement, event);
                            }

                        } catch (SQLException sqlException) {
                            logException(sqlException, "Error in insertion purchase event, " + sqlException.getMessage());
                        }
                    });

                    // process notification
                    LOGGER.debug("Got iOS server notification");
                    Collection<AirlyticsPurchase> updatedPurchase = processReceiptFromNotification(notificationEvent.clone(), purchaseMetaData);

                    if (updatedPurchase != null) {

                        List<BaseSubscriptionEvent> subscriptionEvents = new ArrayList<>();

                        for (AirlyticsPurchase airlyticsPurchase : updatedPurchase) {
                            String purchaseProcessResult = "success";
                            try {
                                subscriptionEvents.addAll(getSubscriptionEventsByPurchaseTransaction(airlyticsPurchase,
                                        getPurchaseTransactionByIdWithUserId, purchaseMetaData));
                                PurchasesDAO.insertUpdatePurchase(purchaseStatement, airlyticsPurchase);
                                if (airlyticsPurchase.getUserId() != null) {
                                    PurchasesDAO.insertUpdatePurchaseUser(purchaseUserIdInsertStatement, airlyticsPurchase.getId(),
                                            airlyticsPurchase.getUserId(), hashing.hashMurmur2(airlyticsPurchase.getUserId()));

                                }
                            } catch (SQLException e) {
                                logException(e, "Error in processRecords, " + e.getMessage());
                                purchaseProcessResult = "error";
                            } finally {
                                processedPurchasesCounter.labels(PurchaseMetaData.Type.NOTIFICATION.name(), purchaseProcessResult,
                                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                            }
                        }

                        subscriptionEvents = removeDuplications(subscriptionEvents, submittedSubscriptionEvents);

                        // send all subscription events as one batch
                        List<BaseSubscriptionEvent> sortedSubscriptionEvents = subscriptionEvents.stream().
                                sorted(Comparator.comparingLong(BaseSubscriptionEvent::getEventTime)).collect(Collectors.toList());
                        sendSubscriptionEventsBatch(sortedSubscriptionEvents, eventProducerProxyClient, 3);

                    } else {
                        result = "error";
                    }
                }
            } else {
                result = "error";
            }
            processedEventsCounter.labels(config.getNotificationsTopic(), result,
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
        }
    }

    private Collection<AirlyticsPurchase> processReceiptFromNotification(IOSubscriptionPurchase notificationEvent,
                                                                         PurchaseMetaData purchaseMetaData) {
        try {
            return new IOSAirlyticsPurchaseState(notificationEvent, purchaseMetaData,
                    inAppAirlyticsProducts).getUpdatedAirlyticsPurchase();
        } catch (Exception e) {
            logException(e, "Error in processReceiptFromNotification, error: " + e.getMessage());
            return null;
        }

    }

    protected Collection<AirlyticsPurchase> processReceiptFromPurchase(IOSubscriptionPurchase newSubscriptionPurchase,
                                                                       PurchaseMetaData purchaseMetaData) {
        try {
            return new IOSAirlyticsPurchaseState(newSubscriptionPurchase,
                    purchaseMetaData,
                    inAppAirlyticsProducts).getUpdatedAirlyticsPurchase();
        } catch (Exception e) {
            logException(e, "Error in processReceiptFromPurchase, error: " + e.getMessage());
            return null;
        }
    }

    @CheckForNull
    private PurchaseMetaData retrievePurchaseMetaData(ConsumerRecord<String, JsonNode> record) throws JsonProcessingException {

        if (record.topic().equals(this.config.getPurchasesTopic())) {
            PurchaseMetaData purchaseMetaData = null;

            // allow processing old format RENEWAL_CHECK events which currently in the topic. Later will be removed
            if ((record.value().get("purchaseToken") != null && record.value().get("transactionId") != null)
                    || (record.value().get("encodedReceipt") != null && record.value().get("transactionId") != null)) {
                purchaseMetaData = new PurchaseMetaData();
                purchaseMetaData.setType(PurchaseMetaData.Type.RENEWAL_CHECK);

                purchaseMetaData.setPlatform(getPlatform());
                String purchaseToken = record.value().has("encodedReceipt") ? record.value().get("encodedReceipt").asText() :
                        record.value().get("purchaseToken").asText();
                purchaseMetaData.setPurchaseToken(purchaseToken);
                purchaseMetaData.setReceipt(purchaseToken);
                purchaseMetaData.setTransactionId(record.value().get("transactionId").asText());

                // we need the original event id to use it as a input for generation back-end events.
                purchaseMetaData.setOriginalEventId(record.value().has("eventId") ?
                        record.value().get("eventId").asText() :
                        UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());

                return purchaseMetaData;
            }


            PurchaseEvent purchaseEvent = new PurchaseEvent(record);
            Iterator<Map.Entry<String, JsonNode>> attributeIterator = purchaseEvent.getAttributes().fields();
            while (attributeIterator.hasNext()) {
                Map.Entry<String, JsonNode> attribute = attributeIterator.next();

                if (attribute.getKey().equals("encodedReceipt")) {
                    purchaseMetaData = new PurchaseMetaData();
                    purchaseMetaData.setType(PurchaseMetaData.Type.APP);
                    purchaseMetaData.setUserId(purchaseEvent.getUserId());
                    purchaseMetaData.setReceipt(attribute.getValue().asText());
                    purchaseMetaData.setPurchaseToken(attribute.getValue().asText());
                    purchaseMetaData.setPlatform(getPlatform());

                    // we need the original event id to use it as a input for generation back-end events.
                    purchaseMetaData.setOriginalEventId(record.value().has("eventId") ?
                            record.value().get("eventId").asText() :
                            UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());
                }
            }


            if (purchaseMetaData == null) {
                processedEventsCounter.labels(record.topic(), "error",
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                LOGGER.error("Malformed purchase event:" + record.value().toPrettyString());
            }
            return purchaseMetaData;
        } else {
            IOSNotificationEvent notificationEvent = objectMapper.treeToValue(record.value(), IOSNotificationEvent.class);

            // process notification
            if (notificationEvent != null) {
                LOGGER.debug("Got iOS server notification");

                PurchaseMetaData purchaseMetaData = new PurchaseMetaData();
                purchaseMetaData.setType(PurchaseMetaData.Type.NOTIFICATION);
                purchaseMetaData.setPlatform(getPlatform());
                purchaseMetaData.setNotificationType(notificationEvent.getNotificationType());
                purchaseMetaData.setReceipt(notificationEvent.getUnifiedReceipt() == null ? null :
                        notificationEvent.getUnifiedReceipt().getLatestReceipt());
                purchaseMetaData.setPurchaseToken(notificationEvent.getUnifiedReceipt() == null ? null :
                        notificationEvent.getUnifiedReceipt().getLatestReceipt());
                purchaseMetaData.setAutoRenewStatusChangeDateMs(notificationEvent.getAutoRenewStatusChangeDateMs());
                purchaseMetaData.setProductId(notificationEvent.getAutoRenewProductId());
                purchaseMetaData.setCancellationDateMs(notificationEvent.getCancellationDateMs());

                // need the original event id to use it as a input for generation back-end events.
                // notification doesn't have its own eventId that's why we use the whole notification value as an input.
                purchaseMetaData.setOriginalEventId(UUID.nameUUIDFromBytes(record.value().asText().getBytes()).toString());

                return purchaseMetaData;
            } else {
                processedEventsCounter.labels(record.topic(), "error",
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                LOGGER.error("Malformed purchase event:" + record.value().toPrettyString());
            }
        }
        return null;
    }
}
