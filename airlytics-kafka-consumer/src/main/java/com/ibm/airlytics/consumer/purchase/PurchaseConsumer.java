package com.ibm.airlytics.consumer.purchase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.purchase.events.*;
import com.ibm.airlytics.eventproxy.EventApiClient;
import com.ibm.airlytics.eventproxy.EventApiException;
import com.ibm.airlytics.producer.Producer;
import com.ibm.airlytics.utilities.Constants;
import com.ibm.airlytics.utilities.Hashing;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@SuppressWarnings("DuplicatedCode")
public abstract class PurchaseConsumer extends AirlyticsConsumer {
    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(PurchaseConsumer.class.getName());
    protected Hashing hashing;

    protected static final Counter processedEventsCounter = Counter.build()
            .name("airlytics_purchase_consumer_events_processed_total")
            .help("Total records processed by the purchase consumer.")
            .labelNames("type", "result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Counter processedPurchasesCounter = Counter.build()
            .name("airlytics_purchase_consumer_purchases_processed_total")
            .help("Total records processed by the purchase consumer.")
            .labelNames("type", "result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    protected static final Counter subscriptionEventsCounter = Counter.build()
            .name("airlytics_purchase_consumer_subscription_events")
            .help("Total number of subscription events are sent")
            .labelNames("result", "type", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    private static final Counter commitsCounter = Counter.build()
            .name("airlytics_purchase_consumer_commits_total")
            .help("Total commits to the db by the purchase consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Summary dbOperationsLatencySummary = Summary.build()
            .name("airlytics_purchase_consumer_db_latency_seconds")
            .help("DB operation latency seconds.")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Summary dbCommandsSummary = Summary.build()
            .name("airlytics_purchase_consumer_db_commands")
            .help("Number of db commands executed from purchase consumer")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Counter apiRequestCounter = Counter.build()
            .name("airlytics_purchase_consumer_api_request")
            .help("Number of Mobile API calls executed")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Counter apiResponseCounter = Counter.build()
            .name("airlytics_purchase_consumer_api_call_response")
            .help("Number of Mobile API calls response")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    protected static final Counter pollingCommandsCounter = Counter.build()
            .name("airlytics_purchase_consumer_polling_status_check")
            .help("History of status check purchases polling commands")
            .labelNames("type", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();


    protected Connection executionConn;
    // separate connection to allow batch reading (not possible to read batches and commit in while reading)
    protected Connection longReadQueryConn;
    protected PurchaseConsumerConfig config;
    private PurchaseConsumerConfig newConfig = null;
    protected PreparedStatement purchaseStatement = null;
    protected PreparedStatement purchaseStoreResponseUpdateStatement = null;
    protected PreparedStatement devPurchaseStoreResponseUpdateStatement = null;
    protected PreparedStatement devPurchaseStatement = null;
    protected PreparedStatement purchaseUserIdInsertStatement = null;
    protected PreparedStatement purchaseUserIdDevInsertStatement = null;
    protected PreparedStatement getPurchaseTransactionByIdWithUserId = null;
    protected PreparedStatement getDevPurchaseTransactionByIdWithUserId = null;
    protected PreparedStatement getPurchaseTransactionById = null;
    protected PreparedStatement getDevPurchaseTransactionById = null;
    protected PreparedStatement purchaseEventUpdateStatement = null;
    protected PreparedStatement purchaseEventDevUpdateStatement = null;
    protected PreparedStatement purchaseEventInsertOnConflictDoNothingStatement = null;
    protected PreparedStatement purchaseEventDevInsertOnConflictDoNothingStatement = null;
    protected PreparedStatement getPurchaseEventsByID = null;
    protected PreparedStatement getDevPurchaseEventsByID = null;
    protected int insertsInPurchaseBatch = 0;
    protected int insertsInPurchaseUsersBatch = 0;
    protected List<AirlyticsPurchase> eventsBatchToEventProxy = new ArrayList<>();
    protected long totalProgress = 0;
    protected long lastProgress = 0;
    protected static final ObjectMapper objectMapper = new ObjectMapper();
    protected Instant lastRecordProcessed = Instant.now();
    protected Producer purchaseTopic = null;
    protected EventApiClient eventProducerProxyClient = null;
    protected EventApiClient devEventProducerProxyClient = null;
    protected final Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts = new Hashtable<>();
    protected static final ScheduledThreadPoolExecutor renewalRefreshScheduler = new ScheduledThreadPoolExecutor(1);
    protected static final ScheduledThreadPoolExecutor refreshAllPurchasesScheduler = new ScheduledThreadPoolExecutor(1);
    protected static final ScheduledThreadPoolExecutor renewalRefreshShortIntervalScheduler = new ScheduledThreadPoolExecutor(1);
    private static final ScheduledThreadPoolExecutor inAppProductsListRefreshScheduler = new ScheduledThreadPoolExecutor(1);
    protected final ArrayList<PreparedStatement> preparedStatements = new ArrayList<>();


    public static final int CACHE_SIZE = 2000;
    // hold the cache of 2000 recently send subscription event to prevent duplicates
    // subscription-events and user-attribute-detected events to prevent double event sending.
    protected Map<String, BaseSubscriptionEvent> submittedSubscriptionEvents = null;


    public enum PurchaseEventType {
        UPGRADED("UPGRADED"), // IOS - upgrade
        UPGRADE_PURCHASED("UPGRADE_PURCHASED"), // IOS - start after upgrade
        RENEWAL_PLAN_CHANGED("RENEWAL_PLAN_CHANGED"), //iOS Indicates that the customer made a change in their subscription plan
        RENEWAL_STATUS_CHANGED("RENEWAL_STATUS_CHANGED"), //iOS Indicates a change in the subscription renewal status
        INTERACTIVE_RENEWAL("INTERACTIVE_RENEWAL"), //  iOS - customer renewed a subscription interactively
        CONSUMPTION_REQUEST("CONSUMPTION_REQUEST"),// iOS - Indicates that the customer initiated a refund request
        PAUSED("PAUSED"), // Android (A new subscription was purchased).
        PLACED_ON_HOLD("PLACED_ON_HOLD"), // Android (A new subscription is on hold).
        REACTIVATED("REACTIVATED"), //  Android: enabled auto-renew
        PRICE_CHANGE_CONFIRMED("PRICE_CHANGE_CONFIRMED"),  // Android: new price confirmed by the user.
        DEFERRED("DEFERRED"), // Android (A subscription's recurrence time has been extended.)
        PAUSE_SCHEDULE_CHANGED("PAUSE_SCHEDULE_CHANGED"), // Android ( A sub pause schedule has been changed)
        CANCELED("CANCELED"), // common - refund
        TRIAL_STARTED("TRIAL_STARTED"),  //  common - trial purchase
        PURCHASED("PURCHASED"), // common - regular purchase
        EXPIRED("EXPIRED"), // common - expiration date
        RECOVERED("RECOVERED"), // common - renewed after billing problem
        RENEWED("RENEWED"), // common - renewed subscription
        IN_GRACE_PERIOD("IN_GRACE_PERIOD"), //common - got into grace
        BILLING_ISSUE("BILLING_ISSUE"), //common - got into billing_issue
        RENEWAL_FAILED("RENEWAL_FAILED"), //common - renewal failed due to billing or another reason
        TRIAL_CONVERTED("TRIAL_CONVERTED"), //common - event on trial converted to paid
        TRIAL_EXPIRED("TRIAL_EXPIRED"); //common - event on trial expired
        // semicolon needed when fields / methods follow

        private final String eventType;

        PurchaseEventType(String eventType) {
            this.eventType = eventType;
        }

        public String getEventType() {
            return eventType;
        }

        public static String getNameByType(String eventType) {
            if (eventType == null) {
                return null;
            }
            for (PurchaseConsumer.PurchaseEventType e : PurchaseConsumer.PurchaseEventType.values()) {
                if (eventType == e.eventType) return e.name();
            }
            return null;
        }

        public static PurchaseConsumer.PurchaseEventType getValueByType(String status) {
            if (status == null) {
                return null;
            }
            for (PurchaseConsumer.PurchaseEventType e : PurchaseConsumer.PurchaseEventType.values()) {
                if (status == e.eventType) return e;
            }
            return null;
        }
    }

    @SuppressWarnings("unused")
    public enum StoreResponse {
        OK,
        OLD,
        NOT_VALID,
        NETWORK_ERROR
    }

    public enum SubscriptionStatus {
        CANCELED("CANCELED"),
        ACTIVE("ACTIVE"),
        EXPIRED("EXPIRED"),
        UPGRADED("UPGRADED"),
        PAUSED("PAUSED"),
        ON_HOLD("ON HOLD"); // semicolon needed when fields / methods follow

        private final String status;

        SubscriptionStatus(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }

        public static String getNameByType(String status) {
            if (status == null) {
                return null;
            }
            for (PurchaseConsumer.SubscriptionStatus e : PurchaseConsumer.SubscriptionStatus.values()) {
                if (status == e.status) return e.name();
            }
            return null;
        }

        public static PurchaseConsumer.SubscriptionStatus getValueByType(String status) {
            if (status == null) {
                return null;
            }
            for (PurchaseConsumer.SubscriptionStatus e : PurchaseConsumer.SubscriptionStatus.values()) {
                if (status == e.status) return e;
            }
            return null;
        }
    }

    public PurchaseConsumer() {
        super();
    }

    public PurchaseConsumer(PurchaseConsumerConfig config) {
        super(config);
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

        this.eventProducerProxyClient = new EventApiClient(this.config.getEventApiBaseUrl(), this.config.getEventApiPath(),
                this.config.getEventProxyApiKey());

        this.devEventProducerProxyClient = new EventApiClient(this.config.getEventApiBaseUrl(), this.config.getEventApiPath(),
                this.config.getEventProxyDevApiKey());

        renewalRefreshScheduler.scheduleAtFixedRate(this::updatePurchasesRenewalStatus,
                config.getUpdatePurchaseRenewalStatusInitDelay(),
                config.getUpdatePurchaseRenewalStatusInterval(),
                TimeUnit.SECONDS);

        renewalRefreshShortIntervalScheduler.scheduleAtFixedRate(this::updateStaledPurchasesStatus,
                config.getUpdatePurchaseRenewalStatusInitDelay(),
                config.getUpdatePurchaseRenewalStatusShortInterval(),
                TimeUnit.SECONDS);

        refreshAllPurchasesScheduler.scheduleAtFixedRate(this::updateAllPurchasesStatus,
                config.getFullPurchaseUpdateInitDelay(),
                config.getFullPurchaseUpdateInterval(),
                TimeUnit.SECONDS);

        purchaseTopic = new Producer(config.getPurchasesTopic(), config.getBootstrapServers(),
                config.getSecurityProtocol(), config.getProducerCompressionType(), config.getLingerMs());

        inAppProductsListRefreshScheduler.scheduleAtFixedRate(this::refreshInAppsList,
                config.getRefreshInAppProductsLisInitDelay(), config.getRefreshInAppProductsListInterval(), TimeUnit.SECONDS);

    }

    private void sendPollingNotification(AirlyticsPurchase airlyticsPurchase) {
        JsonNode node = objectMapper.valueToTree(new RenewalCheckEvent(
                airlyticsPurchase.getReceipt() != null ? airlyticsPurchase.getReceipt() : airlyticsPurchase.getId(),
                airlyticsPurchase.getProduct(),
                airlyticsPurchase.getId(), System.currentTimeMillis(), getPlatform()));
        // send to purchase topic
        purchaseTopic.sendRecord(airlyticsPurchase.getId(), airlyticsPurchase.getId(), node, System.currentTimeMillis());
    }

    private void updateStaledPurchasesStatus() {
        try {
            PurchasesDAO.notifyPurchaseCandidatesForUpdate(
                    PurchasesDAO.getQueryStalePurchaseCandidatesPreparedStatement(longReadQueryConn, config.getPurchasesTable(),
                            getPlatform(), filterNonRenewalProduct()),
                    dbOperationsLatencySummary, pollingCommandsCounter, "hourly", this::sendPollingNotification);
        } catch (SQLException e) {
            logException(e, "Error in updateStaledPurchasesStatus,details: " + e.getMessage());
        }
    }

    // platform should be derived from the product name,
    // is used in the purchases table, probably later will be removed
    public static String getPlatform() {
        return AirlockManager.getProduct().toLowerCase().contains("ios") ? "ios" : "android";
    }

    private void updateAllPurchasesStatus() {
        try {
            PurchasesDAO.notifyPurchaseCandidatesForUpdate(
                    PurchasesDAO.getAllPurchases(longReadQueryConn, config.getPurchasesTable(), getPlatform()),
                    dbOperationsLatencySummary, pollingCommandsCounter, "full", this::sendPollingNotification);
        } catch (SQLException e) {
            logException(e, "Error in updateAllPurchasesStatus,details: " + e.getMessage());
        }
    }

    private void updatePurchasesRenewalStatus() {
        try {
            PurchasesDAO.notifyPurchaseCandidatesForUpdate(
                    PurchasesDAO.getQueryRenewedPurchaseCandidatesPreparedStatement(longReadQueryConn, config.getPurchasesTable(),
                            getPlatform(), filterNonRenewalProduct()),
                    dbOperationsLatencySummary, pollingCommandsCounter, "daily", this::sendPollingNotification);
        } catch (SQLException e) {
            logException(e, "Error in updatePurchasesRenewalStatus, details: " + e.getMessage());
        }
    }


    protected abstract void refreshInAppsList();


    protected void prepareStatements() {
        try {
            purchaseStatement = PurchasesDAO.getInsertPurchasePreparedStatement(executionConn, config.getPurchasesTable());
            purchaseUserIdInsertStatement = PurchasesDAO.getInsertPurchaseUserIdPreparedStatement(executionConn, config.getPurchasesUsersTable());

            devPurchaseStatement = PurchasesDAO.getInsertPurchasePreparedStatement(executionConn, config.getPurchasesDevTable());
            purchaseUserIdDevInsertStatement = PurchasesDAO.getInsertPurchaseUserIdPreparedStatement(executionConn, config.getPurchasesUsersDevTable());
            purchaseStoreResponseUpdateStatement = PurchasesDAO.getUpdatePurchaseStoreResponsePreparedStatement(executionConn, config.getPurchasesTable(), getPlatform());
            devPurchaseStoreResponseUpdateStatement = PurchasesDAO.getUpdatePurchaseStoreResponsePreparedStatement(executionConn, config.getPurchasesDevTable(), getPlatform());

            getPurchaseTransactionByIdWithUserId = PurchasesDAO.getTransactionByIdWithUserIdPreparedStatement(executionConn, config.getPurchasesTable(), config.getPurchasesUsersTable());
            getDevPurchaseTransactionByIdWithUserId = PurchasesDAO.getTransactionByIdWithUserIdPreparedStatement(executionConn, config.getPurchasesDevTable(), config.getPurchasesUsersDevTable());

            getPurchaseTransactionById = PurchasesDAO.getTransactionByIdPreparedStatement(executionConn, config.getPurchasesTable());
            getDevPurchaseTransactionById = PurchasesDAO.getTransactionByIdPreparedStatement(executionConn, config.getPurchasesDevTable());

            purchaseEventUpdateStatement = PurchasesDAO.getUpdatePurchaseEventPreparedStmt(executionConn, config.getPurchasesEventsTable());
            purchaseEventDevUpdateStatement = PurchasesDAO.getUpdatePurchaseEventPreparedStmt(executionConn, config.getPurchasesEventsDevTable());

            purchaseEventInsertOnConflictDoNothingStatement = PurchasesDAO.getInsertOnConflictDoNothingPurchaseEventPreparedStmt(executionConn, config.getPurchasesEventsTable());
            purchaseEventDevInsertOnConflictDoNothingStatement = PurchasesDAO.getInsertOnConflictDoNothingPurchaseEventPreparedStmt(executionConn, config.getPurchasesEventsDevTable());

            getPurchaseEventsByID = PurchasesDAO.getPurchaseEventsByPurchaseIdPreparedStatement(executionConn, config.getPurchasesEventsTable());
            getDevPurchaseEventsByID = PurchasesDAO.getPurchaseEventsByPurchaseIdPreparedStatement(executionConn, config.getPurchasesEventsDevTable());

            preparedStatements.add(purchaseStatement);
            preparedStatements.add(purchaseUserIdInsertStatement);
            preparedStatements.add(devPurchaseStatement);
            preparedStatements.add(purchaseUserIdDevInsertStatement);
            preparedStatements.add(purchaseStoreResponseUpdateStatement);
            preparedStatements.add(devPurchaseStoreResponseUpdateStatement);
            preparedStatements.add(getPurchaseTransactionByIdWithUserId);
            preparedStatements.add(getDevPurchaseTransactionByIdWithUserId);
            preparedStatements.add(getPurchaseTransactionById);
            preparedStatements.add(getDevPurchaseTransactionById);
            preparedStatements.add(purchaseEventDevUpdateStatement);
            preparedStatements.add(purchaseEventUpdateStatement);
            preparedStatements.add(purchaseEventDevInsertOnConflictDoNothingStatement);
            preparedStatements.add(purchaseEventInsertOnConflictDoNothingStatement);
            preparedStatements.add(getPurchaseEventsByID);
            preparedStatements.add(getDevPurchaseEventsByID);

        } catch (SQLException e) {
            logException(e, "Error while init Purchase Consumer, details: " + e.getMessage());
            stop();
        }
    }


    protected boolean ifPurchaseEventExists(List<AirlyticsPurchaseEvent> events,String id, String name, Long time) {
        return events.stream().anyMatch(event -> event.getName().equals(name) &&
                event.getPurchaseId().equals(id) &&
                event.getEventTime().equals(time));
    }


    protected boolean ifPurchaseEventExists(List<AirlyticsPurchaseEvent> events, AirlyticsPurchaseEvent eventToLookFor) {
        return events.stream().anyMatch(event -> event.getName().equals(eventToLookFor.getName()) &&
                event.getPurchaseId().equals(eventToLookFor.getPurchaseId()) &&
                event.getEventTime().equals(eventToLookFor.getEventTime()));
    }

    public List<String> filterNonRenewalProduct() {
        return this.inAppAirlyticsProducts.values().stream().filter(product -> !product.isAutoRenewing()).
                map(product -> product.getProductId()).collect(Collectors.toList());
    }

    protected static Connection initConnection(PurchaseConsumerConfig config) throws ClassNotFoundException, SQLException {
        // Setup connection pool
        Class.forName(Constants.JDBC_DRIVER);

        Properties props = new Properties();
        props.setProperty("user", config.getDbUsername());
        props.setProperty("password", config.getDbPassword());
        props.setProperty("reWriteBatchedInserts", "true");
        if (config.isUseSSL()) {
            props.setProperty("ssl", "true");
            props.setProperty("sslmode", "verify-full");
        }
        Connection conn = DriverManager.getConnection(config.getDbUrl(), props);
        conn.setAutoCommit(false);
        return conn;
    }


    /**
     * The method filters out subscription events from a give purchase
     * In the case we got to process two diff notification types for the same purchase
     * in a single batch from the queue.
     *
     * @param newSubscriptionEvent        the list of subscription events
     * @param submittedSubscriptionEvents all submitted subscription events in the scope of specific queue batch
     * @return the list without duplications
     */
    public List<BaseSubscriptionEvent> removeDuplications(Collection<BaseSubscriptionEvent> newSubscriptionEvent,
                                                          Map<String, BaseSubscriptionEvent> submittedSubscriptionEvents) {
        if (newSubscriptionEvent == null || newSubscriptionEvent.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<BaseSubscriptionEvent> noDuplicationsArray = new ArrayList<>();
        newSubscriptionEvent.forEach(subscriptionEvent -> {
            String key = subscriptionEvent.getUserId() + subscriptionEvent.getName() + subscriptionEvent.getEventTime() +
                    subscriptionEvent.getPremiumProductId();
            if (!submittedSubscriptionEvents.containsKey(key)) {
                noDuplicationsArray.add(subscriptionEvent);
                submittedSubscriptionEvents.put(key, subscriptionEvent);
            }
        });
        return Collections.unmodifiableList(noDuplicationsArray);
    }


    public ArrayList<BaseSubscriptionEvent> getSubscriptionEventsPerPurchase(AirlyticsPurchase updatedAirlyticsPurchase,
                                                                             AirlyticsPurchase currentAirlyticsPurchaseWithUserId,
                                                                             String originalEventId,
                                                                             String airlockProductId,
                                                                             Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts,
                                                                             Counter subscriptionEventsCounter,
                                                                             String platform,
                                                                             long maxSubscriptionEventAge
    ) throws IllegalSubscriptionEventArguments {
        ArrayList<BaseSubscriptionEvent> events = new ArrayList<>();
        // the transaction exists, probably its state changed if yes, send user-attribute-detection event
        if (currentAirlyticsPurchaseWithUserId != null &&
                currentAirlyticsPurchaseWithUserId.getUserId() != null) {

            // found user id, set the missed userId in the updated purchase
            updatedAirlyticsPurchase.setUserId(currentAirlyticsPurchaseWithUserId.getUserId());

            Long subscriptionEventTime = null;
            if (currentAirlyticsPurchaseWithUserId.isCancelledNow(updatedAirlyticsPurchase)) {
                subscriptionEventTime = updatedAirlyticsPurchase.getCancellationDate();
                events.add(new SubscriptionExpiredEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                        airlockProductId, originalEventId, platform));
                events.add(new SubscriptionCancelledEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                        airlockProductId, originalEventId, platform));

            } else if (currentAirlyticsPurchaseWithUserId.isExpiredNow(updatedAirlyticsPurchase)) {
                subscriptionEventTime = updatedAirlyticsPurchase.getExpirationDate();
                events.add(new SubscriptionExpiredEvent(currentAirlyticsPurchaseWithUserId,
                        updatedAirlyticsPurchase, airlockProductId, originalEventId, platform));
            }

            if (currentAirlyticsPurchaseWithUserId.isUpgradedNow(updatedAirlyticsPurchase)) {
                if (updatedAirlyticsPurchase.getUpgradedToProduct() != null) {
                    subscriptionEventTime = updatedAirlyticsPurchase.getUpgradedToProduct().getPurchaseDateMs();
                    events.add(new SubscriptionUpgradedEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                            airlockProductId, inAppAirlyticsProducts, originalEventId, platform));
                }
            }

            if (currentAirlyticsPurchaseWithUserId.isRenewedNow(updatedAirlyticsPurchase)) {
                subscriptionEventTime = currentAirlyticsPurchaseWithUserId.getExpirationDate();
                events.add(new SubscriptionRenewedEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                        airlockProductId, originalEventId, platform));

            }

            if (currentAirlyticsPurchaseWithUserId.isRenewalStatusChangedNow(updatedAirlyticsPurchase)) {
                if (updatedAirlyticsPurchase.getAutoRenewStatusChangeDate() != null) {
                    subscriptionEventTime = updatedAirlyticsPurchase.getAutoRenewStatusChangeDate();
                    events.add(new SubscriptionRenewalStatusChangedSubscriptionEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                            airlockProductId, originalEventId, platform));
                }
            }


            // send UserAttributeDetectedEvent only if any of the sub event happened and
            // the previous state has been changed
            if (events.size() > 0 && !currentAirlyticsPurchaseWithUserId.stateEquals(updatedAirlyticsPurchase)
                    && subscriptionEventTime != null) {
                events.add(
                        new UserAttributeDetectedEvent(currentAirlyticsPurchaseWithUserId, updatedAirlyticsPurchase,
                                airlockProductId, subscriptionEventTime, originalEventId, platform));
            }
        } else if (updatedAirlyticsPurchase.getUserId() != null && updatedAirlyticsPurchase.getUpgradedFromProduct() == null) {
            // if new purchase is older the MaxSubscriptionEventAge skip it
            if ((System.currentTimeMillis() - updatedAirlyticsPurchase.getStartDate()) / 1000 < maxSubscriptionEventAge) {
                events.addAll(getSubscriptionFromNewPurchase(updatedAirlyticsPurchase, originalEventId,
                        airlockProductId, inAppAirlyticsProducts, platform));
            } else {
                if (subscriptionEventsCounter != null) {
                    subscriptionEventsCounter.labels("old purchase",
                            SubscriptionPurchasedEvent.EVENT_NAME, AirlockManager.getEnvVar(),
                            platform).inc();
                }
            }
        }
        return events;
    }


    public ArrayList<BaseSubscriptionEvent> getSubscriptionEvents(AirlyticsPurchase updatedAirlyticsPurchase,
                                                                  PreparedStatement preparedStatement,
                                                                  String originalEventId) throws SQLException, IllegalSubscriptionEventArguments {
        AirlyticsPurchase currentAirlyticsPurchaseWithUserId = PurchasesDAO.getTransactionByIdWithUserId(preparedStatement,
                dbCommandsSummary, updatedAirlyticsPurchase.getId());

        return getSubscriptionEventsPerPurchase(updatedAirlyticsPurchase,
                currentAirlyticsPurchaseWithUserId,
                originalEventId, config.getAirlockProductId(), inAppAirlyticsProducts, subscriptionEventsCounter,
                getPlatform(), config.getMaxSubscriptionEventAge());


    }

    public ArrayList<BaseSubscriptionEvent> getSubscriptionFromNewPurchase(AirlyticsPurchase newAirlyticsPurchase,
                                                                           String originalEventId,
                                                                           String airlockProductId,
                                                                           Map<String, AirlyticsInAppProduct> inAppAirlyticsProducts,
                                                                           String platform) throws IllegalSubscriptionEventArguments {
        // the purchase event should be sent in any way.
        ArrayList<BaseSubscriptionEvent> events = new ArrayList<>();
        events.add(new SubscriptionPurchasedEvent(null,
                newAirlyticsPurchase, airlockProductId, originalEventId, platform));

        // check if there are historical renewals
        AirlyticsPurchase previousRenewal = newAirlyticsPurchase;
        for (AirlyticsPurchase airlyticsPurchase : newAirlyticsPurchase.getRenewalsHistory()) {
            events.add(new SubscriptionRenewedEvent(previousRenewal,
                    newAirlyticsPurchase, airlockProductId, originalEventId, platform));
            previousRenewal = airlyticsPurchase;
        }

        // send upgrade event
        if (newAirlyticsPurchase.getUpgraded() != null && newAirlyticsPurchase.getUpgraded()) {
            events.add(new SubscriptionUpgradedEvent(null,
                    newAirlyticsPurchase, airlockProductId, inAppAirlyticsProducts, originalEventId, platform));
        } else if (newAirlyticsPurchase.getCancellationDate() != null) {
            // send cancelled event
            events.add(new SubscriptionCancelledEvent(null,
                    newAirlyticsPurchase, airlockProductId, originalEventId, platform));
            events.add(new SubscriptionExpiredEvent(null,
                    newAirlyticsPurchase, airlockProductId, originalEventId, platform));
        } else if (!newAirlyticsPurchase.getActive()) {
            // the subscription doesn't exist in the DB either recently purchased or sent with the delay.
            // if not active meaning was expired.
            events.add(new SubscriptionExpiredEvent(null,
                    newAirlyticsPurchase, airlockProductId, originalEventId, platform));
        }

        // send UserAttributeDetectedEvent only if a subscription is active
        if (newAirlyticsPurchase.getActive() || newAirlyticsPurchase.getUpgradedToProductActive()) {
            events.add(new UserAttributeDetectedEvent(null,
                    newAirlyticsPurchase, airlockProductId,
                    newAirlyticsPurchase.getStartDate(), originalEventId, platform));
        }
        return events;
    }

    protected void sendSubscriptionEventsBatch(Collection<BaseSubscriptionEvent> events,
                                               EventApiClient eventProducerProxyClient, int attempt) {


        // we skip sending subscription events if this capability is off or there is nothing to send
        if (events == null || events.isEmpty() || !config.isSubscriptionEventsEnabled()) {
            return;
        }

        // the subscription events batch sending failed.
        // walk through all events and send them to the error topic
        if (attempt == 0) {
            if (errorsProducer != null) {
                events.stream().forEach(baseSubscriptionEvent -> {
                    purchaseTopic.sendRecord(baseSubscriptionEvent.getEventId(), baseSubscriptionEvent.getEventId(),
                            objectMapper.valueToTree(baseSubscriptionEvent), System.currentTimeMillis());
                    subscriptionEventsCounter.labels("event not sent", baseSubscriptionEvent.getName(),
                            AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();

                });
            }
        }

        List<LinkedHashMap> validationErrors = new ArrayList<>();
        JSONObject batch = new JSONObject();
        JSONArray batchEvents = new JSONArray();
        batch.put("events", batchEvents);
        events.stream().forEach(baseSubscriptionEvent -> {
            batchEvents.put(baseSubscriptionEvent);
        });

        try {
            eventProducerProxyClient.post(objectMapper.writeValueAsString(batch.toMap()));
        } catch (IOException e) {
            logException(e, e.getMessage());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException interruptedException) {
                // ignore
            }
            sendSubscriptionEventsBatch(events, eventProducerProxyClient, attempt--);
        } catch (EventApiException e) {
            try {
                validationErrors.addAll(objectMapper.readValue(e.getResponseBody(), List.class));
                handleValidationErrors(validationErrors, events);
            } catch (JsonProcessingException jsonProcessingException) {
                // ignore
            }
        }

        events.stream().forEach(event -> {
            if (!validationErrors.stream().
                    filter(error -> error.get("eventId").equals(event.getEventId())).findFirst().isPresent()) {
                subscriptionEventsCounter.labels("success", event.getName(),
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }
        });
    }


    private void handleValidationErrors(List<LinkedHashMap> errors, Collection<BaseSubscriptionEvent> events) {
        errors.forEach(error -> {
            if (error.containsKey("eventId") && error.get("eventId") != null) {
                BaseSubscriptionEvent eventFailedValidation
                        = events.stream().filter(event -> event.getEventId().equals(error.get("eventId"))).findFirst().get();
                try {
                    LOGGER.warn(eventFailedValidation.toJson());
                } catch (JsonProcessingException e) {
                    //ignore
                }
                subscriptionEventsCounter.labels("validation error", eventFailedValidation.getName(), AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
            }
        });
    }

    protected Collection<BaseSubscriptionEvent> getSubscriptionEventsByPurchaseTransaction(AirlyticsPurchase updatedAirlyticsPurchase, PreparedStatement preparedStatement,
                                                                                           PurchaseMetaData purchaseMetaData) {
        try {
            //set product price to the AirlyticsPurchase
            updatedAirlyticsPurchase.setPriceUsdMicros(inAppAirlyticsProducts.get(updatedAirlyticsPurchase.getProduct()).getPriceUSDMicros());
            return getSubscriptionEvents(updatedAirlyticsPurchase,
                    preparedStatement, purchaseMetaData.originalEventId);
        } catch (SQLException sqlException) {
            LOGGER.error(sqlException.getMessage());
        } catch (IllegalSubscriptionEventArguments illegalArguments) {
            LOGGER.warn(illegalArguments.getMessage());
            subscriptionEventsCounter.labels("creation error",
                    illegalArguments.getEventName(), AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
        }
        return Collections.emptyList();
    }


    @Override
    protected abstract int processRecords(ConsumerRecords<String, JsonNode> records);


    protected void logException(Exception e, String introMessage) {
        if (e instanceof SQLException) {
            SQLException se = (SQLException) e;
            LOGGER.error(introMessage + " SQLState: " + ((SQLException) e).getSQLState() + " " + e.getMessage());
            e.printStackTrace();
            while (se.getNextException() != null) {
                se = se.getNextException();
                LOGGER.error(se.getMessage());
            }
        } else {
            LOGGER.error(introMessage + " " + e.getMessage());
        }
    }

    protected void flushInsertsAndUpdates() throws SQLException {
        Summary.Timer insertTimer = startDBLatencyTimer("insertPurchase");

        for (PreparedStatement preparedStatement : preparedStatements) {
            try {
                preparedStatement.executeBatch();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        insertTimer.observeDuration();
        observeDBCommands("insertPurchase", insertsInPurchaseBatch);
        insertsInPurchaseBatch = 0;

        insertTimer = startDBLatencyTimer("insertPurchaseUser");
        purchaseUserIdInsertStatement.executeBatch();
        insertTimer.observeDuration();
        observeDBCommands("insertPurchaseUser", insertsInPurchaseUsersBatch);
        insertsInPurchaseUsersBatch = 0;

        commitDB();
    }

    protected void commitDB() throws SQLException {
        Summary.Timer commitTimer = startDBLatencyTimer("commit");
        executionConn.commit();
        commitTimer.observeDuration();
        observeDBCommands("commit", 1);
        commitsCounter.labels(AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
    }

    @Override
    protected void commit() {
        try {
            flushInsertsAndUpdates();
            commitDB();
            super.commit();

            //once the commit is done the events should be send to the Events Proxy
            //sendEventsBatchToEventProxy(this.eventsBatchToEventProxy, this.eventProducerProxyClient, 1);
        } catch (Exception e) {
            logException(e, "Error during purchase-attribute consumer commit function. Stopping Consumer.");
            // Don't bother to rollback kafka. Just exit the process (close the consumer on the way) and let Kube restart this service.
            // Since we use "enable.auto.commit=false" this will effectively rollback Kafka.
            e.printStackTrace();
            rollback();
            stop();
        }
    }

    private void closeIfNotNull(AutoCloseable autoCloseable) throws Exception {
        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }

    @Override
    public void close() throws Exception {
        for (PreparedStatement preparedStatement : preparedStatements) {
            closeIfNotNull(preparedStatement);
        }
        closeIfNotNull(executionConn);
        closeIfNotNull(longReadQueryConn);
        super.close();
    }

    private String healthMessage = "";

    @Override
    public boolean isHealthy() {
        if (!super.isHealthy())
            return false;

        try {
            if (!executionConn.isValid(3)) {
                healthMessage = "DB Connection Invalid";
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            healthMessage = e.getMessage();
            return false;
        }

        healthMessage = "Healthy!";
        return true;
    }

    @Override
    protected boolean supportsMultipleTopics() {
        return true;
    }


    @Override
    public String getHealthMessage() {
        return healthMessage;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        PurchaseConsumerConfig config = new PurchaseConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            logException(e, "Error during init/update Airlock configuration.");
            stop();
        }
    }

    // Note that this will NOT update server URLs etc. If those change the consumer must be restarted.
    // Useful mostly for dynamic field configuration changes.
    protected synchronized void updateToNewConfigIfExists() {
        if (newConfig != null) {
            config = newConfig;
            newConfig = null;
        }
    }

    private Summary.Timer startDBLatencyTimer(String operation) {
        return dbOperationsLatencySummary.labels(operation,
                AirlockManager.getEnvVar(), AirlockManager.getProduct()).startTimer();
    }

    private void observeDBCommands(String operation, int amount) {
        dbCommandsSummary.labels(operation,
                AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(amount);
    }

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    public static class PurchaseMetaData {

        public enum Type {
            RENEWAL_CHECK,
            APP,
            NOTIFICATION
        }

        private Type type;
        private String purchaseToken;
        private String originalEventId;
        private String receipt;
        private String userId;
        private String productId;
        private String notificationType;
        private Long notificationTime;
        private String platform;
        private String transactionId;
        @Nullable
        private Long autoRenewStatusChangeDateMs;

        @Nullable
        private Long cancellationDateMs;

        public String getReceipt() {
            return receipt;
        }

        public String getTransactionId() {
            return transactionId;
        }

        public void setReceipt(String receipt) {
            this.receipt = receipt;
        }

        public String getOriginalEventId() {
            return originalEventId;
        }

        public void setOriginalEventId(String originalEventId) {
            this.originalEventId = originalEventId;
        }

        @Nullable
        public Long getCancellationDateMs() {
            return cancellationDateMs;
        }

        public void setCancellationDateMs(@Nullable Long cancellationDateMs) {
            this.cancellationDateMs = cancellationDateMs;
        }

        public String getPlatform() {
            return platform;
        }

        public void setPlatform(String platform) {
            this.platform = platform;
        }

        public void setNotificationTime(Long notificationTime) {
            this.notificationTime = notificationTime;
        }

        public String getPurchaseToken() {
            return purchaseToken;
        }

        public Long getNotificationTime() {
            return notificationTime;
        }

        public void setPurchaseToken(String purchaseToken) {
            this.purchaseToken = purchaseToken;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public void setNotificationType(String notificationType) {
            this.notificationType = notificationType;
        }

        public String getNotificationType() {
            return this.notificationType;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public void setTransactionId(String transactionId) {
            this.transactionId = transactionId;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        @CheckForNull
        public Long getAutoRenewStatusChangeDateMs() {
            return autoRenewStatusChangeDateMs;
        }

        public void setAutoRenewStatusChangeDateMs(Long autoRenewStatusChangeDateMs) {
            this.autoRenewStatusChangeDateMs = autoRenewStatusChangeDateMs;
        }
    }


    public static class Pair<T, U> {

        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }

    @SuppressWarnings("unused")
    private boolean sendEventsBatchToEventProxy(List<AirlyticsPurchase> purchases, EventApiClient eventApiClient, int attempt) {

        if (attempt == 3) {
            return false;
        }

        JSONObject batch = new JSONObject();
        final JSONArray batchArray = new JSONArray();
        batch.put("events", batchArray);
        purchases.forEach(batchArray::put);

        try {
            eventApiClient.post(batch.toString());
            // all succeeded
            return true;
        } catch (IOException e) {
            attempt++;
            try {
                Thread.sleep(attempt * 1000L);
            } catch (InterruptedException e1) {
                // it seems, the process is being stopped, so, stop retrying
            }
            return sendEventsBatchToEventProxy(purchases, eventApiClient, attempt);
        } catch (EventApiException e) {

            if (e.is202Accepted()) {
                return true;// do not report the error
            } else { // log error only for responses other than 202
                LOGGER.error("Error sending events:" + e.getMessage(), e);
            }
        }
        return false;
    }
}
