package com.ibm.airlytics.consumer.purchase;

import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;

import javax.annotation.CheckForNull;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;


public class PurchasesDAO {

    public static Calendar UTC = Calendar.getInstance();
    public static String ANDROID = "android";
    public static String IOS = "ios";

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    private static final AirlyticsLogger LOGGER = AirlyticsLogger.getLogger(PurchasesDAO.class.getName());

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getUpdatePurchaseEventPreparedStmt(Connection dbConnection, String tableName) throws SQLException {
        return dbConnection.prepareStatement(
                "update " + tableName + " set product = ?, revenue_usd_micros = ?," +
                        " platform = ?, expiration_date = ?, expiration_reason = ?" +
                        ",auto_renew_status = ?, payment_state = ?, upgraded_from = ?," +
                        " upgraded_to = ?, cancellation_reason = ?, survey_response = ?," +
                        " intro_pricing = ?, receipt = ? , name = ? " +
                        " where purchase_id = ? " +
                        " and event_time = ? AND" +
                        " (SELECT count(*) FROM " + tableName + " WHERE purchase_id = ? " +
                        " AND event_time = ?) = 1");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getInsertOnConflictDoNothingPurchaseEventPreparedStmt(Connection dbConnection, String tableName) throws SQLException {
        return dbConnection.prepareStatement(
                "INSERT INTO " + tableName + " (purchase_id, event_time, name, " +
                        " product, revenue_usd_micros, platform, expiration_date, expiration_reason, auto_renew_status," +
                        " payment_state, upgraded_from, upgraded_to," +
                        " cancellation_reason, survey_response, intro_pricing, receipt" +
                        " ) " +
                        " select ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ON CONFLICT DO NOTHING ");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getInsertPurchasePreparedStatement(Connection dbConnection, String tableName) throws SQLException {
        return dbConnection.prepareStatement(
                "INSERT INTO " + tableName + " (id, product, start_date, " +
                        " expiration_date, actual_end_date, auto_renew_status, auto_renew_status_change_date, period_start_date," +
                        " periods, duration, revenue_usd_micros, grace, active, trial, intro_pricing, is_upgraded, payment_state, expiration_reason," +
                        " cancellation_reason,  survey_response, cancellation_date, platform, linked_purchase_token, receipt, original_transaction_id, last_modified_date, modifier_source, last_payment_action_date, " +
                        " store_response, trial_start_date, trial_end_date, upgraded_from, subscription_status) " +
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE(?,'APP'),?,?,?,?,?,?) ON CONFLICT ON CONSTRAINT purchases_pkey DO UPDATE SET product=?, start_date=?," +
                        " expiration_date=?, actual_end_date=?, auto_renew_status=?, auto_renew_status_change_date=CASE WHEN ?::timestamp is not null then ? else " + tableName + ".auto_renew_status_change_date end, period_start_date=?," +
                        " periods=?, duration=?, revenue_usd_micros=?, grace=COALESCE(?, " + tableName + ".grace), active=?, trial=?, intro_pricing=?, is_upgraded=?," +
                        " payment_state=?, expiration_reason=?, cancellation_reason=?, survey_response=?, cancellation_date=?, platform=?, linked_purchase_token=?, receipt=?, original_transaction_id=?," +
                        " last_modified_date=?, modifier_source=CASE WHEN ?!='APP' and ? is not null then ? else " + tableName + ".modifier_source end, last_payment_action_date=?, store_response=?, trial_start_date=COALESCE(?, " + tableName + ".trial_start_date), " +
                        " trial_end_date=COALESCE(?, " + tableName + ".trial_end_date), upgraded_from=?, subscription_status=?");
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getInsertPurchaseUserIdPreparedStatement(Connection dbConnection, String tableName) throws SQLException {
        return dbConnection.prepareStatement(
                "INSERT INTO " + tableName + " (purchase_id, user_id, creation_date, shard) " +
                        " VALUES (?,?,?,?) ON CONFLICT ON CONSTRAINT purchase_user_id_pkey DO UPDATE SET purchase_id=?, user_id=?, shard=?");
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getUpdatePurchaseStoreResponsePreparedStatement(Connection dbConnection, String tableName, String platform) throws SQLException {
        if (platform != null && platform.equals(IOS)) {
            return dbConnection.prepareStatement(
                    "UPDATE  " + tableName + " SET active = false, grace = false, last_modified_date = ?, modifier_source = ?, store_response = ? WHERE " +
                            "  md5(receipt) = md5(?)");
        }
        if (platform != null && platform.equals(ANDROID)) {
            return dbConnection.prepareStatement(
                    "UPDATE  " + tableName + " SET active = false, grace = false, last_modified_date = ?, modifier_source = ?, store_response = ? WHERE  id = ?");
        }
        return null;
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getQueryStalePurchaseCandidatesPreparedStatement(Connection
                                                                                             dbConnection, String tableName,
                                                                                     String platform, List<String> notRenewableProduct) throws SQLException {
        String excludeNotRenewableProductSCondition = createExcludeNonRenewableCondition(notRenewableProduct);
        return dbConnection.prepareStatement(
                "SELECT  *  FROM " + tableName + " WHERE ((expiration_date BETWEEN clock_timestamp() - INTERVAL '1 HOUR' AND clock_timestamp() + INTERVAL '1 HOUR' AND (" + excludeNotRenewableProductSCondition + "))\n" +
                        "or (active = true and actual_end_date < clock_timestamp() - interval '1 day' and grace = false) or (active = false and actual_end_date > clock_timestamp() and (" + excludeNotRenewableProductSCondition + ")) " +
                        "or (is_upgraded = true and cancellation_date is null and (" + excludeNotRenewableProductSCondition + "))) and platform = '" + platform + "'");
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getPurchaseEventsByPurchaseIdPreparedStatement(Connection dbConnection, String tableName) throws
            SQLException {
        return dbConnection.prepareStatement(
                "SELECT *  FROM " + tableName + " WHERE purchase_id = ?");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getAllPurchases(Connection dbConnection, String tableName, String platform) throws
            SQLException {
        return dbConnection.prepareStatement(
                "SELECT id, product, receipt, platform  FROM " + tableName + " WHERE platform = '" + platform + "'");

    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getQueryRenewedPurchaseCandidatesPreparedStatement(Connection
                                                                                               dbConnection, String tableName,
                                                                                       String platform, List<String> notRenewableProduct) throws SQLException {
        String excludeNotRenewableProductSCondition = createExcludeNonRenewableCondition(notRenewableProduct);
        return dbConnection.prepareStatement(
                "SELECT  *  FROM " + tableName + " WHERE ((expiration_date BETWEEN clock_timestamp() - INTERVAL '3 DAY' AND clock_timestamp() + INTERVAL '3 DAY' )\n" +
                        "or (expiration_date < clock_timestamp() and (expiration_date::DATE - last_modified_date::DATE) >= 3)\n" +
                        "or store_response = 'NETWORK_ERROR') and platform = '" + platform + "' and  (" + excludeNotRenewableProductSCondition + ")");
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getTransactionByIdPreparedStatement(Connection dbConnection, String
            purchasesTable) throws SQLException {
        return dbConnection.prepareStatement(
                "SELECT  *  FROM " + purchasesTable +
                        " WHERE  id=? ");
    }

    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getTransactionByIdWithUserIdPreparedStatement(Connection dbConnection, String
            purchasesTable, String userIdToPurchasesTable) throws SQLException {
        return dbConnection.prepareStatement(
                "SELECT  *  FROM " + purchasesTable + " purchases_t JOIN  " + userIdToPurchasesTable + " users_to_purchases_t on users_to_purchases_t.purchase_id = purchases_t.id" +
                        " WHERE  purchases_t.id = ? ORDER BY users_to_purchases_t.creation_date DESC");
    }


    @SuppressWarnings("SqlResolve")
    public static PreparedStatement getProductIdByUserId(Connection dbConnection) throws SQLException {
        return dbConnection.prepareStatement(
                "SELECT  premium_product  FROM  users.users where id = ?");
    }

    private static void setNullableStringField(int index, String value, PreparedStatement insertStatement) throws
            SQLException {
        if (value == null) {
            insertStatement.setNull(index, Types.VARCHAR);
        } else {
            insertStatement.setString(index, value);
        }
    }

    private static void setNullableBoolField(int index, Boolean value, PreparedStatement insertStatement) throws
            SQLException {
        if (value == null) {
            insertStatement.setNull(index, Types.BOOLEAN);
        } else {
            insertStatement.setBoolean(index, value);
        }
    }

    private static void setNullableTimeStampField(int index, Long value, PreparedStatement insertStatement) throws
            SQLException {
        if (value == null) {
            insertStatement.setNull(index, Types.TIMESTAMP);
        } else {
            insertStatement.setTimestamp(index, new Timestamp(value), UTC);
        }
    }

    private static void insertPurchaseEventValues(PreparedStatement insertStatement, AirlyticsPurchaseEvent event,
                                                  int step) throws SQLException {
        insertStatement.setString(1 + step, event.getProduct());
        insertStatement.setLong(2 + step, event.getRevenueUsd());
        insertStatement.setString(3 + step, event.getPlatform());
        insertStatement.setTimestamp(4 + step, new Timestamp(event.getExpirationDate()));
        insertStatement.setString(5 + step, event.getExpirationReason());
        insertStatement.setBoolean(6 + step, event.getAutoRenewStatus());
        insertStatement.setString(7 + step, event.getPaymentState());
        insertStatement.setString(8 + step, event.getUpgradedFrom());
        insertStatement.setString(9 + step, event.getUpgradedTo());
        insertStatement.setString(10 + step, event.getCancellationReason());
        insertStatement.setString(11 + step, event.getSurveyResponse());
        insertStatement.setBoolean(12 + step, event.getIntroPricing());
        insertStatement.setString(13 + step, event.getReceipt());
    }

    public static void insertPurchaseEvent(PreparedStatement insertStatement, AirlyticsPurchaseEvent event) throws
            SQLException {
        insertStatement.setString(1, event.getPurchaseId());
        insertStatement.setTimestamp(2, new Timestamp(event.getEventTime()));
        insertStatement.setString(3, event.getName());
        insertPurchaseEventValues(insertStatement, event, 3);
        insertStatement.addBatch();
    }

    public static void updatePurchaseEvent(PreparedStatement insertStatement, AirlyticsPurchaseEvent event) throws
            SQLException {
        insertPurchaseEventValues(insertStatement, event, 0);
        insertStatement.setString(14, event.getName());
        insertStatement.setString(15, event.getPurchaseId());
        insertStatement.setTimestamp(16, new Timestamp(event.getEventTime()));
        insertStatement.setString(17, event.getPurchaseId());
        insertStatement.setTimestamp(18, new Timestamp(event.getEventTime()));
        insertStatement.addBatch();
    }

    private static void insertPurchaseUserIsValues(PreparedStatement insertStatement, String purchaseId, String
            userId, long creationTime, int shard) throws SQLException {
        insertStatement.setString(1, purchaseId);
        insertStatement.setString(2, userId);
        insertStatement.setTimestamp(3, new Timestamp(creationTime));
        insertStatement.setInt(4, shard);
        insertStatement.setString(5, purchaseId);
        insertStatement.setString(6, userId);
        insertStatement.setInt(7, shard);
    }

    private static void insertValues(PreparedStatement insertStatement, AirlyticsPurchase purchase, int step,
                                     boolean isUpdate) throws SQLException {
        insertStatement.setString(2 + step, purchase.getProduct());
        setNullableTimeStampField(3 + step, purchase.getStartDate(), insertStatement);
        setNullableTimeStampField(4 + step, purchase.getExpirationDate(), insertStatement);
        setNullableTimeStampField(5 + step, purchase.getActualEndDate(), insertStatement);
        setNullableBoolField(6 + step, purchase.getAutoRenewStatus(), insertStatement);
        setNullableTimeStampField(7 + step, purchase.getAutoRenewStatusChangeDate(), insertStatement);
        if (isUpdate) {
            step++;
            setNullableTimeStampField(7 + step, purchase.getAutoRenewStatusChangeDate(), insertStatement);
        }
        setNullableTimeStampField(8 + step, purchase.getPeriodStartDate(), insertStatement);
        insertStatement.setDouble(9 + step, purchase.getPeriods());
        insertStatement.setString(10 + step, purchase.getDuration());
        insertStatement.setLong(11 + step, purchase.getRevenueUsd());
        setNullableBoolField(12 + step, purchase.getGrace(), insertStatement);
        setNullableBoolField(13 + step, purchase.getActive(), insertStatement);
        setNullableBoolField(14 + step, purchase.getTrial(), insertStatement);
        setNullableBoolField(15 + step, purchase.getIntroPricing(), insertStatement);
        setNullableBoolField(16 + step, purchase.getUpgraded(), insertStatement);
        setNullableStringField(17 + step, purchase.getPaymentState(), insertStatement);
        setNullableStringField(18 + step, purchase.getExpirationReason(), insertStatement);
        setNullableStringField(19 + step, purchase.getCancellationReason(), insertStatement);
        setNullableStringField(20 + step, purchase.getSurveyResponse(), insertStatement);
        setNullableTimeStampField(21 + step, purchase.getCancellationDate(), insertStatement);
        setNullableStringField(22 + step, purchase.getPlatform(), insertStatement);
        setNullableStringField(23 + step, purchase.getLinkedPurchaseToken(), insertStatement);
        setNullableStringField(24 + step, purchase.getReceipt(), insertStatement);
        setNullableStringField(25 + step, purchase.getOriginalTransactionId(), insertStatement);
        setNullableTimeStampField(26 + step, purchase.getLastModifiedDate(), insertStatement);
        setNullableStringField(27 + step, purchase.getModifierSource(), insertStatement);
        if (isUpdate) {
            setNullableStringField(27 + ++step, purchase.getModifierSource(), insertStatement);
            setNullableStringField(27 + ++step, purchase.getModifierSource(), insertStatement);
        }
        setNullableTimeStampField(28 + step, purchase.getLastPaymentActionDate(), insertStatement);
        setNullableStringField(29 + step, purchase.getStoreResponse(), insertStatement);
        setNullableTimeStampField(30 + step, purchase.getTrialStartDate(), insertStatement);
        setNullableTimeStampField(31 + step, purchase.getTrialEndDate(), insertStatement);
        setNullableStringField(32 + step, purchase.getPurchaseIdUpgradedFrom(), insertStatement);
        setNullableStringField(33 + step, purchase.getSubscriptionStatus(), insertStatement);
    }

    public static void insertUpdatePurchaseUser(PreparedStatement insertStatement, String purchaseId, String userId,
                                                int shard) throws SQLException {
        insertPurchaseUserIsValues(insertStatement, purchaseId, userId, System.currentTimeMillis(), shard);
        insertStatement.addBatch();
    }

    public static synchronized void setPurchaseInActiveAndUpdateStoreResponse(PreparedStatement
                                                                                      updatePurchaseStatement,
                                                                              PurchaseConsumer.PurchaseMetaData purchaseMetaData,
                                                                              PurchaseConsumer.StoreResponse storeResponse) throws SQLException {
        updatePurchaseStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        updatePurchaseStatement.setString(2, purchaseMetaData.getType().name());
        updatePurchaseStatement.setString(3, storeResponse.name());
        updatePurchaseStatement.setString(4, purchaseMetaData.getPurchaseToken());
        updatePurchaseStatement.addBatch();
    }

    public static synchronized void insertUpdatePurchase(PreparedStatement insertStatement, AirlyticsPurchase
            purchase) throws SQLException {
        insertStatement.setString(1, purchase.getId());
        insertValues(insertStatement, purchase, 0, false);
        insertValues(insertStatement, purchase, 32, true);
        insertStatement.addBatch();
    }

    private static AirlyticsPurchase partialReadRow(ResultSet rs) throws SQLException {
        return partialReadRow(rs, rs.getString(AirlyticsPurchase.Field.ID.name()));
    }

    private static AirlyticsPurchase partialReadRow(ResultSet rs, String id) throws SQLException {
        AirlyticsPurchase purchase = new AirlyticsPurchase();
        purchase.setId(id);
        purchase.setProduct(rs.getString(AirlyticsPurchase.Field.PRODUCT.name()));
        purchase.setPlatform(rs.getString(AirlyticsPurchase.Field.PLATFORM.name()));
        purchase.setReceipt(rs.getString(AirlyticsPurchase.Field.RECEIPT.name()));
        return purchase;
    }

    private static AirlyticsPurchase readRowWithUserId(ResultSet rs, String id) throws SQLException {
        if (rs != null) {
            AirlyticsPurchase airlyticsPurchase = readRowById(rs, id);
            airlyticsPurchase.setUserId(rs.getString(AirlyticsPurchase.Field.USER_ID.name()));
            return airlyticsPurchase;
        }
        return null;
    }

    private static AirlyticsPurchaseEvent readAirlyticsPurchaseEvents(ResultSet rs, String id) throws SQLException {
        AirlyticsPurchaseEvent airlyticsPurchaseEvent = new AirlyticsPurchaseEvent();
        airlyticsPurchaseEvent.setPurchaseId(id);
        airlyticsPurchaseEvent.setEventTime(getTimeValueOfNullableColumn(rs, AirlyticsPurchaseEvent.Field.EVENT_TIME.name()));
        airlyticsPurchaseEvent.setName(rs.getString(AirlyticsPurchaseEvent.Field.NAME.name()));
        airlyticsPurchaseEvent.setProduct(rs.getString(AirlyticsPurchaseEvent.Field.PRODUCT.name()));
        airlyticsPurchaseEvent.setPlatform(rs.getString(AirlyticsPurchaseEvent.Field.PLATFORM.name()));
        airlyticsPurchaseEvent.setExpirationDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchaseEvent.Field.EXPIRATION_DATE.name()));
        airlyticsPurchaseEvent.setExpirationReason(rs.getString(AirlyticsPurchaseEvent.Field.EXPIRATION_REASON.name()));
        airlyticsPurchaseEvent.setAutoRenewStatus(rs.getBoolean(AirlyticsPurchaseEvent.Field.AUTO_RENEW_STATUS.name()));
        airlyticsPurchaseEvent.setPaymentState(rs.getString(AirlyticsPurchaseEvent.Field.PAYMENT_STATE.name()));
        airlyticsPurchaseEvent.setUpgradedFrom(rs.getString(AirlyticsPurchaseEvent.Field.UPGRADED_FROM.name()));
        airlyticsPurchaseEvent.setUpgradedTo(rs.getString(AirlyticsPurchaseEvent.Field.UPGRADED_TO.name()));
        airlyticsPurchaseEvent.setCancellationReason(rs.getString(AirlyticsPurchaseEvent.Field.CANCELLATION_REASON.name()));
        airlyticsPurchaseEvent.setSurveyResponse(rs.getString(AirlyticsPurchaseEvent.Field.SURVEY_RESPONSE.name()));
        airlyticsPurchaseEvent.setIntroPricing(rs.getBoolean(AirlyticsPurchaseEvent.Field.INTRO_PRICING.name()));
        airlyticsPurchaseEvent.setRevenueUsd(rs.getLong(AirlyticsPurchaseEvent.Field.REVENUE_USD_MICROS.name()));
        airlyticsPurchaseEvent.setReceipt(rs.getString(AirlyticsPurchaseEvent.Field.RECEIPT.name()));
        return airlyticsPurchaseEvent;
    }

    private static AirlyticsPurchase readRowById(ResultSet rs, String id) throws SQLException {
        AirlyticsPurchase purchase = new AirlyticsPurchase();
        purchase.setId(id);
        purchase.setProduct(rs.getString(AirlyticsPurchase.Field.PRODUCT.name()));
        purchase.setStartDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.START_DATE.name()));
        purchase.setExpirationDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.EXPIRATION_DATE.name()));
        purchase.setActualEndDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.ACTUAL_END_DATE.name()));
        purchase.setAutoRenewStatus(rs.getBoolean(AirlyticsPurchase.Field.AUTO_RENEW_STATUS.name()));
        purchase.setAutoRenewStatusChangeDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.AUTO_RENEW_STATUS_CHANGE_DATE.name()));
        purchase.setPeriodStartDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.PERIOD_START_DATE.name()));
        purchase.setPeriods(rs.getDouble(AirlyticsPurchase.Field.PERIODS.name()));
        purchase.setDuration(rs.getString(AirlyticsPurchase.Field.DURATION.name()));
        purchase.setRevenueUsd(rs.getLong(AirlyticsPurchase.Field.REVENUE_USD_MICROS.name()));
        purchase.setGrace(rs.getBoolean(AirlyticsPurchase.Field.GRACE.name()));
        purchase.setActive(rs.getBoolean(AirlyticsPurchase.Field.ACTIVE.name()));
        purchase.setTrial(rs.getBoolean(AirlyticsPurchase.Field.TRIAL.name()));
        purchase.setIntroPricing(rs.getBoolean(AirlyticsPurchase.Field.INTRO_PRICING.name()));
        purchase.setPaymentState(rs.getString(AirlyticsPurchase.Field.PAYMENT_STATE.name()));
        purchase.setExpirationReason(rs.getString(AirlyticsPurchase.Field.EXPIRATION_REASON.name()));
        purchase.setUpgraded(rs.getBoolean(AirlyticsPurchase.Field.IS_UPGRADED.name()));
        purchase.setCancellationReason(rs.getString(AirlyticsPurchase.Field.CANCELLATION_REASON.name()));
        purchase.setSurveyResponse(rs.getString(AirlyticsPurchase.Field.SURVEY_RESPONSE.name()));
        purchase.setCancellationDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.CANCELLATION_DATE.name()));
        purchase.setPlatform(rs.getString(AirlyticsPurchase.Field.PLATFORM.name()));
        purchase.setLinkedPurchaseToken(rs.getString(AirlyticsPurchase.Field.LINKED_PURCHASE_TOKEN.name()));
        purchase.setReceipt(rs.getString(AirlyticsPurchase.Field.RECEIPT.name()));
        purchase.setOriginalTransactionId(rs.getString(AirlyticsPurchase.Field.ORIGINAL_TRANSACTION_ID.name()));
        purchase.setModifierSource(rs.getString(AirlyticsPurchase.Field.MODIFIER_SOURCE.name()));
        purchase.setLastModifiedDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.LAST_MODIFIED_DATE.name()));
        purchase.setStoreResponse(rs.getString(AirlyticsPurchase.Field.STORE_RESPONSE.name()));
        purchase.setTrialStartDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.TRIAL_START_DATE.name()));
        purchase.setTrialEndDate(getTimeValueOfNullableColumn(rs, AirlyticsPurchase.Field.TRIAL_END_DATE.name()));
        purchase.setPurchaseIdUpgradedFrom(rs.getString(AirlyticsPurchase.Field.UPGRADED_FROM.name()));
        purchase.setSubscriptionStatus(rs.getString(AirlyticsPurchase.Field.SUBSCRIPTION_STATUS.name()));

        return purchase;
    }

    public interface CallBack {
        void onComplete(AirlyticsPurchase airlyticsPurchase);
    }

    public interface BatchCallBack {
        void onComplete(Collection<AirlyticsPurchase> batch);
    }

    @CheckForNull
    public static AirlyticsPurchase getTransactionById(PreparedStatement queryStatement, Summary
            dbOperationsLatencySummary, String transactionId) throws SQLException {

        ResultSet rs = readPurchaseTransaction(queryStatement,
                dbOperationsLatencySummary,
                transactionId);
        if (rs != null) {
            return readRowById(rs, transactionId);
        } else {
            return null;
        }
    }

    private static ResultSet readPurchaseTransaction(PreparedStatement queryStatement, Summary
            dbOperationsLatencySummary, String transactionId) throws SQLException {
        ResultSet rs;
        queryStatement.setString(1, transactionId);
        try {
            rs = dbOperationsLatencySummary.labels("queryToFetchByIdWithUserId",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).time(
                    (Callable<ResultSet>) queryStatement::executeQuery);
        } catch (RuntimeException e) {
            // We need this mess because of Prometheus wrapping the internal exception when using time()
            if (e.getCause() != null && e.getCause() instanceof SQLException)
                throw (SQLException) e.getCause();
            else
                throw e;
        }

        if (rs.next()) {
            return rs;
        } else {
            return null;
        }
    }


    @CheckForNull
    public static AirlyticsPurchase getTransactionByIdWithUserId(PreparedStatement queryStatement, Summary
            dbOperationsLatencySummary, String transactionId) throws SQLException {

        return readRowWithUserId(readPurchaseTransaction(queryStatement, dbOperationsLatencySummary,
                transactionId), transactionId);

    }

    @CheckForNull
    public static List<AirlyticsPurchaseEvent> getPurchaseEventsByPurchaseId(PreparedStatement queryStatement,
                                                                             String purchaseId) throws SQLException {

        ResultSet rs;
        try {
            queryStatement.setString(1, purchaseId);
            rs = queryStatement.executeQuery();
        } catch (RuntimeException e) {
            // We need this mess because of Prometheus wrapping the internal exception when using time()
            if (e.getCause() != null && e.getCause() instanceof SQLException)
                throw (SQLException) e.getCause();
            else
                throw e;
        }

        ArrayList<AirlyticsPurchaseEvent> purchaseEvents = new ArrayList<>();
        if (rs.next()) {
            do {
                purchaseEvents.add(readAirlyticsPurchaseEvents(rs, purchaseId));
            } while (rs.next());
        }
        return purchaseEvents;
    }

    public static void readBatchPurchaseCandidatesForUpdate(PreparedStatement queryStatement, BatchCallBack
            callBack, int batchSize) throws SQLException {
        ResultSet rs;
        try {
            queryStatement.setFetchSize(batchSize);
            rs = queryStatement.executeQuery();
        } catch (RuntimeException e) {
            // We need this mess because of Prometheus wrapping the internal exception when using time()
            if (e.getCause() != null && e.getCause() instanceof SQLException)
                throw (SQLException) e.getCause();
            else
                throw e;
        }

        ArrayList<AirlyticsPurchase> batch = new ArrayList<>();
        if (rs.next()) {
            do {
                batch.add(partialReadRow(rs));
                if (batch.size() == batchSize) {
                    callBack.onComplete(new ArrayList<>(batch));
                    batch.clear();
                }
            } while (rs.next());
        }
    }

    public static void notifyPurchaseCandidatesForUpdate(PreparedStatement queryStatement, Summary
            dbOperationsLatencySummary,
                                                         Counter metrics, String type, CallBack callBack) throws SQLException {
        ResultSet rs;
        try {
            queryStatement.setFetchSize(1000);
            rs = dbOperationsLatencySummary.labels("queryForUpdate",
                    AirlockManager.getEnvVar(), AirlockManager.getProduct()).time(
                    (Callable<ResultSet>) queryStatement::executeQuery);
        } catch (RuntimeException e) {
            // We need this mess because of Prometheus wrapping the internal exception when using time()
            if (e.getCause() != null && e.getCause() instanceof SQLException)
                throw (SQLException) e.getCause();
            else
                throw e;
        }

        int rowsIndex = 0;
        if (rs.next()) {
            do {
                callBack.onComplete(partialReadRow(rs));
                metrics.labels(type, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                rowsIndex++;
            } while (rs.next());
        }
        LOGGER.info("Number of [" + type + "] updated rows [" + rowsIndex + "] by polling");
    }

    @CheckForNull
    private static Long getTimeValueOfNullableColumn(ResultSet rs, String columnName) throws SQLException {
        return rs.getTimestamp(columnName) == null ? null : rs.getTimestamp(columnName).getTime();
    }

    private static String createExcludeNonRenewableCondition(List<String> notRenewableProduct) {
        String excludeNotRenewableProductCondition = "";
        for (String productName : notRenewableProduct) {
            excludeNotRenewableProductCondition = excludeNotRenewableProductCondition + " product!='" + productName + "' OR";
        }
        return excludeNotRenewableProductCondition.substring(0, excludeNotRenewableProductCondition.length() - 3);
    }
}
