package com.ibm.airlytics.consumer.userdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumer;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.utilities.Hashing;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.apache.kafka.clients.consumer.*;
import org.apache.log4j.Logger;
import org.postgresql.core.TransactionState;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.List;

public class UserDBConsumer extends AirlyticsConsumer {
    private static final Logger LOGGER = Logger.getLogger(UserDBConsumer.class.getName());
    private final Hashing hashing;
    private final static String JDBC_DRIVER = "org.postgresql.Driver";

    static final Counter recordsProcessedCounter = Counter.build()
            .name("airlytics_userdb_records_processed_total")
            .help("Total records processed by the userdb consumer.")
            .labelNames("event_name", "result", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Counter commitsCounter = Counter.build()
            .name("airlytics_userdb_commits_total")
            .help("Total commits to the db by the userdb consumer.")
            .labelNames(AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Summary dbOperationsLatencySummary = Summary.build()
            .name("airlytics_userdb_db_latency_seconds")
            .help("DB operation latency seconds.")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Summary dbCommandsSummary = Summary.build()
            .name("airlytics_userdb_db_commands")
            .help("Number of db commands executed")
            .labelNames("operation", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    static final Counter userCacheLookupsCounter = Counter.build()
            .name("airlytics_userdb_cache_lookups_total")
            .help("userdb user cache lookups")
            .labelNames("hit", AirlyticsConsumerConstants.ENV, AirlyticsConsumerConstants.PRODUCT).register();

    private Connection conn;
    private UserDBConsumerConfig config;
    private UserDBConsumerConfig newConfig = null;
    private PreparedStatement insertStatement, updateSessionTimesStatement, queryStatement, updateImpressionLtvStatement, insertPollAnswer, insertPollAnswerPI;
    private List<PreparedStatement> additionalInsertStatements = new ArrayList<>();
    private UserCache userCache;
    private List<StatementWithLineage> dynamicUpdateStatements = new LinkedList<>();
    private DynamicUpdatesAggregator dynamicUpdatesAggregator = new DynamicUpdatesAggregator();
    private int insertsInBatches = 0;
    private int updateSessionTimesInBatch = 0;
    private int updateLtvInBatch = 0;

    private long totalProgress = 0;
    private long lastProgress = 0;
    private Instant lastRecordProcessed = Instant.now();

    private static Connection initConnection(UserDBConsumerConfig config) throws ClassNotFoundException, SQLException {
        // Setup connection pool
        Class.forName(JDBC_DRIVER);

        Properties props = new Properties();
        props.setProperty("user", config.getDbUsername());
        props.setProperty("password", config.getDbPassword());
        props.setProperty("reWriteBatchedInserts", "true");
        if (config.isUseSSL()) {
            props.setProperty("ssl", "true");
            props.setProperty("sslmode", "verify-full");
        }

        Connection conn;

        try {
            conn = DriverManager.getConnection(config.getDbUrl(), props);
        } catch (SQLException e) {
            LOGGER.error("Cannot connect to DB at URL: " + config.getDbUrl());
            throw e;
        }
        
        conn.setAutoCommit(false);
        return conn;
    }

    public UserDBConsumer(UserDBConsumerConfig config) {
        super(config);
        this.config = config;
        hashing = new Hashing(config.getNumberOfShards());
        userCache = new UserCache(config.getUserCacheSizeInRecords());

        try {
            conn = initConnection(config);
            prepareStatements();

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            //System.out.println(e.getMessage());
            stop();
        }
    }

    private void prepareStatements() throws SQLException {
        insertStatement = conn.prepareStatement(
                "INSERT INTO " + config.getUserSchema() + "." + config.getUserTable() + " (id, status, first_session, last_session, " +
                        "platform, application, shard, app_version) " +
                        "VALUES (?,?,?,?,?,?,?,?)");

        for (String additionalTable : config.getAdditionalUserTables()) {
            additionalInsertStatements.add(
                    conn.prepareStatement("INSERT INTO " + config.getUserSchema() + "." + additionalTable + " (id, shard) VALUES (?,?)"));
        }

        updateSessionTimesStatement = conn.prepareStatement(
                "UPDATE " + config.getUserSchema() + "." + config.getUserTable() +
                        " SET first_session = ?, last_session = ?, app_version = ?, status = 'ACTIVE', uninstall_date = null WHERE id = ? ");

        queryStatement = conn.prepareStatement(
                "SELECT first_session, last_session, app_version FROM " + config.getUserSchema() + "." + config.getUserTable() + " WHERE id = ?");

        updateImpressionLtvStatement = conn.prepareStatement(
                "UPDATE " + config.getUserSchema() + "." + config.getUserTable() +
                        " SET impression_usd_micros = impression_usd_micros + ? WHERE id = ?");

        insertPollAnswer = createPollAnswerInsertStatement(config.getPollAnswersTable());
        insertPollAnswerPI = createPollAnswerInsertStatement(config.getPollAnswersPITable());
    }

    private PreparedStatement createPollAnswerInsertStatement(String tableName) throws SQLException {
        return conn.prepareStatement("INSERT INTO " + config.getUserSchema() + "." + tableName +
                "(event_id, shard, user_id, poll_id, question_id, question_title, answer_ids, answer_titles, open_answer, event_time)" +
                "VALUES  (?,?,?,?,?,?,?,?,?,?)" +
                " ON CONFLICT DO NOTHING");
    }

    private static boolean filterEvent(UserEvent event) {
        return (
                event.getEventName().equals("session-start") ||
                event.getEventName().equals("user-attributes") ||
                event.getEventName().equals("user-attribute-detected") ||
                event.getEventName().equals("stream-results") ||
                event.getEventName().equals("poll-question-answered") ||
                event.getEventName().equals("page-viewed")
                /*|| event.getEventName().equals("ad-impression")*/
        );
    }

    private void insertNewPollAnswer(UserEvent event) throws SQLException, UserDBConsumerException {

        JsonNode attributes = event.getAttributes();
        if (attributes == null) {
            throw new UserDBConsumerException("Mandatory field 'attributes' is missing in event " + event.getEventName());
        }

        int shard = hashing.hashMurmur2(event.getUserId());

        JsonNode answerIdsArrayNode = attributes.get("answerIds");
        Array answerIdsArray = createSQLArray(answerIdsArrayNode);

        JsonNode answerTitlesArrayNode = attributes.get("answerTitles");
        Array answerTitlesArray = createSQLArray(answerTitlesArrayNode);

        String openAnswerStr = null;
        JsonNode openAnswerNode = attributes.get("openAnswer");
        if (openAnswerNode != null && !openAnswerNode.isNull()) {
            openAnswerStr = openAnswerNode.asText();
        }

        JsonNode piNode = attributes.get("pi");
        if (piNode == null || !piNode.isBoolean()) {
            throw new UserDBConsumerException("Mandatory field 'pi' is missing in event " + event.getEventName());
        }
        PreparedStatement pollAnswer = piNode.asBoolean(false) ? insertPollAnswerPI : insertPollAnswer;

        pollAnswer.setString(1, event.getEventId());
        pollAnswer.setInt(2, shard);
        pollAnswer.setString(3, event.getUserId());
        pollAnswer.setString(4, attributes.get("pollId").asText());
        pollAnswer.setString(5, attributes.get("questionId").asText());
        pollAnswer.setString(6, attributes.get("questionTitle").asText());

        if (answerIdsArray == null) {
            pollAnswer.setNull(7, Types.ARRAY);
        } else {
            pollAnswer.setArray(7, answerIdsArray);
        }

        if (answerTitlesArray == null) {
            pollAnswer.setNull(8, Types.ARRAY);
        } else {
            pollAnswer.setArray(8, answerTitlesArray);
        }

        if (openAnswerStr == null) {
            pollAnswer.setNull(9, Types.VARCHAR);
        } else {
            pollAnswer.setString(9, openAnswerStr);
        }

        pollAnswer.setTimestamp(10, event.getEventTime());

        pollAnswer.addBatch();
    }

    private Array createSQLArray(JsonNode arrayNode) {
        if(arrayNode == null || arrayNode.isNull() || !arrayNode.isArray()) {
            return null;
        }

        ArrayList<String> l = new ObjectMapper().convertValue(arrayNode, ArrayList.class);
        try {
            return conn.createArrayOf("VARCHAR", l.toArray());
        } catch (SQLException e) {
            return null;
        }
    }

    private UserBasicRow insertNewUser(UserEvent event) throws SQLException {
        int shard = hashing.hashMurmur2(event.getUserId());

        insertStatement.setString(1, event.getUserId());
        insertStatement.setString(2, "ACTIVE");
        insertStatement.setTimestamp(3, event.getEventTime()); // first_session
        insertStatement.setTimestamp(4, event.getEventTime()); // last_session
        insertStatement.setString(5, event.getPlatform());
        insertStatement.setString(6, event.getProduct());
        insertStatement.setInt(7, shard);
        insertStatement.setString(8, event.getAppVersion());

        insertStatement.addBatch();
        ++insertsInBatches;

        for (PreparedStatement additionalStatement : additionalInsertStatements) {
            additionalStatement.setString(1, event.getUserId());
            additionalStatement.setInt(2, shard);
            additionalStatement.addBatch();
            ++insertsInBatches;
        }

        UserBasicRow result = new UserBasicRow(event.getEventTime(), event.getEventTime(), event.getAppVersion());
        userCache.put(event.getUserId(), result);

        return result;
    }

    private void updateUserSessionTimes(UserEvent event, UserBasicRow userInDB) throws SQLException {
        Timestamp firstSession = userInDB.getFirstSession();
        Timestamp lastSession = userInDB.getLastSession();

        boolean needsUpdate = false;
        String latestAppVersion = userInDB.getAppVersion();

        if (firstSession.after(event.getEventTime())) {
            firstSession = event.getEventTime();
            needsUpdate = true;
        } else if (lastSession.before(event.getEventTime())) {
            lastSession = event.getEventTime();
            latestAppVersion = event.getAppVersion(); // We need to update the app version since the event is later
            needsUpdate = true;
        }

        // Note: the update will invalidate the uninstallation time, and revert the user back to status 'ACTIVE'
        // This is expected even if the event is an old one. The uninstallation process will run again to verify.
        if (needsUpdate) {
            updateSessionTimesStatement.setTimestamp(1, firstSession);
            updateSessionTimesStatement.setTimestamp(2, lastSession);
            updateSessionTimesStatement.setString(3, latestAppVersion);
            updateSessionTimesStatement.setString(4, event.getUserId());

            updateSessionTimesStatement.addBatch();
            ++updateSessionTimesInBatch;

            // this will update the object in the cache
            userInDB.setFirstSession(firstSession);
            userInDB.setLastSession(lastSession);
        }
    }

    private void updateUserImpressionLtv(UserEvent event) throws SQLException {
        if (event.getRevenue() != null && event.getRevenue().longValue() != 0) {
            updateImpressionLtvStatement.setLong(1, event.getRevenue());
            updateImpressionLtvStatement.setString(2, event.getUserId());
            updateImpressionLtvStatement.addBatch();

            ++updateLtvInBatch;
        }
    }

    // This utility class simply stores the dynamic updates of a single user.
    // Once we're done with a single user, it creates a single SQL statement for that user.
    // After the last user in the batch, flush is also called directly.
    private class DynamicUpdatesAggregator {
        private String lastUserId = null;
        EventLineage userEventsAndRecords = new EventLineage();

        private void addDynamicUpdate(String userId, UserEvent event, ConsumerRecord<String, JsonNode> record) {

            if (!userId.equals(lastUserId) && lastUserId != null) {
                flushDynamicUpdates();
            }

            lastUserId = userId;
            userEventsAndRecords.put(event, record);
        }

        private void flushDynamicUpdates() {
            if (lastUserId != null && !userEventsAndRecords.isEmpty()) {
                UserDBConsumer.this.updateDynamicAttributes(lastUserId, userEventsAndRecords);
                userEventsAndRecords.clear();
            }
        }
    }

    // Can be called with multiple events of the same user, and will generate a single SQL update statement
    private void updateDynamicAttributes(String userId, EventLineage eventLineage) {
        // One StatementBuilder per table
        Map<String, UpdateStatementBuilder> statementBuilderPerTable = new HashMap<>();
        Map<String, EventLineage> eventLineagePerTable = new HashMap<>();
        boolean createStatement = false;

        for (Map.Entry<UserEvent, ConsumerRecord<String, JsonNode>> eventAndRecord : eventLineage.entrySet()) {
            UserEvent event = eventAndRecord.getKey();
            ConsumerRecord<String, JsonNode> record = eventAndRecord.getValue();

            Map<String, DynamicAttributeConfig> eventConfig = config.getDynamicEventConfigMap().get(event.getEventName());
            // If event name has attributes to be sent to DB columns
            if (eventConfig != null) {
                // Go over all attributes in the event
                Iterator<Map.Entry<String, JsonNode>> attributeIterator = event.getAttributes().fields();

                while (attributeIterator.hasNext()) {
                    Map.Entry<String, JsonNode> attribute = attributeIterator.next();

                    // Check if this attribute name is associated with a DB column
                    DynamicAttributeConfig attributeConfig = eventConfig.get(attribute.getKey());
                    if (attributeConfig != null) {
                        createStatement = true;
                        // retrieve the correct statement builder based on the table, creating a new one when necessary
                        String dbTable = config.getUserSchema() + "." + attributeConfig.getDbTable();
                        UpdateStatementBuilder statementBuilder = statementBuilderPerTable.computeIfAbsent(dbTable, k -> new UpdateStatementBuilder());
                        EventLineage eventLineageForTable = eventLineagePerTable.computeIfAbsent(dbTable, k -> new EventLineage());
                        // Add the field and value to this update statement, and record the event lineage
                        statementBuilder.addField(attributeConfig.getDbColumn(), attributeConfig.getType(), attribute.getValue());
                        eventLineageForTable.put(event, record);
                    }
                }
            }
        }

        if (createStatement) {
            // Go over all statementBuilders, and generate a statement per table
            statementBuilderPerTable.forEach((dbTable, statementBuilder) -> {
                String result = "success";
                EventLineage eventLineageForTable = eventLineagePerTable.get(dbTable);
                int partition = eventLineageForTable.getUserPartition();

                try {
                    PreparedStatement updateStatement = statementBuilder.prepareStatement(dbTable, userId, conn);
                    if (updateStatement != null) {
                        dynamicUpdateStatements.add(new StatementWithLineage(updateStatement, userId, eventLineageForTable));
                    }
                } catch (SQLException | UpdateStatementBuilderException e) {
                    result = "error";
                    String eventIds = eventLineageForTable.getEventIds();
                    logException(e, "Error in updateDynamicAttributes, partition=" + partition + ",eventIds: " + eventIds);
                    for (ConsumerRecord<String, JsonNode> record : eventLineageForTable.getRecords()) {
                        errorsProducer.sendRecord(record.key(), record.value(), record.timestamp());
                    }
                } finally {
                    for (ConsumerRecord<String, JsonNode> record : eventLineageForTable.getRecords()) {
                        String eventName = (record.value().get("name") == null ? "unknown" : record.value().get("name").asText());
                        recordsProcessedCounter.labels(eventName, result, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                    }
                }
            });
        }
    }

    private UserBasicRow queryUser(String userId) throws SQLException {
            String cacheLookupResult;

            UserBasicRow result = userCache.get(userId);
            if (result == null) {
                queryStatement.setString(1, userId);
                ResultSet rs;

                try {
                    rs = dbOperationsLatencySummary.labels("query",
                            AirlockManager.getEnvVar(), AirlockManager.getProduct()).time(
                            () -> queryStatement.executeQuery());
                } catch (RuntimeException e) {
                    // We need this mess because of Prometheus wrapping the internal exception when using time()
                    if (e.getCause() != null && e.getCause() instanceof SQLException)
                        throw (SQLException) e.getCause();
                    else
                        throw e;
                }

                dbCommandsSummary.labels("query",
                        AirlockManager.getEnvVar(), AirlockManager.getProduct()).observe(1);

                if (rs == null || !rs.next()) {
                    result = null;
                    cacheLookupResult = "miss_not_in_db";
                }
                else {
                    result = new UserBasicRow(rs.getTimestamp("first_session"),
                            rs.getTimestamp("last_session"), rs.getString("app_version"));
                    userCache.put(userId, result);
                    cacheLookupResult = "miss_exists_in_db";
                }
            }
            else {
                cacheLookupResult = "hit";
            }

        userCacheLookupsCounter.labels(cacheLookupResult, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
        return result;
    }

    private List<ConsumerRecord<String, JsonNode>> sortRecordsByUserIdAndEventTime(
            ConsumerRecords<String, JsonNode> records) {
        ArrayList<ConsumerRecord<String, JsonNode>> sortedRecords = new ArrayList(records.count());
        records.forEach(sortedRecords::add);
        sortedRecords.sort(UserEvent.eventRecordComparator);
        return sortedRecords;
    }

    private UserBasicRow insertUserIfNeeded(UserEvent event) throws SQLException {
        UserBasicRow userInDB = queryUser(event.getUserId());
        // Insert new user if the user does not exist yet
        if (userInDB == null)
            userInDB = insertNewUser(event);
        return userInDB;
    }

    @Override
    protected int processRecords(ConsumerRecords<String, JsonNode> records) {
        if (records.isEmpty())
            return 0;

        updateToNewConfigIfExists();

        int recordsProcessed = 0;

        for (ConsumerRecord<String, JsonNode> record : sortRecordsByUserIdAndEventTime(records)) {
            UserEvent event = null;
            UserBasicRow userInDB = null;
            String result = "success";
            boolean delayCountingRecords = false;

            try {
                event = new UserEvent(record);

                if (filterEvent(event)) {
                    switch (event.getEventName()) {
                        case "session-start":
                            userInDB = insertUserIfNeeded(event);
                            updateUserSessionTimes(event, userInDB);
                            break;
                        case "page-viewed":
                            if (AirlockManager.getProduct().equals("Weather Web")) {
                                userInDB = insertUserIfNeeded(event);
                                updateUserSessionTimes(event, userInDB);
                            }
                            break;
                        case "stream-results":
                        case "user-attributes":
                        case "user-attribute-detected":
                            insertUserIfNeeded(event);
                            dynamicUpdatesAggregator.addDynamicUpdate(event.getUserId(), event, record);
                            delayCountingRecords = true;
                            break;
                        case "poll-question-answered":
                            insertNewPollAnswer(event);
                            break;
                        /*case "ad-impression":
                            updateUserImpressionLtv(event);
                            break;*/
                    }
                }
            } catch (SQLException | NullPointerException | UserDBConsumerException e) {
                String eventId = (record.value().get("eventId") == null ? "unknown" : record.value().get("eventId").asText());
                logException(e, "Error in processRecords, partition=" + record.partition() + ", eventId: " + eventId);
                errorsProducer.sendRecord(record.key(), record.value(), record.timestamp());
                result = "error";
            } finally {
                if (!delayCountingRecords) {
                    String eventName = (record.value().get("name") == null ? "unknown" : record.value().get("name").asText());
                    recordsProcessedCounter.labels(eventName, result, AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc();
                }
                ++recordsProcessed;
            }
        }

        commit();

        totalProgress += recordsProcessed;

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

    private void logException(Exception e, String introMessage) {
        if (e instanceof SQLException) {
            SQLException se = (SQLException)e;
            LOGGER.error(introMessage + " SQLState: " + ((SQLException) e).getSQLState() + " " + e.getMessage());
            e.printStackTrace();
            while (se.getNextException() != null) {
                se = se.getNextException();
                LOGGER.error(se.getMessage());
            }
        }
        else {
            LOGGER.error(introMessage + " " + e.getMessage());
            e.printStackTrace();
        }
    }

    private boolean isCurrentTransactionAborted() {
        boolean isAborted = false;

        TransactionState transactionState = null;
        if (conn instanceof PgConnection)
            transactionState = ((PgConnection)conn).getTransactionState();

        try {
            if (transactionState == null || transactionState == TransactionState.FAILED || !conn.isValid(500)) {
                isAborted = true;
            }
        } catch (SQLException e) {
            isAborted = true;
        }

        return isAborted;
    }

    private boolean shouldDiscardFailedStatementAndRetryTransaction(SQLException e) {
        if (config.getSqlStatesContinuationPrefixes().stream().anyMatch(
                prefix -> e.getSQLState().startsWith(prefix))) {
            return true;
        }

        return false;
    }

    private void executeDynamicUpdates() throws SQLException, UserDBConsumerException {
        dynamicUpdatesAggregator.flushDynamicUpdates();

        Summary.Child latency = dbOperationsLatencySummary.labels("dynamicUpdate",
                AirlockManager.getEnvVar(), AirlockManager.getProduct());
        Summary.Timer timer;
        Summary.Child commands = dbCommandsSummary.labels("dynamicUpdate",
                AirlockManager.getEnvVar(), AirlockManager.getProduct());

        Iterator<StatementWithLineage> statementsIterator = dynamicUpdateStatements.iterator();
        while (statementsIterator.hasNext()) {
            StatementWithLineage statementWithLineage = statementsIterator.next();
            PreparedStatement statement = statementWithLineage.getStatement();
            try {
                timer = latency.startTimer();
                int rowsAffected = statement.executeUpdate();
                if (rowsAffected != 1) {
                    throw new UserDBConsumerException("Update did not have the expected outcome. " +
                            rowsAffected + " rows affected (should have been 1)");
                }
                timer.observeDuration();
                commands.observe(1);
            } catch (SQLException e) {
                String eventIds = statementWithLineage.getLineage().getEventIds();
                Integer partition = statementWithLineage.getLineage().getUserPartition();

                boolean transactionAborted = isCurrentTransactionAborted();
                boolean shouldRetry = shouldDiscardFailedStatementAndRetryTransaction(e);

                // In most cases, unless it's a non-transaction-destroying error, we have to abort the whole transaction
                // (Stop consumer etc).
                if (transactionAborted && !shouldRetry) {
                    logException(e, "Fatal SQL exception when executing dynamic field update statement, partition="
                            + partition + ", for eventIds: " + eventIds);
                    // All statements and other resources will be closed as part of shutdown
                    throw e;
                }

                // If by some miracle the transaction is not aborted, or if it is but we should retry and skip the failed
                // statement
                logException(e, "Non-Fatal SQL exception when executing dynamic field update statement, partition="
                        + partition + ", for eventIds: " + eventIds);
                if (statementWithLineage.getLineage().getRecords() != null) {
                    for (ConsumerRecord<String, JsonNode> badRecord : statementWithLineage.getLineage().getRecords()) {
                        errorsProducer.sendRecord(badRecord.key(), badRecord.value(), badRecord.timestamp());
                    }
                }

                if (transactionAborted && shouldRetry) {
                    try {
                        conn.rollback();
                    } catch (SQLException e2) {
                        // If the rollback failed too, we should shut down the consumer
                        logException(e2, "Fatal SQL Exception when trying to rollback a failed dynamic update transaction");
                        throw e2;
                    }
                    // Remove the failed statement, reset the iterator, and retry
                    statement.close();
                    statementsIterator.remove();
                    statementsIterator = dynamicUpdateStatements.iterator();
                }
                // In this case, intentionally do NOT throw an exception and do not shut down consumer.
                // Events might need to be re-ingested, but at least we don't have to loose the whole transaction.
            }
            catch (UserDBConsumerException e) {
                String eventIds = statementWithLineage.getLineage().getEventIds();
                throw new UserDBConsumerException("Fatal exception when executing dynamic field update statement for eventIds: " + eventIds, e);
            }
        }

        for (StatementWithLineage statementWithLineage : dynamicUpdateStatements) {
            statementWithLineage.getStatement().close();
        }
        dynamicUpdateStatements.clear();
    }

    // The conn.commit in the middle is to reduce the chance for deadlock since the updates will traverse
    // twice the order of the userId in the same transaction.
    // This way, updateSessionTimes will happen first in order of userId and eventTime, and then the same thing
    // For the dynamic update statements in the next transaction. (inserts can't cause a deadlock since there are no
    // dirty reads allowed.
    // TODO: check return values from executeBatch commands and issue warnings in case not enough rows were affected
    private void flushInsertsAndUpdates() throws SQLException, UserDBConsumerException {
        Summary.Timer insertTimer = startDBLatencyTimer("insert");
        insertStatement.executeBatch();
        for (PreparedStatement additionalInsertStatement : additionalInsertStatements) {
            additionalInsertStatement.executeBatch();
        }
        insertTimer.observeDuration();
        observeDBCommands("insert", insertsInBatches);
        insertsInBatches = 0;

        Summary.Timer updateTimer = startDBLatencyTimer("updateSessionTimes");
        updateSessionTimesStatement.executeBatch();
        updateTimer.observeDuration();
        observeDBCommands("updateSessionTimes", updateSessionTimesInBatch);
        updateSessionTimesInBatch = 0;

        if (updateLtvInBatch > 0) {
            commitDB();
            Summary.Timer updateLtvTimer = startDBLatencyTimer("updateLtv");
            updateImpressionLtvStatement.executeBatch();
            updateLtvTimer.observeDuration();
            observeDBCommands("updateLtv", updateLtvInBatch);
            updateLtvInBatch = 0;
        }

        commitDB();
        executeDynamicUpdates();
        insertPollAnswer.executeBatch();
        insertPollAnswerPI.executeBatch();
    }

    @Override
    protected void rollback() {
        userCache.clear();
        dynamicUpdateStatements.clear();
        super.rollback();
    }

    protected void commitDB() throws SQLException {
        Summary.Timer commitTimer = startDBLatencyTimer("commit");

        conn.commit();

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
        } catch (SQLException | UserDBConsumerException e) {
            logException(e, "Error during userdb-consumer commit function. Stopping Consumer.");
            // Don't bother to rollback kafka. Just exit the process (close the consumer on the way) and let Kube restart this service.
            // Since we use "enable.auto.commit=false" this will effectively rollback Kafka.
            // rollback();
            stop();
        }
    }

    private void closeIfNotNull(AutoCloseable c) throws Exception {
        if (c != null)
            c.close();
    }

    @Override
    public void close() throws Exception {
        closeIfNotNull(insertStatement);
        closeIfNotNull(queryStatement);
        closeIfNotNull(updateSessionTimesStatement);
        for (StatementWithLineage statement: dynamicUpdateStatements)
            closeIfNotNull(statement.getStatement());
        closeIfNotNull(conn);

        super.close();
    }

    private String healthMessage = "";

    @Override
    public boolean isHealthy() {
        if (!super.isHealthy())
            return false;

        try {
            if (!conn.isValid(3)) {
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
    public String getHealthMessage() {
        return healthMessage;
    }

    @Override
    public synchronized void newConfigurationAvailable() {
        UserDBConsumerConfig config = new UserDBConsumerConfig();
        try {
            config.initWithAirlock();
            newConfig = config;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Note that this will NOT update server URLs etc. If those change the consumer must be restarted.
    // Useful mostly for dynamic field configuration changes.
    private synchronized void updateToNewConfigIfExists() {
        if (newConfig != null) {
            // Support changing the size of the user cache on the fly
            if (newConfig.getUserCacheSizeInRecords() != config.getUserCacheSizeInRecords()) {
                // We recreate the cache completely since this operation is very rare, and memory optimization will be
                // best this way (also no good way to reduce the size of LinkedHashMap)
                userCache = new UserCache(newConfig.getUserCacheSizeInRecords());
            }
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
}
