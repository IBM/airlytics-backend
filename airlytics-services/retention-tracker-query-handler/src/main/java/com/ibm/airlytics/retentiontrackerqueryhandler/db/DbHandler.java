package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.retentiontracker.Constants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockConstants;
import com.ibm.airlytics.retentiontracker.airlock.AirlockManager;
import com.ibm.airlytics.retentiontracker.health.HealthCheckable;
import com.ibm.airlytics.retentiontracker.log.RetentionTrackerLogger;
import com.ibm.airlytics.retentiontracker.model.User;
import com.ibm.airlytics.retentiontracker.publisher.QueuePublisher;
import com.ibm.airlytics.retentiontracker.utils.TrackerTimer;
import com.ibm.airlytics.retentiontrackerqueryhandler.polls.PollResults;
import com.ibm.airlytics.retentiontrackerqueryhandler.publisher.QueryQueuePublisher;
import com.ibm.airlytics.retentiontrackerqueryhandler.utils.ConfigurationManager;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.sun.rowset.CachedRowSetImpl;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.json.JSONObject;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.sql.rowset.CachedRowSet;
import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static com.ibm.airlytics.retentiontracker.Constants.HEALTY;

@Component
@DependsOn({"ConfigurationManager", "Airlock"})
public class DbHandler implements HealthCheckable {

    String dburl;
    String userName;
    String dburlRO;
    String userNameRO;
    String healthMessage = HEALTY;
    String newHealthMessage = HEALTY;
    String pooledHealthMessage = HEALTY;
    String pooledHealthMessageRO = HEALTY;
    int newHealthFailures = 0;
    int pooledHealthFailures = 0;
    long batchSize;


    private final QueuePublisher queuePublisher;
    private final ConfigurationManager configurationManager;
    private ThreadPoolExecutor executor;
    private ComboPooledDataSource datasource;
    private ComboPooledDataSource datasourceRO;

    static final Counter queryForNonActiveUsersCounter = Counter.build()
            .name("retention_tracker_query_for_platform_total")
            .help("Total number of queries for non active users.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();

    static final Counter queryForNonActiveUsersResults = Counter.build()
            .name("retention_tracker_query_for_platform_results_total")
            .help("Total number of users returned from non active users query.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();

    static final Counter inactivatedUsersCounter = Counter.build()
            .name("retention_tracker_inactivated_users_total")
            .help("Total number of users that discovered as uninstalled.")
            .labelNames(Constants.PRODUCT, Constants.ENV)
            .register();

    static final Summary nonActiveDbOperationsSummary = Summary.build()
            .name("retention_tracker_query_for_non_active_users_latency_seconds")
            .help("Query for non-active users latency in seconds")
            .labelNames(Constants.OPERATION, Constants.PRODUCT, Constants.ENV).register();

    static final Summary inactivateUsersDbOperationsSummary = Summary.build()
            .name("retention_tracker_inactivate_users_latency_seconds")
            .help("Users inactivation process latency in seconds")
            .labelNames(Constants.OPERATION, Constants.PRODUCT, Constants.ENV).register();

    static final Summary dbPrunerOperationsSummary = Summary.build()
            .name("db_pruner_actions_latency_seconds")
            .help("DB pruner process latency in seconds")
            .labelNames(Constants.OPERATION, Constants.ENV).register();

    static final Summary dbDumperOperationsSummary = Summary.build()
            .name("db_dumper_latency_seconds")
            .help("DB dumper get db records by shard latency in seconds")
            .labelNames(Constants.SHARD, Constants.ENV).register();



    private RetentionTrackerLogger logger = RetentionTrackerLogger.getLogger(DbHandler.class.getName());

    public DbHandler(ConfigurationManager configurationManager, Environment env, QueryQueuePublisher queuePublisher) throws KeyManagementException, TimeoutException, NoSuchAlgorithmException, IOException, URISyntaxException, SQLException, ClassNotFoundException, PropertyVetoException {
        dburl = configurationManager.getDbUrl();
        userName = configurationManager.getDbUser();
        dburlRO = configurationManager.getDbUrlRO();
        userNameRO = configurationManager.getDbUserRO();
        batchSize = configurationManager.getBatchSize();
        this.queuePublisher=queuePublisher;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(configurationManager.getInactivateThreads());
        this.configurationManager = configurationManager;
        this.datasource = loadPooledConnection();
        this.datasourceRO = loadPooledConnectionRO();
    }

    private Connection getDBConnection() throws SQLException, ClassNotFoundException {
        try {
            Class.forName("org.postgresql.Driver");
            Properties props = new Properties();
            props.setProperty("user", userName);
            props.setProperty("password", configurationManager.getDbPass());
            props.setProperty("ssl", "true");
            props.setProperty("sslmode", "verify-full");
            Connection conn = DriverManager.getConnection(dburl, props);
            newHealthMessage = HEALTY;
            newHealthFailures = 0;
            return conn;
        } catch (SQLException | ClassNotFoundException e) {
            newHealthMessage = e.getMessage();
            newHealthFailures++;
            throw e;
        }

    }

    private Connection getDBConnectionFromPool() throws SQLException, ClassNotFoundException {
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = datasource.getConnection();
            pooledHealthFailures = 0;
            pooledHealthMessage = HEALTY;
            return conn;
        } catch (SQLException | ClassNotFoundException e) {
            pooledHealthMessage = e.getMessage();
            pooledHealthFailures++;
            throw e;
        }
    }

    private Connection getDBROConnectionFromPool() throws SQLException, ClassNotFoundException {
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = datasourceRO.getConnection();
            pooledHealthFailures = 0;
            pooledHealthMessageRO = HEALTY;
            return conn;
        } catch (SQLException | ClassNotFoundException e) {
            pooledHealthMessageRO = e.getMessage();
            pooledHealthFailures++;
            throw e;
        }
    }

    private ComboPooledDataSource loadPooledConnection() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass( "org.postgresql.Driver" ); //loads the jdbc driver
        cpds.setJdbcUrl( dburl );
        cpds.setUser(userName);
        cpds.setPassword(configurationManager.getDbPass());
        Properties props = new Properties();
        props.setProperty("user", userName);
        props.setProperty("password", configurationManager.getDbPass());
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", "verify-full");
        cpds.setProperties(props);

        cpds.setMinPoolSize(configurationManager.getDbConnectionMinPoolSize());
        cpds.setAcquireIncrement(configurationManager.getDbConnectionAcquireIncrement());
        cpds.setMaxPoolSize(configurationManager.getDbConnectionMaxPoolSize());
        cpds.setInitialPoolSize(configurationManager.getDbConnectionInitialPoolSize());

        return cpds;
    }

    private ComboPooledDataSource loadPooledConnectionRO() throws PropertyVetoException {
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        cpds.setDriverClass( "org.postgresql.Driver" ); //loads the jdbc driver
        cpds.setJdbcUrl( dburlRO );
        cpds.setUser(userNameRO);
        cpds.setPassword(configurationManager.getDbPassRO());
        Properties props = new Properties();
        props.setProperty("user", userNameRO);
        props.setProperty("password", configurationManager.getDbPassRO());
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", "verify-full");
        cpds.setProperties(props);

        cpds.setMinPoolSize(configurationManager.getDbConnectionMinPoolSize());
        cpds.setAcquireIncrement(configurationManager.getDbConnectionAcquireIncrement());
        cpds.setMaxPoolSize(configurationManager.getDbConnectionMaxPoolSize());
        cpds.setInitialPoolSize(configurationManager.getDbConnectionInitialPoolSize());

        return cpds;
    }

    public void getNonActiveUsers() throws ClassNotFoundException, SQLException, IOException {
        logger.debug("getNonActiveUsers()");
        TrackerTimer timer = new TrackerTimer();
        Feature inactiveUsers = AirlockManager.getInstance().getFeature(AirlockConstants.query.INACTIVE_USERS_QUERY);
        String query = inactiveUsers.getConfiguration().getString("query");
        String select = query;//String.format(query, platform);
        String excludedStr = inactiveUsers.getConfiguration().getString("prefixesToExclude");
        if (excludedStr == null) excludedStr = "";
        List<String> excludedPrefixes = excludedStr.isEmpty() ? new ArrayList<>() : Arrays.asList(excludedStr.split("\\s*,\\s*"));
        logger.debug("getting non active users:"+select);
        String update = inactiveUsers.getConfiguration().getString("update_batch");
        ResultSet rs = null;
        try (
                Connection conn = getDBConnectionFromPool();
                Connection connRO = getDBROConnectionFromPool();
                Statement stmt = connRO.createStatement();
                PreparedStatement updateStatement = conn.prepareStatement(update);
                ){
            Summary.Timer rsTimer = nonActiveDbOperationsSummary.labels("query", AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).startTimer();
            try {
                rs = stmt.executeQuery(select);
            } finally {
                rsTimer.observeDuration();
            }
            double time = timer.getTime();
            long count = 0;
            ArrayList<String> batch = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                String userID = rs.getString("id");
                String token = rs.getString("push_token");
                if (isTokenExcluded(token, excludedPrefixes)) {
                    logger.debug("excluded user with token:"+token);
                    continue;
                }
                logger.debug("got user with id:" + userID);
                ids.add(userID);
                batch.add(token);
                if (batch.size() >= batchSize) {
                    queuePublisher.publishMessage(batch.toString());
                    batch.clear();
                }
                ++count;
            }
            if (batch.size() > 0) {
                queuePublisher.publishMessage(batch.toString());
            }
            queryForNonActiveUsersResults.labels(AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).inc(count);
            logger.debug("got " + count + " results in " + timer.getTotalTime() + " seconds. (query time is " + time + " seconds)");

            //update rows
            boolean isAutoCommit = conn.getAutoCommit();
            Collections.sort(ids);
            if (ids.size() > 0) {
                logger.debug("performing update:"+update);
                timer.getTime();
                Timestamp now = new Timestamp(System.currentTimeMillis());
                try {
                    conn.setAutoCommit(false);
                    for (String id : ids) {
                        logger.debug("updating with time:"+now+", id:"+id);
                        updateStatement.setTimestamp(1, now);
                        updateStatement.setString(2, id);
                        updateStatement.addBatch();
                    }
                    Summary.Timer batchTimer = nonActiveDbOperationsSummary.labels("update", AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).startTimer();
                    try {
                        updateStatement.executeBatch();
                    } finally {
                        batchTimer.observeDuration();
                    }
                    conn.commit();

                    logger.debug("updated users as sent notification("+AirlockManager.getAirlock().getProduct()+"): update time is "+timer.getTime()+" seconds (total getNonActiveUsers() time is "+timer.getTotalTime()+" seconds)");
                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                } finally {
                    conn.setAutoCommit(isAutoCommit);
                }
            }
            queryForNonActiveUsersCounter.labels(AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).inc();
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    private boolean isTokenExcluded(String token, List<String> excludedPrefixes) {
        for (String prefix : excludedPrefixes) {
            if (token.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    public void setUsersInactiveById(List<String> ids, String platform) throws SQLException, ClassNotFoundException {
        logger.info("setUserInactive() for ids " + ids);
        TrackerTimer timer = new TrackerTimer();
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.retention.SET_USERS_IDS_INACTIVE);
        String arrStr = getStringsArrayString(ids);
        String updateTemplate = feature.getConfiguration().getString("update");
        String update = String.format(updateTemplate, arrStr);

        try(Connection conn = getDBConnectionFromPool();
            PreparedStatement updateStatement = conn.prepareStatement(update)) {
            updateStatement.setString(1, "UNINSTALLED");
            updateStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            Summary.Timer updateTimer = inactivateUsersDbOperationsSummary.labels("update", platform, ConfigurationManager.getEnvVar()).startTimer();
            try {
                updateStatement.execute();
            } finally {
                updateTimer.observeDuration();
            }

            logger.debug("inactivated user with ids "+ids);
            logger.debug("total time of setUsersInactiveById is "+timer.getTime()+" seconds");
            inactivatedUsersCounter.labels(AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).inc(ids.size());
        }
    }
    private void setUsersInactive(List<String> tokens, String platform) throws SQLException, ClassNotFoundException {
        logger.debug("setUserInactive() for tokens " + tokens);
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.retention.SET_USER_INACTIVE);
        String selectT = feature.getConfiguration().getString("select");
        String arrStr = getStringsArrayString(tokens);
        String select = String.format(selectT, arrStr);//"SELECT id FROM "+tableName+" WHERE push_token = '" + token + "'";

        String updateT = feature.getConfiguration().getString("update");
        String update = String.format(updateT, select);//"UPDATE "+tableName+" SET status = ?, uninstall_date = ? WHERE id IN (" + select + ")";

        try(Connection conn = getDBConnectionFromPool();
            PreparedStatement updateStatement = conn.prepareStatement(update)) {
            updateStatement.setString(1, "UNINSTALLED");
            updateStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            updateStatement.execute();

            logger.debug("inactivated user with tokens "+tokens);
            inactivatedUsersCounter.labels(AirlockManager.getAirlock().getProduct(), ConfigurationManager.getEnvVar()).inc(tokens.size());
        }
    }

    public List<User> getInactiveUsers(List<String> tokens) throws SQLException, ClassNotFoundException {
        logger.debug("getInactiveUsers() for " + tokens.size()+" tokens");
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.retention.GET_INACTIVE_USERS);
        String selectT = feature.getConfiguration().getString("select");
        String arrStr = getStringsArrayString(tokens);
        String select = String.format(selectT, arrStr);
        logger.debug("performing select:"+select);
        ResultSet rs = null;
        try (Connection conn = getDBROConnectionFromPool();
             Statement stmt = conn.createStatement();
             ){
            Summary.Timer timer = inactivateUsersDbOperationsSummary.labels("query_for_tokens", "", ConfigurationManager.getEnvVar()).startTimer();
            try {
                rs = stmt.executeQuery(select);
            } finally {
                timer.observeDuration();
            }
            ArrayList<User> users = new ArrayList<>();
            while (rs.next()) {
                users.add(buildUser(rs));
            }
            return users;
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

    }

    private User buildUser(ResultSet rs) throws SQLException {
        String id = rs.getString("id");
        String platform = rs.getString("platform");
        String token = rs.getString("push_token");
        String productId = configurationManager.getProductId(platform);
        User user = new User(id, token, platform, productId);
        return user;
    }

    public void inactivateUsers(List<String> tokens, String platform) throws SQLException, ClassNotFoundException {
        try {
            setUsersInactive(tokens, platform);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("failed in inactivating users "+tokens+" \n"+e.getMessage());
            throw e;
        }
    }

    private String getValuesArrayString(List<String> values) {
        StringBuilder builder = new StringBuilder();
        for (String val : values) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append("('"+val+"')");
        }
        return builder.toString();
    }
    private String getStringsArrayString(List<String> tokens) {
        StringBuilder builder = new StringBuilder();
        for (String token : tokens) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append("'"+token+"'");
        }
        return builder.toString();
    }


    public class ResultSetWithResources {
        private Connection connection;

        private List<Statement> statements = new LinkedList<>();

        public ResultSet getResult() {
            return result;
        }

        private ResultSet result;
        public ResultSetWithResources(ResultSet rs, List<Statement> statements, Connection connection) {
            this.connection=connection;
            this.result=rs;
            if (statements == null) {
                this.statements = new LinkedList<>();
            } else {
                this.statements=statements;
            }

        }

        public void closeResources() {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException throwables) {
                    logger.warn("failed closing result-set:"+throwables.getMessage());
                }
            }
            for (Statement statement : statements) {
                try {
                    statement.close();
                } catch (SQLException throwables) {
                    logger.warn("failed closing statement:"+throwables.getMessage());
                }

            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException throwables) {
                logger.warn("failed closing connection:"+throwables.getMessage());
            }
        }
    }
    public CachedRowSet getDbRecords(int shard) throws SQLException, ClassNotFoundException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String selectT = dumper.getConfiguration().getString("shardedQuery");
        String select = String.format(selectT, shard);
        ResultSet rs = null;
        try (Connection conn = getDBConnectionFromPool();
             Statement stmt = conn.createStatement();) {
            Summary.Timer timer = dbDumperOperationsSummary.labels(Integer.toString(shard),ConfigurationManager.getEnvVar()).startTimer();
            TrackerTimer ttimer = new TrackerTimer();
            try {
                rs = stmt.executeQuery(select);
            }
            finally {
                timer.observeDuration();
                logger.debug("getDBRecords time for shard "+shard+":"+ttimer.getTime());
            }

            CachedRowSet rowSet = new CachedRowSetImpl();
            rowSet.populate(rs);
            return rowSet;

        } finally {
            if (rs != null) {
                rs.close();
            }
        }

    }

    public ResultSetWithResources getDbRecordsForTable(String schema, String table, int shard, String orderByField) throws SQLException, ClassNotFoundException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String selectT = dumper.getConfiguration().getString("shardedQueryForTable");
        String select = String.format(selectT, schema, table, shard, orderByField);
        return getCachedRowSetForQuery(select, table, Integer.toString(shard));

    }

    public ResultSetWithResources getDBRecordsForTableForShardNull(String schema, String table, String orderByField) throws SQLException, ClassNotFoundException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String selectT = dumper.getConfiguration().getString("nullShardedQueryForTable");
        String select = String.format(selectT, schema, table, orderByField);
        return getCachedRowSetForQuery(select, table, "Null");
    }

    public ResultSetWithResources getDbRecordsForTable(String schema, String table, String orderByField) throws SQLException, ClassNotFoundException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String selectT = dumper.getConfiguration().getString("queryForTable");
        String select = String.format(selectT, schema, table, orderByField);
        return getCachedRowSetForQuery(select, table, "All");

    }

    private ResultSetWithResources getCachedRowSetForQuery(String select, String table, String shard) throws SQLException, ClassNotFoundException {
        ResultSetWithResources resultSetWithResources = null;
        ResultSet rs;
        Connection conn = getDBConnectionFromPool();
        Statement stmt = conn.createStatement();
        try {
            Summary.Timer timer = dbDumperOperationsSummary.labels(shard,ConfigurationManager.getEnvVar()).startTimer();
            TrackerTimer ttimer = new TrackerTimer();
            try {
                rs = stmt.executeQuery(select);
            }
            finally {
                timer.observeDuration();
                logger.debug("getDBRecords time for table "+table+" and shard "+shard+":"+ttimer.getTime());
            }
            resultSetWithResources = new ResultSetWithResources(rs, Collections.singletonList(stmt), conn);
            return resultSetWithResources;
//            RowSetFactory factory = RowSetProvider.newFactory();
//            CachedRowSet rowSet = factory.createCachedRowSet();
////            CachedRowSet rowSet = new CachedRowSetImpl();
//            rowSet.populate(rs);
//            return rowSet;

        } finally {
//            if (rs != null) {
//                rs.close();
//            }
        }
    }

    public ResultSetWithResources getDBSchema() throws ClassNotFoundException, SQLException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String select = dumper.getConfiguration().getString("getDbSchema");
        Connection conn = getDBConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(select);
        List<Statement> statements = new LinkedList<>();
        statements.add(stmt);
        ResultSetWithResources toRet = new ResultSetWithResources(rs, statements, conn);
        return toRet;

    }
    public ResultSetWithResources getDBSchemaForTable(String schema, String table) throws ClassNotFoundException, SQLException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String select = dumper.getConfiguration().getString("getDbSchemaForTable");
        Connection conn = getDBConnection();
        PreparedStatement stmt = conn.prepareStatement(select);
        stmt.setString(1, table);
        stmt.setString(2, schema);
        ResultSet rs = stmt.executeQuery();
        List<Statement> statements = new LinkedList<>();
        statements.add(stmt);
        ResultSetWithResources toRet = new ResultSetWithResources(rs, statements, conn);
        return toRet;
    }


    public List<DBSchemaField> getDBSchemaFields() throws ClassNotFoundException, SQLException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String select = dumper.getConfiguration().getString("getDbSchema");
        List<DBSchemaField> toRet = new ArrayList<>();
        try (Connection conn = getDBConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs =  stmt.executeQuery(select)) {
            while (rs.next()) {
                String fieldName = rs.getString("column_name");
                String type = rs.getString("data_type");
                String isNullable = rs.getString("is_nullable");
                toRet.add(new DBSchemaField(fieldName, type, isNullable));
            }
        }
        return toRet;
    }

    public List<DBSchemaField> getDBSchemaFields(String table, String schema) throws ClassNotFoundException, SQLException {
        Feature dumper = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_DUMPER);
        String select = dumper.getConfiguration().getString("getDbSchemaForTable");
        List<DBSchemaField> toRet = new ArrayList<>();
        String tablePar = table;
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select);

             ) {
            stmt.setString(1, tablePar);
            stmt.setString(2, schema);
            try (ResultSet rs =  stmt.executeQuery();) {
                while (rs.next()) {
                    String fieldName = rs.getString("column_name");
                    String type = rs.getString("data_type");
                    String isNullable = rs.getString("is_nullable");
                    toRet.add(new DBSchemaField(fieldName, type, isNullable));
                }
            }
        }
        return toRet;
    }

    public class DBSchemaField {
        public String fieldName;
        public String type;
        public String isNullable;

        public DBSchemaField(String fieldName, String type, String isNullable) {
            this.fieldName = fieldName;
            this.type = type;
            this.isNullable = isNullable;
        }
    }

    private List<String> getDbFields(String schema, String table) throws SQLException, ClassNotFoundException {
        ResultSetWithResources rsWithResources = null;
        try  {
            rsWithResources = getDBSchemaForTable(schema, table);
            ResultSet dbDef = rsWithResources.result;
            Set<String> columnsSet = new HashSet<>();
            while (dbDef.next()) {
                columnsSet.add(dbDef.getString("column_name"));
            }
            return new ArrayList<>(columnsSet);
        } finally {
            if (rsWithResources != null) {
                rsWithResources.closeResources();
            }
        }
    }

    public Runnable getPruneProcess() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    pruneDevUsersFromAllTables();
                    pruneOldUsersFromAllTables();
                } catch (SQLException | ClassNotFoundException e) {
                    logger.error("error in pruneOldUsers process:"+e.getMessage());
                    e.printStackTrace();
                }
            }
        };
    }

    public void pruneDevUsersFromAllTables() throws SQLException, ClassNotFoundException {
        Feature inactiveUsers = AirlockManager.getInstance().getFeature(AirlockConstants.query.REMOVE_DEV_USERS);
        if (!inactiveUsers.isOn()) {
            logger.error("pruneDevUsers() the relevant feature "+inactiveUsers.getName()+" is OFF:"+inactiveUsers.getTraceInfo());
            return;
        }

        String deleteArr = inactiveUsers.getConfiguration().getString("deleteBatch");
        List<String> deletes = Arrays.asList(deleteArr.split(";"));
        String select = inactiveUsers.getConfiguration().getString("select");
        List<PreparedStatement> deleteStatements = new ArrayList<>();

        try (Connection conn = getDBConnection();
             Connection connRO = getDBROConnectionFromPool();
             Statement stmt = connRO.createStatement();
             ResultSet rs = stmt.executeQuery(select)) {
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                logger.info("found dev user to delete with id:"+id);
                ids.add(id);
            }
            conn.setAutoCommit(false);
            for (String delete : deletes) {
                PreparedStatement deleteStatement = conn.prepareStatement(delete);
                deleteStatements.add(deleteStatement);
            }
            try {
                for (PreparedStatement updateStatement : deleteStatements) {
                    for (String id : ids) {
                        updateStatement.setString(1, id);
                        updateStatement.addBatch();
                    }
                    updateStatement.executeBatch();
                }
                conn.commit();
                logger.info(ids.size()+" users marked as dev_user deleted from all tables");
            } catch (Exception e) {
                conn.rollback();
                logger.error("error in pruneDevUsersFromAllTables:"+e.getMessage());
                throw e;
            }
        }
    }
    private void pruneDevUsers() throws SQLException, ClassNotFoundException {
        Feature inactiveUsers = AirlockManager.getInstance().getFeature(AirlockConstants.query.REMOVE_DEV_USERS);
        if (!inactiveUsers.isOn()) {
            logger.error("pruneDevUsers() the relevant feature "+inactiveUsers.getName()+" is OFF:"+inactiveUsers.getTraceInfo());
            return;
        }
        String delete = inactiveUsers.getConfiguration().getString("delete");
        try (Connection conn = getDBConnection();
             Statement deleteStmt = conn.createStatement();) {
            Summary.Timer deleteTimer = dbPrunerOperationsSummary.labels("prune_dev_users", ConfigurationManager.getEnvVar()).startTimer();
            int deletedCount = 0;
            try {
                deletedCount = deleteStmt.executeUpdate(delete);
            } finally {
                deleteTimer.observeDuration();
            }

            logger.debug(deletedCount+" users marked as dev_user deleted from table 'users'");
        }
    }

    public void pruneOldUsersFromAllTables() throws SQLException, ClassNotFoundException {
        Feature dbPruner= AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_PRUNER);
        if (!dbPruner.isOn()) {
            logger.error("pruneDevUsers() the relevant feature "+dbPruner.getName()+" is OFF:"+dbPruner.getTraceInfo());
            return;
        }
        //prepare sql statements
        String select = dbPruner.getConfiguration().getString("select");
        String deleteArr = dbPruner.getConfiguration().getString("deleteBatch");
        List<String> deletes = Arrays.asList(deleteArr.split(";"));
        String insertArr = dbPruner.getConfiguration().getString("insertBatch");
        List<String> inserts = Arrays.asList(insertArr.split(";"));
        if (inserts.size() <=0 && deletes.size() <= 0) {
            logger.warn("no archive for old users configured");
            return;
        }
        List<PreparedStatement> deleteStatements = new ArrayList<>();
        List<PreparedStatement> insertStatements = new ArrayList<>();

        try (Connection conn = getDBConnection();
             Connection connRO = getDBROConnectionFromPool();
             Statement stmt = connRO.createStatement();
             ResultSet rs = stmt.executeQuery(select)) {
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                logger.info("found user to archive with id:"+id);
                ids.add(id);
            }
            conn.setAutoCommit(false);
            for (String insert : inserts) {
                PreparedStatement insertStatement = conn.prepareStatement(insert);
                insertStatements.add(insertStatement);
            }
            for (String delete : deletes) {
                PreparedStatement deleteStatement = conn.prepareStatement(delete);
                deleteStatements.add(deleteStatement);
            }
            try {
                for (PreparedStatement updateStatement : insertStatements) {
                    for (String id : ids) {
                        updateStatement.setString(1, id);
                        updateStatement.addBatch();
                    }
                    updateStatement.executeBatch();
                }
                for (PreparedStatement updateStatement : deleteStatements) {
                    for (String id : ids) {
                        updateStatement.setString(1, id);
                        updateStatement.addBatch();
                    }
                    updateStatement.executeBatch();
                }
                conn.commit();
                logger.info(ids.size()+" users archived");
            } catch (Exception e) {
                conn.rollback();
                logger.error("error in pruneOldUsersFromAllTables:"+e.getMessage());
                throw e;
            }
        }
    }
    public void pruneOldUsers() throws ClassNotFoundException, SQLException {
        Feature inactiveUsers = AirlockManager.getInstance().getFeature(AirlockConstants.query.DB_PRUNER);
        String condition = inactiveUsers.getConfiguration().getString("condition");
        String queryTemplate = inactiveUsers.getConfiguration().getString("query");
        String queryPiTemplate = inactiveUsers.getConfiguration().getString("pi_query");
        String insertTemplate = inactiveUsers.getConfiguration().getString("insert");
        String insertPiTemplate = inactiveUsers.getConfiguration().getString("insert_pi");
        String deleteTemplate = inactiveUsers.getConfiguration().getString("delete");
        String retainedFieldsStr = inactiveUsers.getConfiguration().getString("retained_fields");
        String deletePi = inactiveUsers.getConfiguration().getString("delete_pi");

        String select = buildPruneSelect(queryTemplate, condition);
        String piSelect = buildPruneSelect(queryPiTemplate, condition, "users", "users_pi");
        String insert = buildInsertToArchiveString(insertTemplate, select, retainedFieldsStr);
        String piInsert = buildInsertToArchivePiString(insertPiTemplate, piSelect);
        String delete = String.format(deleteTemplate, condition);
        Summary.Timer operationsTimer = dbPrunerOperationsSummary.labels("prune_old_users", ConfigurationManager.getEnvVar()).startTimer();
        try (Connection conn = getDBConnection();
             PreparedStatement insertStmt = conn.prepareStatement(insert);
             PreparedStatement insertPiStmt = conn.prepareStatement(piInsert);
             Statement deleteStmt = conn.createStatement()) {

            //insert to users_pi_archive first
            insertPiStmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            insertPiStmt.execute();

            //insert to users_archive
            insertStmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            insertStmt.execute();

            int deletedCount = deleteStmt.executeUpdate(delete);
            logger.debug(deletedCount+" users deleted from table 'users' and inserted to archive");

            int deletedPiCount = deleteStmt.executeUpdate(deletePi);
            logger.debug(deletedPiCount+" users deleted from table 'users_pi' and inserted to archive");
        } finally {
            operationsTimer.observeDuration();
        }
    }

    public JSONObject getUserJSON(String userId) throws SQLException, ClassNotFoundException {
        logger.debug("getUserJSON for id:"+userId);
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.retention.GET_USER_BY_ID);
        String select = feature.getConfiguration().getString("query");
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select)) {
            stmt.setString(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int numColumns = metaData.getColumnCount();
                JSONObject toRet = new JSONObject();
                if (rs.next()) {
                    for (int i=1; i<=numColumns; ++i) {
                        String name = metaData.getColumnName(i);
                        String typeName = metaData.getColumnTypeName(i);
                        FieldConfig config = new FieldConfig(typeName, "yes");
                        Object value = getValue(rs, config, i);
                        toRet.put(name, value);
                    }
                }
                return toRet;
            }
        }
    }

    public PollResults getPollsResults(String prodcutId, int answersProcessedRows, int piAnswersProcessedRows) throws SQLException, ClassNotFoundException {
        logger.debug("getPollsResults");
        PollResults pr = new PollResults(prodcutId);
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.query.POLLS);
        String selectFormat = feature.getConfiguration().getString("query");
        String select = String.format(selectFormat,answersProcessedRows, piAnswersProcessedRows, answersProcessedRows, piAnswersProcessedRows);

        try (Connection conn = getDBConnection();
            PreparedStatement stmt = conn.prepareStatement(select)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    countAnswer(rs,pr);
                }
            }
        }
        pr.sumProcessedRows();
        return pr;
    }

    public String readPollCache() throws SQLException, ClassNotFoundException {
        logger.debug("readPollCache");
        String content = null;
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.query.POLLS);
        String select = feature.getConfiguration().getString("readCache");

        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    content = rs.getString("content_json");
                }
            }
        }
        return content;
    }

    public void updatePollCache(String jsonStr) throws SQLException, ClassNotFoundException {
        logger.debug("updatePollCache");
        String content = null;
        Feature feature = AirlockManager.getInstance().getFeature(AirlockConstants.query.POLLS);
        String updateFormat = feature.getConfiguration().getString("updateCache");
        String update = String.format(updateFormat,jsonStr,jsonStr);
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(update)) {
             stmt.executeUpdate();
        }
    }

    private void countAnswer(ResultSet rs, PollResults pr) throws SQLException {
        String pollId = rs.getString("poll_id");
        String questionId = rs.getString("question_id");
        String answerId = rs.getString("answer_id");
        Integer answers = rs.getInt("answers");
        Integer totalResponses = rs.getInt("total_responses");
        Boolean pi = rs.getBoolean("pi");

        if (answerId.equals("open answer")) {
            pr.setOpenAnswerCount(pollId,questionId,answers,totalResponses,pi);
        } else {
            pr.setPredefinedAnswerCount(pollId, questionId, answerId, answers, totalResponses,pi);
        }
    }

    private Object getValue(ResultSet rs, FieldConfig fieldConfig, int idx) throws SQLException {
        switch (fieldConfig.getType()) {
            case LONG:
                String val = null;
                if (rs.getTimestamp(idx) != null) {
                    val = rs.getTimestamp(idx).toString();
                }
                return val;//rs.getTimestamp(idx);
            case FLOAT:
                return rs.getFloat(idx);
            case STRING:
                return rs.getString(idx);
            case DOUBLE:
                return rs.getDouble(idx);
            case BOOLEAN:
                return rs.getBoolean(idx);
            case INTEGER:
                return rs.getInt(idx);
            default:
                throw new IllegalStateException("Unexpected value: " + fieldConfig.getType());
        }
    }
    private String buildPruneSelect(String queryTemplate, String condition) throws SQLException, ClassNotFoundException {
        return buildPruneSelect(queryTemplate, condition, "users", "users");
    }
    private String buildPruneSelect(String queryTemplate, String condition, String schema, String table) throws SQLException, ClassNotFoundException {
        String fieldsStr = getDbFieldAsString(schema, table)+", ?";
        String res = String.format(queryTemplate, fieldsStr, condition);
        return res;
    }
    private String getDbFieldAsString() throws SQLException, ClassNotFoundException {
        return getDbFieldAsString("users", "users");
    }
    private String getDbFieldAsString(String schema, String table) throws SQLException, ClassNotFoundException {
        List<String> columns = getDbFields(schema, table);
        StringBuilder sb = new StringBuilder("");
        boolean isFirst = true;
        for (String column : columns) {
            if (!isFirst) {
                sb.append(",");
            }
            sb.append(" "+column);
            isFirst = false;
        }
        return sb.toString();
    }

    private String buildInsertToArchivePiString(String insertTemplate, String select) throws SQLException, ClassNotFoundException {
        String whatToInsert = getDbFieldAsString("users","users_pi")+", archive_date";
        String insertPart = String.format(insertTemplate, whatToInsert, select);
        return insertPart;
    }

    private String buildInsertToArchiveString(String insertTemplate, String select, String retainedFieldsStr) throws SQLException, ClassNotFoundException {
        List<String> retainedFields = Arrays.asList(retainedFieldsStr.split(","));
        List<String> columns = getDbFields("users", "users");
        String whatToInsert = getDbFieldAsString()+", archive_date";
        String insertPart = String.format(insertTemplate, whatToInsert, select);
        StringBuilder sb = new StringBuilder(insertPart);
        // build the ON CONFLICT UPDATE part

        boolean isFirst = true;
        for (String column : columns) {
            if (retainedFields.contains(column)) continue;
            if (!isFirst) {
                sb.append(",");
            }
            sb.append(" "+column+" = EXCLUDED."+column);
            isFirst = false;
        }
        return sb.toString();
    }

    public void listenForNonActiveUsers(long initialDelay, long delay) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    getNonActiveUsers();
                } catch (ClassNotFoundException | SQLException | IOException e) {
                    logger.error("Failed in getNonActiveUsers:"+e.getMessage());
                }
            }
        },initialDelay, delay, TimeUnit.MINUTES);
    }

    @PreDestroy
    public void onDestroy() throws Exception {
        if (datasource != null) {
            datasource.close();
        }
    }

    @Override
    public boolean isHealthy() {
        int numFailures = pooledHealthFailures + newHealthFailures;
        int threshold = 1; //maybe more?
        if (numFailures > threshold) {
            String message = "";
            if (!pooledHealthMessage.equals(HEALTY)) {
                message = pooledHealthMessage;
            }
            if (!pooledHealthMessageRO.equals(HEALTY)) {
                if (!message.isEmpty()) {
                    message = message + ", ";
                }
                message = message + pooledHealthMessageRO;
            }
            if (!newHealthMessage.equals(HEALTY)) {
                if (!message.isEmpty()) {
                    message = message + ", ";
                }
                message = message + newHealthMessage;
            }
            healthMessage = message;
            return false;
        }
        healthMessage = HEALTY;
        return true;
    }

    @Override
    public String getHealthMessage() {
        return healthMessage;
    }
}

