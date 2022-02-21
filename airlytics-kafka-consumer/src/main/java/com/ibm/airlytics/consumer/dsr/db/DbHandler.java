package com.ibm.airlytics.consumer.dsr.db;

import com.ibm.airlock.common.data.Feature;
import com.ibm.airlytics.airlock.AirlockConstants;
import com.ibm.airlytics.airlock.AirlockManager;
import com.ibm.airlytics.consumer.AirlyticsConsumerConstants;
import com.ibm.airlytics.consumer.dsr.DSRConsumer;
import com.ibm.airlytics.consumer.dsr.retriever.FieldConfig;
import com.ibm.airlytics.utilities.logs.AirlyticsLogger;
import com.sun.rowset.CachedRowSetImpl;
import io.prometheus.client.Counter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.sql.rowset.CachedRowSet;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ibm.airlytics.consumer.AirlyticsConsumerConstants.*;


public class DbHandler {

    String dburl;
    String userName;
    long batchSize;
    private DBConfig config;
    private static String UPS_ID_FIELD = "ups_id";
    private static String ID_FIELD = "id";
    private static String REGISTERED_USER_ID = "registered_user_id";

    private ThreadPoolExecutor executor;

    static final Counter deletedUsersCounter = Counter.build()
            .name("retention_tracker_deleted_users_total")
            .help("Total number of users that was explicitly deleted by request.")
            .labelNames(AirlyticsConsumerConstants.PRODUCT)
            .register();

    private static final AirlyticsLogger logger = AirlyticsLogger.getLogger((DbHandler.class.getName()));
    public DbHandler(DBConfig dbconfig) throws IOException, SQLException, ClassNotFoundException {
        config = dbconfig;
        config.initWithAirlock();
        dburl = config.getDbUrl();
        userName = config.getDbUsername();
    }


    private Connection getDBConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        Properties props = new Properties();
        props.setProperty("user", userName);
        props.setProperty("password", config.getDbPassword());
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", "verify-full");
        Connection conn = DriverManager.getConnection(dburl, props);
        return conn;
    }

    public JSONObject getUserJSON(String upsId) throws SQLException, ClassNotFoundException {
        logger.debug("getUserJSON for id:"+upsId);
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.db.GET_USER_BY_UPS_ID);
        String select = feature.getConfiguration().getString("query");
        try (Connection conn = getDBConnection();
            PreparedStatement stmt = conn.prepareStatement(select)) {
            stmt.setString(1, upsId);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int numColumns = metaData.getColumnCount();
                JSONObject toRet = null;
                if (rs.next()) {
                    toRet = new JSONObject();
                    for (int i=1; i<=numColumns; ++i) {
                        String name = metaData.getColumnName(i);
                        String typeName = metaData.getColumnTypeName(i);
                        FieldConfig config = new FieldConfig(typeName, "yes");
                        Object value = getValue(rs, config, i);
                        if (value!=null) {
                            toRet.put(name, value);
                        }
                    }
                }
                return toRet;
            }
        }


    }

    private Map<String,JSONObject> getUsersJSONs(List<String> upsIds, String queryString, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        logger.info("getUserJSONs for "+upsIds.size()+" ups_ids:"+upsIds);
        if (upsIds.size() <= 0) {
            return new HashMap<>();
        }
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.db.GET_USERS_BY_UPS_IDS);
        String selectTemplate = feature.getConfiguration().getString(queryString);
        if (selectTemplate==null || selectTemplate.isEmpty()) {
            logger.error(queryString+" is empty for GET_USERS_BY_UPS_IDS");
            return new HashMap<>();
        }
        String idsStr = getStringsArrayString(upsIds);
        String select = String.format(selectTemplate, idsStr);
        logger.debug("getUsersJSONs select:"+select);
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int numColumns = metaData.getColumnCount();
            Map<String, JSONObject> toRet = new HashMap<>();
            while (rs.next()) {
                JSONObject currObj = new JSONObject();
                String currUpsId = null;
                for (int i=1; i<=numColumns; ++i) {
                    String name = metaData.getColumnName(i);
                    logger.debug("column found:"+name);
                    if (excludedColumns == null || !excludedColumns.contains(name)) {
                        String typeName = metaData.getColumnTypeName(i);
                        FieldConfig config = new FieldConfig(typeName, "yes");
                        Object value = getValue(rs, config, i);
                        if (value!=null) {
                            currObj.put(name, value);
                        }
                    }
                    if (name.equals(UPS_ID_FIELD)) {
                        currUpsId = rs.getString(name);
                    }
                }
                if (currUpsId != null) {
                    toRet.put(currUpsId, currObj);
                    logger.info("found data in DB for user with ups_id:"+currUpsId);
                } else {
                    logger.warn("getUserJSONs: did not find upsId");
                }
            }
            return toRet;
        }
    }

    public Map<String, List<String>> getDeviceIdsByRegisteredUser(List<String> userIds) throws SQLException, ClassNotFoundException {
        logger.info("getDeviceIdsByRegisteredUser for "+userIds.size()+" ids:"+userIds);
        if (userIds.size() <= 0) {
            return new HashMap<>();
        }
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.db.GET_IDS_FROM_REGISTERED_USER_ID);
        String selectTemplate = feature.getConfiguration().getString("query");
        String idsStr = getStringsArrayString(userIds);
        String select = String.format(selectTemplate, idsStr, idsStr);
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select);
             ResultSet rs = stmt.executeQuery()) {
            Map<String, List<String>> toRet = new HashMap<>();
            while (rs.next()) {
                JSONObject currObj = new JSONObject();
                String currDeviceID = rs.getString(ID_FIELD);
                String currRegisteredUserId = rs.getString(REGISTERED_USER_ID);
                if (currDeviceID!=null && currRegisteredUserId!=null) {
                    logger.info("found data in DB for user with user id:"+currRegisteredUserId);
                    List<String> devices = toRet.getOrDefault(currRegisteredUserId, new ArrayList<>());
                    devices.add(currDeviceID);
                    toRet.put(currRegisteredUserId, devices);
                }
            }
            return toRet;
        }
    }
    private Map<String,JSONObject> getUsersJSONsById(List<String> ids, String queryString, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        logger.info("getUserJSONs for "+ids.size()+" ids:"+ids);
        if (ids.size() <= 0) {
            return new HashMap<>();
        }
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.db.GET_USERS_BY_IDS);
        String selectTemplate = feature.getConfiguration().getString(queryString);
        String idsStr = getStringsArrayString(ids);
        String select = String.format(selectTemplate, idsStr);
        logger.debug("getUsersJSONsById select:"+select);
        try (Connection conn = getDBConnection();
             PreparedStatement stmt = conn.prepareStatement(select);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int numColumns = metaData.getColumnCount();
            Map<String, JSONObject> toRet = new HashMap<>();
            while (rs.next()) {
                JSONObject currObj = new JSONObject();
                String currId = null;
                for (int i=1; i<=numColumns; ++i) {
                    String name = metaData.getColumnName(i);
                    logger.debug("column found:"+name);
                    if (excludedColumns == null || !excludedColumns.contains(name)) {
                        String typeName = metaData.getColumnTypeName(i);
                        FieldConfig config = new FieldConfig(typeName, "yes");
                        Object value = getValue(rs, config, i);
                        if (value!=null) {
                            currObj.put(name, value);
                        }
                    }
                    if (name.equals(ID_FIELD)) {
                        currId = rs.getString(name);
                    }
                }
                if (currId != null) {
                    toRet.put(currId, currObj);
                    logger.info("found data in DB for user with id:"+currId);
                } else {
                    logger.warn("getUserJSONsById: did not find id");
                }
            }
            return toRet;
        }
    }
    public Map<String,JSONObject> getUsersJSONs(List<String> upsIds, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        return getUsersJSONs(upsIds, "multiTablesQuery", excludedColumns);
    }

    public Map<String,JSONObject> getUsersJSONsByIds(List<String> ids, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        return getUsersJSONsById(ids, "multiTablesQuery", excludedColumns);
    }

    public Map<String,JSONObject> getDevUsersJSONs(List<String> upsIds, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        return getUsersJSONs(upsIds, "multiTablesQuery_dev", excludedColumns);
    }

    public Map<String,JSONObject> getDevUsersJSONsByIds(List<String> ids, List<String> excludedColumns) throws SQLException, ClassNotFoundException {
        return getUsersJSONsById(ids, "multiTablesQuery_dev", excludedColumns);
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
                float floatVal = rs.getFloat(idx);
                if (rs.wasNull()) {
                    return null;
                } else {
                    return floatVal;
                }
            case STRING:
                return rs.getString(idx);
            case DOUBLE:
                double doubleVal = rs.getDouble(idx);
                if (rs.wasNull()) {
                    return null;
                } else {
                    return doubleVal;
                }
            case BOOLEAN:
                boolean boolVal = rs.getBoolean(idx);
                if (rs.wasNull()) {
                    return null;
                } else {
                    return boolVal;
                }
            case INTEGER:
                int value = rs.getInt(idx);
                if (rs.wasNull()) {
                    return null;
                } else {
                    return value;
                }
            case ARRAY:
                return getJsonArray(idx, rs);
            case JSON:
                String objStr = rs.getString(idx);
                if (objStr != null && !objStr.isEmpty()) {
                    try {
                        return new JSONObject(objStr);
                    } catch (JSONException e) {
                        logger.info("could not convert to JSONObject, trying JSONArray");
                        return new JSONArray(objStr);
                    }
                } else {
                    return null;
                }
            default:
                throw new IllegalStateException("Unexpected value: " + fieldConfig.getType());
        }
    }

    private JSONArray getJsonArray(int idx, ResultSet source) throws SQLException {
        Array array = source.getArray(idx);
        if (array != null) {
            JSONArray jsonArr = new JSONArray();
            Object[] objsArr = (Object[]) array.getArray();
            for (Object obj : objsArr) {
                if (obj instanceof Timestamp) {
                    Timestamp ts = (Timestamp) obj;
                    jsonArr.put(ts.getTime());
                } else {
                    jsonArr.put(obj);
                }
            }
            return jsonArr;
        }
        return null;
    }
    public CachedRowSet getDBSchema() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        Feature dumper = AirlockManager.getAirlock().getFeature(AirlockConstants.db.GET_DB_SCHEMA);
        String select = dumper.getConfiguration().getString("queryWithPI");
        try (Connection conn = getDBConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(select)) {
            CachedRowSet rowSet = new CachedRowSetImpl();
            rowSet.populate(rs);
            return rowSet;
        }
    }

    public Map<String,JSONObject> getUsersForDelete(List<String> upsIds) throws SQLException, ClassNotFoundException {
        logger.info("getUsersIds() for ups_ids " + upsIds);
        return getUsersJSONs(upsIds, "usersForDelete", new ArrayList<>());
    }

    public Map<String,JSONObject> getDevUsersForDelete(List<String> upsIds) throws SQLException, ClassNotFoundException {
        logger.info("getUsersIds() for ups_ids " + upsIds);
        return getUsersJSONs(upsIds, "usersForDelete_dev", new ArrayList<>());
    }

    public Map<String,JSONObject> getUsersForDeleteById(List<String> ids) throws SQLException, ClassNotFoundException {
        logger.info("getUsersIds() for ids " + ids);
        return getUsersJSONsById(ids, "usersForDelete", new ArrayList<>());
    }

    public Map<String,JSONObject> getDevUsersForDeleteById(List<String> ids) throws SQLException, ClassNotFoundException {
        logger.info("getDevUsersIds() for ids " + ids);
        return getUsersJSONsById(ids, "usersForDelete_dev", new ArrayList<>());
    }

    public void deleteUsersByUps(List<String> upsIds, String product) throws SQLException, ClassNotFoundException {
        deleteUsersByUps(upsIds, product, "update_multiple","query_for_ids");
    }

    public void deleteDevUsersByUps(List<String> upsIds, String product) throws SQLException, ClassNotFoundException {
        deleteUsersByUps(upsIds, product, "update_multiple_dev","query_for_ids_dev");
    }

    private void deleteUsersByUps(List<String> upsIds, String product, String queryKey, String queryForIdsKey) throws SQLException, ClassNotFoundException {
        logger.info("deleteUser() for ups_ids " + upsIds);
        if (upsIds.isEmpty()) return;
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.delete.DELETE_USER);

        String updateArr = feature.getConfiguration().getString(queryKey);
        List<String> deletes = Arrays.asList(updateArr.split(";"));
        String queryTemplate = feature.getConfiguration().getString(queryForIdsKey);
        String arrStr = getStringsArrayString(upsIds);
        String query = String.format(queryTemplate, arrStr);
        List<PreparedStatement> deleteStatements = new ArrayList<>();

        try (Connection conn = getDBConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            List<String> ids = new ArrayList<>();
            while (rs.next()) {
                String id = rs.getString("id");
                logger.info("found user to delete with id:"+id);
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
            } catch (Exception e) {
                conn.rollback();
                DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_DELETE, DSR_REQUEST_RESULT_FAILED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(upsIds.size());
                throw e;
            }
            DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_DELETE, DSR_REQUEST_RESULT_EXECUTED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(ids.size());
            if (ids.size() < upsIds.size()) {
                DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_DELETE, DSR_REQUEST_RESULT_NOT_FOUND,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(upsIds.size()-ids.size());
            }

            logger.info("deleted users with ups_id "+upsIds);
            logger.info("deleted users with ids "+ids);
            logger.info("deleted "+ids.size()+" users out of "+upsIds.size()+" requests");
            String platform = getProductPlatform(product);
            deletedUsersCounter.labels(platform).inc(upsIds.size());
        } finally {
            if (deleteStatements != null) {
                for (PreparedStatement statement : deleteStatements) {
                    statement.close();
                }
            }
        }
    }

    public void deleteUsersByIds(List<String> ids, String product) throws SQLException, ClassNotFoundException {
        deleteUsersByIds(ids, product, "update_multiple");
    }

    public void deleteDevUsersByIds(List<String> ids, String product) throws SQLException, ClassNotFoundException {
        deleteUsersByIds(ids, product, "update_multiple_dev");
    }

    private void deleteUsersByIds(List<String> ids, String product, String queryKey) throws SQLException, ClassNotFoundException {
        logger.info("deleteUser() for ids " + ids);
        if (ids.isEmpty()) return;
        Feature feature = AirlockManager.getAirlock().getFeature(AirlockConstants.delete.DELETE_USER);

        String updateArr = feature.getConfiguration().getString(queryKey);
        List<String> deletes = Arrays.asList(updateArr.split(";"));
        List<PreparedStatement> deleteStatements = new ArrayList<>();

        try (Connection conn = getDBConnection()) {
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
            } catch (Exception e) {
                conn.rollback();
                DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_DELETE, DSR_REQUEST_RESULT_FAILED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(ids.size());
                throw e;
            }
            DSRConsumer.dsrRequestsProcessedCounter.labels(AirlyticsConsumerConstants.DSR_REQUEST_TYPE_DELETE, DSR_REQUEST_RESULT_EXECUTED,AirlockManager.getEnvVar(), AirlockManager.getProduct()).inc(ids.size());

            logger.info("deleted users with ids by id "+ids);
            String platform = getProductPlatform(product);
            deletedUsersCounter.labels(platform).inc(ids.size());
        } finally {
            if (deleteStatements != null) {
                for (PreparedStatement statement : deleteStatements) {
                    statement.close();
                }
            }
        }
    }
    private String getProductPlatform(String product) {
        return product.toLowerCase().contains("ios") ? "ios" : "android";
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
}