package com.ibm.weather.airlytics.amplitude.db;

import com.ibm.weather.airlytics.amplitude.dto.AmplitudeEvent;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.jdbc.PgArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.util.ResourceUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Repository
public class AmplitudeExportDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(AmplitudeExportDao.class);

    protected DataSource dataSource;

    protected JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    @Value("${shards.pause.after:20}")
    private int shardsToPause;

    @Value("${shards.pause.ms:90000}")
    private long pauseMs;

    @Value("classpath:mappings.csv")
    private Resource mappingsResource;

    // column name to attribute name
    private Map<String, String> userMappings;
    private Map<String, String> purchaseMappings;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void postConstruct() {
        LOGGER.info(dataSource.toString());
        jdbcTemplate = new JdbcTemplate(dataSource);

        try {
            userMappings = parseCsv("users");
            purchaseMappings = parseCsv("purchases");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long validateConnection() {
        return jdbcTemplate.queryForObject("select 1", Long.class);
    }

    public void sendUserData(final String platform, final int shard, final Consumer<AmplitudeEvent> consumer) {
        LOGGER.info("Selecting users for shard {}", shard);
        String sql = "select * from users where platform = ? and status = ? and shard = ?";

        try {

            jdbcTemplate.query(
                    sql,
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            Map<String, String> mappings = userMappings;
                            String userId = rs.getString("id");
                            Timestamp lastSession = rs.getTimestamp("last_session");

                            AmplitudeEvent result = buildAmplitudeEvent(rs, mappings, userId, lastSession, platform);
                            consumer.accept(result);
                        }
                    },
                    platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Amplitude history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendUserData(platform, shard, consumer);
        }
        waitIfNeeded(shard);
    }

    public void sendPurchases(final String platform, final int shard, final Consumer<AmplitudeEvent> consumer) {
        LOGGER.info("Selecting subscriptions for shard {}", shard);
        String sql =
                "SELECT DISTINCT ON (pu.user_id) user_id, p.product, p.start_date, p.expiration_date, p.active, u.app_version, u.os_version, u.device_model, u.device_language" +
                        " FROM users u INNER JOIN purchases_users pu ON u.id = pu.user_id" +
                        " INNER JOIN purchases p ON pu.purchase_id = p.id" +
                        " WHERE p.trial = ? and p.platform = ? AND u.status = ? AND pu.shard = ?" +
                        " ORDER BY pu.user_id, p.last_payment_action_date desc";

        try {
            jdbcTemplate.query(
                    sql,
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            Map<String, String> mappings = purchaseMappings;
                            String userId = rs.getString("user_id");
                            Timestamp lastSession = rs.getTimestamp("start_date");

                            AmplitudeEvent result = buildAmplitudeEvent(rs, mappings, userId, lastSession, platform);
                            consumer.accept(result);
                        }
                    },
                    false, platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Amplitude history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendPurchases(platform, shard, consumer);
        }
        waitIfNeeded(shard);
    }

    public void sendDummyEvents(final String platform, final int shard, final Consumer<AmplitudeEvent> consumer) {
        LOGGER.info("Selecting users for events for shard {}", shard);
        String sql = "select id, app_version, os_version, device_model, device_language from users where platform = ? and status = ? and shard = ?";

        try {

            jdbcTemplate.query(
                    sql,
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            Map<String, String> mappings = userMappings;
                            String userId = rs.getString("id");

                            AmplitudeEvent result = new AmplitudeEvent();
                            result.setEvent_type(AmplitudeEvent.AMP_TYPE_DUMMY);
                            result.setUser_id(userId);
                            result.setDevice_id(userId);
                            result.setTime(Instant.now().toEpochMilli());
                            result.setSession_id(-1L);

                            if("ios".equalsIgnoreCase(platform)) result.setPlatform("iOS");
                            else if("android".equalsIgnoreCase(platform)) result.setPlatform("Android");

                            result.setApp_version(rs.getString("app_version"));
                            result.setOs_version(rs.getString("os_version"));
                            result.setDevice_model(rs.getString("device_model"));
                            result.setLanguage(rs.getString("device_language"));

                            result.setEvent_properties(Collections.emptyMap());

                            consumer.accept(result);
                        }
                    },
                    platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Amplitude history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendUserData(platform, shard, consumer);
        }
        waitIfNeeded(shard);
    }

    private Map<String, String> parseCsv(final String table) throws IOException {

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(mappingsResource.getInputStream()))) {
            return reader.lines()
                    .map(l -> l.split("\\,"))
                    .filter(s -> s[1].startsWith(table + "."))
                    .map(s -> {
                        return new String[]{s[1].substring(s[1].indexOf('.') + 1), s[0]};
                    })
                    .collect(Collectors.toMap(s -> s[0], s -> s[1]));

        }
    }

    public void sendCountryLanguage(final String platform, final List<String> userIds, final Consumer<AmplitudeEvent> consumer) {
        LOGGER.info("Selecting country and language for batch");
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT id, device_country, device_language FROM users");
        sql.append(" WHERE id IN (");

        for(int i = 0; i < userIds.size(); i++) {
            sql.append("'").append(userIds.get(i)).append("'");

            if(i < (userIds.size() - 1)) {
                sql.append(',');
            }
        }
        sql.append(')');

        try {
            jdbcTemplate.query(
                    sql.toString(),
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            String userId = rs.getString("id");
                            String country = rs.getString("device_country");
                            String language = rs.getString("device_language");

                            if(!StringUtils.isAllBlank(country, language)) {
                                AmplitudeEvent result = buildAmplitudeEventForCountryLanguage(userId, country, language, platform);
                                consumer.accept(result);
                            }
                        }
                    });
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Amplitude history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendCountryLanguage(platform, userIds, consumer);
        }
    }

    private AmplitudeEvent buildAmplitudeEvent(ResultSet rs, Map<String, String> mappings, String userId, Timestamp lastSession, String platform) throws SQLException {
        AmplitudeEvent result = new AmplitudeEvent();
        result.setEvent_type(AmplitudeEvent.AMP_TYPE_IDENTIFY);
        result.setUser_id(userId);
        result.setDevice_id(userId);

        if(lastSession != null) {
            result.setTime(lastSession.getTime());
        }
        result.setSession_id(-1L);

        if("ios".equalsIgnoreCase(platform)) result.setPlatform("iOS");
        else if("android".equalsIgnoreCase(platform)) result.setPlatform("Android");

        result.setApp_version(rs.getString("app_version"));
        result.setOs_version(rs.getString("os_version"));
        result.setDevice_model(rs.getString("device_model"));
        result.setLanguage(rs.getString("device_language"));
        Map<String, Object> userProperties = new HashMap<>();
        result.setUser_properties(userProperties);

        for(String col : mappings.keySet()) {
            Object val = rs.getObject(col);

            if(val != null) {

                if(val instanceof Timestamp) {
                    Timestamp ts = (Timestamp) val;
                    val = ts.toInstant().toString();
                } else if(val instanceof Boolean) {
                    val = val.toString();
                } else if(val instanceof PgArray) {
                    PgArray pga = (PgArray)val;
                    List<String> lst = Arrays.asList((String[]) pga.getArray());

                    if(!lst.isEmpty()) {
                        val = lst;
                    } else {
                        val = null;
                    }
                } else if(val instanceof Number || val instanceof String) {
                    val = val;
                } else {
                    val = val.toString();
                }
            }
            userProperties.put(mappings.get(col), val);
        }
        return result;
    }

    private AmplitudeEvent buildAmplitudeEventForCountryLanguage(String userId, String country, String language, String platform) throws SQLException {
        AmplitudeEvent result = new AmplitudeEvent();
        result.setEvent_type(AmplitudeEvent.AMP_TYPE_DUMMY);
        result.setUser_id(userId);
        result.setDevice_id(userId);
        result.setSession_id(-1L);
        result.setTime(Instant.now().toEpochMilli());

        if ("ios".equalsIgnoreCase(platform)) result.setPlatform("iOS");
        else if ("android".equalsIgnoreCase(platform)) result.setPlatform("Android");

        result.setCountry(country);
        result.setLanguage(language);

        result.setUser_properties(Collections.emptyMap());
        result.setEvent_properties(Collections.emptyMap());
        return result;
    }

    private void waitIfNeeded(int shard) {
        if (shardsToPause > 0 && pauseMs > 0L && ((shard + 1) % shardsToPause) == 0) {
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs);
            try {
                Thread.sleep(pauseMs);
            } catch (InterruptedException e) {
            }
        }
    }
}
