package com.ibm.weather.airlytics.braze.db;

import com.ibm.weather.airlytics.braze.dto.InstallOrPurchase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.function.Consumer;

@Repository
public class BrazeExportDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrazeExportDao.class);

    protected DataSource dataSource;

    protected JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    @Value("${shards.pause.after:20}")
    private int shardsToPause;

    @Value("${shards.pause.ms:90000}")
    private long pauseMs;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void postConstruct() {
        LOGGER.info(dataSource.toString());
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public long validateConnection() {
        return jdbcTemplate.queryForObject("select 1", Long.class);
    }

    public void sendInstallsAndTrials(final String platform, final int shard, final Consumer<InstallOrPurchase> consumer) {
        LOGGER.info("Selecting installs and trials for shard {}", shard);
        String sql =
                "select id, install_date, premium_trial_id, premium_trial_start, premium_trial_length" +
                        " from users" +
                        " where install_date is not null and platform = ? and status = ? and shard = ?" +
                        " order by install_date";

        try {

            jdbcTemplate.query(
                    sql,
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            String userId = rs.getString("id");
                            Timestamp ts = rs.getTimestamp("install_date");
                            Instant installDate = ts != null ? ts.toInstant() : null;
                            String product = rs.getString("premium_trial_id");
                            ts = rs.getTimestamp("premium_trial_start");
                            Instant start = ts != null ? ts.toInstant() : null;
                            int length = rs.getInt("premium_trial_length");
                            consumer.accept(new InstallOrPurchase(userId, installDate, product, (product != null), start, length));
                        }
                    },
                    platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Braze history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendInstallsAndTrials(platform, shard, consumer);
        }
        waitIfNeeded(shard);
    }

    public void sendPurchases(final String platform, final int shard, final Consumer<InstallOrPurchase> consumer) {
        LOGGER.info("Selecting subscriptions for shard {}", shard);
        String sql =
                "SELECT DISTINCT ON (pu.user_id) user_id, p.product, p.start_date, p.expiration_date" +
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
                            String userId = rs.getString("user_id");
                            String product = rs.getString("product");
                            Timestamp ts = rs.getTimestamp("start_date");
                            Instant start = ts != null ? ts.toInstant() : null;
                            ts = rs.getTimestamp("expiration_date");
                            Instant end = ts != null ? ts.toInstant() : null;
                            consumer.accept(new InstallOrPurchase(userId, product, start, end));
                        }
                    },
                    false, platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Braze history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendPurchases(platform, shard, consumer);
        }
        waitIfNeeded(shard);
    }

    public void sendPremiumAndAutorenewal(final String platform, final int shard, final Consumer<InstallOrPurchase> consumer) {
        LOGGER.info("Selecting premium and auto-renewal for shard {}", shard);
        String sql =
                "SELECT DISTINCT ON (pu.user_id) user_id, p.active, p.auto_renew_status, p.auto_renew_status_change_date" +
                        " FROM users u INNER JOIN purchases_users pu ON u.id = pu.user_id" +
                        " INNER JOIN purchases p ON pu.purchase_id = p.id" +
                        " WHERE p.platform = ? AND u.status = ? AND pu.shard = ?" +
                        " ORDER BY pu.user_id, p.last_payment_action_date desc";

        try {
            jdbcTemplate.query(
                    sql,
                    new RowCallbackHandler() {

                        @Override
                        public void processRow(ResultSet rs) throws SQLException {
                            String userId = rs.getString("user_id");
                            boolean premium = rs.getBoolean("active");
                            boolean autoRenew = rs.getBoolean("auto_renew_status");
                            Timestamp ts = rs.getTimestamp("auto_renew_status_change_date");
                            Instant autoRenewDate = ts != null ? ts.toInstant() : null;
                            consumer.accept(new InstallOrPurchase(userId, premium, autoRenew, autoRenewDate));
                        }
                    },
                    platform, "ACTIVE", shard);
        } catch(Exception dae) {// DataAccessException
            LOGGER.warn("Error uploading Braze history, re-trying...", dae);
            LOGGER.info("Giving DB a break. Sleep for {}ms", pauseMs*2L);

            try {
                Thread.sleep(pauseMs*2L);
            } catch (InterruptedException e) {
            }
            sendPremiumAndAutorenewal(platform, shard, consumer);
        }
        waitIfNeeded(shard);
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
