package com.ibm.weather.airlytics.cohorts.db;

import com.ibm.weather.airlytics.cohorts.dto.CohortConfig;
import com.ibm.weather.airlytics.cohorts.dto.UserCohort;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Repository
@Transactional
public class UserCohortsDao extends BaseUserCohortsDao {

    private static final Logger logger = LoggerFactory.getLogger(UserCohortsDao.class);

    @Autowired
    @Qualifier("datasourceRW")
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void postConstruct() {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public long validateConnection() {
        return jdbcTemplate
                .queryForObject("select 1", Long.class);
    }

    public long countAudience(String cohortId) {
        return jdbcTemplate
                .queryForObject("select count(*) from user_cohorts where cohort_id = ? and pending_deletion = ?", Long.class, cohortId, false);
    }

    public void addUserCohortIfNotExists(String userId, int shard, String cohortId, String value) {
        String sql =
                "insert into user_cohorts(user_id, shard, cohort_id, cohort_value) select ?, ?, ?, ? where not exists (" +
                "select 1 from user_cohorts where user_id = ? and cohort_id = ?)";
        jdbcTemplate.update(sql, userId, shard, cohortId, value, userId, cohortId);
    }

    public List<UserCohort> getAllUsersCohorts(List<String> cohortIds) {
        String sql = buildUsersCohortsQuery(cohortIds, false);
        Object[] params = new Object[cohortIds.size()];
        IntStream.range(0, cohortIds.size()).forEach(i -> params[i] = cohortIds.get(i));

        return jdbcTemplate.query(
                sql,
                (rs, rowNum) ->
                        new UserCohort(
                                rs.getString("user_id"),
                                rs.getString("ups_id"),
                                rs.getString("cohort_id"),
                                rs.getString("cohort_value"),
                                rs.getBoolean("pending_deletion"),
                                rs.getBoolean("pending_export")),
                params);

    }

    public int processAllUsersCohorts(List<String> cohortIds, final Consumer<UserCohort> callback) {
        String sql = buildUsersCohortsQuery(cohortIds, false);
        Object[] params = new Object[cohortIds.size()];
        IntStream.range(0, cohortIds.size()).forEach(i -> params[i] = cohortIds.get(i));
        final AtomicInteger cnt = new AtomicInteger(0);
        final Set<String> addedUpsIds = new HashSet<>();

        jdbcTemplate.query(
                sql,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        String upsId = rs.getString("ups_id");

                        if(upsId == null || !addedUpsIds.contains(upsId)) {
                            cnt.incrementAndGet();
                            callback.accept(
                                    new UserCohort(
                                            rs.getString("user_id"),
                                            upsId,
                                            rs.getString("cohort_id"),
                                            rs.getString("cohort_value"),
                                            rs.getBoolean("pending_deletion"),
                                            rs.getBoolean("pending_export")));
                            addedUpsIds.add(upsId);
                        }
                    }
                },
                params);
        return cnt.get();
    }

    public List<UserCohort> getUsersCohortsForExport(List<String> cohortIds) {
        String sql = buildUsersCohortsQuery(cohortIds, true);
        Object[] params = new Object[cohortIds.size()];
        IntStream.range(0, cohortIds.size()).forEach(i -> params[i] = cohortIds.get(i));

        return jdbcTemplate.query(
                sql,
                (rs, rowNum) ->
                        new UserCohort(
                                rs.getString("user_id"),
                                rs.getString("ups_id"),
                                rs.getString("cohort_id"),
                                rs.getString("cohort_value"),
                                rs.getBoolean("pending_deletion"),
                                rs.getBoolean("pending_export")),
                params);

    }

    public int processUsersCohortsForExport(List<String> cohortIds, final Consumer<UserCohort> callback) {
        String sql = buildUsersCohortsQuery(cohortIds, true);
        Object[] params = new Object[cohortIds.size()];
        IntStream.range(0, cohortIds.size()).forEach(i -> params[i] = cohortIds.get(i));
        final AtomicInteger cnt = new AtomicInteger(0);
        final Set<String> addedUpsIds = new HashSet<>();

        jdbcTemplate.query(
                sql,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        String upsId = rs.getString("ups_id");

                        if(upsId == null || !addedUpsIds.contains(upsId)) {
                            cnt.incrementAndGet();
                            callback.accept(
                                    new UserCohort(
                                            rs.getString("user_id"),
                                            upsId,
                                            rs.getString("cohort_id"),
                                            rs.getString("cohort_value"),
                                            rs.getBoolean("pending_deletion"),
                                            rs.getBoolean("pending_export")));
                            addedUpsIds.add(upsId);
                        }
                    }
                },
                params);
        return cnt.get();
    }

    public void markCohortsExported(List<String> cohortIds) {
        String sql = buildMarkCohortsExportedQuery(cohortIds);
        Object[] params = new Object[cohortIds.size()];
        IntStream.range(0, cohortIds.size()).forEach(i -> params[i] = cohortIds.get(i));
        jdbcTemplate.update(sql, params);
    }

    public void runCohortCalculation(CohortConfig cohortConfig, List<String> joinedTables, Map<String, String> joinColumns, boolean withUps) {
        int updated = deleteCohortPendingDeletion(cohortConfig.getUniqueId(), true);
        logger.info("{} previously deleted users deleted from cohort {}", updated, cohortConfig.getUniqueId());
        updated = createTempTableForCohortCondition(joinedTables, joinColumns, cohortConfig.getQueryCondition(), cohortConfig.getQueryAdditionalValue(), cohortConfig.getName(), withUps);
        logger.info("{} users found in cohort {}", updated, cohortConfig.getUniqueId());
        updated = createTempTableForDeleted(cohortConfig.getUniqueId(), cohortConfig.getName());
        logger.info("{} users leaving cohort {}", updated, cohortConfig.getUniqueId());
        updated = updateExpiredUserCohorts(cohortConfig.getUniqueId(), cohortConfig.getName());
        logger.info("{} users marked as deleted in cohort {}", updated, cohortConfig.getUniqueId());
        updated = deleteChangedUserCohorts(cohortConfig.getName());
        logger.info("{} users have new value in cohort {}", updated, cohortConfig.getUniqueId());
        updated = insertNewUserCohorts(cohortConfig.getUniqueId(), cohortConfig.getName());
        logger.info("{} users added to cohort {}", updated, cohortConfig.getUniqueId());
        dropTempTableForDeleted(cohortConfig.getName());
        dropTempTable(cohortConfig.getName());
        long cnt = countAudience(cohortConfig.getUniqueId());
        logger.info("{} users found in cohort {}", cnt, cohortConfig.getUniqueId());
        cohortConfig.setUsersNumber(cnt);
    }

    public void markCohortForDeletion(String cohortId) {
        String sql = "update user_cohorts set pending_deletion = ? where cohort_id = ?";
        jdbcTemplate.update(sql, true, cohortId);
    }

    public int deleteAllCohort(String cohortId) {
        String sql = "delete from user_cohorts where cohort_id = ?";
        return jdbcTemplate.update(sql, cohortId);
    }

    public Optional<Instant> getLastCalculationTime(String cohortId) {
        String sql = "select max(created_at) from user_cohorts where cohort_id = ?";
        Timestamp ts = jdbcTemplate.queryForObject(sql, Timestamp.class, cohortId);

        return (ts == null) ? Optional.empty() : Optional.of(ts.toInstant());
    }

    private int createTempTableForCohortCondition(
            List<String> joinedTables,
            Map<String, String> joinColumns,
            String cohortCondition,
            String valueExpression,
            String cohortName,
            boolean withUps) {
        String value = getValidValue(valueExpression);
        String tempTableName = getTempTableName(cohortName);
        StringBuilder sb = new StringBuilder();
        sb.append("create temp table ").append(tempTableName).append(" as ");
        sb.append("select users.id, users.shard");
        if(withUps) sb.append(", users_pi.ups_id");
        else sb.append(", null as ups_id");
        sb.append(", cast(").append(value).append(" as varchar) as cohort_value");
        sb.append(" from users");
        if(withUps) sb.append(" inner join users_pi on users.id = users_pi.id");
        appendAdditionalJoins(joinColumns, joinedTables, sb);
        sb.append(" where");
        if(withUps) sb.append(" users_pi.ups_id is not null and");
        sb.append(" (").append(cohortCondition).append(")");

        String sql = sb.toString();
        logger.info("Running {}", sql);
        jdbcTemplate.execute(sql);

        sql = "create index if not exists " + tempTableName + "tmp__idx1 on " + tempTableName + "(id)";
        jdbcTemplate.execute(sql);

        sql = "create index if not exists " + tempTableName + "tmp_idx2 on " + tempTableName + "(cohort_value)";
        jdbcTemplate.execute(sql);

        Long cnt = jdbcTemplate.queryForObject("select count(*) from " + tempTableName, Long.class);

        return cnt.intValue();
    }

    private void dropTempTable(String cohortName) {
        String tempTableName = getTempTableName(cohortName);
        String sql = "drop index " + tempTableName + "tmp__idx1";
        jdbcTemplate.execute(sql);

        sql = "drop index " + tempTableName + "tmp_idx2";
        jdbcTemplate.execute(sql);

        sql = "drop table " + tempTableName;
        jdbcTemplate.execute(sql);
    }

    private int createTempTableForDeleted(String cohortId, String cohortName) {
        String tempTable = getTempTableName(cohortName);
        String tempTableForDeleted = tempTable + "_deleted";

        StringBuilder sb = new StringBuilder();
        sb.append("create temp table ").append(tempTableForDeleted).append(" as");
        sb.append(" select uc.user_id");
        sb.append(" from user_cohorts as uc");
        sb.append(" left join ").append(tempTable).append(" as updated");
        sb.append(" on uc.user_id = updated.id");
        sb.append(" where updated.id is null");
        sb.append(" and uc.cohort_id = '").append(cohortId).append("'");

        String sql = sb.toString();
        logger.info("Running {}", sql);
        jdbcTemplate.execute(sql);

        Long cnt = jdbcTemplate.queryForObject("select count(*) from " + tempTableForDeleted, Long.class);

        return cnt.intValue();
    }

    private void dropTempTableForDeleted(String cohortName) {
        String tempTableName = getTempTableName(cohortName) + "_deleted";
        String sql = "drop table " + tempTableName;
        jdbcTemplate.execute(sql);
    }

    private int updateExpiredUserCohorts(String cohortId, String cohortName) {
        String tempTable = getTempTableName(cohortName);
        String tempTableForDeleted = tempTable + "_deleted";
        StringBuilder sb = new StringBuilder();
        sb.append("update user_cohorts uc set pending_deletion = true");
        sb.append(" from ").append(tempTableForDeleted).append(" ttfd");
        sb.append(" where uc.user_id = ttfd.user_id");
        sb.append(" and uc.cohort_id = '").append(cohortId).append("'");

        String sql = sb.toString();
        logger.info("Running {}", sql);

        return jdbcTemplate.update(sql);
    }

    private int insertNewUserCohorts(String cohortId, String cohortName) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into user_cohorts(user_id, shard, ups_id, cohort_id, cohort_name, cohort_value)");
        sb.append(" select updated.id, updated.shard, updated.ups_id,");
        sb.append(" '").append(cohortId).append("',");
        sb.append(" '").append(cohortName).append("',");
        sb.append(" updated.cohort_value");
        sb.append(" from ");
        sb.append(getTempTableName(cohortName)).append(" as updated");
        sb.append(" left join user_cohorts as uc");
        sb.append(" on updated.id = uc.user_id");
        sb.append(" and uc.cohort_id = '").append(cohortId).append("'");
        sb.append(" where uc.user_id is null");
        sb.append(" and updated.cohort_value is not null");

        String sql = sb.toString();
        logger.info("Running {}", sql);

        return jdbcTemplate.update(sql);
    }

    private int deleteChangedUserCohorts(String cohortName) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete from user_cohorts uc");
        sb.append(" using ").append(getTempTableName(cohortName)).append(" updated");
        sb.append(" where uc.user_id = updated.id");
        sb.append(" and uc.cohort_value != updated.cohort_value");

        String sql = sb.toString();
        logger.info("Running {}", sql);

        return jdbcTemplate.update(sql);
    }

    private int deleteCohortPendingDeletion(String cohortId, boolean isPendingDeletion) {
        String sql = "delete from user_cohorts where cohort_id = ? and pending_deletion = ?";
        logger.info("Running {}", sql);
        return jdbcTemplate.update(sql, cohortId, isPendingDeletion);
    }

    private String getTempTableName(String cohortName) {
        String namepart = cohortName.toLowerCase().replaceAll("\\W", "");

        if(namepart.length() > 40) {
            namepart = namepart.substring(0, 40);
        }
        return "tmp_" + namepart;
    }

    private String buildUsersCohortsQuery(List<String> cohortIds, boolean isDelta) {
        StringBuilder sb = new StringBuilder();
        sb.append("select user_id, ups_id, cohort_id, cohort_value, pending_deletion, pending_export from user_cohorts");
        sb.append(" where cohort_id in (");

        for(int i = 0; i < cohortIds.size(); i++) {

            if(i < (cohortIds.size() - 1)) {
                sb.append("?,");
            } else {
                sb.append("?)");
            }
        }

        if(isDelta) {
            sb.append(" and (pending_deletion = true or pending_export = true)");
        }
        return sb.toString();
    }

    private String buildMarkCohortsExportedQuery(List<String> cohortIds) {
        StringBuilder sb = new StringBuilder();
        sb.append("update user_cohorts set pending_export = false");
        sb.append(" where pending_export = true and cohort_id in (");

        for(int i = 0; i < cohortIds.size(); i++) {

            if(i < (cohortIds.size() - 1)) {
                sb.append("?,");
            } else {
                sb.append("?)");
            }
        }
        return sb.toString();
    }
}
