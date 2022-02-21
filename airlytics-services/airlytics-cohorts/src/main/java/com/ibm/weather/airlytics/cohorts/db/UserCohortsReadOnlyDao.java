package com.ibm.weather.airlytics.cohorts.db;

import com.ibm.weather.airlytics.cohorts.dto.CohortConfig;
import com.ibm.weather.airlytics.cohorts.dto.UserCohort;
import org.apache.commons.collections4.CollectionUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

@Repository
public class UserCohortsReadOnlyDao extends BaseUserCohortsDao {

    private static final Logger logger = LoggerFactory.getLogger(UserCohortsReadOnlyDao.class);

    private static final String VALIDATED_COHORT = "validate";

    @Autowired
    @Qualifier("datasourceRO")
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void postConstruct() {
        logger.info(dataSource.toString());
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public long validateConnection() {
        return jdbcTemplate
                .queryForObject("select 1", Long.class);
    }

    public long countAllAudiences() {
        return jdbcTemplate
                .queryForObject("select count(*) from user_cohorts", Long.class);
    }

    public Map<String, String> getTableColumnNames(String schema, String tableName) {
        String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
        final Map<String, String> result = new HashMap<>();

        jdbcTemplate.query(
                sql,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        result.put(rs.getString("column_name"), rs.getString("data_type"));
                    }
                },
                schema, tableName);
        return result;
    }

    public long validateCondition(
            List<String> joinedTables,
            Map<String, String> joinColumns,
            String cohortCondition,
            String valueExpression,
            boolean withUps,
            boolean limit) {
        return (long)validateCohortCondition(joinedTables, joinColumns, cohortCondition, valueExpression, VALIDATED_COHORT, withUps, limit);
    }

    public String buildCohortValidationQuery(
            List<String> joinedTables,
            Map<String, String> joinColumns,
            String cohortCondition,
            String valueExpression,
            boolean withUps) {
        String value = getValidValue(valueExpression);
        StringBuilder sb = new StringBuilder();
        sb.append("select users.id");
        if(withUps) sb.append(", users_pi.ups_id");
        sb.append(", ").append(value).append(" as cohort_value");
        sb.append(" from users");
        if(withUps) sb.append(" inner join users_pi on users.id = users_pi.id");
        appendAdditionalJoins(joinColumns, joinedTables, sb);
        sb.append(" where");
        if(withUps) sb.append(" users_pi.ups_id is not null and");
        sb.append(" (").append(cohortCondition).append(")");

        return sb.toString();
    }

    private int validateCohortCondition(
            List<String> joinedTables,
            Map<String, String> joinColumns,
            String cohortCondition,
            String valueExpression,
            String cohortName,
            boolean withUps,
            boolean limit) {
        String value = getValidValue(valueExpression);
        StringBuilder sb = new StringBuilder();
        sb.append("select users.id");
        if(withUps) sb.append(", users_pi.ups_id");
        sb.append(", cast(").append(value).append(" as varchar) as cohort_value");
        sb.append(" from users");
        if(withUps) sb.append(" inner join users_pi on users.id = users_pi.id");
        appendAdditionalJoins(joinColumns, joinedTables, sb);
        sb.append(" where");
        if(withUps) sb.append(" users_pi.ups_id is not null and");
        sb.append(" (").append(cohortCondition).append(')');

        if(limit) {
            sb.append(" limit 1");
        }

        final AtomicInteger cnt = new AtomicInteger(0);

        jdbcTemplate.query(
                sb.toString(),
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        if(limit || rs.getString("cohort_value") != null) {
                            cnt.incrementAndGet();
                        }
                    }
                });
        return cnt.get();
    }
}
