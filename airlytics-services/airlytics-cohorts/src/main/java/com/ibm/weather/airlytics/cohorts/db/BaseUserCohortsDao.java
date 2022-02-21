package com.ibm.weather.airlytics.cohorts.db;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

public class BaseUserCohortsDao {

    protected DataSource dataSource;

    protected JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    protected String getValidValue(String valueExpression) {
        return StringUtils.isBlank(valueExpression) ? "'true'" : valueExpression;
    }

    protected void appendAdditionalJoins(Map<String, String> joinColumns, List<String> joinedTables, StringBuilder sb) {

        if(CollectionUtils.isNotEmpty(joinedTables)) {
            joinedTables.forEach(t -> {
                sb.append(" inner join ").append(t)
                        .append(" on users.id = ")
                        .append(t).append('.').append(resolveJoin(joinColumns, t));
            });
        }
    }

    private String resolveJoin(Map<String, String> joinColumns, String table) {
        String result = null;

        if(joinColumns != null) {
            result = joinColumns.get(table);
        }
        return (result == null) ? "user_id" : result;
    }
}
