package com.ibm.weather.airlytics.dataimport.db;

import com.ibm.weather.airlytics.dataimport.dto.DataImportAirlockConfig;
import com.ibm.weather.airlytics.dataimport.dto.ImportJobDefinition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Repository
@Transactional
public class UserFeaturesDao {
    private static final Logger logger = LoggerFactory.getLogger(UserFeaturesDao.class);

    public static final String USER_ID_COLUMN = "user_id";
    public static final String FEATURE_TABLE_NAME_PREFIX = "user_features_";

    private DataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void postConstruct() {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public JdbcTemplate getJdbcTemplate() { return jdbcTemplate; }

    public long validateConnection() {
        return jdbcTemplate
                .queryForObject("select 1", Long.class);
    }

    public Map<String, String> getTableColumns(String schema, String tableName) {
        String sql = "SELECT column_name, udt_name, character_maximum_length FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
        final Map<String, String> result = new HashMap<>();

        jdbcTemplate.query(
                sql,
                new RowCallbackHandler() {
                    @Override
                    public void processRow(ResultSet rs) throws SQLException {
                        String dbType = rs.getString("udt_name");

                        if("varchar".equalsIgnoreCase(dbType)) {
                            int length = rs.getInt("character_maximum_length");
                            dbType = dbType + '(' + length + ')';
                        }
                        result.put(rs.getString("column_name").toLowerCase(), dbType);
                    }
                },
                schema, tableName);
        logger.info("Found the following columns in {}.{}: {}", schema, tableName, result.keySet());
        return result;
    }

    public int runImportUpsertJob(ImportJobDefinition job, DataImportAirlockConfig config) {
        String tableName = getTempTableName(job);
        createTempTable(job, tableName);
        tableImport(job, tableName, true);
        int result = countImportedRows(job, tableName, true);
        logger.info("Ingested {} rows. Now, running upsert...", result);
        int updated = upsert(job, tableName);
        logger.info("Finishing job {}: ingested {} rows, affected {}.", job.getJobId(), result, updated);
        dropTable(job, tableName, true);
        return result;
    }

    public int runImportReplaceJob(ImportJobDefinition job, DataImportAirlockConfig config) {
        String tableName = getReplacementTableName(job);

        if(config.isFeatureTable(job.getTargetTable())) {
            dropAllFeaturesView(job, config);
        }
        createReplacementTable(job, tableName);
        tableImport(job, tableName, false);
        int result = countImportedRows(job, tableName, false);
        logger.info("Ingested {} rows. Now, running replacement...", result);
        replaceTargetTable(job, tableName);
        grantPrivilegesOnTable(job, config, job.getTargetTable());

        if(config.isFeatureTable(job.getTargetTable())) {
            createAllFeaturesView(job, config);
            String viewName = config.isPiTable(job.getTargetTable()) ? "user_features_all_pi" : "user_features_all";
            grantPrivilegesOnTable(job, config, viewName);
        }
        logger.info("Finishing job {}: ingested {} rows.", job.getJobId(), result);
        return result;
    }

    private void createTempTable(ImportJobDefinition job, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TEMP TABLE ").append(tableName);
        sb.append(" AS SELECT * FROM ").append(job.getTargetSchema()).append('.').append(job.getTargetTable()).append(" LIMIT 0");

        /*// only create relevant columns
        sb.append('(');
        int idx = 0;

        for(String col : job.getColumns().keySet()) {
            sb.append(col).append(' ');
            sb.append(job.getColumns().get(col)).append(" null");

            if((++idx) < job.getColumns().size()) {
                sb.append(',');
            }
        }
        sb.append(')');*/
        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());
    }

    private void createReplacementTable(ImportJobDefinition job, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(job.getTargetSchema()).append('.').append(tableName);
        sb.append(" (LIKE ").append(job.getTargetSchema()).append('.').append(job.getTargetTable()).append(" INCLUDING ALL)");

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());
    }

    private void tableImport(ImportJobDefinition job, String tableName, boolean isTemp) {
        StringBuilder sb = new StringBuilder();

        if(job.getS3Uri() != null && StringUtils.isNoneBlank(job.getS3Uri().getS3Bucket(), job.getS3Uri().getS3FilePath())) {
            sb.append("SELECT aws_s3.table_import_from_s3(");
            sb.append("'");
            if(!isTemp) sb.append(job.getTargetSchema()).append('.');
            sb.append(tableName).append("',");

            // list relevant column names
            sb.append("'");
            appendColumnNamesList(job, sb, false, null);
            sb.append("',");
            sb.append("'DELIMITER '','' CSV");

            if(job.isWithHeader()) {
                sb.append(" HEADER");
            }
            sb.append("',");
            sb.append("aws_commons.create_s3_uri(");
            sb.append("'").append(job.getS3Uri().getS3Bucket()).append("',");
            sb.append("'").append(job.getS3Uri().getS3FilePath()).append("',");
            sb.append("'").append(job.getS3Uri().getS3Region()).append("')");
            sb.append(')');
        } else {
            // for unit-tests
            sb.append("copy ");
            if(!isTemp) sb.append(job.getTargetSchema()).append('.');
            sb.append(tableName);
            sb.append('(');
            appendColumnNamesList(job, sb, false, null);
            sb.append(") from '");
            sb.append(job.getFile());
            sb.append("' DELIMITER ',' CSV");

            if(job.isWithHeader()) {
                sb.append(" HEADER");
            }
        }
        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());
    }

    private int countImportedRows(ImportJobDefinition job, String tableName, boolean isTemp) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT count(*) FROM ");
        if(!isTemp) sb.append(job.getTargetSchema()).append('.');
        sb.append(tableName);

        logger.info(sb.toString());
        return jdbcTemplate.queryForObject(sb.toString(), Integer.class);
    }

    private int upsert(ImportJobDefinition job, String tableName) {
        boolean singleColumn = job.getColumns().size() == 2;// user_id + inserted column
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(job.getTargetSchema()).append('.').append(job.getTargetTable());
        sb.append('(');
        appendColumnNamesList(job, sb, false, null);
        sb.append(",shard");
        sb.append(')');
        sb.append(" SELECT ");
        appendColumnNamesList(job, sb, false, "t.");
        sb.append(", u.shard FROM ").append(tableName);
        sb.append(" t INNER JOIN ").append(job.getTargetSchema()).append('.').append("users u ON t.").append(USER_ID_COLUMN).append("=u.id");
        sb.append(" ON CONFLICT(").append(USER_ID_COLUMN).append(") DO UPDATE SET ");
        if(!singleColumn) sb.append('(');
        appendColumnNamesList(job, sb, true, null);
        if(!singleColumn) sb.append(')');
        sb.append(" = ");
        if(!singleColumn) sb.append('(');
        appendColumnNamesList(job, sb, true, "EXCLUDED.");
        if(!singleColumn) sb.append(')');

        logger.info(sb.toString());
        return jdbcTemplate.update(sb.toString());
    }

    private void replaceTargetTable(ImportJobDefinition job, String tableName) {
        String old = getReplacedTableName(job);

        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(job.getTargetSchema()).append('.').append(job.getTargetTable());
        sb.append(" RENAME TO ").append(old);

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());

        sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(job.getTargetSchema()).append('.').append(tableName);
        sb.append(" RENAME TO ").append(job.getTargetTable());

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());

        sb = new StringBuilder();
        sb.append("DROP TABLE ").append(job.getTargetSchema()).append('.').append(old);

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());
    }

    private void grantPrivilegesOnTable(ImportJobDefinition job, DataImportAirlockConfig config, String table) {
        StringBuilder sb = new StringBuilder();
        sb.append("GRANT SELECT, INSERT, UPDATE, DELETE ON ");
        sb.append(job.getTargetSchema()).append('.').append(table);
        sb.append(" TO airlytics_all_rw");

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());

        sb = new StringBuilder();
        sb.append("GRANT SELECT ON ");
        sb.append(job.getTargetSchema()).append('.').append(table);
        sb.append(" TO ");
        String roUser = config.isPiTable(job.getTargetTable()) ? "airlytics_all_ro_with_pi" : "airlytics_all_ro";
        sb.append(roUser);

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());

        sb = new StringBuilder();
        sb.append("GRANT SELECT ON ");
        sb.append(job.getTargetSchema()).append('.').append(table);
        sb.append(" TO airlytics_cohorts");

        logger.info(sb.toString());
        jdbcTemplate.execute(sb.toString());
    }

    private void dropTable(ImportJobDefinition job, String tableName, boolean isTemp) {
        jdbcTemplate.execute("DROP TABLE " + (isTemp ? "" : (job.getTargetSchema() + ".")) + tableName);
    }

    private String getTempTableName(ImportJobDefinition job) {
        return "tmp_" + job.getJobId().replace('-', '_');
    }

    private String getReplacementTableName(ImportJobDefinition job) {
        return job.getTargetTable() + "_" + job.getJobId().replace('-', '_');
    }

    private String getReplacedTableName(ImportJobDefinition job) {
        return "old_" + job.getTargetTable() + "_" + job.getJobId().replace('-', '_');
    }

    private void appendColumnNamesList(ImportJobDefinition job, StringBuilder sb, boolean skipFirst, String columnPrefix) {
        int idx = 0;

        for(String col : job.getColumns().keySet()) {
            idx++;

            if(skipFirst && idx == 1) {
                continue;
            }

            if(columnPrefix != null) {
                sb.append(columnPrefix);
            }
            sb.append(col);

            if(idx < job.getColumns().size()) {
                sb.append(',');
            }
        }
    }

    private static final List IGNORE_COLUMNS = Arrays.asList("user_id", "shard", "created_at");

    private void dropAllFeaturesView(ImportJobDefinition job, DataImportAirlockConfig config) {
        String viewName = config.isPiTable(job.getTargetTable()) ? "user_features_all_pi" : "user_features_all";
        jdbcTemplate.execute("DROP VIEW " + job.getTargetSchema() + "." + viewName);
    }

    private void createAllFeaturesView(ImportJobDefinition job, DataImportAirlockConfig config) {
        List<String> tableNames = config.isPiTable(job.getTargetTable()) ? config.getPiFeatureTables() : config.getFeatureTables();
        String viewName = config.isPiTable(job.getTargetTable()) ? "user_features_all_pi" : "user_features_all";
        StringBuilder sql = new StringBuilder();

        sql.append("CREATE VIEW ").append(job.getTargetSchema()).append('.').append(viewName).append(" AS (");
        sql.append("SELECT users.id, users.shard");

        Set<String> columns;

        for(String table : tableNames) {
            sql.append(", ");
            columns = getTableColumns(job.getTargetSchema(), table).keySet();
            columns.removeAll(IGNORE_COLUMNS);

            for(String column : columns) {
                sql.append(column).append(", ");
            }
            String tablePrefix = table;

            if(table.startsWith(FEATURE_TABLE_NAME_PREFIX)) {
                tablePrefix = tablePrefix.substring(FEATURE_TABLE_NAME_PREFIX.length());
            }
            sql.append(table).append(".created_at AS ").append(tablePrefix).append("_created_at");
        }
        sql.append(" FROM ").append(job.getTargetSchema()).append(".users ");

        for(String table : tableNames) {
            sql.append(" LEFT OUTER JOIN ");
            sql.append(job.getTargetSchema()).append(".").append(table);
            sql.append(" ON users.id = ");
            sql.append(table).append(".user_id");
        }

        sql.append(") ");
        logger.info("Creating {} view", viewName);

        jdbcTemplate.execute(sql.toString());
    }

}
