package com.ibm.weather.airlytics.cohorts.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.ibm.weather.airlytics.cohorts.AbstractCohortsUnitTest;
import com.ibm.weather.airlytics.cohorts.dto.*;
import com.ibm.weather.airlytics.cohorts.integrations.TestAirlockCohortsClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.BadSqlGrammarException;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CohortCalculationServiceTestCohorts extends AbstractCohortsUnitTest {

    final private ObjectMapper mapper = new ObjectMapper();

    private List<String> premiumApp1Users = new LinkedList<>();

    private CohortCalculationService service;

    private ScheduledCohortsService scheduledService;

    private TestAirlockCohortsClient airlockClient;

    private AirCohortsConfigService featureConfigService = new TestAirCohortsConfigService();

    private static final String insertUserSql =
            "insert into users" +
                    "(id,shard,premium,application,app_version,premium_expiration,experiment,sessions_30d,in_app_message_displayed_30d,last_session,device_model)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?)";

    private static final String insertUserPiSql = "insert into users_pi(id,ups_id) values (?,?)";

    private static final String insertUserFeaturesSql =
            "insert into user_features(user_id,shard,test_feature_number,test_feature_list,test_feature_json)" +
                    " values (?,?,?,?,?)";

    @BeforeEach
    public void setup() throws Exception {
        cleanup();

        String userId = UUID.randomUUID().toString();
        String upsId = UUID.randomUUID().toString();
        boolean premium = false;
        String app = TestAirlockCohortsClient.DB_APP_NAME;
        String appV = "1.0";
        Instant exp = Instant.now().plus(20, ChronoUnit.DAYS);
        dao.getJdbcTemplate().update(insertUserSql, userId, 1, premium, app, appV, new Timestamp(exp.toEpochMilli()),
                "experiment 1",30,11, new Timestamp(System.currentTimeMillis()), "001_1213124");
        dao.getJdbcTemplate().update(insertUserPiSql, userId, upsId);

        Connection conn = dao.getJdbcTemplate().getDataSource().getConnection();
        Array array = conn.createArrayOf("varchar", Arrays.asList("PA", "GE").toArray());
        conn.close();
        dao.getJdbcTemplate().update(insertUserFeaturesSql, userId, 10, 1, array, null);

        userId = UUID.randomUUID().toString();
        upsId = UUID.randomUUID().toString();
        premium = true;
        exp = Instant.now().plus(30, ChronoUnit.DAYS);
        dao.getJdbcTemplate().update(insertUserSql, userId, 2, premium, app, appV, new Timestamp(exp.toEpochMilli()),
                null,30,12, new Timestamp(System.currentTimeMillis()), "002_1213124");
        dao.getJdbcTemplate().update(insertUserPiSql, userId, upsId);
        premiumApp1Users.add(userId);
        PGobject json = new PGobject();
        json.setType("jsonb");
        json.setValue(mapper.writeValueAsString(Collections.singletonMap("NY", 4)));
        dao.getJdbcTemplate().update(insertUserFeaturesSql, userId, 10, 2, array, json);

        airlockClient = new TestAirlockCohortsClient();
        service = new CohortCalculationService(airlockClient, dao, rodao, featureConfigService, "=users");
        service.setAirlyticsVarsForTest("TEST", "TEST");
    }

    @Test
    public void testCohortCalculation() throws Exception {
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(1);

        airlockClient.setCondition("premium_expiration < (CURRENT_DATE + INTERVAL '30 day')");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        Boolean deleted = dao.getJdbcTemplate().queryForObject("select pending_deletion from user_cohorts where user_id = ?", Boolean.class, premiumApp1Users.get(0));
        assertThat(deleted).isNotNull();
        assertThat(deleted).isEqualTo(true);

        airlockClient.setCondition("app_version like '1%'");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(2);

        airlockClient.setCondition("app_version = '1.0'");
        service.runCohortCalculation("unittest_cohort_2");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_2")).isEqualTo(2);

        List<UserCohort> ucList = dao.getAllUsersCohorts(Arrays.asList("unittest_cohort_1", "unittest_cohort_2"));
        Map<String, Map<String, UserCohort>> cohortIdsToCohortsPerUser =
                ucList.stream().collect(
                        Collectors.groupingBy(UserCohort::getUserId, Collectors.toMap(UserCohort::getCohortId, Function.identity())));
        for(String uid : cohortIdsToCohortsPerUser.keySet()) {
            Map<String, UserCohort> map = cohortIdsToCohortsPerUser.get(uid);
            StringBuilder sb = new StringBuilder();

            for(String cid : map.keySet()) {
                sb.append(cid).append(':').append(map.get(cid).isPendingDeletion()).append('\t');
            }
            System.out.println(uid + "\t" + sb.toString());
        }

    }

    @Test
    public void testCohortCalculationUps() throws Exception {
        airlockClient.setExportType(CohortExportConfig.UPS_EXPORT);
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(1);

        airlockClient.setCondition("premium_expiration < (CURRENT_DATE + INTERVAL '30 day')");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        Boolean deleted = dao.getJdbcTemplate().queryForObject("select pending_deletion from user_cohorts where user_id = ?", Boolean.class, premiumApp1Users.get(0));
        assertThat(deleted).isNotNull();
        assertThat(deleted).isEqualTo(true);

        airlockClient.setCondition("app_version like '1%'");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(2);

        airlockClient.setCondition("app_version = '1.0'");
        service.runCohortCalculation("unittest_cohort_2");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_2")).isEqualTo(2);

        List<UserCohort> ucList = dao.getAllUsersCohorts(Arrays.asList("unittest_cohort_1", "unittest_cohort_2"));
        Map<String, Map<String, UserCohort>> cohortIdsToCohortsPerUser =
                ucList.stream().collect(
                        Collectors.groupingBy(UserCohort::getUserId, Collectors.toMap(UserCohort::getCohortId, Function.identity())));
        for(String uid : cohortIdsToCohortsPerUser.keySet()) {
            Map<String, UserCohort> map = cohortIdsToCohortsPerUser.get(uid);
            StringBuilder sb = new StringBuilder();

            for(String cid : map.keySet()) {
                sb.append(cid).append(':').append(map.get(cid).isPendingDeletion()).append('\t');
            }
            System.out.println(uid + "\t" + sb.toString());
        }

    }

    @Test
    public void testCohortDeltaCalculation() throws Exception {
        boolean premium = true;
        String app = TestAirlockCohortsClient.DB_APP_NAME;
        String appV = "1.0";
        Instant exp = Instant.now().plus(40, ChronoUnit.DAYS);

        // existing that did not change
        String userId = UUID.randomUUID().toString();
        String upsId = UUID.randomUUID().toString();
        dao.getJdbcTemplate().update(insertUserSql, userId, 1, premium, app, appV,
                new Timestamp(exp.toEpochMilli()), "experiment 1",30,11,
                new Timestamp(System.currentTimeMillis()), "001_1213124");
        dao.getJdbcTemplate().update(insertUserPiSql, userId, upsId);

        String insertUserCohortSql = "INSERT INTO user_cohorts" +
                "(user_id, shard, ups_id, cohort_id, cohort_value, pending_deletion, pending_export, created_at)" +
                " VALUES(?,?,?,?,?,?,?,?)";
        dao.getJdbcTemplate().update(insertUserCohortSql, userId, 1, upsId, "unittest_cohort_1", "true", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));
        // to test exclusion in outer joins
        dao.getJdbcTemplate().update(insertUserCohortSql, userId, 1, upsId, "unittest_cohort_2", "true", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));

        // existing that changed
        String userId2 = UUID.randomUUID().toString();
        String upsId2 = UUID.randomUUID().toString();
        dao.getJdbcTemplate().update(insertUserSql, userId2, 2, premium, app, appV,
                new Timestamp(exp.toEpochMilli()), "experiment 1",30,11,
                new Timestamp(System.currentTimeMillis()), "001_1213124");
        dao.getJdbcTemplate().update(insertUserPiSql, userId2, upsId2);
        dao.getJdbcTemplate().update(insertUserCohortSql, userId2, 2, upsId2, "unittest_cohort_1", "false", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));
        // to test exclusion in outer joins
        dao.getJdbcTemplate().update(insertUserCohortSql, userId2, 2, upsId2, "unittest_cohort_2", "false", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));

        // existing that left the cohort
        String userId3 = UUID.randomUUID().toString();
        String upsId3 = UUID.randomUUID().toString();
        dao.getJdbcTemplate().update(insertUserSql, userId3, 3, false, app, appV,
                new Timestamp(exp.toEpochMilli()), "experiment 1",30,11,
                new Timestamp(System.currentTimeMillis()), "001_1213124");
        dao.getJdbcTemplate().update(insertUserPiSql, userId3, upsId3);
        dao.getJdbcTemplate().update(insertUserCohortSql, userId3, 3, upsId3, "unittest_cohort_1", "true", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));
        // to test exclusion in outer joins
        dao.getJdbcTemplate().update(insertUserCohortSql, userId3, 3, upsId3, "unittest_cohort_2", "true", false, false,
                new Timestamp(Instant.now().minus(2, ChronoUnit.HOURS).toEpochMilli()));

        System.out.println("Before calc");
        printUserCohorts();
        service.runCohortCalculation("unittest_cohort_1");
        System.out.println("After calc");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(3);

        String selectExportFlagSql = "select pending_export from user_cohorts where user_id = ? and cohort_id = ?";
        Boolean flag = dao.getJdbcTemplate().queryForObject(selectExportFlagSql, Boolean.class, premiumApp1Users.get(0), "unittest_cohort_1");
        assertThat(flag).isNotNull();
        assertThat(flag).isEqualTo(true);

        flag = dao.getJdbcTemplate().queryForObject(selectExportFlagSql, Boolean.class, userId, "unittest_cohort_1");
        assertThat(flag).isNotNull();
        assertThat(flag).isEqualTo(false);

        flag = dao.getJdbcTemplate().queryForObject(selectExportFlagSql, Boolean.class, userId2, "unittest_cohort_1");
        assertThat(flag).isNotNull();
        assertThat(flag).isEqualTo(true);

        String selectDeleteFlagSql = "select pending_deletion from user_cohorts where user_id = ? and cohort_id = ?";
        flag = dao.getJdbcTemplate().queryForObject(selectDeleteFlagSql, Boolean.class, userId3, "unittest_cohort_1");
        assertThat(flag).isNotNull();
        assertThat(flag).isEqualTo(true);
    }

    @Test
    public void testCohortDeletion() throws Exception {
        String sql = "INSERT INTO user_cohorts" +
                "(user_id, shard, ups_id, cohort_id, cohort_value, pending_deletion)" +
                " VALUES(?,?,?,?,?,?)";
        dao.getJdbcTemplate().update(sql, UUID.randomUUID().toString(), 1, "ups1", "unittest_cohort_1", "true", false);
        dao.getJdbcTemplate().update(sql, UUID.randomUUID().toString(), 2, "ups2", "unittest_cohort_1", "true", false);
        dao.getJdbcTemplate().update(sql, UUID.randomUUID().toString(), 3, "ups3", "unittest_cohort_1", "true", false);
        dao.getJdbcTemplate().update(sql, UUID.randomUUID().toString(), 4, "ups4", "unittest_cohort_1", "true", false);

        service.markCohortForDeletion("unittest_cohort_1");
        printUserCohorts();
        assertThat(dao.countAudience("unittest_cohort_1")).isEqualTo(0);
        List<UserCohort> all = dao.getAllUsersCohorts(Collections.singletonList("unittest_cohort_1"));
        assertThat(all).hasSize(4);
    }

    @Test
    public void testValidation() throws Exception {
        BasicJobStatusReport report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null,"premium = true", "in_app_message_displayed_30d", CohortExportConfig.DB_ONLY_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);

        report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "in_app_message_displayed_30d = 0", "sessions_30d", CohortExportConfig.DB_ONLY_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(0);

        Exception e = assertThrows(BadSqlGrammarException.class, () -> {
            service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "userid = '12'", "in_app_message_displayed_30d", CohortExportConfig.DB_ONLY_EXPORT, false);
        });
        e.printStackTrace();

        e = assertThrows(BadSqlGrammarException.class, () -> {
            service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "in_app_message_displayed_30d > 0", "userid", CohortExportConfig.DB_ONLY_EXPORT, false);
        });
        e.printStackTrace();

        report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "experiment not in ('experiment 2')", "experiment", CohortExportConfig.DB_ONLY_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);
    }

    @Test
    public void testValidationUps() throws Exception {
        BasicJobStatusReport report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null,"premium = true", "in_app_message_displayed_30d", CohortExportConfig.UPS_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);

        report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "in_app_message_displayed_30d = 0", "sessions_30d", CohortExportConfig.UPS_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(0);

        Exception e = assertThrows(BadSqlGrammarException.class, () -> {
            service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "userid = '12'", "in_app_message_displayed_30d", CohortExportConfig.UPS_EXPORT, false);
        });
        e.printStackTrace();

        e = assertThrows(BadSqlGrammarException.class, () -> {
            service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "in_app_message_displayed_30d > 0", "userid", CohortExportConfig.UPS_EXPORT, false);
        });
        e.printStackTrace();

        report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, null, "experiment not in ('experiment 2')", "experiment", CohortExportConfig.UPS_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);
    }

    @Test
    public void testJoinValidation() throws Exception {
        BasicJobStatusReport report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, Arrays.asList("user_features"), "user_features.test_feature_json is not null", "user_features.test_feature_json", CohortExportConfig.DB_ONLY_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);

        report = service.validateCondition(TestAirlockCohortsClient.PRODUCT_ID, Arrays.asList("user_features"), "user_features.test_feature_json is not null", "user_features.test_feature_json", CohortExportConfig.UPS_EXPORT, false);
        System.out.println(new Gson().toJson(report));
        assertThat(report.getStatus()).isEqualTo(ExportJobStatusReport.JobStatus.COMPLETED);
        assertThat(report.getUsersNumber()).isEqualTo(1);
    }

    @Test
    public void testJoinedCohortCalculation() throws Exception {
        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_json");
        airlockClient.setCondition("user_features.test_feature_json is not null");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();

        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_list");
        airlockClient.setCondition("user_features.test_feature_list is not null");
        service.runCohortCalculation("unittest_cohort_2");
        printUserCohorts();
    }

    @Test
    public void testJoinedCohortCalculationNulls() throws Exception {
        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_json");
        airlockClient.setCondition("user_features.test_feature_list is not null");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();

        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_list");
        airlockClient.setCondition("user_features.test_feature_list is not null");
        service.runCohortCalculation("unittest_cohort_2");
        printUserCohorts();
    }

    @Test
    public void testJoinedCohortCalculationUps() throws Exception {
        airlockClient.setExportType(CohortExportConfig.UPS_EXPORT);
        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_json");
        airlockClient.setCondition("user_features.test_feature_list is not null");
        service.runCohortCalculation("unittest_cohort_1");
        printUserCohorts();

        airlockClient.setJoinedTables(Arrays.asList("user_features"));
        airlockClient.setExpression("user_features.test_feature_list");
        airlockClient.setCondition("user_features.test_feature_list is not null");
        service.runCohortCalculation("unittest_cohort_2");
        printUserCohorts();
        dao.getJdbcTemplate().query("select * from user_cohorts", (rs) -> {
            System.out.println(convertArrayValue(rs.getString("cohort_value")));
        });
    }

    private Object convertArrayValue(String sValue) {

        if(sValue.startsWith("[") || sValue.startsWith("{")) {
            sValue = sValue.substring(1);
        }

        if(sValue.endsWith("]") || sValue.endsWith("}")) {
            sValue = sValue.substring(0, sValue.length() - 1);
        }
        String[] a = sValue.split("\\,\\s?");
        return Arrays.asList(a);
    }

    private void printUserCohorts() {
        System.out.println(
            String.format(
                    "%s\t%s\t%s\t%s\t%s\t%s",
                    "user_id",
                    "ups_id",
                    "cohort_id",
                    "cohort_value",
                    "pending_deletion",
                    "pending_export"));
        dao.getJdbcTemplate().query("select * from user_cohorts", (rs) -> {
            System.out.println(
                    String.format(
                            "%s\t%s\t%s\t%s\t%s\t%s",
                            rs.getString("user_id"),
                            rs.getString("ups_id"),
                            rs.getString("cohort_id"),
                            rs.getString("cohort_value"),
                            rs.getBoolean("pending_deletion"),
                            rs.getBoolean("pending_export")));
        });
    }
}
