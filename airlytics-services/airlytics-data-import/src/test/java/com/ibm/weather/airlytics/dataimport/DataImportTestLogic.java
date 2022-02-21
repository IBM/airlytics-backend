package com.ibm.weather.airlytics.dataimport;

import com.ibm.weather.airlytics.common.airlock.AirlockException;
import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.dto.DataImportConfig;
import com.ibm.weather.airlytics.dataimport.dto.ImportJobDefinition;
import com.ibm.weather.airlytics.dataimport.services.DataImportService;
import com.ibm.weather.airlytics.dataimport.services.DataImportServiceException;
import com.ibm.weather.airlytics.dataimport.services.S3FileService;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DataImportTestLogic {

    private UserFeaturesDao dao;

    private S3FileService s3Service;

    private DataImportService dataImportService;

    public DataImportTestLogic(UserFeaturesDao dao, S3FileService s3Service, DataImportService dataImportService) {
        this.dao = dao;
        this.s3Service = s3Service;
        this.dataImportService = dataImportService;
    }

    public void testUpdate1(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(2);

        //"6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8",0.123,"{NY,CN}","Test message 1","true"
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(((BigDecimal) row.get("ai_high_churn_probability")).doubleValue()).isEqualTo(0.123);
        assertThat(row.get("ai_states_current_location_30d").toString()).isEqualTo("{NY,CN}");
        assertThat(row.get("ai_push_message")).isEqualTo("Test message 1");
        assertThat(row.get("ai_wears_hats")).isEqualTo(true);

        //"7900f30b-8f47-4829-95c5-9d4d6e8564ee",0.456,"{CA,TX}","Test message 2","false"
        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("7900f30b-8f47-4829-95c5-9d4d6e8564ee");
        assertThat(row.get("shard")).isEqualTo(1);
        assertThat(((BigDecimal) row.get("ai_high_churn_probability")).doubleValue()).isEqualTo(0.456);
        assertThat(row.get("ai_states_current_location_30d").toString()).isEqualTo("{CA,TX}");
        assertThat(row.get("ai_push_message")).isEqualTo("Test message 2");
        assertThat(row.get("ai_wears_hats")).isEqualTo(false);

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testUpdate2(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(2);

        //"6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8",0.789,"{OH,OR}","Test message 3","false"
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(((BigDecimal) row.get("ai_high_churn_probability")).doubleValue()).isEqualTo(0.789);
        assertThat(row.get("ai_states_current_location_30d").toString()).isEqualTo("{OH,OR}");
        assertThat(row.get("ai_push_message")).isEqualTo("Test message 3");
        assertThat(row.get("ai_wears_hats")).isEqualTo(false);

        //"7900f30b-8f47-4829-95c5-9d4d6e8564ee",,,,
        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("7900f30b-8f47-4829-95c5-9d4d6e8564ee");
        assertThat(row.get("shard")).isEqualTo(1);
        assertThat(row.get("ai_high_churn_probability")).isNull();
        assertThat(row.get("ai_states_current_location_30d")).isNull();
        assertThat(row.get("ai_push_message")).isNull();
        assertThat(row.get("ai_wears_hats")).isNull();

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testUpdate3(String table, boolean replace, String product, File file, String Id) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, Id, file.getAbsolutePath(), table, replace);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(3);

        //"6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8","true"
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(((BigDecimal)row.get("ai_high_churn_probability")).doubleValue()).isEqualTo(0.789);
        assertThat(row.get("ai_states_current_location_30d").toString()).isEqualTo("{OH,OR}");
        assertThat(row.get("ai_push_message")).isEqualTo("Test message 3");
        assertThat(row.get("ai_wears_hats")).isEqualTo(true);

        //"fda4bac3-6896-47dc-b0de-3d5a57ca1231","false"
        row = rows.get(2);
        assertThat(row.get("user_id")).isEqualTo("fda4bac3-6896-47dc-b0de-3d5a57ca1231");
        assertThat(row.get("shard")).isEqualTo(3);
        assertThat(row.get("ai_high_churn_probability")).isNull();
        assertThat(row.get("ai_states_current_location_30d")).isNull();
        assertThat(row.get("ai_push_message")).isNull();
        assertThat(row.get("ai_wears_hats")).isEqualTo(false);

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testUpdatePi1(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(2);

        //"6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8","true"
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(row.get("ai_pi_flag")).isEqualTo(true);

        //"7900f30b-8f47-4829-95c5-9d4d6e8564ee","false"
        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("7900f30b-8f47-4829-95c5-9d4d6e8564ee");
        assertThat(row.get("shard")).isEqualTo(1);
        assertThat(row.get("ai_pi_flag")).isEqualTo(false);

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all_pi order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testVideoPlayed(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(4L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(4);

        //user_id,shard,ai_core_video_played_any_d30,ai_core_video_played_any_d60,ai_core_video_played_any_d7
        //82F0D188-48F4-4210-9549-396386A955B1,481,9,9,9
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("82F0D188-48F4-4210-9549-396386A955B1");
        assertThat(row.get("shard")).isEqualTo(481);
        assertThat(((Integer) row.get("ai_core_video_played_any_d30")).intValue()).isEqualTo(9);
        assertThat(((Integer) row.get("ai_core_video_played_any_d60")).intValue()).isEqualTo(9);
        assertThat(((Integer) row.get("ai_core_video_played_any_d7")).intValue()).isEqualTo(9);

        //5E4631A4-1E1E-42D3-8953-95F39B113CF7,710,4,4,4
        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("5E4631A4-1E1E-42D3-8953-95F39B113CF7");
        assertThat(row.get("shard")).isEqualTo(710);
        assertThat(((Integer) row.get("ai_core_video_played_any_d30")).intValue()).isEqualTo(4);
        assertThat(((Integer) row.get("ai_core_video_played_any_d60")).intValue()).isEqualTo(4);
        assertThat(((Integer) row.get("ai_core_video_played_any_d7")).intValue()).isEqualTo(4);

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testAggregationImport(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        request.setAffectedColumns(Arrays.asList("shard", "revenue_d1", "revenue_total"));
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from " + table + " order by shard");
        assertThat(rows).hasSize(2);

        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(((Integer) row.get("revenue_d1")).intValue()).isEqualTo(100);
        assertThat(((Integer) row.get("revenue_total")).intValue()).isEqualTo(200);

        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("fda4bac3-6896-47dc-b0de-3d5a57ca1231");
        assertThat(row.get("shard")).isEqualTo(1);
        assertThat(((Integer) row.get("revenue_d1")).intValue()).isEqualTo(300);
        assertThat(((Integer) row.get("revenue_total")).intValue()).isEqualTo(400);

        //test view
        rows = dao.getJdbcTemplate().queryForList("select * from user_features_all order by shard");
        assertThat(rows).hasSize(7);
    }

    public void testDevAggregationImport(String table, boolean replace, String product, File file, String jobId) throws AirlockException, DataImportServiceException {
        DataImportConfig request = new DataImportConfig(product, jobId, file.getAbsolutePath(), table, replace);
        request.setAffectedColumns(Arrays.asList("shard", "revenue_d1", "revenue_total"));
        request.setDevUsers(true);
        long cnt = dataImportService.executeJob(request);
        assertThat(cnt).isEqualTo(2L);

        List<Map<String, Object>> rows = dao.getJdbcTemplate().queryForList("select * from users_dev." + table + " order by shard");
        assertThat(rows).hasSize(2);

        Map<String, Object> row = rows.get(0);
        assertThat(row.get("user_id")).isEqualTo("6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8");
        assertThat(row.get("shard")).isEqualTo(0);
        assertThat(((Integer) row.get("revenue_d1")).intValue()).isEqualTo(100);
        assertThat(((Integer) row.get("revenue_total")).intValue()).isEqualTo(200);

        row = rows.get(1);
        assertThat(row.get("user_id")).isEqualTo("fda4bac3-6896-47dc-b0de-3d5a57ca1231");
        assertThat(row.get("shard")).isEqualTo(1);
        assertThat(((Integer) row.get("revenue_d1")).intValue()).isEqualTo(300);
        assertThat(((Integer) row.get("revenue_total")).intValue()).isEqualTo(400);

    }
}
