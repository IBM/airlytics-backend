package com.ibm.weather.airlytics.dataimport.services;

import com.ibm.weather.airlytics.dataimport.AbstractDataImportUnitTest;
import com.ibm.weather.airlytics.dataimport.DataImportTestLogic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

public class DataImportServiceTest extends AbstractDataImportUnitTest {

    private DataImportService service;

    private DataImportTestLogic test;

    private static final String[] VIDEO_USERS = {
            "82F0D188-48F4-4210-9549-396386A955B1",
            "5E4631A4-1E1E-42D3-8953-95F39B113CF7",
            "9CDF3767-4E16-47F4-9B64-D412F1510813",
            "BFD67EB4-4604-4E8E-9567-853CD797182C"};

    private static final int[] VIDEO_USERS_SHARDS = {481, 710, 815, 816};

    @BeforeEach
    public void setup() {
        service = new DataImportService(s3Service, dao, airlockClient, featureConfigService, "=users");
        service.setAirlyticsVarsForTest("TEST", "TEST");
        cleanup();
        String sql = "insert into users(id, shard, premium) values (?, ?, ?)";
        dao.getJdbcTemplate().update(sql, "6b3e2f4b-4b5a-462f-8700-1db11d6dd2e8", 0, true);
        dao.getJdbcTemplate().update(sql, "7900f30b-8f47-4829-95c5-9d4d6e8564ee", 1, false);
        dao.getJdbcTemplate().update(sql, "fda4bac3-6896-47dc-b0de-3d5a57ca1231", 3, false);

        for(int i = 0; i < VIDEO_USERS.length; i++) {
            dao.getJdbcTemplate().update(sql, VIDEO_USERS[i], VIDEO_USERS_SHARDS[i], false);
        }
        test = new DataImportTestLogic(dao, s3Service, service);
    }

    @Test
    public void test() throws Exception {
        String table = "user_features";
        boolean replace = false;

        String product = UUID.randomUUID().toString();
        File f1 = new File("src/test/resources/update1.csv");
        String job1 = UUID.randomUUID().toString();
        File f2 = new File("src/test/resources/update2.csv");
        String job2 = UUID.randomUUID().toString();
        File f3 = new File("src/test/resources/update3.csv");
        String job3 = UUID.randomUUID().toString();

        test.testUpdate1(table, replace, product, f1, job1);
        test.testUpdate2(table, replace, product, f2, job2);
        test.testUpdate3(table, replace, product, f3, job3);
    }
}
