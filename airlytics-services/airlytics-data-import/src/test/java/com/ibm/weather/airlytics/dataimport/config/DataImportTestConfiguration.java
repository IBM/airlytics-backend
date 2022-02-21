package com.ibm.weather.airlytics.dataimport.config;

import com.ibm.weather.airlytics.dataimport.db.TestDB;
import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.integrations.AirlockDataImportClient;
import com.ibm.weather.airlytics.dataimport.integrations.TestAirlockDataImportClient;
import com.ibm.weather.airlytics.dataimport.services.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

@Configuration
@ComponentScan(basePackages = "com.ibm.weather.airlytics.dataimport.db")
@EnableTransactionManagement
public class DataImportTestConfiguration {

    @Bean
    @Primary
    public AirlockDataImportClient airlockUserFeaturesClient1() {
        return new TestAirlockDataImportClient();
    }

    @Bean
    @Primary
    public TestDB testDB() throws Exception {
        return new TestDB();
    }

    @Bean
    @Primary
    public DataSource dataSource1(@Autowired TestDB db) throws Exception {
        return db.getDataSource();
    }

    @Bean
    @Primary
    public S3FileService s3FileService1(@Autowired UserFeaturesDao dao) throws Exception {
        return new TestS3FileService(dao);
    }

    @Bean
    @Primary
    public DataImportConfigService dataImportConfigService() {
        return new TestDataImportConfigService();
    }

    @Bean
    @Primary
    public DataImportService dataImportService1(
            @Autowired S3FileService s3Service,
            @Autowired UserFeaturesDao userFeaturesDao,
            @Autowired AirlockDataImportClient airlockClient,
            @Autowired DataImportConfigService dataImportConfigService) {
        DataImportService svc = new DataImportService(s3Service, userFeaturesDao, airlockClient, dataImportConfigService, "=users");
        svc.setAirlyticsVarsForTest("TEST", "TEST");
        return svc;
    }

    @Bean
    @Primary
    DataSourceTransactionManager tm(@Autowired DataSource datasource) {
        DataSourceTransactionManager txm  = new DataSourceTransactionManager(datasource);
        return txm;
    }
}
