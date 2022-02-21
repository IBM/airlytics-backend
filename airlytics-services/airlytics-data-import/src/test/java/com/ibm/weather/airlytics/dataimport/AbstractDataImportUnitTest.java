package com.ibm.weather.airlytics.dataimport;

import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import com.ibm.weather.airlytics.dataimport.integrations.TestAirlockDataImportClient;
import com.ibm.weather.airlytics.dataimport.services.TestDataImportConfigService;
import com.ibm.weather.airlytics.dataimport.services.TestS3FileService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.jdbc.DataSourceBuilder;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class AbstractDataImportUnitTest {

    private static EmbeddedPostgres postgres;

    protected static UserFeaturesDao dao;

    protected static TestS3FileService s3Service;

    protected static TestAirlockDataImportClient airlockClient;

    protected static TestDataImportConfigService featureConfigService;

    @BeforeAll
    public static void setupDb() throws Exception {
        postgres = new EmbeddedPostgres();
        String url = postgres.start("localhost", 5433, "unittest", "user", "password");
        Connection conn = DriverManager.getConnection(url);
        File f = new File("src/test/resources/createUserFeaturesTable.sql");
        postgres.getProcess().get().importFromFile(f);

        dao = new UserFeaturesDao();
        dao.setDataSource(getDataSource(url));
        dao.postConstruct();

        s3Service = new TestS3FileService(dao);
        airlockClient = new TestAirlockDataImportClient();
        featureConfigService = new TestDataImportConfigService();
    }

    public static DataSource getDataSource(String url) {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.postgresql.Driver");
        dataSourceBuilder.url(url+"&currentSchema=users");
        return dataSourceBuilder.build();
    }

    @AfterEach
    public void cleanup()  {
        dao.getJdbcTemplate().update("delete from user_features");
        dao.getJdbcTemplate().update("delete from users");
    }

    @AfterAll
    public static void closeDb() throws Exception {
        postgres.stop();
    }
}
