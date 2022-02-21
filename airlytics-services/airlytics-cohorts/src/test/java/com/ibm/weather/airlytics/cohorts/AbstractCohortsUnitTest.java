package com.ibm.weather.airlytics.cohorts;

import com.ibm.weather.airlytics.cohorts.db.UserCohortsDao;
import com.ibm.weather.airlytics.cohorts.db.UserCohortsReadOnlyDao;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class AbstractCohortsUnitTest {

    private static EmbeddedPostgres postgres;

    protected static UserCohortsDao dao;

    protected static UserCohortsReadOnlyDao rodao;

    @BeforeAll
    public static void setupDb() throws Exception {
        postgres = new EmbeddedPostgres();
        String url = postgres.start("localhost", 5433, "unittest", "user", "password");
        Connection conn = DriverManager.getConnection(url);
        File f = new File("src/test/resources/createUserCohortsTable.sql");
        postgres.getProcess().get().importFromFile(f);

        dao = new UserCohortsDao();
        dao.setDataSource(getDataSource(url));
        dao.postConstruct();

        rodao = new UserCohortsReadOnlyDao();
        rodao.setDataSource(getDataSource(url));
        rodao.postConstruct();
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
        dao.getJdbcTemplate().update("delete from user_cohorts where cohort_id like ?", "unittest_cohort_%");
        dao.getJdbcTemplate().update("delete from users where application like ?", "Unit Test App %");
    }

    @AfterAll
    public static void closeDb() throws Exception {
        postgres.stop();
    }
}
