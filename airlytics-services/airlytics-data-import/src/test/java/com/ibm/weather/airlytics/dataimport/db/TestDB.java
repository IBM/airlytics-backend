package com.ibm.weather.airlytics.dataimport.db;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Component;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class TestDB {

    private EmbeddedPostgres postgres;

    private String dbUrl;

    public TestDB() throws Exception {
        postgres = new EmbeddedPostgres();
        dbUrl = postgres.start("localhost", 5433, "unittest", "user", "password");
        Connection conn = DriverManager.getConnection(dbUrl);
        File f = new File("src/test/resources/createUserFeaturesTable.sql");
        postgres.getProcess().get().importFromFile(f);
    }

    public DataSource getDataSource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.postgresql.Driver");
        dataSourceBuilder.url(dbUrl+"&currentSchema=users");
        return dataSourceBuilder.build();
    }

    public void closeDb() throws Exception {
        postgres.stop();
    }
}
