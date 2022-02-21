package com.ibm.airlytics.consumer.purchase;

import com.ibm.airlytics.utilities.Constants;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

@RunWith(Parameterized.class)
public class InternalAirlyticsPurchaseDBDataAccuracyTest {

    @Parameterized.Parameters(name = "{index}: Testing app={0}")
    public static Iterable<? extends Object> data() {
        //return Arrays.asList("android_product", "ios_product", "android_wu", "ios_wu", "ios_storm");
        return Arrays.asList("android_wu","android_product");
    }

    /**
     * This value is initialized with values from data() array
     */

    @Parameterized.Parameter
    public String appName;


    protected static final String CONFIG_FOLDER = "test-config";
    protected static final String SQL_QUERY_FOLDER = "purchase-data-validation-sql";


    protected static String USER_DB_PASSWORD_ENV = "USER_DB_PASSWORD_ENV";
    protected static String USER_DB_URL_ENV = "USER_DB_URL_ENV";
    protected static String USER_DB_NAME = "airlytics_all_ro";

    protected Properties properties;
    protected Connection executionConn;


    protected Connection initConnection() throws ClassNotFoundException, SQLException {
        // Setup connection pool
        Class.forName(Constants.JDBC_DRIVER);

        Properties props = new Properties();
        props.setProperty("user", USER_DB_NAME);
        props.setProperty("password", System.getenv(USER_DB_PASSWORD_ENV) == null ?
                properties.getProperty(USER_DB_PASSWORD_ENV) : System.getenv(USER_DB_PASSWORD_ENV));
        props.setProperty("reWriteBatchedInserts", "true");
        Connection conn = DriverManager.getConnection(System.getenv(USER_DB_URL_ENV) == null ?
                properties.getProperty(USER_DB_URL_ENV) : System.getenv(USER_DB_URL_ENV), props);
        conn.setAutoCommit(false);
        return conn;
    }

    @Before
    public void setup() {
        try {
            readConfig();
            executionConn = initConnection();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void expiredSubShouldBeNotActive() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("expiredSubShouldBeNotActive"), appName));
    }

    @Test
    public void periodStartDateShouldBeBiggerThenStartDate() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("periodStartDateShouldBeBiggerThenStartDate"), appName));
    }

    @Test
    public void cancelledSubShouldHaveAtLeastOnePaidPeriod() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("cancelledSubShouldHaveAtLeastOnePaidPeriod"), appName, appName));
    }

    @Test
    public void expiredTrialShouldBeAppearAsTRIAL_EXPIRED() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("expiredTrialShouldBeAppearAsTRIAL_EXPIRED"), appName, appName, appName));
    }

    @Test
    public void purchasesTablePeriodsShouldBeEqualToPurchaseEvents() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("purchasesTablePeriodsShouldBeEqualToPurchaseEvents"),
                        appName, appName, appName, appName));
    }

    @Test
    public void subReplacedCancellationShouldBeUpdatedNoExpired() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("subReplacedCancellationShouldBeUpdatedNoExpired"), appName));
    }

    @Test
    public void onUpgradedRevenueShouldBeNegative() throws IOException, SQLException {
        validateEmptyResultsQueryGroupedByProduct(String.
                format(readSQLQuery("onUpgradedRevenueShouldBeNegative"), appName));
    }

    protected void validateEmptyResultsQueryGroupedByProduct(String query) throws SQLException {
        Map<String, Integer> testResults = new HashMap();

        ResultSet resultSet = executionConn.createStatement().executeQuery(query);
        if (resultSet.next()) {
            do {
                testResults.put(resultSet.getString(1), resultSet.getInt(2));
            } while (resultSet.next());
        }
        Iterator<String> it = testResults.keySet().iterator();

        while (it.hasNext()) {
            String productName = it.next();
            Assert.assertEquals("App:" + appName + " product:[" + productName + "]", 0, testResults.get(productName),5.0);
        }
    }


    protected String readSQLQuery(String path) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(SQL_QUERY_FOLDER + File.separator + path + ".sql")) {
            return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
        }
    }


    protected void readConfig() throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(CONFIG_FOLDER + File.separator + "InternalAirlyticsPurchaseDBDataAccuracyTest.properties")) {
            String props = IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);

            properties = new Properties();
            InputStream is = new ByteArrayInputStream(props.getBytes());
            properties.load(is);
        }
    }
}