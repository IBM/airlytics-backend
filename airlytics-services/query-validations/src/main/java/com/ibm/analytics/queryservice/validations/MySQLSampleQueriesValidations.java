package com.ibm.analytics.queryservice.validations;

import com.ibm.analytics.queryservice.ServiceRunner;
import com.ibm.analytics.queryservice.queryHandler.MySqlQueryHandler;
import com.ibm.analytics.queryservice.queryHandler.QueryHandler;
import com.ibm.analytics.queryservice.reporter.ReporterMessage;
import org.json.JSONObject;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class MySQLSampleQueriesValidations extends BaseValidation {

    @BeforeClass
    @Parameters({"platform", "daysBefore", "testVersion", "clientType", "dbHostPort","dbName", "tableName", "errorTableName", "minVersion", "doLoadData"})
    void setUp(String platformAndMode, int daysBefore, String testVersion, String clientType, String dbHostPort, String dbName, String tableName, String errorTableName, @Optional("minVersion") String minVersion, @Optional("doLoadData") boolean doLoadData) {
        this.testVersion = testVersion;
        this.platformAndMode = platformAndMode;
        this.minVersion = minVersion;
        String[] platformAndModeArray = platformAndMode.split("_");
        String platform = platformAndModeArray[0];
        String mode = "";
        if (platformAndModeArray.length > 1) {
            mode = platformAndModeArray[1];
        }
        this.dbName = dbName;
        table = tableName;
        errorTable = errorTableName;
        day = QueryHandler.getDayAgoString(daysBefore);
        try {
            queryHandler = new MySqlQueryHandler(dbName, dbHostPort);
        } catch (SQLException throwables) {
            System.out.println(throwables.getMessage());
            return;
        }
        if (doLoadData) {
            loadDataToDB(dbName, tableName);
        }

        new File("lastReportRunDate");
        eventCounts = new HashMap<>();
        errorEventCounts = new HashMap<>();
        successTestsMap = new HashMap<>();

        List<List<String>> eventRows = getEventCounters(table, day);
        for (List<String> values : eventRows) {
            if (!eventCounts.containsKey(values.get(0))) {
                eventCounts.put(values.get(0), new HashMap<>());
            }
            String appVersion = values.get(0);
            String eventName = values.get(1);
            long count = Long.valueOf(values.get(2));

            String sql = "select count(*)  from " + dbName + "." + table + " where day = " + day + " and appversion = '" + appVersion + "' and name = '" + eventName + "'";
            report(ReporterMessage.Levels.info, appVersion, eventName, count, sql);
            eventCounts.get(appVersion).put(eventName, count);
        }

        List<List<String>> errorEventRows = getEventCounters(errorTable, day);
        for (List<String> values : errorEventRows) {
            if (!errorEventCounts.containsKey(values.get(0))) {
                errorEventCounts.put(values.get(0), new HashMap<>());
            }
            errorEventCounts.get(values.get(0)).put(values.get(1), Long.valueOf(values.get(2)));
        }
        expectedResults = new HashMap<>();

        try {
            JSONObject successTestsJson = new JSONObject(new String(Files.readAllBytes(Paths.get(ServiceRunner.OUTPUT_DIRECTORY + File.separator + "successTests_" + this.platformAndMode + ".json"))));
            successTestsMap = successTestsJson.toMap();
        } catch (IOException ignore) {}

        try {
            JSONObject expectetResultsJson = new JSONObject(new String(Files.readAllBytes(Paths.get("src/main/resources/expected-results-" + this.platformAndMode + ".json"))));
            String expectedResultsMode = expectetResultsJson.optString("mode");
            if (expectedResultsMode.equals(mode)) {
                JSONObject successRates = expectetResultsJson.optJSONObject("successRates");
                if (successRates != null) {
                    Iterator<String> keys = successRates.keys();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        expectedResults.put(key, successRates.optInt(key));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadDataToDB(String dbName, String tableName) {
        this.queryHandler.loadSampleData(dbName, tableName);
    }

    @Test
    void compareCountOnVersionTest() {
        compareCountOnVersionTest("app-start", "session-start", 1);
    }

    @Test
    void compareTotalTest() {
        compareCountOnVersionTest("session-end", "session-start", 1);
    }

    @Test
    void eventSpecificTest() {
        String eventName = "app-start";
        baseCountValidationTest(testVersion, eventName, expectedResults.get(eventName));
    }

    @Test
    void getQueryDataTest(){
        String query = "select count(*), name from " + table + " where day = "+day+ " " +" group by name having count(*) > 10";
        List<List<String>> queryData = getQueryData(query, false);
        for (List<String> data: queryData){
            String count = data.get(0);
            assetTrue(Integer.valueOf(count) > 10,new ReporterMessage(ReporterMessage.Levels.error,platformAndMode,"1.0","low-rate-of-event-" + data.get(1),Integer.valueOf(count), 10,query),true, false);
        }
    }
}
