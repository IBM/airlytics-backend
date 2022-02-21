package com.ibm.airlytics.utilities.validationService.queryservice.validations;

import com.ibm.airlytics.utilities.validationService.queryservice.config.ServiceConfig;
import com.ibm.airlytics.utilities.validationService.queryservice.queryHandler.AthenaAWSQueryHandler;
import com.ibm.airlytics.utilities.validationService.queryservice.queryHandler.QueryHandler;
import com.ibm.airlytics.utilities.validationService.queryservice.reporter.ReporterMessage;
import org.json.JSONObject;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class AthenaAWSQueriesValidations extends BaseValidation {

    @BeforeClass
    @Parameters({"platform", "daysBefore", "testVersion","dbName" ,"tableName", "errorTableName", "catalog", "outputBucket", "minVersion"})
    void setUp(String platformAndMode, int daysBefore,String testVersion, String dbName, String tableName, String errorTableName, String catalog, String outputBucket, @Optional("10.25.0") String minVersion) {
        this.testVersion = testVersion;
        this.platformAndMode = platformAndMode;
        this.minVersion = minVersion;
        String[] platformAndModeArray = platformAndMode.split("_");
        String platform = platformAndModeArray[0];
        String mode = platformAndModeArray[1];
        this.dbName = dbName;
        table = tableName;
        errorTable = errorTableName;
        day = QueryHandler.getDayAgoString(daysBefore);
        QueryHandler queryHandle = new AthenaAWSQueryHandler(dbName, catalog, outputBucket);
        new File("lastReportRunDate");
        eventCounts = new HashMap<>();
        errorEventCounts = new HashMap<>();
        successTestsMap = new HashMap<>();

        List<List<String>> eventRows = getEventCounters(table, day);
        for (List<String> values : eventRows) {
            if (!eventCounts.containsKey(values.get(0))){
                eventCounts.put(values.get(0), new HashMap<>());
            }
            String appVersion = values.get(0);
            String eventName = values.get(1);
            long count = Long.valueOf(values.get(2));

            String sql = "select count(*)  from airlytics." + table + " where day = " + day + " and appversion = '" + appVersion + "' and name = '" + eventName + "'";
            report(ReporterMessage.Levels.info, appVersion, eventName, count, sql );
            eventCounts.get(appVersion).put(eventName, count);
        }
        List<List<String>> errorEventRows = getEventCounters(errorTable, day);
        for (List<String> values : errorEventRows) {
            if (!errorEventCounts.containsKey(values.get(0))){
                errorEventCounts.put(values.get(0), new HashMap<>());
            }
            errorEventCounts.get(values.get(0)).put(values.get(1), Long.valueOf(values.get(2)));
        }
        String appversionsQuery = "select distinct(appversion) from  airlytics.airlytics_events_" + this.platformAndMode +  " where day = " + AthenaAWSQueryHandler.getTodaysString();

        expectedResults = new HashMap<>();

        try {
            JSONObject successTestsJson = new JSONObject(new String(Files.readAllBytes(Paths.get( ServiceConfig.OUTPUT_DIRECTORY + File.separator + "successTests_" + this.platformAndMode + ".json"))));

            successTestsMap = successTestsJson.toMap();
        } catch (IOException ignore) {

        }
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

    @Test
    void compareCountOnVersionTest() {
        compareCountOnVersionTest("event1", "event2", -1);
    }

    @Test
    void compareTotalTest() {
        compareTotalTest("event1", "event2", 1.5);
    }

//    @Test
//    void purchaseAttemptedTest() {
//        String eventName = "purchase-attempted";
//        baseCountValidationTest(testVersion,eventName, expectedResults.get(eventName));
////        day = AthenaQueryUtil.getDayAgoString(daysBefore);
//
//        String condition = "name = '"+eventName+"' and cast(json_extract_scalar(event, '$.attributes.completed') as BOOLEAN) = true";
//        long subscriptions = getQueryResult(table, day, condition);
//        if (subscriptions < 100){
//            ServiceLogger.getLogger(this.getClass().getName()).fatal("Table " + table + " do not have the number of expected entries impression_usd_micros greater than 0 for date " + day + "(expected :" + expectedRate + " actual: " + count + ")");
//        }
//        long totalPurchaseEvents = getTotalEventCount(eventName);
//        String sql = "1. select count(*)  from airlytics." + table + " where day = " + day + " and " + condition +"\\n 2. select count(*) from " + table + " where name = '"+ eventName+"' and day = " + day  +"\\n 3. compare rate is: " + 10;
//        compareTotalTest(eventName, eventName+"Completed",subscriptions,totalPurchaseEvents,10,sql);
//    }

    @Test
    void eventSpecificTest() {
        String eventName = "event1";
        baseCountValidationTest(testVersion,eventName, expectedResults.get(eventName));
    }

    @Test
    void getQueryDataTest(){
        String query = "select count(*),<otherColumn> from " + table + " where day = "+day+ " " +" group by <column1>, <column1> having count(*) = 2";
        List<List<String>> data = getQueryData(query, false);
        for (List<String> versionData: data){
            String version = versionData.get(1);
            assetTrue(versionData.get(0) == null,new ReporterMessage(ReporterMessage.Levels.error,platformAndMode,version,"empty-session-" + version,Integer.valueOf(versionData.get(0)), 0,query),true, false);
        }
    }

}
