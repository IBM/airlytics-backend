package com.ibm.analytics.queryservice.validations;

import com.ibm.analytics.queryservice.ServiceRunner;
import com.ibm.analytics.queryservice.airlock.AirlockManager;
import com.ibm.analytics.queryservice.queryHandler.Constants;
import com.ibm.analytics.queryservice.queryHandler.QueryHandler;
import com.ibm.analytics.queryservice.reporter.ReporterMessage;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.Reporter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BaseValidation {

    QueryHandler queryHandler;
    Map<String, Map<String, Long>> eventCounts;
    Map<String, Map<String, Long>> errorEventCounts;
    Map<String, Integer> expectedResults;
    String day;
    String platformAndMode;
    String testVersion;
    String minVersion;
    String table;
    String errorTable;
    Map<String, Object> successTestsMap;
    protected String currentTestName;
    String dbName = "dbName";


    @BeforeClass
    void setUp() {
        AirlockManager.getInstance();
    }

    @AfterClass
    void saveTestDetails() throws IOException {
        JSONObject successTestsJson = new JSONObject();
        for (String key: successTestsMap.keySet()){
            successTestsJson.put(key, successTestsMap.get(key));
        }
        if (platformAndMode.isEmpty()){
            platformAndMode = "general";
        }
        Path path = Paths.get(ServiceRunner.OUTPUT_DIRECTORY + File.separator + "successTests_" + platformAndMode + ".json");
        byte[] strToBytes = successTestsJson.toString().getBytes();
        try {
            Files.write(path, strToBytes);
        } catch (IOException ignore) {
            //TODO log warning
        }
    }

    protected void baseCountValidationTest(String appVersion, String eventName, Integer expectedRate) {
        if (expectedRate == null){
            return;
        }
        baseCountValidationTest(appVersion, eventName, expectedRate, false);
    }

    protected void baseCountValidationTest(String appVersion, String eventName, Integer expectedRate, boolean isMaxRate) {
        if (expectedRate == null){
            return;
        }
        String info = "select count(*)  from " + dbName +"." + table + " where day = " + day + " and appversion = '" + appVersion + "' and name = '" + eventName + "'";
        if (!isMaxRate && expectedRate > 0) {
            assetTrue(eventCounts.containsKey(appVersion), new ReporterMessage(ReporterMessage.Levels.error,platformAndMode, appVersion, eventName, 0, expectedRate, info));
            assetTrue(eventCounts.get(appVersion).containsKey(eventName), new ReporterMessage(ReporterMessage.Levels.error, platformAndMode, appVersion, eventName, 0, expectedRate, info));
        }
        Long eventCount = eventCounts.get(appVersion).get(eventName);
        if (eventCount != null && expectedRate > 0) {
            if (isMaxRate) {
                assetTrue(eventCount < expectedRate, new ReporterMessage(ReporterMessage.Levels.error, platformAndMode, appVersion, eventName, eventCount, expectedRate, info));
            } else {
                assetTrue(eventCount > expectedRate, new ReporterMessage(ReporterMessage.Levels.error, platformAndMode, appVersion, eventName, eventCount, expectedRate, info));
            }
        }
        report(ReporterMessage.Levels.test, appVersion, eventName, eventCount, expectedRate, info);
    }

    protected long getQueryResult(String table, String atDay, String condition) {
        long events = -1;
        try {
            events = queryHandler.submitQueryAndGetCountResult(Constants.SAMPLE_COUNT_QUERY, table, atDay, condition);
        } catch (Exception ex) {
            Assert.fail("Test failed: " + ex.getMessage());
        }
        return events;
    }

    protected int getQueryResult(String table, String atDay) {
        int events = -1;
//        try {
//            events = AthenaQueryUtil.submitQueryAndGetCountResult(client, Constants.ATHENA_SAMPLE_COUNT_QUERY, table, atDay);
//        } catch (Exception ex) {
//            Assert.fail("Test failed: " + ex.getMessage());
//        }
        return events;
    }

    protected void assetTrue(boolean condition, ReporterMessage reporterMessage, boolean reportAsTest) {
        assetTrue(condition, reporterMessage, reportAsTest, true);
    }

    protected void assetTrue(boolean condition, ReporterMessage reporterMessage, boolean reportAsTest, boolean onAssertionStop) {
        if (!condition) {
            if (successTestsMap.containsKey(reporterMessage.getEventName())){
                reporterMessage.appendAlertStr("#################################\nLast time test passed was on :" + successTestsMap.get(reporterMessage.getEventName()) +"\n#################################");
            }
            Reporter.log(reporterMessage.toString());
            if (onAssertionStop) {
                Assert.fail("Test failed - check test report for info");
            }
        } else {
            if (reportAsTest) {
                reporterMessage.setLevel(ReporterMessage.Levels.test);
                Reporter.log(reporterMessage.toString());
                successTestsMap.put(reporterMessage.getEventName(),day);
            }
        }
    }

    protected void assetTrue(boolean condition, ReporterMessage reporterMessage) {
        assetTrue(condition, reporterMessage, false);
    }

    protected void report(ReporterMessage.Levels level, String appVersion, String eventName, long count, int expectedRate, String sql) {
        Reporter.log(new ReporterMessage(level, platformAndMode, appVersion, eventName, count, expectedRate, sql).toString());
    }

    protected void report(ReporterMessage.Levels level, String appVersion, String eventName, long count, String sql) {
        Reporter.log(new ReporterMessage(level, platformAndMode, appVersion, eventName, count, sql).toString());
    }

    protected List<List<String>> getEventCounters(String table, String day) {
        List<List<String>> events = new ArrayList<>();
        try {
            events = queryHandler.submitEventsCounterQueryAndGeResults(table, day);
        } catch (Exception ex) {
            Assert.fail("Test failed: " + ex.getMessage());
        }
        return events;
    }

    protected List<List<String>> getQueryData(String query, boolean includeHeaders) {
        List<List<String>> events = null;
        try {
            events = queryHandler.submitQueryAndGeResults(query, includeHeaders);
        } catch (Exception ex) {
            Assert.fail("Test failed: " + ex.getMessage());
        }
        if (events == null){
            events = new ArrayList<>();
        }
        return events;
    }

    protected void compareCountOnVersionTest(String eventName, String comparableEvent, double compareRate) {
        long event1Count = eventCounts.get(testVersion).get(eventName);
        long event2Count = eventCounts.get(testVersion).get(comparableEvent);
        double absRate = Math.abs(compareRate);
        String info = "1. select count(*) from " + table + " where name = '"+  eventName+"' and appversion = '"+ testVersion +"'\n" +
                "2. select count(*) from " + table + " where name = '"+ comparableEvent+"' and appversion = '"+ testVersion +"'\n"+
                "3. compare rate is: " + compareRate;
        boolean condition = event1Count * absRate >= event2Count;
        if (compareRate < 0){
            condition = event1Count * (absRate) < event2Count;
        }
        String testID = eventName + "_" + testVersion + " validation";
        assetTrue(condition, new ReporterMessage(ReporterMessage.Levels.error, platformAndMode, testVersion,testID ,event1Count, (int) (event2Count/absRate), info), true);
    }

    /**
     * Method to run a test to compare between events regardless to the appversion on the specific date that the test suite ran
     * @param eventName the event to compare
     * @param comparableEvent the event to be compared
     * @param compareRate the rate that the event is expected if rate is positive the expected rate is eventNameRate * compareRate > comparableEventRate if rate is negative the expected rate is eventNameRate * abs(compareRate) < comparableEventRate
     */
    protected void compareTotalTest(String eventName, String comparableEvent, double compareRate) {
        long event1Count = getTotalEventCount(eventName);
        long event2Count = getTotalEventCount(comparableEvent);
        compareTotalTest(eventName, comparableEvent, event1Count, event2Count,  compareRate, null);
    }

    /**
     *
     * Method to run a test to compare between events regardless to the appversion on the specific date that the test suite ran
     * @param eventName the event to compare
     * @param comparableEvent the event to be compared
     * @param event1Count number of events to compare
     * @param event2Count number of events to be compared
     * @param compareRate the rate that the event is expected if rate is positive the expected rate is eventNameRate * compareRate > comparableEventRate if rate is negative the expected rate is eventNameRate * abs(compareRate) < comparableEventRate
     * @param alertMessage the message that will be displayed on report when clicking the show button - displays details about the query and test
     */
    protected void compareTotalTest(String eventName, String comparableEvent, long event1Count, long event2Count, double compareRate, String alertMessage) {
        double absRate = Math.abs(compareRate);
        String sql = alertMessage;
        if (alertMessage == null){
            sql = "1. select count(*) from " + table + " where name = '"+  eventName+"'\n 2. select count(*) from " + table + " where name = '"+  comparableEvent+"'\n 3. compare rate is: " + compareRate;
        }
        boolean condition = event1Count * absRate > event2Count;
        if (compareRate < 0){
            condition = event1Count * (absRate) < event2Count;
        }
        assetTrue(condition, new ReporterMessage(ReporterMessage.Levels.error, platformAndMode,eventName +" validation",event1Count, (long) (event2Count/absRate), sql), true);
    }

    /**
     * Get the total events count for specific date for all existing appversions
     * @param eventName the event name to get the count for
     * @return the count for all events of this type
     */
    protected long getTotalEventCount(String eventName) {
        long eventsCount = 0;
        for(Map<String,Long> appVerEventsCount : eventCounts.values()){
            if (appVerEventsCount.containsKey(eventName)) {
                eventsCount += appVerEventsCount.get(eventName);
            }
        }
        return eventsCount;
    }
}
