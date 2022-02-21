package com.ibm.airlytics.utilities.validationService.queryservice.queryHandler;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class QueryHandler {

    public static final String SAMPLE_EVENTS_COUNT_QUERY = "select appversion, name, count(*)  from %s where day = %s group by appversion, name";

    public long submitQueryAndGetCountResult(String query, String table, String day, String condition) throws InterruptedException {
        return -1;
    };

    public List<List<String>> submitEventsCounterQueryAndGeResults(String table, String day) throws InterruptedException {
        return submitQueryAndGeResults(String.format(SAMPLE_EVENTS_COUNT_QUERY, table, day), false);
    }

    public List<List<String>> submitQueryAndGeResults(String query) throws InterruptedException {
        return null;
    }

    public List<List<String>> submitQueryAndGeResults(String query, boolean includeHeaders) throws InterruptedException {
        waitForQueryToComplete("");
        return processRowResults("", includeHeaders);
    }

    /**
     * Submits a sample query to Athena and returns the execution ID of the query.
     */
    private String submitQuery(String query, Object client, String table, String day, String condition) {
        if (!condition.isEmpty()){
            condition = " and " + condition;
        }
        return submitQuery(String.format(query, table, day) +  condition, client);
    }

    /**
     * Submits a sample query to Athena and returns the execution ID of the query.
     */
    private String submitQuery(String query, Object client) {
        return "";
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */

    private void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
    }

    private long processCountResult(String queryExecutionId) {
        return 0;
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    public List<List<String>> processRowResults(String queryExecutionId, boolean includeHeaders) {
        return new ArrayList<>();
    }


    public static String getTodaysString() {
        return getDayAgoString(0,true);
    }

    public static String getDayAgoString(int days, boolean includeQuote) {
        String headerTrailerChar = "'";
        if (!includeQuote){
            headerTrailerChar = "";
        }
        return headerTrailerChar + new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days))) + headerTrailerChar;
    }

    public static String getDayAgoString(int days) {
        return getDayAgoString(days,true);
    }

    public void loadSampleData(String dbName, String tableName){
    }
}
