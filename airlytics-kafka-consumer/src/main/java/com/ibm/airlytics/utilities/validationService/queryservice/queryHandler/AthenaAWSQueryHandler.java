package com.ibm.airlytics.utilities.validationService.queryservice.queryHandler;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.ArrayList;
import java.util.List;

public class AthenaAWSQueryHandler extends QueryHandler {

    private AthenaClient athenaClient;
    private String outputBucket;
    private String db;
    private String catalog;

    public AthenaAWSQueryHandler(String db, String catalog, String outputBucket ) {
        this.db = db;
        this.catalog = catalog;
        this.outputBucket = outputBucket;
        if (this.athenaClient == null){
            athenaClient = AthenaClient.builder()
                    .region(Region.US_EAST_1).credentialsProvider(() -> new AwsCredentials() {
                        @Override
                        public String accessKeyId() {
                            return System.getenv("QUERY_SERVICE_ACCESS_KEY");
                        }

                        @Override
                        public String secretAccessKey() {
                            return System.getenv("QUERY_SERVICE_SECRET_KEY");
                        }
                    })
                    .build();
        }
    }

    public long submitQueryAndGetCountResult(String query, String table, String day, String condition) throws InterruptedException {
        String queryExecutionId = submitQuery(query, table, day, condition);
        waitForQueryToComplete(queryExecutionId);
        return processCountResult(queryExecutionId);
    }

    public List<List<String>> submitEventsCounterQueryAndGeResults(String table, String day) throws InterruptedException {
        return submitQueryAndGeResults(String.format(Constants.SAMPLE_EVENTS_COUNT_QUERY, table, day), false);
    }

    public List<List<String>> submitQueryAndGeResults(String query) throws InterruptedException {
        return submitQueryAndGeResults(query, true);
    }

    public List<List<String>> submitQueryAndGeResults(String query, boolean includeHeaders) throws InterruptedException {
        String queryExecutionId = submitQuery(query);
        waitForQueryToComplete(queryExecutionId);
        return processRowResults(queryExecutionId, includeHeaders);
    }

    /**
     * Submits a sample query to Athena and returns the execution ID of the query.
     */
    private String submitQuery(String query, String table, String day, String condition) {
        if (!condition.isEmpty()){
            condition = " and " + condition;
        }
        return submitQuery(String.format(query, table, day) +  condition);
    }

    /**
     * Submits a sample query to Athena and returns the execution ID of the query.
     */
    private String submitQuery(String query) {
        try {

            // The QueryExecutionContext allows us to set the Database.
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                    .database(db)
                    .catalog(catalog)
                    .build();

            // The result configuration specifies where the results of the query should go in S3 and encryption options
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                    // You can provide encryption options for the output that is written.
                    // .withEncryptionConfiguration(encryptionConfiguration)
                    .outputLocation(outputBucket)
                    .build();

            // Create the StartQueryExecutionRequest to send to Athena which will start the query.
            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(query)
                    .queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration).build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return "";
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */

    private void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(Constants.SLEEP_AMOUNT_IN_MS);
            }
        }
    }

    private long processCountResult(String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId).build();
        GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
        GetQueryResultsResponse response = getQueryResultsResults.iterator().next();
        int resultSize = response.resultSet().rows().size();
        if (resultSize < 2) {
            throw new RuntimeException("Results were not returned");
        }
        List<Row> results = response.resultSet().rows();
        return Long.parseLong(results.get(1).data().get(0).varCharValue());
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    public List<List<String>> processRowResults(String queryExecutionId, boolean includeHeaders) {
        List<List<String>> valuesList = new ArrayList<>();
        GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(queryExecutionId).build();

        GetQueryResultsIterable queryResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

        boolean isColumnsNamesRow = true;
        for (GetQueryResultsResponse result : queryResults) {
            List<Row> results = result.resultSet().rows();
            for (Row row : results) {
                if (isColumnsNamesRow) {
                    isColumnsNamesRow = false;
                    if (!includeHeaders) {
                        continue;
                    }
                }
                List<String> values = new ArrayList<>();
                for (Datum datum : row.data()) {
                    values.add(datum.varCharValue());
                }
                valuesList.add(values);
            }
        }
        return valuesList;
    }
}
