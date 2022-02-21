package com.ibm.weather.airlytics.amplitude.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.util.List;
import java.util.function.BiConsumer;

@Component
public class AthenaDriver {

    private static final Logger logger = LoggerFactory.getLogger(AthenaDriver.class);

    public static final long SLEEP_AMOUNT_IN_MS = 1000;

    @Value("${athena.region:}")
    protected String athenaRegion;

    @Value("${athena.results.output.bucket:}")
    protected String athenaResultsOutputBucket;

    @Value("${athena.catalog:}")
    protected String athenaCatalog;

    @Value("${athena.db:}")
    protected String athenaDb;

    public AthenaClient getAthenaClient() throws Exception {
        return AthenaClient.builder()
                .region(getRegion())
                .build();
    }

    // Submits a sample query to Amazon Athena and returns the execution ID of the query
    public String submitAthenaQuery(
            String query,
            AthenaClient athenaClient) throws AthenaException {

        try {
            // The QueryExecutionContext allows us to set the database
            QueryExecutionContext queryExecutionContext = null;

            if (athenaDb != null) {
                queryExecutionContext = QueryExecutionContext.builder()
                        .catalog(athenaCatalog)
                        .database(athenaDb).build();
            }
            else {
                queryExecutionContext = QueryExecutionContext.builder().catalog(athenaCatalog).build();
            }

            // The result configuration specifies where the results of the query should go
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                    .outputLocation("s3://" + athenaResultsOutputBucket + "/")
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(query)
                    .queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration)
                    .build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();
        } catch (AthenaException e) {
            throw e;
        }
    }

    // Wait for an Amazon Athena query to complete, fail or to be cancelled
    public QueryExecutionState waitForQueryToComplete(String queryExecutionId, AthenaClient athenaClient) {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;

        while (true) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            QueryExecutionState queryState = getQueryExecutionResponse.queryExecution().status().state();

            switch (queryState) {
                case FAILED:
                    logger.error(
                            "Query Execution {} has failed: {}",
                            queryExecutionId,
                            getQueryExecutionResponse
                                    .queryExecution()
                                    .status()
                                    .stateChangeReason());
                    return queryState;
                case CANCELLED:
                    logger.error("Query Execution {} has been cancelled", queryExecutionId);
                    return queryState;
                case SUCCEEDED:
                    return queryState;
                default:
                    try {
                        Thread.sleep(SLEEP_AMOUNT_IN_MS);
                    } catch (InterruptedException e) {
                        break;
                    }
            }
        }
    }

    // This code retrieves the results of a query
    public void processResultRows(
            String queryExecutionId,
            AthenaClient athenaClient,
            BiConsumer<List<Row>, List<ColumnInfo>> processRow) throws AthenaException {

        try {
            // Max Results can be set but if its not set,
            // it will choose the maximum page size
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);

            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                List<Row> results = result.resultSet().rows();
                processRow.accept(results, columnInfoList);
            }

        } catch (AthenaException e) {
            throw e;
        }
    }

    private Region getRegion() throws Exception {

        if (athenaRegion.equalsIgnoreCase("eu-west-1")) {
            return Region.EU_WEST_1;
        }
        else if (athenaRegion.equalsIgnoreCase("us-east-1")) {
            return Region.US_EAST_1;
        }
        else {
            throw new Exception("unsupported athena region: " + athenaRegion + ". Currently only eu-west-1 and us-east-1 are supported");
        }
    }
}
