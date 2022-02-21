package com.ibm.analytics.queryservice.athena;

public class Constants {

    public static final int CLIENT_EXECUTION_TIMEOUT = 100000;
    public static final String ATHENA_OUTPUT_BUCKET = "s3://airlytics-athena-results/test-results"; //change the bucket name to match your environment
    public static final String ATHENA_SAMPLE_COUNT_QUERY = "select count(*) from  %s where day = %s";
    public static final String ATHENA_SAMPLE_EVENTS_COUNT_QUERY = "select appversion, name, count(*)  from %s where day = %s group by appversion, name";
    public static final long SLEEP_AMOUNT_IN_MS = 1000;
    public static final String ATHENA_DEFAULT_DATABASE = "airlytics";
    public static final String CATALOG = "AwsDataCatalog";
}
