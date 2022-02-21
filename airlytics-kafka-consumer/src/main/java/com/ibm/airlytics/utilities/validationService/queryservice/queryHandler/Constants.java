package com.ibm.airlytics.utilities.validationService.queryservice.queryHandler;

public class Constants {
    public static final String SAMPLE_COUNT_QUERY = "select count(*) from  %s where day = %s";
    public static final String SAMPLE_EVENTS_COUNT_QUERY = "select appversion, name, count(*)  from %s where day = %s group by appversion, name";
    public static final long SLEEP_AMOUNT_IN_MS = 1000;
}
