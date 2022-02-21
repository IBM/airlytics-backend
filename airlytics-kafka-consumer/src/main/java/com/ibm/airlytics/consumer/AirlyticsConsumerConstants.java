package com.ibm.airlytics.consumer;

public class AirlyticsConsumerConstants {
    public static final String AWS_ACCESS_KEY_PARAM = "AWS_ACCESS_KEY_ID";
    public static final String AWS_ACCESS_SECRET_PARAM = "AWS_SECRET_ACCESS_KEY";
    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_ERROR = "error";
    public static final String ENV = "env";
    public static final String PRODUCT = "product";
    public static final String CONSUMER = "consumer";
    public static final String SOURCE = "source";
    public static final String OPERATION = "operation";

    public static final String PARQUET_MERGED_FILE_NAME = "events.snappy.parquet";
    public static final String PARQUET_FILE_EXTENSION = ".snappy.parquet";
    public static final String SCORE_FILE_PREFIX = "_score_";
    public static final String USER_TO_DELETE_FILE_PREFIX = "_user_";
    public static final String USER_TO_DELETE_DAY_MARKER = "_day_";
    public static final String DSR_REQUEST_ID_SCHEMA_FIELD = "requestId";
    public static final String DSR_RESULT_PREFIX = "part-";
    public static final String DSR_REQUEST_TYPE = "type";
    public static final String DSR_REQUEST_TYPE_PORTABILITY = "portability";
    public static final String DSR_REQUEST_TYPE_DELETE = "delete";
    public static final String DSR_REQUEST_RESULT = "result";
    public static final String DSR_REQUEST_RESULT_EXECUTED = "executed";
    public static final String DSR_REQUEST_RESULT_FAILED = "failed";
    public static final String DSR_REQUEST_RESULT_NOT_FOUND = "not found";
    public static final String REAL_TIME_ATTRIBUTE_UNKNOWN = "<unknown>";
    public static final String REAL_TIME_ATTRIBUTE_NULL = "<null>";
    public static final String EVENT_NAME_FIELD = "name";
    public static final String USER_ID_FIELD = "userId";
    public static final String REAL_TIME_SKIP_REASON = "reason";
    public static final String REAL_TIME_SKIP_REASON_NAME = "name not configured";
    public static final String REAL_TIME_SKIP_REASON_TIME = "time too far away";

    public static final String EVENT_ATTRIBUTES_FIELD = "attributes";
    public static final String EVENT_PREVIOUS_VALUES_FIELD = "previousValues";
    public static final String EVENT_CUSTOM_DIMENSIONS_FIELD = "customDimensions";
    public static final String SCHEMA_VERSION_FIELD_NAME = "schemaVersion";
    public static final String VERSION_DELIMITER="::";
    public static final String DEFAULT_VERSION="default";


    public static enum STORAGE_TYPE {
        S3,
        FILE_SYSTEM
    }

    public static enum INFORMATION_SOURCE {
        BASE,
        PI,
        SANS_PI
    }

    public static final String TIMESTAMP_FIELD_NAME = "receivedTime";
    public static final String OFFSET_FIELD_NAME = "internalOffset";
    public static final String NULL_VALUE_FIELDS_FIELD_NAME = "nullValueFields";
    public static final String WRITE_TIME_FIELD_NAME = "writeTime";
    public static final String SOURCE_FIELD_NAME = "source";
    public static final String DEVICE_TIME_FIELD_NAME = "deviceTime";
    public static final String ORIGINAL_EVENT_TIME_FIELD_NAME = "originalEventTime";
    public static final String DEVICE_TIME_HEADER_KEY = "x-current-device-time";
    public static final String ORIGINAL_EVENT_TIME_HEADER_KEY = "original-event-time";
}
