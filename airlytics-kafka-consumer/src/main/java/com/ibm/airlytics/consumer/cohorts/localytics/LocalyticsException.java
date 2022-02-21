package com.ibm.airlytics.consumer.cohorts.localytics;

public class LocalyticsException extends Exception {

    public LocalyticsException(String message) {
        super(message);
    }

    public LocalyticsException(String message, Throwable cause) {
        super(message, cause);
    }

    public LocalyticsException(Throwable cause) {
        super(cause);
    }
}
