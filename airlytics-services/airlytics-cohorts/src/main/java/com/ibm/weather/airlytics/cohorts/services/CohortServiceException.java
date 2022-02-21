package com.ibm.weather.airlytics.cohorts.services;

public class CohortServiceException extends Exception {

    public CohortServiceException(String message) {
        super(message);
    }

    public CohortServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public CohortServiceException(Throwable cause) {
        super(cause);
    }
}
