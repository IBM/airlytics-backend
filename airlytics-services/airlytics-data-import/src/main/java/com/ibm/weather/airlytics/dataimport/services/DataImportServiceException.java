package com.ibm.weather.airlytics.dataimport.services;

public class DataImportServiceException extends Exception {

    public DataImportServiceException(String message) {
        super(message);
    }

    public DataImportServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataImportServiceException(Throwable cause) {
        super(cause);
    }
}
