package com.ibm.airlytics.consumer.cohorts.airlock;

public class AirlockException extends Exception {

    public AirlockException(String message) {
        super(message);
    }

    public AirlockException(String message, Throwable cause) {
        super(message, cause);
    }

    public AirlockException(Throwable cause) {
        super(cause);
    }
}
