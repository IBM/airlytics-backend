package com.ibm.weather.airlytics.common.airlock;

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
