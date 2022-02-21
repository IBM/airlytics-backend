package com.ibm.airlytics.consumer.purchase.android;

import java.util.List;

public class GoogleAPIResponseError {
    public int code;
    public List<Error> errors;
    public String message;

    static public class Error {
        public String domain;
        public String message;
        public String reason;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public List<Error> getErrors() {
        return errors;
    }

    public void setErrors(List<Error> errors) {
        this.errors = errors;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
