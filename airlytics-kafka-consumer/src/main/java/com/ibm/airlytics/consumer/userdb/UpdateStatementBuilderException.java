package com.ibm.airlytics.consumer.userdb;

public class UpdateStatementBuilderException extends Exception {
    public UpdateStatementBuilderException(String s) {
        super(s);
    }

    public UpdateStatementBuilderException(String s, Exception e) {
        super(s, e);
    }
}
