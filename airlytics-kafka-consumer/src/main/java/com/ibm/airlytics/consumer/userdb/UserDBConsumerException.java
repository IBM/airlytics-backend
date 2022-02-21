package com.ibm.airlytics.consumer.userdb;

public class UserDBConsumerException extends Exception {
    public UserDBConsumerException(String s) {
        super(s);
    }

    public UserDBConsumerException(String s, Exception e) {
        super(s, e);
    }
}
