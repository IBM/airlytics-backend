package com.ibm.airlytics.consumer.purchase.events;

public class IllegalSubscriptionEventArguments extends Exception {
    private String eventName;

    public IllegalSubscriptionEventArguments(String eventName, Throwable e) {
        super(e);
        this.eventName = eventName;
    }

    public String getMessage() {
        return "Creation [" + eventName + "] failed" + super.getMessage();
    }

    public String getEventName() {
        return eventName;
    }
}
