package com.ibm.airlytics.retentiontrackerqueryhandler.events;

import java.util.LinkedList;

public class Events {
    LinkedList<UninstallDetectedEvent> events = new LinkedList<>();

    public LinkedList<UninstallDetectedEvent> getEvents() {
        return events;
    }

}
