package com.ibm.airlytics.consumer.dsr;

import org.json.JSONObject;

import java.util.List;

public class DSRResponse {
    private DSRRequest request;
    private JSONObject dbData;
    private List<JSONObject> events;

    public DSRResponse(DSRRequest request, JSONObject responseData, List<JSONObject> events) {
        this.request = request;
        this.dbData = responseData;
        this.events = events;
    }

    public DSRRequest getRequest() {
        return request;
    }

    public JSONObject getDbData() {
        return dbData;
    }


    public List<JSONObject> getEvents() {
        return events;
    }
}
