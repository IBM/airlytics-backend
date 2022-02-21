package com.ibm.weather.airlytics.amplitude.dto;

import java.util.List;

public class AmplitudeUploadRequestBody {

    private String api_key;

    private List<AmplitudeEvent> events;

    public AmplitudeUploadRequestBody() {
    }

    public AmplitudeUploadRequestBody(String api_key, List<AmplitudeEvent> events) {
        this.api_key = api_key;
        this.events = events;
    }

    public String getApi_key() {
        return api_key;
    }

    public void setApi_key(String api_key) {
        this.api_key = api_key;
    }

    public List<AmplitudeEvent> getEvents() {
        return events;
    }

    public void setEvents(List<AmplitudeEvent> events) {
        this.events = events;
    }
}
