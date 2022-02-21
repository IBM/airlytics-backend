package com.ibm.weather.airlytics.braze.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class BrazeTrackingResponse {

    private int code;

    private String message;

    private Integer attributes_processed;

    private Integer events_processed;

    private Integer purchases_processed;

    private List<JsonNode> errors;

    public BrazeTrackingResponse() {
    }

    public BrazeTrackingResponse(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Integer getAttributes_processed() {
        return attributes_processed;
    }

    public void setAttributes_processed(Integer attributes_processed) {
        this.attributes_processed = attributes_processed;
    }

    public Integer getEvents_processed() {
        return events_processed;
    }

    public void setEvents_processed(Integer events_processed) {
        this.events_processed = events_processed;
    }

    public Integer getPurchases_processed() {
        return purchases_processed;
    }

    public void setPurchases_processed(Integer purchases_processed) {
        this.purchases_processed = purchases_processed;
    }

    public List<JsonNode> getErrors() {
        return errors;
    }

    public void setErrors(List<JsonNode> errors) {
        this.errors = errors;
    }
}
