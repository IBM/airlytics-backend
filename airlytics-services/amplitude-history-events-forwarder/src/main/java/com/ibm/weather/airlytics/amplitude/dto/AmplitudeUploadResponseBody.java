package com.ibm.weather.airlytics.amplitude.dto;

public class AmplitudeUploadResponseBody {

    private int code;

    private int events_ingested;

    private int payload_size_bytes;

    private long server_upload_time;

    public AmplitudeUploadResponseBody() {
    }

    public AmplitudeUploadResponseBody(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getEvents_ingested() {
        return events_ingested;
    }

    public void setEvents_ingested(int events_ingested) {
        this.events_ingested = events_ingested;
    }

    public int getPayload_size_bytes() {
        return payload_size_bytes;
    }

    public void setPayload_size_bytes(int payload_size_bytes) {
        this.payload_size_bytes = payload_size_bytes;
    }

    public long getServer_upload_time() {
        return server_upload_time;
    }

    public void setServer_upload_time(long server_upload_time) {
        this.server_upload_time = server_upload_time;
    }
}
