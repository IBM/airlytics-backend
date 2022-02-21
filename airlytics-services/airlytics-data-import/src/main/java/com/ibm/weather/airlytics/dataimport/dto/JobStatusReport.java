package com.ibm.weather.airlytics.dataimport.dto;

import java.util.List;

public class JobStatusReport {

    public enum JobStatus { PENDING, RUNNING, FAILED, COMPLETED }

    private JobStatus status;
    private String statusMessage;
    private String detailedMessage;
    private Integer successfulImports;
    private List<String> affectedColumns;

    public JobStatusReport() {
    }

    public JobStatusReport(JobStatus status, String statusMessage, String detailedMessage, Integer successfulImports, List<String> affectedColumns) {
        this.status = status;
        this.statusMessage = statusMessage;
        this.detailedMessage = detailedMessage;
        this.successfulImports = successfulImports;
        this.affectedColumns = affectedColumns;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public void setDetailedMessage(String detailedMessage) {
        this.detailedMessage = detailedMessage;
    }

    public Integer getSuccessfulImports() {
        return successfulImports;
    }

    public void setSuccessfulImports(Integer successfulImports) {
        this.successfulImports = successfulImports;
    }

    public List<String> getAffectedColumns() {
        return affectedColumns;
    }

    public void setAffectedColumns(List<String> affectedColumns) {
        this.affectedColumns = affectedColumns;
    }
}
