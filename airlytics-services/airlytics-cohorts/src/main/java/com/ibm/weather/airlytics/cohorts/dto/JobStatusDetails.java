package com.ibm.weather.airlytics.cohorts.dto;

public class JobStatusDetails {

    private BasicJobStatusReport.JobStatus status;
    private String activityId;
    private String detailedMessage;
    private Integer pendingImports;
    private Integer successfulImports;
    private Integer parsedImports;
    private Integer failedImports;
    private Integer totalImports;

    public JobStatusDetails() {
    }

    public JobStatusDetails(String activityId) {
        this.activityId = activityId;
    }

    public JobStatusDetails(String activityId, String detailedMessage) {
        this.activityId = activityId;
        this.detailedMessage = detailedMessage;
    }

    public BasicJobStatusReport.JobStatus getStatus() {
        return status;
    }

    public void setStatus(BasicJobStatusReport.JobStatus status) {
        this.status = status;
    }

    public String getActivityId() {
        return activityId;
    }

    public void setActivityId(String activityId) {
        this.activityId = activityId;
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public void setDetailedMessage(String detailedMessage) {
        this.detailedMessage = detailedMessage;
    }

    public Integer getPendingImports() {
        return pendingImports;
    }

    public void setPendingImports(Integer pendingImports) {
        this.pendingImports = pendingImports;
    }

    public Integer getSuccessfulImports() {
        return successfulImports;
    }

    public void setSuccessfulImports(Integer successfulImports) {
        this.successfulImports = successfulImports;
    }

    public Integer getParsedImports() {
        return parsedImports;
    }

    public void setParsedImports(Integer parsedImports) {
        this.parsedImports = parsedImports;
    }

    public Integer getFailedImports() {
        return failedImports;
    }

    public void setFailedImports(Integer failedImports) {
        this.failedImports = failedImports;
    }

    public Integer getTotalImports() {
        return totalImports;
    }

    public void setTotalImports(Integer totalImports) {
        this.totalImports = totalImports;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobStatusDetails that = (JobStatusDetails) o;

        if (status != that.status) return false;
        if (activityId != null ? !activityId.equals(that.activityId) : that.activityId != null) return false;
        if (detailedMessage != null ? !detailedMessage.equals(that.detailedMessage) : that.detailedMessage != null)
            return false;
        if (pendingImports != null ? !pendingImports.equals(that.pendingImports) : that.pendingImports != null)
            return false;
        if (successfulImports != null ? !successfulImports.equals(that.successfulImports) : that.successfulImports != null)
            return false;
        if (parsedImports != null ? !parsedImports.equals(that.parsedImports) : that.parsedImports != null)
            return false;
        if (failedImports != null ? !failedImports.equals(that.failedImports) : that.failedImports != null)
            return false;
        return totalImports != null ? totalImports.equals(that.totalImports) : that.totalImports == null;
    }

    @Override
    public int hashCode() {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (activityId != null ? activityId.hashCode() : 0);
        result = 31 * result + (detailedMessage != null ? detailedMessage.hashCode() : 0);
        result = 31 * result + (pendingImports != null ? pendingImports.hashCode() : 0);
        result = 31 * result + (successfulImports != null ? successfulImports.hashCode() : 0);
        result = 31 * result + (parsedImports != null ? parsedImports.hashCode() : 0);
        result = 31 * result + (failedImports != null ? failedImports.hashCode() : 0);
        result = 31 * result + (totalImports != null ? totalImports.hashCode() : 0);
        return result;
    }
}
