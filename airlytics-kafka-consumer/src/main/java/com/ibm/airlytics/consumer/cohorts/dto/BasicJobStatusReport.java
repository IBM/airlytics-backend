package com.ibm.airlytics.consumer.cohorts.dto;

public class BasicJobStatusReport {

    public enum JobStatus { PENDING, RUNNING, FAILED, COMPLETED }

    private JobStatus status;
    private String statusMessage;
    private Long usersNumber = null;

    public BasicJobStatusReport() {
    }

    public BasicJobStatusReport(JobStatus status, String statusMessage, Long usersNumber) {
        this.status = status;
        this.statusMessage = statusMessage;
        this.usersNumber = usersNumber;
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

    public Long getUsersNumber() {
        return usersNumber;
    }

    public void setUsersNumber(Long usersNumber) {
        this.usersNumber = usersNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BasicJobStatusReport that = (BasicJobStatusReport) o;

        if (status != that.status) return false;
        if (statusMessage != null ? !statusMessage.equals(that.statusMessage) : that.statusMessage != null)
            return false;
        return usersNumber != null ? usersNumber.equals(that.usersNumber) : that.usersNumber == null;
    }

    @Override
    public int hashCode() {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (statusMessage != null ? statusMessage.hashCode() : 0);
        result = 31 * result + (usersNumber != null ? usersNumber.hashCode() : 0);
        return result;
    }
}
