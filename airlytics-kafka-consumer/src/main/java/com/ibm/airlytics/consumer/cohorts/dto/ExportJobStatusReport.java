package com.ibm.airlytics.consumer.cohorts.dto;

public class ExportJobStatusReport extends BasicJobStatusReport {

    private String exportKey; // Localytics, Amplitude...
    private JobStatusDetails airlyticsStatusDetails;
    private JobStatusDetails thirdPartyStatusDetails;

    public ExportJobStatusReport() {
        super();
    }

    public ExportJobStatusReport(String exportKey, JobStatus status, String statusMessage) {
        super(status, statusMessage, null);
        this.exportKey = exportKey;
    }

    public ExportJobStatusReport(String exportKey, JobStatus status, String statusMessage, Long usersNumber) {
        super(status, statusMessage, usersNumber);
        this.exportKey = exportKey;
    }

    public String getExportKey() {
        return exportKey;
    }

    public void setExportKey(String exportKey) {
        this.exportKey = exportKey;
    }

    public JobStatusDetails getAirlyticsStatusDetails() {
        return airlyticsStatusDetails;
    }

    public void setAirlyticsStatusDetails(JobStatusDetails airlyticsStatusDetails) {
        this.airlyticsStatusDetails = airlyticsStatusDetails;
    }

    public JobStatusDetails getThirdPartyStatusDetails() {
        return thirdPartyStatusDetails;
    }

    public void setThirdPartyStatusDetails(JobStatusDetails thirdPartyStatusDetails) {
        this.thirdPartyStatusDetails = thirdPartyStatusDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ExportJobStatusReport that = (ExportJobStatusReport) o;

        if (exportKey != null ? !exportKey.equals(that.exportKey) : that.exportKey != null) return false;
        if (airlyticsStatusDetails != null ? !airlyticsStatusDetails.equals(that.airlyticsStatusDetails) : that.airlyticsStatusDetails != null)
            return false;
        return thirdPartyStatusDetails != null ? thirdPartyStatusDetails.equals(that.thirdPartyStatusDetails) : that.thirdPartyStatusDetails == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (exportKey != null ? exportKey.hashCode() : 0);
        result = 31 * result + (airlyticsStatusDetails != null ? airlyticsStatusDetails.hashCode() : 0);
        result = 31 * result + (thirdPartyStatusDetails != null ? thirdPartyStatusDetails.hashCode() : 0);
        return result;
    }
}
