package com.ibm.weather.airlytics.cohorts.dto;

import java.util.Date;

public class CohortExportConfig {

    public static final String DB_ONLY_EXPORT = "DB Only";
    public static final String UPS_EXPORT = "UPS";

    private String exportName;
    private String exportStatus;
    private String exportStatusMessage;
    private JobStatusDetails exportStatusDetails;
    private Date lastExportTime;

    public String getExportName() {
        return exportName;
    }

    public void setExportName(String exportName) {
        this.exportName = exportName;
    }

    public String getExportStatus() {
        return exportStatus;
    }

    public void setExportStatus(String exportStatus) {
        this.exportStatus = exportStatus;
    }

    public String getExportStatusMessage() {
        return exportStatusMessage;
    }

    public void setExportStatusMessage(String exportStatusMessage) {
        this.exportStatusMessage = exportStatusMessage;
    }

    public JobStatusDetails getExportStatusDetails() {
        return exportStatusDetails;
    }

    public void setExportStatusDetails(JobStatusDetails exportStatusDetails) {
        this.exportStatusDetails = exportStatusDetails;
    }

    public Date getLastExportTime() {
        return lastExportTime;
    }

    public void setLastExportTime(Date lastExportTime) {
        this.lastExportTime = lastExportTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CohortExportConfig that = (CohortExportConfig) o;

        if (exportName != null ? !exportName.equals(that.exportName) : that.exportName != null) return false;
        if (exportStatus != null ? !exportStatus.equals(that.exportStatus) : that.exportStatus != null) return false;
        if (exportStatusMessage != null ? !exportStatusMessage.equals(that.exportStatusMessage) : that.exportStatusMessage != null)
            return false;
        if (exportStatusDetails != null ? !exportStatusDetails.equals(that.exportStatusDetails) : that.exportStatusDetails != null)
            return false;
        return lastExportTime != null ? lastExportTime.equals(that.lastExportTime) : that.lastExportTime == null;
    }

    @Override
    public int hashCode() {
        int result = (exportName != null ? exportName.hashCode() : 0);
        result = 31 * result + (exportStatus != null ? exportStatus.hashCode() : 0);
        result = 31 * result + (exportStatusMessage != null ? exportStatusMessage.hashCode() : 0);
        result = 31 * result + (exportStatusDetails != null ? exportStatusDetails.hashCode() : 0);
        result = 31 * result + (lastExportTime != null ? lastExportTime.hashCode() : 0);
        return result;
    }
}
