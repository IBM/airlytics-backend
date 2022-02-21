package com.ibm.weather.airlytics.cohorts.dto;

public class UserCohortExport {

    private String exportType; // Localytics, Amplitude...
    private String exportFieldName; // see CohortExportConfig.exportName
    private String oldFieldName; // not null only when renaming

    public UserCohortExport() {
    }

    public UserCohortExport(String exportType, String exportFieldName) {
        this.exportType = exportType;
        this.exportFieldName = exportFieldName;
    }

    public UserCohortExport(String exportType, String exportFieldName, String oldFieldName) {
        this.exportType = exportType;
        this.exportFieldName = exportFieldName;
        this.oldFieldName = oldFieldName;
    }

    public String getExportType() {
        return exportType;
    }

    public void setExportType(String exportType) {
        this.exportType = exportType;
    }

    public String getExportFieldName() {
        return exportFieldName;
    }

    public void setExportFieldName(String exportFieldName) {
        this.exportFieldName = exportFieldName;
    }

    public String getOldFieldName() {
        return oldFieldName;
    }

    public void setOldFieldName(String oldFieldName) {
        this.oldFieldName = oldFieldName;
    }
}
