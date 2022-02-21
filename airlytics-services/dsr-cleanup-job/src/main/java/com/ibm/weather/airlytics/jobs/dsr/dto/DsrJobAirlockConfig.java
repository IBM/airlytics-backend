package com.ibm.weather.airlytics.jobs.dsr.dto;

import com.ibm.weather.airlytics.common.dto.AthenaAirlockConfig;
import com.ibm.weather.airlytics.common.dto.BasicAthenaConfig;

import java.time.LocalDate;
import java.util.List;

public class DsrJobAirlockConfig extends BasicAthenaConfig {

    private String markersBaseFolder;

    private String markersArchiveFolder;

    private String archivedPartAfter;

    private String tempFolder = "dsr-temp";

    private String targetS3Bucket;

    private boolean backupOriginal = false;

    private List<PiTableConfig> cleanupTables;

    public String getMarkersBaseFolder() {
        return markersBaseFolder;
    }

    public void setMarkersBaseFolder(String markersBaseFolder) {
        this.markersBaseFolder = markersBaseFolder;
    }

    public String getMarkersArchiveFolder() {
        return markersArchiveFolder;
    }

    public void setMarkersArchiveFolder(String markersArchiveFolder) {
        this.markersArchiveFolder = markersArchiveFolder;
    }

    public String getArchivedPartAfter() {
        return archivedPartAfter;
    }

    public void setArchivedPartAfter(String archivedPartAfter) {
        this.archivedPartAfter = archivedPartAfter;
    }

    public String getTempFolder() {
        return tempFolder;
    }

    public void setTempFolder(String tempFolder) {
        this.tempFolder = tempFolder;
    }

    public String getTargetS3Bucket() {
        return targetS3Bucket;
    }

    public void setTargetS3Bucket(String targetS3Bucket) {
        this.targetS3Bucket = targetS3Bucket;
    }

    public boolean isBackupOriginal() {
        return backupOriginal;
    }

    public void setBackupOriginal(boolean backupOriginal) {
        this.backupOriginal = backupOriginal;
    }

    public List<PiTableConfig> getCleanupTables() {
        return cleanupTables;
    }

    public void setCleanupTables(List<PiTableConfig> cleanupTables) {
        this.cleanupTables = cleanupTables;
    }
}
