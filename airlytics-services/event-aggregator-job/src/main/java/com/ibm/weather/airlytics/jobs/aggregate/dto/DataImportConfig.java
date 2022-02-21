package com.ibm.weather.airlytics.jobs.aggregate.dto;

import java.util.List;

public class DataImportConfig {

    private String name;

    private String creator;

    private String description = null;

    private String s3File;

    private String targetTable; // default value for backwords compatibilty

    private boolean devUsers = false;// import to prod schema by default

    // whether the data in the table should be replaced or updated
    private boolean overwrite = true;

    private List<String> affectedColumns;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getS3File() {
        return s3File;
    }

    public void setS3File(String s3File) {
        this.s3File = s3File;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public boolean isDevUsers() {
        return devUsers;
    }

    public void setDevUsers(boolean devUsers) {
        this.devUsers = devUsers;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public List<String> getAffectedColumns() {
        return affectedColumns;
    }

    public void setAffectedColumns(List<String> affectedColumns) {
        this.affectedColumns = affectedColumns;
    }
}
