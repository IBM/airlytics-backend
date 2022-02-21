package com.ibm.weather.airlytics.dataimport.dto;

import java.util.Date;
import java.util.List;

public class DataImportConfig {

    private static final String OLD_TABLE = "user_features";

    private String productId;

    private String uniqueId;

    private String name;

    private Date creationDate;

    private String creator;

    private Date lastModified = null;

    private String description = null;

    private String s3File;

    private String targetTable = OLD_TABLE; // default value for backwords compatibilty

    private boolean devUsers = false;// import to prod schema by default

    // whether the data in the table should be replaced or updated
    private boolean overwrite = true;

    private JobStatusReport.JobStatus status;

    private String statusMessage;

    private String detailedMessage;

    private Integer successfulImports;

    private List<String> affectedColumns;

    public DataImportConfig() {
    }

    public DataImportConfig(String productId, String uniqueId, String s3File, String targetTable, boolean overwrite) {
        this.productId = productId;
        this.uniqueId = uniqueId;
        this.s3File = s3File;
        this.targetTable = targetTable;
        this.overwrite = overwrite;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
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
        return !OLD_TABLE.equals(getTargetTable()) && overwrite;// for backwords compatibilty
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public JobStatusReport.JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatusReport.JobStatus status) {
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getAffectedColumns() {
        return affectedColumns;
    }

    public void setAffectedColumns(List<String> affectedColumns) {
        this.affectedColumns = affectedColumns;
    }
}
