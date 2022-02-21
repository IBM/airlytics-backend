package com.ibm.weather.airlytics.dataimport.dto;

import com.ibm.weather.airlytics.dataimport.db.UserFeaturesDao;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImportJobDefinition {

    private String jobId;

    private String file;

    private S3UriData s3Uri;

    private Map<String, String> columns;

    private String targetSchema;

    private String targetTable;

    private boolean overwrite;

    private boolean withHeader;

    public ImportJobDefinition(DataImportConfig request, String targetSchema, DataImportAirlockConfig config) {
        this.jobId = request.getUniqueId();
        this.file = request.getS3File();
        this.s3Uri = new S3UriData();
        this.targetSchema = request.isDevUsers() ? (targetSchema + "_dev") : targetSchema;
        this.targetTable = request.getTargetTable();
        this.overwrite = request.isOverwrite();
        this.withHeader =
                config.isFeatureTable(request.getTargetTable()) ||
                CollectionUtils.isEmpty(request.getAffectedColumns());
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public S3UriData getS3Uri() {
        return s3Uri;
    }

    public void setS3Uri(S3UriData s3Uri) {
        this.s3Uri = s3Uri;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(String targetSchema) {
        this.targetSchema = targetSchema;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public boolean isWithHeader() {
        return withHeader;
    }

    public void setWithHeader(boolean withHeader) {
        this.withHeader = withHeader;
    }

    public List<String> getAffectedColumnNames() {
        if(columns != null) {
            List<String> lst = new ArrayList<>(columns.keySet());
            lst.remove(UserFeaturesDao.USER_ID_COLUMN);
            return lst;
        }
        return null;
    }
}
