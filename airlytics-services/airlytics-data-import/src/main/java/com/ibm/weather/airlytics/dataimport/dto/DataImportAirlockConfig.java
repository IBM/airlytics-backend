package com.ibm.weather.airlytics.dataimport.dto;

import java.util.List;
import java.util.StringJoiner;

public class DataImportAirlockConfig {

    private String airlockApiBaseUrl;
    private boolean resumeInterruptedJobs = false;
    private String dbAwsRegion;
    private String tempBucketFolderPath;
    private boolean alwaysCopyToTemp = false;
    private boolean alwaysGetPut = false;
    private String productId;
    private List<String> featureTables;
    private List<String> piFeatureTables;
    private List<String> athenaImportTables;

    public String getAirlockApiBaseUrl() {
        return airlockApiBaseUrl;
    }

    public void setAirlockApiBaseUrl(String airlockApiBaseUrl) {
        this.airlockApiBaseUrl = airlockApiBaseUrl;
    }

    public boolean isResumeInterruptedJobs() {
        return resumeInterruptedJobs;
    }

    public void setResumeInterruptedJobs(boolean resumeInterruptedJobs) {
        this.resumeInterruptedJobs = resumeInterruptedJobs;
    }

    public String getDbAwsRegion() {
        return dbAwsRegion;
    }

    public void setDbAwsRegion(String dbAwsRegion) {
        this.dbAwsRegion = dbAwsRegion;
    }

    public String getTempBucketFolderPath() {
        return tempBucketFolderPath;
    }

    public void setTempBucketFolderPath(String tempBucketFolderPath) {
        this.tempBucketFolderPath = tempBucketFolderPath;
    }

    public boolean isAlwaysCopyToTemp() {
        return alwaysCopyToTemp;
    }

    public void setAlwaysCopyToTemp(boolean alwaysCopyToTemp) {
        this.alwaysCopyToTemp = alwaysCopyToTemp;
    }

    public boolean isAlwaysGetPut() {
        return alwaysGetPut;
    }

    public void setAlwaysGetPut(boolean alwaysGetPut) {
        this.alwaysGetPut = alwaysGetPut;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public List<String> getFeatureTables() {
        return featureTables;
    }

    public void setFeatureTables(List<String> featureTables) {
        this.featureTables = featureTables;
    }

    public List<String> getPiFeatureTables() {
        return piFeatureTables;
    }

    public void setPiFeatureTables(List<String> piFeatureTables) {
        this.piFeatureTables = piFeatureTables;
    }

    public List<String> getAthenaImportTables() {
        return athenaImportTables;
    }

    public void setAthenaImportTables(List<String> athenaImportTables) {
        this.athenaImportTables = athenaImportTables;
    }

    public boolean isAllowedTable(String name) {
        return (athenaImportTables != null && athenaImportTables.contains(name)) ||
                (featureTables != null && featureTables.contains(name)) ||
                (piFeatureTables != null && piFeatureTables.contains(name));
    }

    public boolean isFeatureTable(String name) {
        return (featureTables != null && featureTables.contains(name)) || (piFeatureTables != null && piFeatureTables.contains(name));
    }

    public boolean isPiTable(String name) {
        return piFeatureTables != null && piFeatureTables.contains(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataImportAirlockConfig that = (DataImportAirlockConfig) o;

        if (resumeInterruptedJobs != that.resumeInterruptedJobs) return false;
        if (alwaysCopyToTemp != that.alwaysCopyToTemp) return false;
        if (alwaysGetPut != that.alwaysGetPut) return false;
        if (airlockApiBaseUrl != null ? !airlockApiBaseUrl.equals(that.airlockApiBaseUrl) : that.airlockApiBaseUrl != null)
            return false;
        if (dbAwsRegion != null ? !dbAwsRegion.equals(that.dbAwsRegion) : that.dbAwsRegion != null) return false;
        if (tempBucketFolderPath != null ? !tempBucketFolderPath.equals(that.tempBucketFolderPath) : that.tempBucketFolderPath != null)
            return false;
        if (productId != null ? !productId.equals(that.productId) : that.productId != null) return false;
        if (featureTables != null ? !featureTables.equals(that.featureTables) : that.featureTables != null)
            return false;
        if (piFeatureTables != null ? !piFeatureTables.equals(that.piFeatureTables) : that.piFeatureTables != null)
            return false;
        return athenaImportTables != null ? athenaImportTables.equals(that.athenaImportTables) : that.athenaImportTables == null;
    }

    @Override
    public int hashCode() {
        int result = airlockApiBaseUrl != null ? airlockApiBaseUrl.hashCode() : 0;
        result = 31 * result + (resumeInterruptedJobs ? 1 : 0);
        result = 31 * result + (dbAwsRegion != null ? dbAwsRegion.hashCode() : 0);
        result = 31 * result + (tempBucketFolderPath != null ? tempBucketFolderPath.hashCode() : 0);
        result = 31 * result + (alwaysCopyToTemp ? 1 : 0);
        result = 31 * result + (alwaysGetPut ? 1 : 0);
        result = 31 * result + (productId != null ? productId.hashCode() : 0);
        result = 31 * result + (featureTables != null ? featureTables.hashCode() : 0);
        result = 31 * result + (piFeatureTables != null ? piFeatureTables.hashCode() : 0);
        result = 31 * result + (athenaImportTables != null ? athenaImportTables.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DataImportAirlockConfig.class.getSimpleName() + "[", "]")
                .add("airlockApiBaseUrl='" + airlockApiBaseUrl + "'")
                .add("resumeInterruptedJobs=" + resumeInterruptedJobs)
                .add("dbAwsRegion='" + dbAwsRegion + "'")
                .add("tempBucketFolderPath='" + tempBucketFolderPath + "'")
                .add("alwaysCopyToTemp=" + alwaysCopyToTemp)
                .add("alwaysGetPut=" + alwaysGetPut)
                .add("productId='" + productId + "'")
                .add("featureTables=" + featureTables)
                .add("piFeatureTables=" + piFeatureTables)
                .add("otherTables=" + athenaImportTables)
                .toString();
    }
}
