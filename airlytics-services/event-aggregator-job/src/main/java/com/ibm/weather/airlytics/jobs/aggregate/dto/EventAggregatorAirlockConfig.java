package com.ibm.weather.airlytics.jobs.aggregate.dto;

import com.ibm.weather.airlytics.common.dto.AthenaAirlockConfig;

import java.time.LocalDate;
import java.util.List;

public class EventAggregatorAirlockConfig extends AthenaAirlockConfig {

    private String eventName = "ad-impression";

    private String aggregatedValueJsonPath = "$.attributes.revenue";

    private String aggregatedValueType = "bigint";

    private String targetColumn = "revenue";

    private String dailyAggregationsS3Path;

    private String dailyAggregationsTable;

    private String historyStartDay = "2021-01-01";

    private boolean checkDataForLastDay = true;

    private boolean exportToCsv = true;// export, when finishing aggregations

    private boolean forceExportToCsv = false;// export at every run

    private String csvOutputBucket;

    private String csvOutputFolder;

    private String csvTablePrefix;

    private boolean importToUserDb = true;

    private String importTargetTable;

    private boolean devUsers = false;// import to prod schema by default

    private String airlockApiBaseUrl;

    private String airlockProductId;

    private List<SecondaryAggregationConfig> secondaryAggregations;

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getAggregatedValueJsonPath() {
        return aggregatedValueJsonPath;
    }

    public void setAggregatedValueJsonPath(String aggregatedValueJsonPath) {
        this.aggregatedValueJsonPath = aggregatedValueJsonPath;
    }

    public String getAggregatedValueType() {
        return aggregatedValueType;
    }

    public void setAggregatedValueType(String aggregatedValueType) {
        this.aggregatedValueType = aggregatedValueType;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

    public String getDailyAggregationsS3Path() {
        return dailyAggregationsS3Path;
    }

    public void setDailyAggregationsS3Path(String dailyAggregationsS3Path) {
        this.dailyAggregationsS3Path = dailyAggregationsS3Path;
    }

    public String getDailyAggregationsTable() {
        return dailyAggregationsTable;
    }

    public void setDailyAggregationsTable(String dailyAggregationsTable) {
        this.dailyAggregationsTable = dailyAggregationsTable;
    }

    public boolean isCheckDataForLastDay() {
        return checkDataForLastDay;
    }

    public void setCheckDataForLastDay(boolean checkDataForLastDay) {
        this.checkDataForLastDay = checkDataForLastDay;
    }

    public boolean isExportToCsv() {
        return exportToCsv;
    }

    public void setExportToCsv(boolean exportToCsv) {
        this.exportToCsv = exportToCsv;
    }

    public boolean isForceExportToCsv() {
        return forceExportToCsv;
    }

    public void setForceExportToCsv(boolean forceExportToCsv) {
        this.forceExportToCsv = forceExportToCsv;
    }

    public String getCsvOutputBucket() {
        return csvOutputBucket;
    }

    public void setCsvOutputBucket(String csvOutputBucket) {
        this.csvOutputBucket = csvOutputBucket;
    }

    public String getCsvOutputFolder() {
        return csvOutputFolder;
    }

    public void setCsvOutputFolder(String csvOutputFolder) {
        this.csvOutputFolder = csvOutputFolder;
    }

    public String getCsvTablePrefix() {
        return csvTablePrefix;
    }

    public void setCsvTablePrefix(String csvTablePrefix) {
        this.csvTablePrefix = csvTablePrefix;
    }

    public boolean isImportToUserDb() {
        return importToUserDb;
    }

    public void setImportToUserDb(boolean importToUserDb) {
        this.importToUserDb = importToUserDb;
    }

    public String getImportTargetTable() {
        return importTargetTable;
    }

    public void setImportTargetTable(String importTargetTable) {
        this.importTargetTable = importTargetTable;
    }

    public boolean isDevUsers() {
        return devUsers;
    }

    public void setDevUsers(boolean devUsers) {
        this.devUsers = devUsers;
    }

    public String getAirlockApiBaseUrl() {
        return airlockApiBaseUrl;
    }

    public void setAirlockApiBaseUrl(String airlockApiBaseUrl) {
        this.airlockApiBaseUrl = airlockApiBaseUrl;
    }

    public String getAirlockProductId() {
        return airlockProductId;
    }

    public void setAirlockProductId(String airlockProductId) {
        this.airlockProductId = airlockProductId;
    }

    public List<SecondaryAggregationConfig> getSecondaryAggregations() {
        return secondaryAggregations;
    }

    public void setSecondaryAggregations(List<SecondaryAggregationConfig> secondaryAggregations) {
        this.secondaryAggregations = secondaryAggregations;
    }

    public String getHistoryStartDay() {
        return historyStartDay;
    }

    public LocalDate getHistoryStartDayDate() {
        return LocalDate.parse(historyStartDay);
    }

    public void setHistoryStartDay(String historyStartDay) {
        this.historyStartDay = historyStartDay;
    }
}
