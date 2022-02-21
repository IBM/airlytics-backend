package com.ibm.weather.airlytics.jobs.aggregate.dto;

import java.util.StringJoiner;

public class SecondaryAggregationConfig {

    private int aggregationWindowDays;

    private String targetFolder;

    private String targetTable;

    public int getAggregationWindowDays() {
        return aggregationWindowDays;
    }

    public void setAggregationWindowDays(int aggregationWindowDays) {
        this.aggregationWindowDays = aggregationWindowDays;
    }

    public String getTargetFolder() {
        return targetFolder;
    }

    public void setTargetFolder(String targetFolder) {
        this.targetFolder = targetFolder;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SecondaryAggregationConfig.class.getSimpleName() + "[", "]")
                .add("aggregationWindowDays=" + aggregationWindowDays)
                .add("targetFolder='" + targetFolder + "'")
                .add("targetTable='" + targetTable + "'")
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondaryAggregationConfig that = (SecondaryAggregationConfig) o;

        if (aggregationWindowDays != that.aggregationWindowDays) return false;
        if (targetFolder != null ? !targetFolder.equals(that.targetFolder) : that.targetFolder != null) return false;
        return targetTable != null ? targetTable.equals(that.targetTable) : that.targetTable == null;
    }

    @Override
    public int hashCode() {
        int result = aggregationWindowDays;
        result = 31 * result + (targetFolder != null ? targetFolder.hashCode() : 0);
        result = 31 * result + (targetTable != null ? targetTable.hashCode() : 0);
        return result;
    }
}
