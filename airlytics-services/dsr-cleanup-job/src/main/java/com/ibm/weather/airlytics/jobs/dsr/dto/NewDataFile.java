package com.ibm.weather.airlytics.jobs.dsr.dto;

public class NewDataFile {

    private String path;

    private PiTableConfig tableConfig;

    public String getPath() {
        return path;
    }

    public NewDataFile path(String path) {
        this.path = path;
        return this;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public PiTableConfig getTableConfig() {
        return tableConfig;
    }

    public NewDataFile tableConfig(PiTableConfig tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public void setTableConfig(PiTableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }
}
