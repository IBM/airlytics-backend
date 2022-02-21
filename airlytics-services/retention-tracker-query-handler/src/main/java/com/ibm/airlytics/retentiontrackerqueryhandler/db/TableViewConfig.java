package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import java.util.List;

public class TableViewConfig {
    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    private String idColumn;
    private List<String> columns;
}
