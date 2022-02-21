package com.ibm.airlytics.retentiontrackerqueryhandler.db.athena;

import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Row;

import java.util.List;

public class AthenaQueryResult {
    public List<ColumnInfo> getColumnInfoList() {
        return columnInfoList;
    }

    public void setColumnInfoList(List<ColumnInfo> columnInfoList) {
        this.columnInfoList = columnInfoList;
    }

    public List<Row> getResults() {
        return results;
    }

    public void setResults(List<Row> results) {
        this.results = results;
    }

    List<ColumnInfo> columnInfoList;
    List<Row> results;
}
