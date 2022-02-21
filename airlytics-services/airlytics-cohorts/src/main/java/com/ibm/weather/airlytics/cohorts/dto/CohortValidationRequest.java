package com.ibm.weather.airlytics.cohorts.dto;

import java.util.List;

public class CohortValidationRequest {

    private String productId;
    private List<String> joinedTables;
    private String queryCondition;
    private String queryAdditionalValue;
    private String exportType;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public List<String> getJoinedTables() {
        return joinedTables;
    }

    public void setJoinedTables(List<String> joinedTables) {
        this.joinedTables = joinedTables;
    }

    public String getQueryCondition() {
        return queryCondition;
    }

    public void setQueryCondition(String queryCondition) {
        this.queryCondition = queryCondition;
    }

    public String getQueryAdditionalValue() {
        return queryAdditionalValue;
    }

    public void setQueryAdditionalValue(String queryAdditionalValue) {
        this.queryAdditionalValue = queryAdditionalValue;
    }

    public String getExportType() {
        return exportType;
    }

    public void setExportType(String exportType) {
        this.exportType = exportType;
    }
}
