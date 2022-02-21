package com.ibm.weather.airlytics.cohorts.dto;

public class CohortRenameRequest {

    private String productId;
    private String cohortId;
    private String exportKey;
    private String oldExportName;
    private String newExportName;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCohortId() {
        return cohortId;
    }

    public void setCohortId(String cohortId) {
        this.cohortId = cohortId;
    }

    public String getExportKey() { return exportKey; }

    public void setExportKey(String exportKey) { this.exportKey = exportKey; }

    public String getOldExportName() {
        return oldExportName;
    }

    public void setOldExportName(String oldExportName) {
        this.oldExportName = oldExportName;
    }

    public String getNewExportName() {
        return newExportName;
    }

    public void setNewExportName(String newExportName) {
        this.newExportName = newExportName;
    }
}
