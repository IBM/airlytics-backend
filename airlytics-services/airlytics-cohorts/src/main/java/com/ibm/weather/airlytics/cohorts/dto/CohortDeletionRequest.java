package com.ibm.weather.airlytics.cohorts.dto;

import java.util.List;

public class CohortDeletionRequest {

    private String productId;
    private String cohortId;
    private boolean deleteFromThirdParties = true;
    private List<UserCohortExport> deletedExports; // delete from certain 3rd parties, but not from Air-Cohorts

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

    public boolean isDeleteFromThirdParties() {
        return deleteFromThirdParties;
    }

    public void setDeleteFromThirdParties(boolean deleteFromThirdParties) {
        this.deleteFromThirdParties = deleteFromThirdParties;
    }

    public List<UserCohortExport> getDeletedExports() {
        return deletedExports;
    }

    public void setDeletedExports(List<UserCohortExport> deletedExports) {
        this.deletedExports = deletedExports;
    }
}
