package com.ibm.weather.airlytics.dataimport.dto;

import java.util.Date;
import java.util.List;

public class GetDataImportsResponse {

    private String productId;
    private Long pruneThreshold;
    private List<DataImportConfig> jobs;
    private Date lastModified;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Long getPruneThreshold() {
        return pruneThreshold;
    }

    public void setPruneThreshold(Long pruneThreshold) {
        this.pruneThreshold = pruneThreshold;
    }

    public List<DataImportConfig> getJobs() {
        return jobs;
    }

    public void setJobs(List<DataImportConfig> jobs) {
        this.jobs = jobs;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }
}
