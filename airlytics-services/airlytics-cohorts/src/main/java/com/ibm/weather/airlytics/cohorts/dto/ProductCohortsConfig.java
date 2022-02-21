package com.ibm.weather.airlytics.cohorts.dto;

import java.util.List;

public class ProductCohortsConfig {

    private String dbApplicationName;

    private List<CohortConfig> cohorts;

    public String getDbApplicationName() {
        return dbApplicationName;
    }

    public void setDbApplicationName(String dbApplicationName) {
        this.dbApplicationName = dbApplicationName;
    }

    public List<CohortConfig> getCohorts() {
        return cohorts;
    }

    public void setCohorts(List<CohortConfig> cohorts) {
        this.cohorts = cohorts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductCohortsConfig that = (ProductCohortsConfig) o;

        if (dbApplicationName != null ? !dbApplicationName.equals(that.dbApplicationName) : that.dbApplicationName != null)
            return false;
        return cohorts != null ? cohorts.equals(that.cohorts) : that.cohorts == null;
    }

    @Override
    public int hashCode() {
        int result = dbApplicationName != null ? dbApplicationName.hashCode() : 0;
        result = 31 * result + (cohorts != null ? cohorts.hashCode() : 0);
        return result;
    }
}
