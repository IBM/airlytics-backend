package com.ibm.airlytics.consumer.cohorts.dto;

import java.time.Instant;
import java.util.List;

public class UserCohort {

    private String eventId;
    private String userId;
    private String upsId;
    private String productId;
    private String cohortId;
    private String cohortName;
    private String cohortValue;
    private ValueType valueType;
    private boolean pendingDeletion = false;
    private boolean pendingExport = true;
    private Instant createdAt;

    private List<UserCohortExport> enabledExports; // only used when sending it to Kafka

    public UserCohort() {
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUpsId() {
        return upsId;
    }

    public void setUpsId(String upsId) {
        this.upsId = upsId;
    }

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

    public String getCohortName() { return cohortName; }

    public void setCohortName(String cohortName) { this.cohortName = cohortName; }

    public String getCohortValue() { return cohortValue; }

    public void setCohortValue(String cohortValue) { this.cohortValue = cohortValue; }

    public ValueType getValueType() {
        return valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public boolean isPendingDeletion() {
        return pendingDeletion;
    }

    public void setPendingDeletion(boolean pendingDeletion) {
        this.pendingDeletion = pendingDeletion;
    }

    public boolean isPendingExport() { return pendingExport; }

    public void setPendingExport(boolean pendingExport) { this.pendingExport = pendingExport; }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public List<UserCohortExport> getEnabledExports() {
        return enabledExports;
    }

    public void setEnabledExports(List<UserCohortExport> enabledExports) {
        this.enabledExports = enabledExports;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserCohort that = (UserCohort) o;

        if (pendingDeletion != that.pendingDeletion) return false;
        if (pendingExport != that.pendingExport) return false;
        if (eventId != null ? !eventId.equals(that.eventId) : that.eventId != null) return false;
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        if (upsId != null ? !upsId.equals(that.upsId) : that.upsId != null) return false;
        if (productId != null ? !productId.equals(that.productId) : that.productId != null) return false;
        if (cohortId != null ? !cohortId.equals(that.cohortId) : that.cohortId != null) return false;
        if (cohortName != null ? !cohortName.equals(that.cohortName) : that.cohortName != null) return false;
        if (cohortValue != null ? !cohortValue.equals(that.cohortValue) : that.cohortValue != null) return false;
        if (valueType != that.valueType) return false;
        if (createdAt != null ? !createdAt.equals(that.createdAt) : that.createdAt != null) return false;
        return enabledExports != null ? enabledExports.equals(that.enabledExports) : that.enabledExports == null;
    }

    @Override
    public int hashCode() {
        int result = eventId != null ? eventId.hashCode() : 0;
        result = 31 * result + (userId != null ? userId.hashCode() : 0);
        result = 31 * result + (upsId != null ? upsId.hashCode() : 0);
        result = 31 * result + (productId != null ? productId.hashCode() : 0);
        result = 31 * result + (cohortId != null ? cohortId.hashCode() : 0);
        result = 31 * result + (cohortName != null ? cohortName.hashCode() : 0);
        result = 31 * result + (cohortValue != null ? cohortValue.hashCode() : 0);
        result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
        result = 31 * result + (pendingDeletion ? 1 : 0);
        result = 31 * result + (pendingExport ? 1 : 0);
        result = 31 * result + (createdAt != null ? createdAt.hashCode() : 0);
        result = 31 * result + (enabledExports != null ? enabledExports.hashCode() : 0);
        return result;
    }
}
