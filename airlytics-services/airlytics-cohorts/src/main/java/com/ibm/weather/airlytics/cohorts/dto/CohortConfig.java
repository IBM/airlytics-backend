package com.ibm.weather.airlytics.cohorts.dto;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class CohortConfig {

    private String productId;
    private String uniqueId;
    private String name;
    private String description;
    private Date creationDate;
    private String creator;
    private String queryCondition;
    private String queryAdditionalValue;
    private String valueType = ValueType.STRING.getLabel();
    private List<String> joinedTables;
    private String calculationStatus;
    private String calculationStatusMessage;
    private CohortCalculationFrequency updateFrequency;
    private Date lastModified;
    private Long usersNumber = null;
    private Integer retriesNumber = null;
    private Map<String, CohortExportConfig> exports;

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getCreator() { return creator; }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getQueryCondition() {
        return queryCondition;
    }

    public void setQueryCondition(String queryCondition) {
        this.queryCondition = queryCondition;
    }

    public String getQueryAdditionalValue() { return queryAdditionalValue; }

    public void setQueryAdditionalValue(String queryAdditionalValue) { this.queryAdditionalValue = queryAdditionalValue; }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public List<String> getJoinedTables() {
        return joinedTables;
    }

    public void setJoinedTables(List<String> joinedTables) {
        this.joinedTables = joinedTables;
    }

    public String getCalculationStatus() { return calculationStatus; }

    public void setCalculationStatus(String calculationStatus) { this.calculationStatus = calculationStatus; }

    public String getCalculationStatusMessage() {
        return calculationStatusMessage;
    }

    public void setCalculationStatusMessage(String calculationStatusMessage) {
        this.calculationStatusMessage = calculationStatusMessage;
    }

    public CohortCalculationFrequency getUpdateFrequency() {
        return updateFrequency;
    }

    public void setUpdateFrequency(CohortCalculationFrequency updateFrequency) { this.updateFrequency = updateFrequency; }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public Long getUsersNumber() { return usersNumber; }

    public void setUsersNumber(Long usersNumber) { this.usersNumber = usersNumber; }

    public Integer getRetriesNumber() {
        return retriesNumber;
    }

    public void setRetriesNumber(Integer retriesNumber) {
        this.retriesNumber = retriesNumber;
    }

    public Map<String, CohortExportConfig> getExports() {
        return exports;
    }

    public void setExports(Map<String, CohortExportConfig> exports) {
        this.exports = exports;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CohortConfig that = (CohortConfig) o;

        if (productId != null ? !productId.equals(that.productId) : that.productId != null) return false;
        if (uniqueId != null ? !uniqueId.equals(that.uniqueId) : that.uniqueId != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;
        if (creator != null ? !creator.equals(that.creator) : that.creator != null) return false;
        if (queryCondition != null ? !queryCondition.equals(that.queryCondition) : that.queryCondition != null)
            return false;
        if (queryAdditionalValue != null ? !queryAdditionalValue.equals(that.queryAdditionalValue) : that.queryAdditionalValue != null)
            return false;
        if (joinedTables != null ? !joinedTables.equals(that.joinedTables) : that.joinedTables != null) return false;
        if (calculationStatus != null ? !calculationStatus.equals(that.calculationStatus) : that.calculationStatus != null)
            return false;
        if (calculationStatusMessage != null ? !calculationStatusMessage.equals(that.calculationStatusMessage) : that.calculationStatusMessage != null)
            return false;
        if (updateFrequency != that.updateFrequency) return false;
        if (lastModified != null ? !lastModified.equals(that.lastModified) : that.lastModified != null) return false;
        if (usersNumber != null ? !usersNumber.equals(that.usersNumber) : that.usersNumber != null) return false;
        return exports != null ? exports.equals(that.exports) : that.exports == null;
    }

    @Override
    public int hashCode() {
        int result = productId != null ? productId.hashCode() : 0;
        result = 31 * result + (uniqueId != null ? uniqueId.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (creator != null ? creator.hashCode() : 0);
        result = 31 * result + (queryCondition != null ? queryCondition.hashCode() : 0);
        result = 31 * result + (queryAdditionalValue != null ? queryAdditionalValue.hashCode() : 0);
        result = 31 * result + (joinedTables != null ? joinedTables.hashCode() : 0);
        result = 31 * result + (calculationStatus != null ? calculationStatus.hashCode() : 0);
        result = 31 * result + (calculationStatusMessage != null ? calculationStatusMessage.hashCode() : 0);
        result = 31 * result + (updateFrequency != null ? updateFrequency.hashCode() : 0);
        result = 31 * result + (lastModified != null ? lastModified.hashCode() : 0);
        result = 31 * result + (usersNumber != null ? usersNumber.hashCode() : 0);
        result = 31 * result + (exports != null ? exports.hashCode() : 0);
        return result;
    }
}
