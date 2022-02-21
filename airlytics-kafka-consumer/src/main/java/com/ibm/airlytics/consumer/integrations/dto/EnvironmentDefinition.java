package com.ibm.airlytics.consumer.integrations.dto;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class EnvironmentDefinition {

    private boolean devEnvironment;

    private Map<String, List<String>> conditions;

    public boolean isDevEnvironment() {
        return devEnvironment;
    }

    public void setDevEnvironment(boolean devEnvironment) {
        this.devEnvironment = devEnvironment;
    }

    public Map<String, List<String>> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, List<String>> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", EnvironmentDefinition.class.getSimpleName() + "[", "]")
                .add("devEnvironment=" + devEnvironment)
                .add("conditions=" + conditions)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EnvironmentDefinition that = (EnvironmentDefinition) o;

        if (devEnvironment != that.devEnvironment) return false;
        return conditions != null ? conditions.equals(that.conditions) : that.conditions == null;
    }

    @Override
    public int hashCode() {
        int result = (devEnvironment ? 1 : 0);
        result = 31 * result + (conditions != null ? conditions.hashCode() : 0);
        return result;
    }
}
