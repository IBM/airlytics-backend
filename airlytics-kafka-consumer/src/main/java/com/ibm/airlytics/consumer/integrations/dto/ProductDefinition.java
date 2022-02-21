package com.ibm.airlytics.consumer.integrations.dto;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class ProductDefinition {

    private String id;
    private String platform;
    private String eventApiKey;
    private String devEventApiKey;
    private String schemaVersion = "1.0";
    private Map<String, List<String>> conditions;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public Map<String, List<String>> getConditions() {
        return conditions;
    }

    public void setConditions(Map<String, List<String>> conditions) {
        this.conditions = conditions;
    }

    public String getEventApiKey() {
        return eventApiKey;
    }

    public void setEventApiKey(String eventApiKey) {
        this.eventApiKey = eventApiKey;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getDevEventApiKey() {
        return devEventApiKey;
    }

    public void setDevEventApiKey(String devEventApiKey) {
        this.devEventApiKey = devEventApiKey;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ProductDefinition.class.getSimpleName() + "[", "]")
                .add("id='" + id + "'")
                .add("platform='" + platform + "'")
                .add("eventApiKey='" + eventApiKey + "'")
                .add("devEventApiKey='" + devEventApiKey + "'")
                .add("schemaVersion='" + schemaVersion + "'")
                .add("conditions=" + conditions)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProductDefinition that = (ProductDefinition) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (platform != null ? !platform.equals(that.platform) : that.platform != null) return false;
        if (eventApiKey != null ? !eventApiKey.equals(that.eventApiKey) : that.eventApiKey != null) return false;
        if (devEventApiKey != null ? !devEventApiKey.equals(that.devEventApiKey) : that.devEventApiKey != null)
            return false;
        if (schemaVersion != null ? !schemaVersion.equals(that.schemaVersion) : that.schemaVersion != null)
            return false;
        return conditions != null ? conditions.equals(that.conditions) : that.conditions == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (platform != null ? platform.hashCode() : 0);
        result = 31 * result + (eventApiKey != null ? eventApiKey.hashCode() : 0);
        result = 31 * result + (devEventApiKey != null ? devEventApiKey.hashCode() : 0);
        result = 31 * result + (schemaVersion != null ? schemaVersion.hashCode() : 0);
        result = 31 * result + (conditions != null ? conditions.hashCode() : 0);
        return result;
    }
}
