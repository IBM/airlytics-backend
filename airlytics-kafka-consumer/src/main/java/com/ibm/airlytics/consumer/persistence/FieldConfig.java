package com.ibm.airlytics.consumer.persistence;

import com.amazonaws.thirdparty.jackson.annotation.JsonIgnoreProperties;

public class FieldConfig {
    public enum FIELD_TYPE {
        STRING,
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        JSON_STRING
    }

    private FIELD_TYPE type;
    private boolean required;

    //optional field.
    // If not specified:
    // For common field the parquet field is the name of the record field
    // For event field the parquet field is <eventName>.<recordField>
    @JsonIgnoreProperties(ignoreUnknown = true)
    private String parquetField;

    //if not specified default is to save
    @JsonIgnoreProperties(ignoreUnknown = true)
    private boolean doNotSave;


    //if not specified default is not personalInformation
    @JsonIgnoreProperties(ignoreUnknown = true)
    private boolean personalInformation;

     //if not specified default is not piMarker
    @JsonIgnoreProperties(ignoreUnknown = true)
    private boolean piMarker;

    public FieldConfig() {
    }

    public FieldConfig(FIELD_TYPE type, boolean required, String parquetField, boolean doNotSave, boolean personalInformation, boolean piMarker) {
        this.type = type;
        this.required = required;
        this.parquetField = parquetField;
        this.doNotSave = doNotSave;
        this.personalInformation = personalInformation;
        this.piMarker = piMarker;
    }

    public FIELD_TYPE getType() {
        return type;
    }

    public void setType(FIELD_TYPE type) {
        this.type = type;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getParquetField() {
        return parquetField;
    }

    public void setParquetField(String parquetField) {
        this.parquetField = parquetField;
    }

    public boolean getDoNotSave() {
        return doNotSave;
    }

    public void setDoNotSave(boolean doNotSave) {
        this.doNotSave = doNotSave;
    }

    public boolean isPersonalInformation() {
        return personalInformation;
    }

    public void setPersonalInformation(boolean personalInformation) {
        this.personalInformation = personalInformation;
    }

    public boolean isPiMarker() {
        return piMarker;
    }

    public void setPiMarker(boolean piMarker) {
        this.piMarker = piMarker;
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("type = " + type.toString());
        sb.append("\nrequired = " + required);
        sb.append("\nparquetField = " + parquetField);
        sb.append("\ndoNotSave = " + doNotSave);
        sb.append("\npersonalInformation = " + personalInformation);
        sb.append("\npiMarker = " + piMarker);
        return sb.toString();
    }
}
