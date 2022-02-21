package com.ibm.airlytics.consumer.dsr.retriever;

import com.ibm.airlytics.utilities.logs.AirlyticsLogger;

import java.util.HashMap;

public class FieldConfig {
    private static final AirlyticsLogger logger = AirlyticsLogger.getLogger(FieldConfig.class.getName());

    public enum FIELD_TYPE {
        STRING,
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        JSON,
        ARRAY,
        NUMBER
    }

    public FieldConfig(String type, String isNullable) {
        if (isNullable != null && isNullable.toLowerCase().equals("no")) {
            setRequired(true);
        } else {
            setRequired(false);
        }

        FIELD_TYPE fType = FIELD_TYPE.STRING; //default
        if (type != null) {
            if (type.startsWith("character") || type.startsWith("varchar")) {
                fType = FIELD_TYPE.STRING;
            } else if (type.startsWith("boolean")|| type.startsWith("bool")) {
                fType = FIELD_TYPE.BOOLEAN;
            } else if (type.startsWith("timestamp") || type.startsWith("date")) {
                fType = FIELD_TYPE.LONG;
            } else if (type.startsWith("int") || type.startsWith("bigint")) {
                fType = FIELD_TYPE.INTEGER;
            } else if (type.startsWith("float")) {
                fType = FIELD_TYPE.FLOAT;
            } else if (type.startsWith("double")) {
                fType = FIELD_TYPE.DOUBLE;
            } else if (type.toLowerCase().startsWith("array") || type.startsWith("_")) {
                fType = FIELD_TYPE.ARRAY;
            } else if (type.toLowerCase().startsWith("json")) {
                fType = FIELD_TYPE.JSON;
            }else if (type.toLowerCase().startsWith("long")) {
                fType = FIELD_TYPE.NUMBER;
            } else {
                logger.error("unknown data type: "+fType);
            }
        }
        setType(fType);
    }
    private FIELD_TYPE type;
    private boolean required;
    private HashMap<String, FieldConfig> subFields;

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

    public HashMap<String, FieldConfig> getSubFields() {
        return subFields;
    }

    public void setSubFields(HashMap<String, FieldConfig> subFields) {
        this.subFields = subFields;
    }
}
