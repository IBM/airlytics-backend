package com.ibm.airlytics.retentiontrackerqueryhandler.db;

import java.util.HashMap;

public class FieldConfig {


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

    public static FieldConfig forAthenaSnapshot(String type, String isNullable) {
        return new FieldConfig(type, isNullable, true);
    }
    public FieldConfig(String type, String isNullable) {
        this(type, isNullable, false);
    }
    public FieldConfig(String type, String isNullable, boolean isForAthena) {
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
                if (isForAthena) {
                    fType = FIELD_TYPE.STRING;
                } else {
                    fType = FIELD_TYPE.LONG;
                }
            } else if (type.startsWith("int")) {
                fType = FIELD_TYPE.INTEGER;
            } else if (type.startsWith("bigint")) {
                fType = FIELD_TYPE.NUMBER;
            } else if (type.startsWith("float")) {
                fType = FIELD_TYPE.FLOAT;
            } else if (type.startsWith("double")) {
                fType = FIELD_TYPE.DOUBLE;
            } else if (type.toLowerCase().startsWith("array")) {
                fType = FIELD_TYPE.ARRAY;
            } else if (type.toLowerCase().startsWith("json")) {
                //currently we handle JSON type as STRING
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
