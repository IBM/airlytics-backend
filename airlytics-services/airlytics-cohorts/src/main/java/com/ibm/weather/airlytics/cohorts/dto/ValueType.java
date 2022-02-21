package com.ibm.weather.airlytics.cohorts.dto;

import java.util.Arrays;
import java.util.Optional;

public enum ValueType {
    STRING("String"),
    BOOL("Boolean"),
    INT("Integer Number"),
    FLOAT("Floating Point Number"),
    DATE("Date"),
    ARRAY("Array"),
    JSON("JSON");

    private String label;

    ValueType(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public static Optional<ValueType> forName(String name) {
        if(name == null) {
            // default
            return Optional.of(STRING);
        }
        return Arrays.stream(ValueType.values()).filter(v -> v.name().equalsIgnoreCase(name)).findAny();
    }

    public static Optional<ValueType> forLabel(String label) {
        if(label == null) {
            // default
            return Optional.of(STRING);
        }
        return Arrays.stream(ValueType.values()).filter(v -> v.getLabel().equalsIgnoreCase(label)).findAny();
    }

    public static Optional<ValueType> forNameOrLabel(String nameOrLabel) {
        Optional<ValueType> valueType = forName(nameOrLabel);

        if(!valueType.isPresent()) {
            valueType = forLabel(nameOrLabel);
        }
        return valueType;
    }
}
