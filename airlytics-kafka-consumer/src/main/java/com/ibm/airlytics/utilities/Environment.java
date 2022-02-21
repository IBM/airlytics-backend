package com.ibm.airlytics.utilities;

public class Environment {
    public static String getAlphanumericEnv(String parameterName, boolean allowNull) throws IllegalArgumentException {
        String paramVal = System.getenv(parameterName);
        if (paramVal == null && allowNull) {
                return null;
        }

        if (paramVal == null || paramVal.isEmpty()) {
            throw new IllegalArgumentException("Environment parameter " + parameterName + " is missing");
        }
        if (!paramVal.matches("[A-Za-z0-9 ]+")) {
            throw new IllegalArgumentException("Environment parameter " + parameterName + " is not alphanumeric");
        }
        return paramVal;
    }

    public static String getNumericEnv(String parameterName, boolean allowNull) throws IllegalArgumentException {
        String paramVal = System.getenv(parameterName);
        if (paramVal == null && allowNull) {
            return null;
        }

        if (paramVal == null || paramVal.isEmpty()) {
            throw new IllegalArgumentException("Environment parameter " + parameterName + " is missing");
        }
        if (!paramVal.matches("[0-9]+")) {
            throw new IllegalArgumentException("Environment parameter " + parameterName + " is not numeric");
        }
        return paramVal;
    }

    public static String getEnv(String parameterName, boolean allowNull) throws IllegalArgumentException {
        String paramVal = System.getenv(parameterName);
        if (paramVal == null && allowNull) {
            return null;
        }

        if (paramVal == null || paramVal.isEmpty()) {
            throw new IllegalArgumentException("Environment parameter " + parameterName + " is missing");
        }

        return paramVal;
    }
}
