package com.ibm.airlytics.utilities;


/**
 * The collection of mathematics method which are used by consumer
 * and doesn't exist in the standard lib
 */
public class MathUtils {

    /**
     * Rounds off the double value util n-position after dot
     * @param value  double value to round
     * @param position decimal digits to round off
     * @return
     */
    public static double roundOff(double value, int position) {
        double temp = java.lang.Math.pow(10.0, position);
        value *= temp;
        value = java.lang.Math.round(value);
        return (value / temp);
    }
}
