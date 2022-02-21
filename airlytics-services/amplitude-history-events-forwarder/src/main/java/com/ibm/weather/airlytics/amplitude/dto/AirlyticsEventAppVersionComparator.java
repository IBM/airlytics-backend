package com.ibm.weather.airlytics.amplitude.dto;

import java.util.Comparator;

public class AirlyticsEventAppVersionComparator implements Comparator<AirlyticsEvent> {

    @Override
    public int compare(AirlyticsEvent e1, AirlyticsEvent e2) {
        return compareVersions(e1.getAppVersion(), e2.getAppVersion());
    }

    /**
     * return res < 0 if s1<s2, 0 if s1===s2, res > 0 if s1>s2
     */
    public static int compareVersions(String s1, String s2) {
        if ((s1 == null || s1.equals("")) && (s2 == null || s2.equals("")))
            return 0;
        if (s1 == null || s1.equals(""))
            return 1;
        if (s2 == null || s2.equals(""))
            return -1;
        String[] s1Array = s1.split("\\.");
        String[] s2Array = s2.split("\\.");
        int numPartsToCompare = Math.min(s1Array.length, s2Array.length);
        for (int i = 0; i < numPartsToCompare; i++) {
            // try to compare numeric
            try {
                int s1Val = Integer.parseInt(s1Array[i]);
                int s2Val = Integer.parseInt(s2Array[i]);
                if (s1Val != s2Val) {
                    return s1Val - s2Val;
                }
            }
            // compare Strings
            catch (NumberFormatException e1) {
                String s1Val = s1Array[i];
                String s2Val = s2Array[i];
                if (s1Val.compareTo(s2Val) != 0) {
                    return s1Val.compareTo(s2Val);
                }
            }
        }
        if (s1Array.length > s2Array.length) {
            // check for the longer array, that all the nodes are negligible (0 or empty string)
            for (int j = numPartsToCompare; j < s1Array.length; j++) {
                if (!s1Array[j].equals("") && !s1Array[j].equals("0")) {
                    return 1;
                }
            }
        } else if (s2Array.length > s1Array.length) {
            // check for the longer array, that all the nodes are negligible (0 or empty string)
            for (int k = numPartsToCompare; k < s2Array.length; k++) {
                if (!s2Array[k].equals("") && !s2Array[k].equals("0")) {
                    return -1;
                }
            }
        }
        return 0;
    }
}
