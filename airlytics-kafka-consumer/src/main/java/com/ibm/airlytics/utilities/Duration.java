package com.ibm.airlytics.utilities;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.util.Calendar;

public class Duration {
    public static long ONE_DAY_IN_MILLISECOND = 1 * 1000 * 60 * 60 * 24;
    public static long FOUR_DAYS_IN_MILLISECOND = 4 * 1000 * 60 * 60 * 24;
    public static long ONE_WEEKS_IN_MILLISECOND = 7 * 1000 * 60 * 60 * 24;

    public static long durationToMilliSeconds(String durationInISO8601) {
        // "P1W" is not supported should be replaced by "P7D"
        if (durationInISO8601 != null && durationInISO8601.equals(Periodicity.WEEKLY.periodicity)) {
            durationInISO8601 = Periodicity.SEVEN_DAYS.periodicity;
        }
        try {
            return DatatypeFactory.newInstance().newDuration(durationInISO8601).getTimeInMillis(Calendar.getInstance());
        } catch (DatatypeConfigurationException e) {
            return 0;
        }
    }

    public enum Periodicity {
        WEEKLY("P1W"),
        SEVEN_DAYS("P7D"),
        MONTHLY("P1M"),
        THREE_MONTHLY("P3M"),
        YEARLY("P1Y");

        private final String periodicity;

        Periodicity(String periodicity) {
            this.periodicity = periodicity;
        }


        public static Periodicity getByType(String periodicity) {
            if (periodicity == null) {
                return null;
            }
            for (Periodicity e : Periodicity.values()) {
                if (periodicity.equals(e.periodicity)) return e;
            }
            return null;
        }

        public static String getNameByType(String periodicity) {
            if (periodicity == null) {
                return null;
            }
            for (Periodicity e : Periodicity.values()) {
                if (periodicity.equals(e.periodicity)) return e.name();
            }
            return null;
        }
    }
}
