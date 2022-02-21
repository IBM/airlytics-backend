package com.ibm.airlytics.retentiontracker.push;

public enum DeviceType {
    APPLICATION,
    BOUNCED,
    UNDEFINED;

    public static DeviceType fromString(String str) {
        if (str != null) {
            switch (str.toLowerCase()) {
                case "application":
                    return DeviceType.APPLICATION;
                case "bounced":
                    return DeviceType.BOUNCED;
            }
        }
        return DeviceType.UNDEFINED;
    }
}
