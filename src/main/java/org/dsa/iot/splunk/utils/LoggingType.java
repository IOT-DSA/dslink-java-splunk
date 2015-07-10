package org.dsa.iot.splunk.utils;

/**
 * @author Samuel Grenier
 */
public enum LoggingType {

    ALL_DATA("All data"),
    NONE("None"),
    INTERVAL("Interval"),
    POINT_CHANGE("Point Change"),
    POINT_TIME("Point Time");

    private final String name;

    LoggingType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static LoggingType toEnum(String s) {
        if (NONE.getName().equals(s)) {
            return NONE;
        } else if (ALL_DATA.getName().equals(s)) {
            return ALL_DATA;
        } else if (INTERVAL.getName().equals(s)) {
            return INTERVAL;
        } else if (POINT_CHANGE.getName().equals(s)) {
            return POINT_CHANGE;
        } else if (POINT_TIME.getName().equals(s)) {
            return POINT_TIME;
        } else {
            throw new IllegalArgumentException("Invalid logging type: " + s);
        }
    }
}
