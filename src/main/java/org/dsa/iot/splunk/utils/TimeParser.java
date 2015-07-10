package org.dsa.iot.splunk.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Samuel Grenier
 */
public class TimeParser {

    private static final ThreadLocal<DateFormat> FORMAT_TIME_ZONE;
    private static final ThreadLocal<DateFormat> FORMAT;

    public static long parse(String time) {
        try {
            if (time.matches(".+[+|-]\\d+:\\d+")) {
                StringBuilder b = new StringBuilder(time);
                b.deleteCharAt(time.lastIndexOf(":"));
                time = b.toString();
                return FORMAT_TIME_ZONE.get().parse(time).getTime();
            } else {
                return FORMAT.get().parse(time).getTime();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String parse(long time) {
        return FORMAT.get().format(new Date(time)) + "-00:00";
    }

    static {
        FORMAT_TIME_ZONE = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                return sdf;
            }
        };

        FORMAT = new ThreadLocal<DateFormat>() {
            @Override
            protected DateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                return sdf;
            }
        };
    }
}
