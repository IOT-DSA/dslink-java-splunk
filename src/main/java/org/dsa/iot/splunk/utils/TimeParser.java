package org.dsa.iot.splunk.utils;

import org.dsa.iot.dslink.util.TimeUtils;

/**
 * @author Samuel Grenier
 */
public class TimeParser {

    public static long parse(String time) {
        return TimeUtils.decode(time);
    }

    public static String parse(long time) {
        return TimeUtils.encode(time, true).toString();
    }

}
