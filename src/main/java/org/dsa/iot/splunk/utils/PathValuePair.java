package org.dsa.iot.splunk.utils;

import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.splunk.etsdb.Watch;

/**
 * @author Samuel Grenier
 */
public class PathValuePair {

    private final Watch watch;
    private final String path;
    private final Value value;
    private final long time;

    public PathValuePair(Watch watch, String path, Value value, long time) {
        this.watch = watch;
        this.path = path;
        this.value = value;
        this.time = time;
    }

    public Watch getWatch() {
        return watch;
    }

    public String getPath() {
        return path;
    }

    public Value getValue() {
        return value;
    }

    public long getTime() {
        return time;
    }
}
