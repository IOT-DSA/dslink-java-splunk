package org.dsa.iot.splunk.stats;

import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.splunk.stats.rollup.*;
import org.dsa.iot.splunk.utils.TimeParser;
import org.vertx.java.core.json.JsonArray;

import java.util.Calendar;
import java.util.TimeZone;

/**
 * @author Samuel Grenier
 */
public class Interval {

    private final Rollup rollup;

    private int seconds = -1;
    private boolean alignSeconds;

    private int minutes = -1;
    private boolean alignMinutes;

    private int hours = -1;
    private boolean alignHours;

    private int days = -1;
    private boolean alignDays;

    private int weeks = -1;
    private boolean alignWeeks;

    private int months = -1;
    private boolean alignMonths;

    private int years = -1;
    private boolean alignYears;

    /**
     * The total amount of time combined to increment by.
     */
    private long incrementTime;

    /**
     * Last timestamp of the last value.
     */
    private long lastValueTimeTrunc = -1;

    /**
     * Last updated value used for roll ups.
     */
    private Value lastValue;

    private Interval(Rollup rollup) {
        this.rollup = rollup;
    }

    /**
     *
     * @param value Value retrieved from the database.
     * @param fullTs Full timestamp of the value.
     * @return An update or null to skip the update.
     */
    public Row getRowUpdate(Value value, long fullTs) {
        final long alignedTs = alignTime(fullTs);

        Row row = null;
        if (alignedTs == lastValueTimeTrunc) {
            // Update the last value with the same time stamp in the interval
            lastValue = value;
            if (rollup != null) {
                rollup.update(value, fullTs);
            }
        } else if (lastValue != null) {
            // Finish up the rollup, the interval for this period is completed
            if (rollup == null) {
                row = getRowUpdate(lastValueTimeTrunc, lastValue);
            } else {
                row = getRowUpdate(lastValueTimeTrunc, rollup.getValue());
            }
            lastValue = null;
        }

        // New interval period has been started
        if (alignedTs - lastValueTimeTrunc >= incrementTime) {
            lastValueTimeTrunc = alignedTs;
            lastValue = value;
            if (rollup != null) {
                rollup.reset();
                rollup.update(lastValue, fullTs);
            }
        }
        return row;
    }

    private Row getRowUpdate(long ts, Value value) {
        Row row = new Row();
        row.addValue(new Value(TimeParser.parse(ts)));
        row.addValue(value);
        return row;
    }

    private long alignTime(long ts) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTimeInMillis(ts);

        if (alignSeconds) {
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignMinutes) {
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignHours) {
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignDays) {
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignWeeks) {
            c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignMonths) {
            c.set(Calendar.DAY_OF_MONTH, 1);
            c.set(Calendar.HOUR, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        if (alignYears) {
            c.set(Calendar.MONTH, 0);
            c.set(Calendar.DAY_OF_MONTH, 1);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
        }

        return c.getTime().getTime();
    }

    private void update(char interval, String number) {
        int num = Integer.parseInt(number);
        switch (interval) {
            case 'S':
                alignSeconds = true;
            case 's':
                check("seconds", seconds);
                seconds = num;
                break;
            case 'M':
                alignMinutes = true;
            case 'm':
                check("minutes", minutes);
                minutes = num;
                break;
            case 'H':
                alignHours = true;
            case 'h':
                check("hours", hours);
                hours = num;
                break;
            case 'D':
                alignDays = true;
            case 'd':
                check("days", days);
                days = num;
                break;
            case 'W':
                alignWeeks = true;
            case 'w':
                check("weeks", weeks);
                weeks = num;
                break;
            case 'N':
                alignMonths = true;
            case 'n':
                check("months", months);
                months = num;
                break;
            case 'Y':
                alignYears = true;
            case 'y':
                check("years", years);
                years = num;
                break;
            default:
                throw new RuntimeException("Unknown char: " + interval);
        }
    }

    private void finishParsing() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);

        if (years > -1) {
            c.add(Calendar.YEAR, years);
        }

        if (months > -1) {
            c.add(Calendar.MONTH, months);
        }

        if (weeks > -1) {
            c.add(Calendar.WEEK_OF_MONTH, weeks);
        }

        if (days > -1) {
            c.add(Calendar.DAY_OF_MONTH, days);
        }

        if (hours > -1) {
            c.add(Calendar.HOUR, hours);
        }

        if (minutes > -1) {
            c.add(Calendar.MINUTE, minutes);
        }

        if (seconds > -1) {
            c.add(Calendar.SECOND, seconds);
        }

        incrementTime = c.getTime().getTime();
    }

    private void check(String type, int num) {
        if (num != -1) {
            throw new RuntimeException(type + " is already set");
        }
    }

    public static Interval parse(String interval, String rollup) {
        if (interval == null
                || "none".equals(interval)
                || "default".equals(interval)) {
            return null;
        }

        Rollup roll = null;
        if ("avg".equals(rollup)) {
            roll = new AvgRollup();
        } else if ("count".equals(rollup)) {
            roll = new CountRollup();
        } else if ("first".equals(rollup)) {
            roll = new FirstRollup();
        } else if ("last".equals(rollup)) {
            roll = new LastRollup();
        } else if ("max".equals(rollup)) {
            roll = new MaxRollup();
        } else if ("min".equals(rollup)) {
            roll = new MinRollup();
        } else if ("sum".equals(rollup)) {
            roll = new SumRollup();
        } else if ("delta".equals(rollup)) {
            roll = new DeltaRollup();
        } else if (!"none".equals(rollup)) {
            throw new RuntimeException("Invalid rollup: " + rollup);
        }

        final Interval i = new Interval(roll);
        char[] chars = interval.toCharArray();
        StringBuilder number = new StringBuilder();
        for (char c : chars) {
            if (Character.isDigit(c)) {
                number.append(c);
            } else {
                i.update(c, number.toString());
                number.delete(0, number.length());
            }
        }

        if (number.length() > 0) {
            throw new RuntimeException("Invalid expression");
        }

        i.finishParsing();
        return i;
    }
}
