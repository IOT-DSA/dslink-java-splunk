package org.dsa.iot.splunk.actions;

import com.splunk.*;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.*;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.node.value.ValueUtils;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.splunk.splunk.Splunk;
import org.dsa.iot.splunk.stats.Interval;
import org.dsa.iot.splunk.utils.TimeParser;
import org.dsa.iot.dslink.util.handler.Handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Samuel Grenier
 */
public class GetHistory implements Handler<ActionResult> {

    private final Splunk splunk;
    private final int subStringPos;

    private GetHistory(Node data, Splunk splunk) {
        this.subStringPos = data.getPath().length();
        this.splunk = splunk;
    }

    @Override
    public void handle(final ActionResult event) {
        String range = event.getParameter("Timerange").getString();
        String[] split = range.split("/");

        final String sFrom = split[0];
        final String sTo = split[1];

        long from = TimeParser.parse(sFrom);
        long to = TimeParser.parse(sTo);

        String path;
        {
            Node node = event.getNode().getParent();
            path = node.getPath();
            path = path.substring(subStringPos);
        }

        final String sInterval = event.getParameter("Interval").getString();
        final String sRollup = event.getParameter("Rollup").getString();
        final Interval interval = Interval.parse(sInterval, sRollup);

        String q = "search path=\"%s\"";
        q += "| spath time | spath value";
        q += "| where time >= %d and time <= %d";
        q += "| sort time";
        q += "| table _raw";
        final String query = String.format(q, path, from, to);

        splunk.getService(new Handler<Service>() {
            @Override
            public void handle(Service service) {
                InputStream stream = service.export(query);
                MultiResultsReaderXml reader = null;
                try {
                    Table t = event.getTable();
                    reader = new MultiResultsReaderXml(stream);
                    for (SearchResults res : reader) {
                        for (Event e : res) {
                            JsonObject obj = new JsonObject(e.get("_raw"));

                            long ms = obj.get("time");
                            Object v = obj.get("value");
                            Value val = ValueUtils.toValue(v);

                            if (interval == null) {
                                String time = TimeParser.parse(ms);
                                Value tVal = new Value(time);
                                t.addRow(Row.make(tVal, val));
                            } else {
                                Row r = interval.getRowUpdate(val, ms);
                                if (r != null) {
                                    t.addRow(r);
                                }
                            }
                        }
                    }
                } catch (IOException ignored) {
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
            }
        });
    }

    public static Action make(Node data, Node parent, Splunk splunk) {
        Action a =  new Action(Permission.READ, new GetHistory(data, splunk));
        initProfile(parent, a);
        return a;
    }

    private static void initProfile(Node node, Action act) {
        {
            Parameter param = new Parameter("Timerange", ValueType.STRING);
            param.setEditorType(EditorType.DATE_RANGE);
            act.addParameter(param);
        }

        {
            Value def = new Value("none");
            Parameter param = new Parameter("Interval", ValueType.STRING, def);
            act.addParameter(param);
        }

        {
            Set<String> enums = new LinkedHashSet<>();
            enums.add("none");
            enums.add("avg");
            enums.add("min");
            enums.add("max");
            enums.add("sum");
            enums.add("first");
            enums.add("last");
            enums.add("count");
            enums.add("delta");
            ValueType e = ValueType.makeEnum(enums);
            Parameter param = new Parameter("Rollup", e);
            act.addParameter(param);
        }

        {
            Parameter param = new Parameter("timestamp", ValueType.TIME);
            act.addResult(param);
        }

        {
            Parameter param = new Parameter("value", ValueType.DYNAMIC);
            act.addResult(param);
        }

        act.setResultType(ResultType.TABLE);
        node.setAction(act);
    }
}
