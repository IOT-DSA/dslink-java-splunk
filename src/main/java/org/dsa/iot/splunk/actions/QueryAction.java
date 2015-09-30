package org.dsa.iot.splunk.actions;

import com.splunk.*;
import org.dsa.iot.dslink.methods.StreamState;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.actions.ResultType;
import org.dsa.iot.dslink.node.actions.table.BatchRow;
import org.dsa.iot.dslink.node.actions.table.Row;
import org.dsa.iot.dslink.node.actions.table.Table;
import org.dsa.iot.dslink.node.value.*;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.splunk.splunk.Splunk;
import org.dsa.iot.dslink.util.handler.Handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Samuel Grenier
 */
public class QueryAction implements Handler<ActionResult> {

    private final Splunk splunk;

    private QueryAction(Splunk splunk) {
        this.splunk = splunk;
    }

    @Override
    public void handle(final ActionResult event) {
        final Table table = event.getTable();
        final JobExportArgs jea = new JobExportArgs();
        jea.setOutputMode(JobExportArgs.OutputMode.XML);
        jea.add("preview", false);
        event.setStreamState(StreamState.OPEN);

        boolean realTime;
        {
            Value v = event.getParameter("Real Time", ValueType.BOOL);
            realTime = v.getBool();
        }

        boolean windowSend = false;
        if (!realTime) {
            table.setMode(Table.Mode.APPEND);
            Value v = event.getParameter("Earliest Time");
            if (v != null) {
                jea.setEarliestTime(v.getString());
            }

            v = event.getParameter("Latest Time");
            if (v != null) {
                String lt = v.getString();
                jea.setLatestTime(lt);
                if ("rt".equals(lt)) {
                    realTime = true;
                    windowSend = true;
                    table.setMode(Table.Mode.REFRESH);
                }
            }
        }

        final ReaderContainer reader = new ReaderContainer();
        if (realTime) {
            jea.setSearchMode(JobExportArgs.SearchMode.REALTIME);
            event.setCloseHandler(new Handler<Void>() {
                @Override
                public void handle(Void event) {
                    close(reader);
                }
            });
        }

        Value v = event.getParameter("Query", ValueType.STRING);
        final String query = v.getString();

        Objects.getDaemonThreadPool().execute(new Runnable() {

            private boolean windowSend;

            private Runnable setWindowSend(boolean send) {
                this.windowSend = send;
                return this;
            }

            @Override
            public void run() {
                final List<String> prevCols = new ArrayList<>();
                final List<Parameter> cols = new LinkedList<>();
                splunk.getService(new Handler<Service>() {
                    @Override
                    public void handle(Service service) {
                        InputStream stream = service.export(query, jea);
                        MultiResultsReaderXml r;
                        try {
                            r = new MultiResultsReaderXml(stream);
                            reader.set(r);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        for (SearchResults results : r) {
                            if (results == null) {
                                continue;
                            } else if (!splunk.isRunning()) {
                                break;
                            }
                            BatchRow row = null;
                            if (windowSend) {
                                row = new BatchRow();
                            }
                            for (Event e : results) {
                                if (!splunk.isRunning()) {
                                    row = null;
                                    break;
                                }
                                List<Parameter> added = setColumns(prevCols, cols, e);
                                if (windowSend && row != null) {
                                    row.addRow(processRow(cols, e));
                                } else {
                                    table.addRow(added, processRow(cols, e));
                                }
                            }
                            if (windowSend && row != null) {
                                table.addBatchRows(cols, row);
                            }
                        }

                        table.close();
                        close(reader);
                    }
                });
            }
        }.setWindowSend(windowSend));
    }

    private List<Parameter> setColumns(List<String> names,
                            List<Parameter> cols,
                            Event event) {
        List<Parameter> added = null;
        for (String s : event.keySet()) {
            if (names.contains(s)) {
                continue;
            }
            names.add(s);
            Parameter p = new Parameter(s, ValueType.STRING);
            cols.add(p);
            if (added == null) {
                added = new LinkedList<>();
            }
            added.add(p);
        }
        return added;
    }

    private Row processRow(List<Parameter> cols, Event e) {
        Row row = new Row();
        for (Parameter param : cols) {
            String s = e.get(param.getName());
            if (s != null) {
                row.addValue(new Value(s));
            } else {
                row.addValue(null);
            }
        }
        return row;
    }

    private void close(ReaderContainer reader) {
        try {
            MultiResultsReaderXml r = reader.get();
            if (r != null) {
                reader.set(null);
                r.close();
            }
        } catch (IOException ignored) {
        }
    }

    public static Action make(Splunk splunk) {
        QueryAction query = new QueryAction(splunk);
        Action act = new Action(Permission.READ, query);
        {
            Value def = new Value("search ");
            Parameter p = new Parameter("Query", ValueType.STRING, def);
            act.addParameter(p);
        }
        {
            Value def = new Value("-d");
            Parameter p = new Parameter("Earliest Time", ValueType.STRING, def);
            act.addParameter(p);
        }
        {
            Value def = new Value("now");
            Parameter p = new Parameter("Latest Time", ValueType.STRING, def);
            act.addParameter(p);
        }
        {
            Value def = new Value(false);
            Parameter p = new Parameter("Real Time", ValueType.BOOL, def);
            act.addParameter(p);
        }
        act.setResultType(ResultType.STREAM);
        return act;
    }

    private static class ReaderContainer {
        private MultiResultsReaderXml reader;

        public void set(MultiResultsReaderXml reader) {
            this.reader = reader;
        }

        public MultiResultsReaderXml get() {
            return reader;
        }
    }
}
