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
import org.dsa.iot.splunk.splunk.Splunk;
import org.vertx.java.core.Handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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
        final JobExportArgs jea = new JobExportArgs();
        jea.setOutputMode(JobExportArgs.OutputMode.XML);
        event.setStreamState(StreamState.OPEN);

        boolean realTime;
        {
            Value v = event.getParameter("Real Time", ValueType.BOOL);
            realTime = v.getBool();
        }

        boolean windowSend = false;
        if (!realTime) {
            Value v = event.getParameter("Earliest Time", ValueType.STRING);
            jea.setEarliestTime(v.getString());

            v = event.getParameter("Latest Time", ValueType.STRING);
            String lt = v.getString();
            jea.setLatestTime(lt);
            if ("rt".equals(lt)) {
                realTime = true;
                windowSend = true;
                event.getTable().setMode(Table.Mode.REFRESH);
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
        final Service service = splunk.getService();

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {

            private boolean windowSend;

            private Runnable setWindowSend(boolean send) {
                this.windowSend = send;
                return this;
            }

            @Override
            public void run() {
                InputStream stream = service.export(query, jea);
                Table table = event.getTable();
                MultiResultsReaderXml r;
                try {
                    r = new MultiResultsReaderXml(stream);
                    reader.set(r);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                List<Parameter> cols = null;
                boolean columnsSet = false;

                for (SearchResults results : r) {
                    if (results == null) {
                        continue;
                    }
                    BatchRow row = null;
                    if (windowSend) {
                        row = new BatchRow();
                    }
                    for (Event e : results) {
                        if (!columnsSet) {
                            columnsSet = true;
                            cols = setColumns(table, e);
                            latch.countDown();
                        }

                        if (windowSend && row != null) {
                            row.addRow(processRow(cols, e));
                        } else {
                            table.addRow(processRow(cols, e));
                        }
                    }
                    if (windowSend && row != null) {
                        table.addBatchRows(row);
                    }
                }

                table.close();
            }
        }.setWindowSend(windowSend));
        thread.start();
        try {
            latch.await();
        } catch (InterruptedException ignored) {
        }
    }

    private List<Parameter> setColumns(Table t, Event e) {
        for (String s : e.keySet()) {
            Parameter p = new Parameter(s, ValueType.STRING);
            t.addColumn(p);
        }
        return t.getColumns();
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
