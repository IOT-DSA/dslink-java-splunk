package org.dsa.iot.splunk.splunk;

import org.dsa.iot.dslink.link.Requester;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.Writable;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.node.value.ValueUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.dsa.iot.splunk.actions.watch.AddWatchAction;
import org.dsa.iot.splunk.utils.LinkPair;
import org.dsa.iot.splunk.utils.LoggingType;
import org.dsa.iot.splunk.utils.PathValuePair;
import org.dsa.iot.splunk.utils.TimeParser;
import org.dsa.iot.dslink.util.handler.Handler;

import java.io.OutputStreamWriter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class WatchGroup {

    private final Queue<PathValuePair> queue;
    private final Splunk splunk;

    private final Node watchGroup;
    private final LinkPair pair;

    private final Object writeLoopLock = new Object();
    private ScheduledFuture<?> writeLoop;
    private int writeTime;

    private LoggingType loggingType;
    private long intervalWriteTime;

    private Node watches;
    private Node data;
    private Node logging;

    public WatchGroup(Splunk splunk, Node watchGroup, LinkPair pair) {
        this.queue = new ConcurrentLinkedDeque<>();
        this.splunk = splunk;
        this.watchGroup = watchGroup;
        this.pair = pair;
    }

    public Splunk getSplunk() {
        return splunk;
    }

    public LinkPair getPair() {
        return pair;
    }

    public Node getWatches() {
        return watches;
    }

    public Node getData() {
        return data;
    }

    public void write(PathValuePair pair) {
        boolean doWrite = false;
        switch (loggingType) {
            case ALL_DATA: {
                doWrite = true;
                break;
            }
            case INTERVAL: {
                Watch w = pair.getWatch();
                long currTime = pair.getTime();
                long lastTime = w.getLastIntervalUpdate();
                if (currTime - lastTime > intervalWriteTime) {
                    doWrite = true;
                    w.setLastIntervalUpdate(currTime);
                }
                break;
            }
            case POINT_CHANGE: {
                Watch w = pair.getWatch();
                Value curr = w.getDataNode().getValue();
                Value update = pair.getValue();
                if ((curr != null && update == null)
                        || (curr == null && update != null)
                        || (curr != null && !curr.equals(update))) {
                    doWrite = true;
                }
                break;
            }
            case POINT_TIME: {
                Watch w = pair.getWatch();
                Value vCurr = w.getDataNode().getValue();
                Value vUpdate = pair.getValue();
                long curr = (vCurr == null) ? 0 : vCurr.getTime();
                long update = (vUpdate == null) ? 0 : vUpdate.getTime();

                if ((vCurr != null) && (curr != update)) {
                    doWrite = true;
                }
                break;
            }
        }

        if (doWrite) {
            if (writeTime <= 0) {
                dbWrite(pair);
                Watch watch = pair.getWatch();
                Value value = new Value(TimeParser.parse(pair.getTime()));
                watch.setEndDate(value);
                watch.initStartValue();
                watch.setLastWrittenValue(pair.getValue());
                return;
            }
            queue.add(pair);
        }
    }

    public void createWatch(String path) {
        final String name = path.replaceAll("/", "%2F");
        if (watches.getChild(name) != null) {
            throw new RuntimeException("Watch name already exists");
        }

        NodeBuilder builder = watches.createChild(name);
        builder.setWritable(Writable.WRITE);
        builder.setValueType(ValueType.makeBool("enabled", "disabled"));
        builder.setValue(new Value(true));

        Watch watch = new Watch(this, builder.getChild(), path);
        watch.init();
        builder.build();
    }

    public void init(boolean subscribe) {
        createDeleteWatchAction();
        createWatchTracker();
        createDataTracker();
        initSettings();
        if (subscribe) {
            restoreSubscriptions();
        }
    }

    private void restoreSubscriptions() {
        Map<String, Node> children = watches.getChildren();
        if (children == null) {
            return;
        }
        for (Node watch : children.values()) {
            if (watch.getAction() != null) {
                continue;
            }

            String path = watch.getName().replaceAll("%2F", "/");
            Watch w = new Watch(this, watch, path);
            w.init();
        }
    }

    private void createDeleteWatchAction() {
        NodeBuilder builder = watchGroup.createChild("deleteWatchGroup");
        builder.setAction(new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                watchGroup.getParent().removeChild(watchGroup);
                Map<String, Node> children = watches.getChildren();
                if (children != null) {
                    Requester req = pair.getRequester().getRequester();
                    for (Node n : children.values()) {
                        if (n.getAction() != null) {
                            continue;
                        }
                        String path = n.getName().replaceAll("%2F", "/");
                        req.unsubscribe(path, null);
                    }
                }
            }
        }));
        builder.getChild().setSerializable(false);
        builder.build();
    }

    private void createWatchTracker() {
        watches = watchGroup.createChild("watches").build();
        NodeBuilder builder = watches.createChild("addWatchPath");
        builder.setAction(AddWatchAction.make(this));
        builder.build();
    }

    private void createDataTracker() {
        NodeBuilder b = watchGroup.createChild("data");
        b.setRoConfig("erasable", new Value(true));
        data = b.build();
        data.setSerializable(false);
    }

    private void initSettings() {
        {
            NodeBuilder builder = watchGroup.createChild("writeTime");
            builder.setDisplayName("Write Time");
            builder.setValueType(ValueType.NUMBER);
            builder.setValue(new Value(5));
            builder.setWritable(Writable.WRITE);
            builder.getListener().setValueHandler(new Handler<ValuePair>() {
                @Override
                public void handle(ValuePair event) {
                    int time = event.getCurrent().getNumber().intValue();
                    WatchGroup.this.writeTime = time;
                    if (time < 0) {
                        WatchGroup.this.writeTime = 0;
                        event.setCurrent(new Value(WatchGroup.this.writeTime));
                    }
                    setupTimer();
                }
            });

            Node node = builder.build();
            writeTime = node.getValue().getNumber().intValue();
            setupTimer();
        }

        {
            NodeBuilder builder = watchGroup.createChild("loggingType");
            builder.setDisplayName("Logging Type");
            {
                Set<String> enums = new LinkedHashSet<>();
                for (LoggingType t : LoggingType.values()) {
                    enums.add(t.getName());
                }
                builder.setValueType(ValueType.makeEnum(enums));
                builder.setValue(new Value(LoggingType.ALL_DATA.getName()));
                builder.setWritable(Writable.WRITE);
                builder.getListener().setValueHandler(new Handler<ValuePair>() {
                    @Override
                    public void handle(ValuePair event) {
                        String sType = event.getCurrent().getString();
                        LoggingType type = LoggingType.toEnum(sType);
                        setupLoggingType(type);
                    }
                });
            }
            logging = builder.build();
            String sType = logging.getValue().getString();
            setupLoggingType(LoggingType.toEnum(sType));
        }
    }

    private void setupTimer() {
        synchronized (writeLoopLock) {
            if (writeLoop != null) {
                writeLoop.cancel(false);
                writeLoop = null;
            }

            if (writeTime <= 0) {
                return;
            }

            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            writeLoop = stpe.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        PathValuePair pair = null;
                        int size = queue.size();
                        for (int i = 0; i < size; ++i) {
                            pair = queue.poll();
                            dbWrite(pair);
                        }

                        if (pair != null) {
                            Watch watch = pair.getWatch();
                            Value value = pair.getValue();
                            watch.initStartValue();
                            watch.setLastWrittenValue(value);

                            String time = TimeParser.parse(value.getTime());
                            value = new Value(time);
                            watch.setEndDate(value);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }

                }
            }, writeTime, writeTime, TimeUnit.SECONDS);
        }
    }

    private void setupLoggingType(LoggingType type) {
        switch (type) {
            case NONE:
            case POINT_CHANGE:
            case ALL_DATA: {
                logging.clearChildren();
                break;
            }
            case INTERVAL: {
                NodeBuilder builder = logging.createChild("interval");
                builder.setDisplayName("Interval");
                builder.setValueType(ValueType.NUMBER);
                builder.setValue(new Value(5));
                builder.setWritable(Writable.WRITE);
                builder.getListener().setValueHandler(new Handler<ValuePair>() {
                    @Override
                    public void handle(ValuePair event) {
                        long time = event.getCurrent().getNumber().longValue();
                        if (time < 0) {
                            time = 0;
                            event.setCurrent(new Value(time));
                        }
                        intervalWriteTime = TimeUnit.SECONDS.toMillis(time);
                    }
                });
                Node node = builder.build();
                intervalWriteTime = node.getValue().getNumber().longValue();
                intervalWriteTime = TimeUnit.SECONDS.toMillis(intervalWriteTime);
                break;
            }
        }
        this.loggingType = type;
    }

    private void dbWrite(final PathValuePair pair) {
        String path = pair.getPath();
        Value value = pair.getValue();
        long time = value.getTime();

        final JsonObject obj = new JsonObject();
        obj.put("timestamp", time);
        obj.put("path", path);
        obj.put("value", value);

        splunk.getWriter(new Handler<OutputStreamWriter>() {
            @Override
            public void handle(OutputStreamWriter writer) {
                try {
                    writer.write(new String(obj.encode()));
                    writer.write("\r\n");
                    writer.flush();
                } catch (Exception e) {
                    dbWrite(pair);
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
