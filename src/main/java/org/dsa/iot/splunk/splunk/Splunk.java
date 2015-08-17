package org.dsa.iot.splunk.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import com.splunk.TcpInput;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.splunk.actions.CreateWatchGroupAction;
import org.dsa.iot.splunk.actions.QueryAction;
import org.dsa.iot.splunk.utils.LinkPair;
import org.vertx.java.core.Handler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Map;

/**
 * @author Samuel Grenier
 */
public class Splunk {

    private LinkPair pair;
    private Node node;

    private ClientReceiver clientReceiver;
    private String input;
    private OutputStreamWriter writer;
    private boolean running = true;
    private boolean writerEnabled = false;

    public Splunk(LinkPair pair, Node node) {
        node.setMetaData(this);
        this.pair = pair;
        this.node = node;
    }

    public void stop() {
        running = false;
        clientReceiver.shutdown();
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException ignored) {
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void init() {
        {
            NodeBuilder builder = node.createChild("createWatchGroup");
            builder.setDisplayName("Create Watch Group");
            builder.setAction(CreateWatchGroupAction.make(this, node, pair));
            builder.setSerializable(false);
            builder.build();
        }
        {
            NodeBuilder builder = node.createChild("query");
            builder.setDisplayName("Query");
            builder.setSerializable(false);
            builder.setAction(QueryAction.make(this));
            builder.build();
        }
        {
            NodeBuilder builder = node.createChild("delete");
            builder.setDisplayName("Delete Server");
            builder.setSerializable(false);
            builder.setAction(new Action(Permission.READ,
                    new Handler<ActionResult>() {
                        @Override
                        public void handle(ActionResult event) {
                            try {
                                stop();
                            } catch (RuntimeException ignored) {
                            }
                            Node node = event.getNode().getParent();
                            node.getParent().removeChild(node);
                        }
                    }));
            builder.build();
        }

        ServiceArgs args = new ServiceArgs();
        args.setScheme(node.getConfig("ssl").getBool() ? "https" : "http");
        args.setHost(node.getConfig("host").getString());
        args.setPort(node.getConfig("port").getNumber().intValue());

        {
            Value v = node.getConfig("username");
            if (v != null) {
                args.setUsername(v.getString());
            }

            char[] b = node.getPassword();
            if (b != null) {
                args.setPassword(new String(b));
            }
        }

        clientReceiver = new ClientReceiver(args);

        Value vIn = node.getConfig("input");
        if (vIn != null) {
            writerEnabled = true;
            input = vIn.getString();
            Map<String, Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children.values()) {
                    if (child.getAction() != null) {
                        continue;
                    }
                    WatchGroup group = new WatchGroup(this, child, pair);
                    group.init(true);
                }
            }
        } else {
            writerEnabled = false;
        }
    }

    public void getService(Handler<Service> onServiceReceived) {
        clientReceiver.get(onServiceReceived, false);
    }

    public void getWriter(final Handler<OutputStreamWriter> onWriterReceived) {
        if (!writerEnabled) {
            return;
        }
        if (writer != null) {
            onWriterReceived.handle(writer);
            return;
        }

        getService(new Handler<Service>() {
            @Override
            public void handle(Service svc) {
                try {
                    TcpInput in = (TcpInput) svc.getInputs().get(input);
                    Socket sock = in.attach();
                    OutputStream stream = sock.getOutputStream();
                    writer = new OutputStreamWriter(stream, "UTF-8");
                    onWriterReceived.handle(writer);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
