package org.dsa.iot.splunk.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import com.splunk.TcpInput;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.splunk.actions.CreateWatchGroupAction;
import org.dsa.iot.splunk.actions.QueryAction;
import org.dsa.iot.splunk.utils.LinkPair;
import org.slf4j.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Map;

/**
 * @author Samuel Grenier
 */
public class Splunk {

    private static final Logger LOGGER = LoggerFactory.getLogger(Splunk.class);
    private LinkPair pair;
    private Node node;

    private ServiceArgs args;
    private Service svc;
    private String input;
    private OutputStreamWriter writer;

    public Splunk(LinkPair pair, Node node) {
        this.pair = pair;
        this.node = node;
    }

    public void init() {
        args = new ServiceArgs();
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

        input = node.getConfig("input").getString();
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

        {
            NodeBuilder builder = node.createChild("createWatchGroup");
            builder.setDisplayName("Create Watch Group");
            builder.setAction(CreateWatchGroupAction.make(this, node, pair));
            builder.getChild().setSerializable(false);
            builder.build();
        }
        {
            NodeBuilder builder = node.createChild("query");
            builder.setDisplayName("Query");
            builder.getChild().setSerializable(false);
            builder.setAction(QueryAction.make(this));
            builder.build();
        }
    }

    public Service getService() {
        if (svc == null) {
            boolean thrown = true;
            while (thrown) {
                try {
                    svc = Service.connect(args);
                    thrown = false;

                    String host = (String) args.get("host");
                    host += ":" + args.get("port");
                    LOGGER.info("Connected to splunk ({})", host);
                } catch (Exception ex) {
                    LOGGER.warn("Failed to connect to splunk");
                    thrown = true;
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
        return svc;
    }

    public void kill() {
        svc = null;
    }

    public OutputStreamWriter getWriter() {
        if (svc == null) {
            svc = getService();
        } else if (writer != null) {
            return writer;
        }

        try {
            TcpInput in = (TcpInput) svc.getInputs().get(input);
            Socket sock = in.attach();
            OutputStream stream = sock.getOutputStream();
            return writer = new OutputStreamWriter(stream, "UTF-8");
        } catch (IOException e) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {
            }
            return getWriter();
        }
    }
}
