package org.dsa.iot.splunk.splunk;

import com.splunk.*;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.splunk.actions.CreateWatchGroupAction;
import org.dsa.iot.splunk.actions.QueryAction;
import org.dsa.iot.splunk.etsdb.WatchGroup;
import org.dsa.iot.splunk.utils.LinkPair;

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
        args.setUsername(node.getConfig("username").getString());
        args.setPassword(new String(node.getPassword()));
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
            svc = Service.connect(args);
        }
        return svc;
    }

    public OutputStreamWriter getWriter() {
        if (svc == null) {
            svc = Service.connect(args);
        } else if (writer != null) {
            return writer;
        }

        TcpInput in = (TcpInput) svc.getInputs().get(input);
        try {
            Socket sock = in.attach();
            OutputStream stream = sock.getOutputStream();
            return writer = new OutputStreamWriter(stream, "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
