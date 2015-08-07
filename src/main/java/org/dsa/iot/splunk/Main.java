package org.dsa.iot.splunk;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.splunk.actions.CreateSplunkDbAction;
import org.dsa.iot.splunk.splunk.Splunk;
import org.dsa.iot.splunk.utils.LinkPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;
import java.util.Map;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private final LinkPair pair = new LinkPair();

    @Override
    public boolean isResponder() {
        return true;
    }

    @Override
    public boolean isRequester() {
        return true;
    }

    @Override
    public void preInit() {
        // Splunk is insecure and requires use of SSLv3
        Security.setProperty("jdk.tls.disabledAlgorithms", "");
    }

    @Override
    public void stop() {
        super.stop();
        DSLink link = pair.getResponder();
        if (link != null) {
            Node node = link.getNodeManager().getSuperRoot();
            Map<String, Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children.values()) {
                    Splunk splunk = child.getMetaData();
                    if (splunk != null) {
                        splunk.stop();
                    }
                }
            }
        }
    }

    @Override
    public void onResponderInitialized(DSLink link) {
        pair.setResponder(link);
        NodeManager manager = link.getNodeManager();
        Node node = manager.getSuperRoot();
        initResponderActions(node);
    }

    @Override
    public void onRequesterConnected(DSLink link) {
        LOGGER.info("Connected");
        pair.setRequester(link);
        subscribeWatchGroups();
    }

    private void subscribeWatchGroups() {
        NodeManager resp = pair.getResponder().getNodeManager();
        Map<String, Node> children = resp.getSuperRoot().getChildren();
        if (children == null) {
            return;
        }

        for (Node group : children.values()) {
            if (group.getAction() != null
                    || "defs".equals(group.getName())) {
                continue;
            }

            try {
                Splunk splunk = new Splunk(pair, group);
                splunk.init();
            } catch (Exception e) {
                LOGGER.error("Error initializing splunk", e);
            }
        }
    }

    private void initResponderActions(Node parent) {
        NodeBuilder builder = parent.createChild("createSplunkDatabase");
        builder.setDisplayName("Create Splunk Database");
        builder.setSerializable(false);
        builder.setAction(CreateSplunkDbAction.make(pair, parent));
        builder.build();
    }

    public static void main(String[] args) {
        DSLinkFactory.start(args, new Main());
    }
}
