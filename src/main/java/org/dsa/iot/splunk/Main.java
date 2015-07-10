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
    public void onResponderInitialized(DSLink link) {
        pair.setResponder(link);
        NodeManager manager = link.getNodeManager();
        Node node = manager.getSuperRoot();
        initResponderActions(node);

        // DEBUG comment
        //node = manager.getNode("/defs/profile/getHistory_", true).getNode();
        //GetHistory.initProfile(node);
        //node = manager.getSuperRoot().getChild("defs");
        //node.setSerializable(false);
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

            Splunk splunk = new Splunk(pair, group);
            splunk.init();
        }
    }

    private void initResponderActions(Node parent) {
        NodeBuilder builder = parent.createChild("createSplunkDatabase");
        builder.setDisplayName("Create Splunk Database");
        builder.setAction(CreateSplunkDbAction.make(pair, parent));
        builder.build();
    }

    public static void main(String[] args) {
        // Splunk is insecure and requires use of SSLv3
        Security.setProperty("jdk.tls.disabledAlgorithms", "");
        DSLinkFactory.startDual("splunk", args, new Main());
    }
}
