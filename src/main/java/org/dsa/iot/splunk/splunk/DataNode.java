package org.dsa.iot.splunk.splunk;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.splunk.actions.GetHistory;
import org.dsa.iot.splunk.utils.LinkPair;
import org.vertx.java.core.Handler;

/**
 * Initializes data nodes.
 *
 * @author Samuel Grenier
 */
public class DataNode {

    public static Node initNodeFromPath(WatchGroup group,
                                        String path) {
        final Node parent = group.getData();
        Node node = parent;
        String[] split = NodeManager.splitPath(path);
        for (String s : split) {
            node = node.createChild(s).build();
        }
        node.setValueType(ValueType.DYNAMIC);

        {
            NodeBuilder b = node.createChild("getHistory", "getHistory");
            b.setAction(GetHistory.make(parent, b.getChild(), group.getSplunk()));
            b.build();
        }

        {
            NodeBuilder b = node.createChild("unsubscribe", "unsubscribe");
            b.setAction(getUnsubscribeAction(group, path));
            b.build();
        }
        return node;
    }

    public static Action getUnsubscribeAction(final WatchGroup group,
                                            final Node watchNode,
                                            final Node dataNode,
                                            final String path) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                LinkPair pair = group.getPair();
                pair.getRequester().getRequester().unsubscribe(path, null);

                watchNode.getParent().removeChild(watchNode);
                dataNode.getParent().removeChild(dataNode);
            }
        });
    }

    private static Action getUnsubscribeAction(final WatchGroup group,
                                               final String path) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                LinkPair pair = group.getPair();
                Node node = event.getNode().getParent();
                pair.getRequester().getRequester().unsubscribe(path, null);
                node.getParent().removeChild(node);

                Node watches = group.getWatches();
                watches.removeChild(path.replaceAll("/", "%2F"));
            }
        });
    }


}
