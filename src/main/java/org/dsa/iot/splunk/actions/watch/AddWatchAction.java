package org.dsa.iot.splunk.actions.watch;

import org.dsa.iot.dslink.node.*;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.splunk.etsdb.WatchGroup;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class AddWatchAction implements Handler<ActionResult> {

    private final WatchGroup group;

    private AddWatchAction(WatchGroup group) {
        this.group = group;
    }

    @Override
    public synchronized void handle(ActionResult event) {
        final String path = event.getParameter("watchPath", ValueType.STRING).getString();
        group.createWatch(path);
    }

    public static Action make(WatchGroup group) {
        AddWatchAction awa = new AddWatchAction(group);
        Action a = new Action(Permission.READ, awa);
        a.addParameter(new Parameter("watchPath", ValueType.STRING));
        return a;
    }
}
