package org.dsa.iot.splunk.actions;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.EditorType;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.splunk.splunk.Splunk;
import org.dsa.iot.splunk.utils.LinkPair;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class CreateSplunkDbAction implements Handler<ActionResult> {

    private final LinkPair pair;
    private final Node parent;

    private CreateSplunkDbAction(LinkPair pair, Node parent) {
        this.pair = pair;
        this.parent = parent;
    }

    @Override
    public void handle(ActionResult event) {
        String name = event.getParameter("name", ValueType.STRING).getString();

        NodeBuilder builder = parent.createChild(name);
        Node child = builder.build();

        String host = event.getParameter("host", ValueType.STRING).getString();
        int port = event.getParameter("port", ValueType.NUMBER).getNumber().intValue();
        boolean ssl = event.getParameter("ssl", ValueType.BOOL).getBool();
        String in = event.getParameter("input", ValueType.STRING).getString();
        String user = event.getParameter("username", ValueType.STRING).getString();
        String pass = event.getParameter("password", ValueType.STRING).getString();

        child.setConfig("host", new Value(host));
        child.setConfig("port", new Value(port));
        child.setConfig("ssl", new Value(ssl));
        child.setConfig("input", new Value(in));
        child.setConfig("username", new Value(user));
        child.setPassword(pass.toCharArray());
        Splunk splunk = new Splunk(pair, child);
        splunk.init();
    }

    public static Action make(LinkPair pair, Node parent) {
        CreateSplunkDbAction csda = new CreateSplunkDbAction(pair, parent);
        Action a = new Action(Permission.READ, csda);

        a.addParameter(new Parameter("name", ValueType.STRING));
        a.addParameter(new Parameter("host", ValueType.STRING, new Value("localhost")));
        a.addParameter(new Parameter("port", ValueType.NUMBER, new Value(8089)));
        a.addParameter(new Parameter("ssl", ValueType.BOOL, new Value(true)));

        a.addParameter(new Parameter("input", ValueType.STRING));
        a.addParameter(new Parameter("username", ValueType.STRING));

        Parameter p = new Parameter("password", ValueType.STRING);
        p.setEditorType(EditorType.PASSWORD);
        a.addParameter(p);
        return a;
    }
}
