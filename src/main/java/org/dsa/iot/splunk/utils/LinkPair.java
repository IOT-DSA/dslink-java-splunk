package org.dsa.iot.splunk.utils;

import org.dsa.iot.dslink.DSLink;

/**
 * @author Samuel Grenier
 */
public class LinkPair {

    private DSLink requester;
    private DSLink responder;

    public DSLink getRequester() {
        return requester;
    }

    public DSLink getResponder() {
        return responder;
    }

    public void setRequester(DSLink link) {
        this.requester = link;
    }

    public void setResponder(DSLink link) {
        this.responder = link;
    }
}
