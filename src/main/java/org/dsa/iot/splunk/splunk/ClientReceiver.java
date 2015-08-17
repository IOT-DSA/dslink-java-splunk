package org.dsa.iot.splunk.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.dsa.iot.commons.GuaranteedReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

/**
 * @author Samuel Grenier
 */
public class ClientReceiver extends GuaranteedReceiver<Service> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientReceiver.class);
    private final ServiceArgs serviceArgs;

    public ClientReceiver(ServiceArgs serviceArgs) {
        super(5);
        if (serviceArgs == null) {
            throw new NullPointerException("serviceArgs");
        }
        this.serviceArgs = serviceArgs;
    }

    @Override
    protected Service instantiate() throws Exception {
        String scheme = (String) serviceArgs.get("scheme");
        String host = (String) serviceArgs.get("host");
        int port = (Integer) serviceArgs.get("port");
        try {
            Service svc = Service.connect(serviceArgs);
            LOGGER.info("Successfully connected to {}://{}:{}", scheme, host, port);
            return svc;
        } catch (Exception e) {
            LOGGER.error("Failed to connect to {}://{}:{}", scheme, host, port);
            throw e;
        }
    }

    @Override
    protected boolean invalidateInstance(Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException
                || cause instanceof XMLStreamException) {
            return true;
        }
        String msg = e.getMessage();
        return msg != null && msg.contains("HTTP 401");
    }
}
