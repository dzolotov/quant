package org.gathe.integration;
/**
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>
 */
import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.eclipse.jetty.server.Server;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class MonitorThread extends Thread {

    private int interval = 120;
    private Logger LOG = Logger.getLogger(this.getClass());
    private EndpointManager endpointManager;
    private Session sessionQ;
    private MessageProducer producer;
    private Server webServer;

    public MonitorThread(EndpointManager endpointManager) {
        LOG.info("Monitor thread initialized");
        this.endpointManager = endpointManager;
        this.sessionQ = endpointManager.getSession();
        this.producer = endpointManager.getMessageProducer();

        webServer = new Server(6080);
        webServer.setHandler(new WebHandler(endpointManager));
        try {
            webServer.start();
        } catch (Exception e) {
        }
    }

    @Override
    public void run() {
        try {
            boolean disconnected = false;
            while (!endpointManager.isDisconnected()) {
                try {
                    LOG.debug("Sending pings");

                    List<String> deadEndpoints = endpointManager.getTimeouted();
                    for (String endpointName : deadEndpoints) {
                        endpointManager.unregister(endpointName);
                    }
                    Set<String> endpoints = endpointManager.enumerateEndpoints();
                    ArrayList<String> visual = new ArrayList<>();
                    String transactionId = UUID.randomUUID().toString();
                    for (String endpointName : endpoints) {
                        visual.add("+" + endpointManager.getEndpointIndex(endpointName));
                        TextMessage tm = (TextMessage) (sessionQ).createTextMessage();
                        tm.setStringProperty("transactionId", transactionId);
                        tm.setSubject("ping." + endpointName);
                        LOG.debug("Data subject: ping." + endpointName);
                        endpointManager.doPing(endpointName);


//                        synchronized (producer) {
                        try {
                            producer.send(tm);
                        } catch (JMSException e) {
                            endpointManager.reconnect();
                            try {
                                producer.send(tm);
                            } catch (JMSException e2) {
                                LOG.error("Twin error: " + e.getLocalizedMessage());
                            }
                        }
                    }
                    Response sync = new Response();
                    endpointManager.setSyncObject(sync);

                    endpointManager.sendAnimation(transactionId, "ping", "<everyone>", "lightgreen", endpointManager.join(visual, ","));

                    try {
                        synchronized (sync) {
                            sync.setResponse("");
                            sync.wait(5000);
                        }
                    } catch (Exception e) {
                    }
                    endpointManager.sendAnimation(transactionId, "ping", "<everyone>", "lightgreen", sync.getResponse());
                    //send ping
                    Thread.sleep(this.interval * 1000);
                } catch (javax.jms.IllegalStateException e) {
                    LOG.info("ESB disconnected in monitor thread");
                    endpointManager.disconnect();
                } catch (Exception e) {
                    LOG.error("Error in monitor loop: " + e.getMessage());
                }
            }
            try {
                webServer.stop();
            } catch (Exception e) {
            }
            ;
        } catch (Exception e) {
            LOG.error("Error in monitor thread: " + e.getMessage());
        }
    }
}
