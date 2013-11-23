package org.gathe.integration;

import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.MessageProducer;
import org.apache.qpid.amqp_1_0.jms.Queue;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.util.*;

/**
 * Created by zolotov on 31.10.13.
 */
public class MonitorThread extends Thread {

    private int interval = 10;
    private Logger LOG = Logger.getLogger(this.getClass());
    private EndpointManager endpointManager;

    public MonitorThread(EndpointManager endpointManager) {
        LOG.info("Monitor thread initialized");
        this.endpointManager = endpointManager;
    }

    @Override
    public void run() {
        try {
            System.setProperty("max_prefetch", "1");
            Class.forName("org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
            Hashtable<String, String> properties = new Hashtable<String, String>();
            String path = new File("queue.properties").getAbsolutePath();
            properties.put("java.naming.provider.url", path);
            properties.put("java.naming.factory.initial", "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
            Context context = new InitialContext(properties);
            javax.jms.ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
            org.apache.qpid.amqp_1_0.jms.Connection connection = (org.apache.qpid.amqp_1_0.jms.Connection) connectionFactory.createConnection();
            connection.setClientID("dispatcher");
            connection.start();
            // org.apache.qpid.amqp_1_0.jms.Session sessionQ = connection.createSession(false, Session.AUTO_...);
            org.apache.qpid.amqp_1_0.jms.Session sessionQ = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            org.apache.qpid.amqp_1_0.jms.Queue integration = (Queue) context.lookup("integration");
            org.apache.qpid.amqp_1_0.jms.MessageProducer producer = sessionQ.createProducer(integration);
            Runtime.getRuntime().addShutdownHook(new MonitorThreadShutdownHook(sessionQ, connection, producer));

            while (true) {
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
                        visual.add("+"+endpointManager.getEndpointIndex(endpointName));
                        TextMessage tm = sessionQ.createTextMessage();
                        tm.setStringProperty("transactionId",transactionId);
                        tm.setSubject("ping." + endpointName);
                        LOG.debug("Data subject: ping." + endpointName);
                        producer.send(tm);
                        endpointManager.doPing(endpointName);
                    }
                    Response sync = new Response();
                    endpointManager.setSyncObject(sync);
                    
                    endpointManager.sendAnimation(transactionId,"ping","<everyone>","lightgreen",endpointManager.join(visual,","));

                    try {
                    synchronized (sync) {
                        sync.setResponse("");
                        sync.wait(5000);
                    }
                    } catch (Exception e) {};
//                    ArrayList<String> systems = endpointManager.getConfirmedSystems();
//                    visual = new ArrayList<>();
//                    for (String system : systems) {
//                        visual.add("-"+endpointManager.getEndpointIndex(system));
//                    }
                    endpointManager.sendAnimation(transactionId,"ping","<everyone>","lightgreen",sync.getResponse());
                    //send ping
                    Thread.sleep(this.interval * 1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    class MonitorThreadShutdownHook extends Thread {
        org.apache.qpid.amqp_1_0.jms.Session session;
        MessageProducer producer;
        org.apache.qpid.amqp_1_0.jms.Connection connection;

        public MonitorThreadShutdownHook(org.apache.qpid.amqp_1_0.jms.Session session, org.apache.qpid.amqp_1_0.jms.Connection connection, MessageProducer producer) {
            this.connection = connection;
            this.session = session;
            this.producer = producer;
        }

        public void run() {
            try {
                producer.close();
            } catch (JMSException e) {
            }
            try {
                session.close();
            } catch (JMSException e) {
            }
            try {
                connection.stop();
                connection.close();
            } catch (Exception e) {
            }
            System.out.println("Monitor thread gracefully shutdown");
        }
    }

}
