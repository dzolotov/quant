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
import org.apache.qpid.amqp_1_0.jms.Queue;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionImpl;
import org.w3c.dom.Document;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.StringWriter;
import java.util.*;

public class ReceiverThread extends Thread {

    Queue dispatcher;
    Logger LOG = Logger.getLogger(this.getClass());
    org.gathe.integration.EndpointManager endpointManager;
    org.apache.qpid.amqp_1_0.jms.impl.MessageProducerImpl producer;
    org.apache.qpid.amqp_1_0.jms.impl.MessageProducerImpl endpointsProducer;
    org.apache.qpid.amqp_1_0.jms.impl.MessageProducerImpl selfProducer;
    org.apache.qpid.amqp_1_0.jms.impl.MessageConsumerImpl messageConsumer;
    org.apache.qpid.amqp_1_0.jms.impl.ConnectionImpl connection;
    private HashMap<String, String> chunks = new HashMap<>();
    protected static HashMap<String, String> colors;

    static {
        colors = new HashMap<>();
        colors.put("get", "white");
        colors.put("remove", "red");
        colors.put("update", "yellow");
        colors.put("identify", "violet");
        colors.put("unify", "violet");
        colors.put("specify", "blue");
        colors.put("check", "pink");
        colors.put("match", "green");
    }

    ArrayList<String> uuidCommands = new ArrayList<>();
    org.apache.qpid.amqp_1_0.jms.impl.SessionImpl session;

    protected String joinStrings(String glue, String[] array) {
        String line = "";
        for (String s : array) line += s + glue;
        return (array.length == 0) ? line : line.substring(0, line.length() - glue.length());
    }


    public void sendToProducer(TextMessage tm) {
        synchronized (producer) {
            try {
                producer.send(tm);
            } catch (JMSException e) {
                //trying to reconnect
                try {
                    this.connect();
                    producer.send(tm);
                } catch (NamingException | JMSException e2) {
                    LOG.error("Twin Exception " + e2.getLocalizedMessage());
                }
            }
        }
    }


    private void sendChunk(TextMessage textMessage, String chunk, int number, int count) {

        synchronized (endpointsProducer) {
            try {
                textMessage.setIntProperty("number", number);
                textMessage.setIntProperty("count", count);
                textMessage.setText(chunk);
                endpointsProducer.send(textMessage);
            } catch (JMSException e) {
                try {
                    connect();
                    textMessage.setIntProperty("number", number);
                    textMessage.setIntProperty("count", count);
                    textMessage.setText(chunk);
                    endpointsProducer.send(textMessage);
                } catch (JMSException | NamingException e2) {
                    LOG.error("Twin error " + e.getLocalizedMessage());
                }
            }
        }

    }

    public void sendToEndpointsProducer(TextMessage textMessage, String content) throws JMSException {


        if (content.isEmpty()) {
            sendChunk(textMessage, "", 0, 1);
        }

        int chunkSize = 32768;          //todo: define as parameter

        //split message
        String subject = textMessage.getSubject();
        HashMap<String, String> headers = new HashMap<>();
        Enumeration<String> props = textMessage.getPropertyNames();
        while (props.hasMoreElements()) {
            String prop = props.nextElement();
            headers.put(prop, textMessage.getStringProperty(prop));
        }

        int count = (content.length() + (chunkSize - 1)) / chunkSize;
        for (int number = 0; number < count; number++) {
            TextMessage tm = this.session.createTextMessage();
            tm.setSubject(subject);
            for (String headerName : headers.keySet()) {
                tm.setStringProperty(headerName, headers.get(headerName));
            }
            int maxLimit = (number + 1) * chunkSize;
            if (maxLimit > content.length()) maxLimit = content.length();
            sendChunk(tm, content.substring(number * chunkSize, maxLimit), number, count);
        }
    }



/*
    public void sendToEndpointsProducer(TextMessage tm) {
        synchronized (endpointsProducer) {
            try {
                endpointsProducer.send(tm);
            } catch (JMSException e) {
                //trying to reconnect
                try {
                    this.connect();
                    endpointsProducer.send(tm);
                } catch (NamingException | JMSException e2) {
                    LOG.error("Twin Exception "+e2.getLocalizedMessage());
                }
            }
        }
    }
*/

    public void connect() throws JMSException, NamingException {
        LOG.info("Connecting to MQ Broker");
        try {
            Class.forName("org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        Hashtable<String, String> properties = new Hashtable<String, String>();
        String path = new File("queue.properties").getAbsolutePath();
        properties.put("java.naming.provider.url", path);
        properties.put("java.naming.factory.initial", "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
        javax.jms.ConnectionFactory connectionFactory = null;
        Context context = null;
        try {
            context = new InitialContext(properties);
            connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
        } catch (NamingException e) {
            e.printStackTrace();
            return;
        }

        connection = (ConnectionImpl) connectionFactory.createConnection();
//            connection.setClientID("dispatcher");
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.dispatcher = (org.apache.qpid.amqp_1_0.jms.Queue) context.lookup("dispatcher");
        messageConsumer = session.createConsumer(dispatcher);
//            ((MessageConsumerImpl) messageConsumer).setMaxPrefetch(1);
        org.apache.qpid.amqp_1_0.jms.Queue integration = (Queue) context.lookup("integration");
        producer = session.createProducer(integration);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        org.apache.qpid.amqp_1_0.jms.Queue endpoints = (Queue) context.lookup("endpoints");
        endpointsProducer = session.createProducer(endpoints);
        endpointsProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        org.apache.qpid.amqp_1_0.jms.Queue selfProducerQueue = (Queue) context.lookup("uno");
        selfProducer = session.createProducer(selfProducerQueue);
        selfProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        endpointManager.setSession(session);
        endpointManager.setMessageProducer(producer);
    }

    public ReceiverThread(EndpointManager endpointManager) {
        LOG.info("Receiver thread initialized");
        this.endpointManager = endpointManager;

        System.setProperty("max_prefetch", "1");
        try {
            this.connect();

            Runtime.getRuntime().addShutdownHook(new ReceiverThreadShutdownHook());
        } catch (Exception e) {
            LOG.error("Error when creating connection in dispatcher to ESB " + e.getMessage());
        }

        uuidCommands.add("get");
        uuidCommands.add("update");
        uuidCommands.add("remove");
        uuidCommands.add("identify");
        uuidCommands.add("specify");
    }

    @Override
    public void run() {
        try {

            String echoMessageId = UUID.randomUUID().toString();
            boolean activated = false;
            boolean checkForEchoResponse = false;
//            boolean isDisconnected = false;

            while (!endpointManager.isDisconnected()) {
                try {
                    Object message = null;
//                    LOG.debug("Receiver is waiting for a message");
//                    try {
                    if (activated) {
//                        LOG.info("Waiting");
//                        MessageConsumerImpl mci2 = (MessageConsumerImpl) messageConsumer;
//                        mci2.
                        message = messageConsumer.receive(50);
//                        LOG.info("Received");
                    } else {
                        message = messageConsumer.receive(200);
                    }
//                    } catch (JMSException e) {
//                        connect();
//                        if (activated) {
//                        LOG.info("Waiting");
//                        MessageConsumerImpl mci2 = (MessageConsumerImpl) messageConsumer;
//                        mci2.
//                            message = messageConsumer.receive(50);
//                        LOG.info("Received");
//                        } else {
//                            message = messageConsumer.receive(200);
//                        }
//                    }
                    if (message == null && !activated) {
                        if (checkForEchoResponse) {
                            LOG.error("Echo message not found: there are another consumers or message routing issue");
                            return;
                        }
                        TextMessage echoMessage = session.createTextMessage();
                        echoMessage.setText("echoing dispatcher");
                        echoMessage.setStringProperty("messageId", echoMessageId);
                        echoMessage.setSubject("dispatcher");
                        LOG.debug("Send echo message " + echoMessage.getText() + "|" + echoMessage.getSubject() + "|" + echoMessage.getStringProperty("messageId"));

                        synchronized (selfProducer) {
                            selfProducer.send(echoMessage);
                        }

                        LOG.info("Sleep to let our doppelgänger retrieve echo message");

                        Thread.sleep(500);
                        LOG.info("Wake up to check echo message presence");
                        checkForEchoResponse = true;
                        continue;
                    }

                    if (message == null) continue;

                    if (!(message instanceof TextMessage)) continue;
                    TextMessage textMessage = (TextMessage) message;
                    textMessage.acknowledge();
                    if (!activated && checkForEchoResponse && echoMessageId.equals(textMessage.getStringProperty("messageId"))) {
                        LOG.info("Echo message found: this dispatcher is only one");
                        TextMessage discoverMessage = session.createTextMessage();
                        discoverMessage.setSubject("discover");
                        discoverMessage.setStringProperty("messageId", UUID.randomUUID().toString());
                        sendToProducer(discoverMessage);
                        activated = true;
                        continue;
                    }

                    LOG.debug("Accepted message " + textMessage);
                    if (textMessage == null || textMessage.getSubject() == null) continue;

                    String content = textMessage.getText();
                    String messageId = textMessage.getStringProperty("messageId");
                    String routingKey = textMessage.getSubject();
                    String uuid = textMessage.getStringProperty("uuid");
                    String transactionId = textMessage.getStringProperty("transactionId");
                    String headers_id = textMessage.getStringProperty("id");
                    String replyTo = textMessage.getReplyTo();
                    LOG.debug("replyTo: " + replyTo);
                    if (transactionId == null) transactionId = "";
//                    LOG.info("Receiver: Message arrived: " + routingKey + " with " + content + " (transaction: " + transactionId + ")");
                    String[] keyParts = routingKey.split("\\.");

                    String action = keyParts[0].toLowerCase();

                    String[] actions = {"got", "identifyresponse", "matchresponse", "unifyresponse", "specifyresponse", "checkresponse", "hello"};
                    List<String> actionsList = Arrays.asList(actions);
                    if (actionsList.contains(action)) {
                        //merge chunks
                        int number = textMessage.getIntProperty("number");
                        int count = textMessage.getIntProperty("count");
                        LOG.info("Data chunk (length: " + content.length() + ") " + number + "/" + count);

                        if (!chunks.containsKey(messageId)) {
                            chunks.put(messageId, "");
                        }

                        chunks.put(messageId, chunks.get(messageId) + content);
                        if (number < count - 1) {
                            endpointManager.interruptRequestThread(messageId);

                            //notify requester
                            LOG.info("Result message id: " + messageId);

                            String replyMessageId = endpointManager.getResponseThread(messageId).getMessageId();
                            LOG.info("Origin message id: " + replyMessageId);

                            String reply = endpointManager.getReplyTo(messageId);
                            TextMessage notifyMessage = session.createTextMessage();
                            notifyMessage.setStringProperty("messageId", replyMessageId);
                            notifyMessage.setStringProperty("transactionId", transactionId);
                            notifyMessage.setSubject(reply);
                            notifyMessage.setStringProperty("waiting", "true");
                            LOG.info("Sending wait notify to ep: " + reply);
                            sendToEndpointsProducer(notifyMessage, "");
                            textMessage.acknowledge();
                            continue;
                        }
                        content = chunks.get(messageId);
                        chunks.remove(messageId);
                        textMessage.acknowledge();
                    }

                    switch (action) {

                        case "hello":
                            if (!activated) continue;
                            endpointManager.register(keyParts[1], content);
                            break;
                        //extract metadata from message body announcement
                        case "bye":
                            if (!activated) continue;
                            //disable endpoint
                            LOG.info("Receiver: Endpoint " + keyParts[1] + " is disconnected");
                            endpointManager.unregister(keyParts[1]);
                            break;
                        case "pong":
                            if (!activated) continue;
                            LOG.info("Receiver: Endpoint " + keyParts[1] + " is confirmed");
                            endpointManager.doPong(keyParts[1]);
                            break;
                        //confirm endpoint
                        case "update":
                        case "remove":
                        case "get":
                            // specify className if need
                            if (action.equalsIgnoreCase("get") && !activated) continue;
                            String className = (keyParts.length > 1) ? keyParts[1] : null;
                            String objectUuid = uuid;

                            LOG.info("Receiver: Request for '" + keyParts[0] + "' with class " + className + " and uuid " + objectUuid);

                            RequestThread th = null;

                            int from = endpointManager.getEndpointIndex(replyTo);

                            endpointManager.sendAnimation(transactionId, action + "." + className, objectUuid, colors.get(action), "-" + from); //get request

                            String topClass = endpointManager.traverseToAbstract(className);
                            String originClass = className;
                            String oldRoutingKey = routingKey;
                            routingKey = action + "." + topClass;

                            switch (action) {
                                case "update":
                                    LOG.info("Receiver: Update notification " + className + " uuid: " + objectUuid + " Content: " + content);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new HashMap<>(), null);
                                    break;

                                case "remove":
                                    LOG.info("Receiver: Remove notification " + className + " uuid: " + objectUuid + " Content: " + content);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new HashMap<>(), null);
                                    break;

                                case "get":
                                    //add animation for get

                                    LOG.info("Receiver: Requesting class " + className + " uuid: " + objectUuid);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new HashMap<>(), new GetResponseThread(transactionId, messageId, className));
                            }

                            if (th != null) {

                                LOG.info("Receiver: specify class " + className + " before action " + keyParts[0]);

                                SpecifyThread specifyThread = new SpecifyThread(transactionId, messageId, objectUuid, replyTo, oldRoutingKey, content, new SpecifyResponseThread(transactionId, messageId, className));
                                specifyThread.addChainThread(th);
                                specifyThread.start();
                            }
                            break;

                        case "identify":
                            if (!activated) continue;
                            String result = endpointManager.fetchStoredId(keyParts[2], uuid);
                            if (result == null) {

                                String identifierClass = endpointManager.searchNearestIdentification(keyParts[1], keyParts[2]);
                                if (identifierClass == null) {
                                    LOG.info("Identifier request can't be resolved");
                                    continue;
                                }
                                routingKey = keyParts[0] + "." + identifierClass + "." + keyParts[2];        //get new routing rule
                                from = endpointManager.getEndpointIndex(replyTo);
                                endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from);
                                LOG.info("Receiver: Identify request for " + keyParts[1] + " " + uuid);
                                th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new HashMap<>(), new IdentifyResponseThread(transactionId, messageId));
                                th.start();
                            } else {
                                TextMessage identifyResponse = session.createTextMessage();
                                identifyResponse.setStringProperty("messageId", messageId);
                                identifyResponse.setStringProperty("transactionId", transactionId);
                                identifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                                LOG.debug("Data: " + result + " message id: " + messageId);
                                LOG.info("Sending to ep: " + identifyResponse);
                                sendToEndpointsProducer(identifyResponse, result);
                            }
                            break;

                        case "unify":
                            if (!activated) continue;
                            result = endpointManager.fetchStoredUuid(keyParts[2], headers_id);
                            if (result == null) {
                                String identifierClass = endpointManager.searchNearestIdentification(keyParts[1], keyParts[2]);
                                if (identifierClass == null) {
                                    LOG.info("Identifier request can't be resolved");
                                    //todo: send null response
                                    continue;
                                }
                                routingKey = keyParts[0] + "." + identifierClass + "." + keyParts[2];        //get new routing rule

                                from = endpointManager.getEndpointIndex(replyTo);
                                endpointManager.sendAnimation(transactionId, routingKey, headers_id, colors.get(keyParts[0]), "-" + from); //get request
                                LOG.info("Receiver: Unify request for " + keyParts[1] + " " + headers_id);
                                th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content, new HashMap<>(), new UnifyResponseThread(transactionId, messageId));
                                th.start();
                            } else {
                                TextMessage unifyResponse = session.createTextMessage();
                                unifyResponse.setStringProperty("messageId", messageId);
                                unifyResponse.setStringProperty("transactionId", transactionId);
                                unifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                                LOG.debug("Data: " + result + " message id: " + messageId);
                                LOG.info("Sending to ep: " + unifyResponse);
                                sendToEndpointsProducer(unifyResponse, result);
                            }
                            break;
                        case "specify":
                            if (!activated) continue;
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from); //get request
                            LOG.info("Receiver: Specifing class " + keyParts[1] + " uuid: " + uuid);
                            th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new HashMap<>(), new SpecifyResponseThread(transactionId, messageId, keyParts[1]));
                            th.start();
                            break;
                        case "matchall":
                            if (!activated) continue;
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from);  //match request
                            LOG.info("Matching for class " + keyParts[1]);
                            //extract filters
                            Enumeration<String> filters = textMessage.getPropertyNames();
                            HashMap<String, String> filterData = new HashMap<>();
                            while (filters.hasMoreElements()) {
                                String filterName = filters.nextElement();
                                if (filterName.startsWith("filter-")) {
                                    String filterCondition = textMessage.getStringProperty(filterName);
                                    filterData.put(filterName, filterCondition);
                                }
                            }
                            filterData.put("mode", "seek");
                            filterData.put("explain", "false");
                            LOG.debug("Filterdata: " + filterData);
                            th = new RequestThread(transactionId, messageId, headers_id, replyTo, "match." + keyParts[1], content, filterData, new MatchResponseThread(transactionId, messageId));
                            th.start();
                            break;

                        case "check":
                            if (!activated) continue;
                            String checkClass = endpointManager.searchNearestCheckpoint(keyParts[1], keyParts[2]);
                            if (checkClass == null) {
                                LOG.info("Checkpoint for identifier " + keyParts[2] + "@" + keyParts[1] + " not found!");
                                continue;
                            }
                            routingKey = keyParts[0] + "." + checkClass + "." + keyParts[2];        //get new routing rule
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, headers_id, colors.get(keyParts[0]), "-" + from); //get request
                            LOG.info("Receiver: Check request for " + keyParts[1] + " " + headers_id);
                            th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content, new HashMap<>(), new CheckResponseThread(transactionId, messageId));
                            th.start();
                            break;

                        case "got":
                            if (!activated) continue;
                            LOG.info("Receiver: Got response from " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "unifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Unify response " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            LOG.info("Unify response parsed");
                            break;
                        case "matchresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Match response " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            LOG.info("Match response parsed");
                            break;
                        case "identifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Identify response " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "checkresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Check response: " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "specifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Specify response from " + keyParts[1]);
                            endpointManager.reactivateSystem(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                    }
                } catch (javax.jms.IllegalStateException e) {
                    LOG.info("ESB disconnected. Closing!");
                    endpointManager.disconnect();
                } catch (Exception e) {
                    LOG.error("Receiver: Error in message loop " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            LOG.error("Error in receiver initialization " + e.getMessage());
        }
        LOG.info("Leaving receiver thread");
    }
/*

    class ScanResponseThread extends ResponseThread {
        public ScanResponseThread(String transactionId, String messageId) {
            super(transactionId, messageId);
        }

        public void run() {
            //response animation
            ArrayList<String> responses = endpointManager.getAllResponses(messageId);
            boolean rescan = false;
            for (String response : responses) {
                LOG.debug("Scan result: " + response);
                String count = response.substring(0, response.indexOf("/"));
                String unbinded = response.substring(response.indexOf("/") + 1);
                if (Integer.getInteger(unbinded) != 0) rescan = true;
            }

            HashMap<String, String> headers = endpointManager.getHeaders(messageId);
            String replyTo = endpointManager.getReplyTo(messageId);

            //extract replyTo
            //extract filters
            headers.put("mode", "seek");
            headers.put("explain", (rescan ? "true" : "false"));

            endpointManager.interruptRequestThread(messageId);
            endpointManager.cleanupResponse(messageId);

            Thread th = new RequestThread(transactionId, messageId, "", replyTo, "match." + endpointManager.getClassNames(messageId), "", headers, new MatchResponseThread(transactionId, messageId));
            th.start();
        }
    }
*/

    class MatchResponseThread extends ResponseThread {
        public MatchResponseThread(String transactionId, String messageId) {
            super(transactionId, messageId);
        }


        public void run() {
            HashMap<String,String> headers = endpointManager.getHeaders(messageId);
            boolean explainMode = (""+headers.get("explain")).equalsIgnoreCase("true");
            if (!explainMode) {
                //response animation
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("match"), endpointManager.getResponseAnimation(messageId));
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("match"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                LOG.debug("Merge responses accepted");
                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
//            for (String response : responses) {
//                LOG.debug("Response: " + response);
//            }
                //todo: merge responses
                List<List<String>> allResponses = new ArrayList<>();
                List<String> result = new ArrayList<>();
                for (String response : responses) {
                    if (response == null) response = "";
                    String[] splittedResponse = response.split(",");
                    List<String> al = Arrays.asList(splittedResponse);
                    allResponses.add(al);
                }
                if (allResponses.size() > 0) {
                    List<String> t = allResponses.get(0);
                    for (String ent : t) {
                        boolean matchAll = true;
                        for (int i = 1; i < allResponses.size(); i++)
                            if (!allResponses.get(i).contains(ent)) {
                                matchAll = false;
                                break;
                            }
                        if (matchAll) result.add(ent);
                    }
                }
                String resultString = joinStrings(",", result.toArray(new String[0]));
//            LOG.debug("Merged response: " + resultString);
                try {
                    TextMessage matchResponse = session.createTextMessage();
                    matchResponse.setStringProperty("transactionId", transactionId);
                    matchResponse.setStringProperty("messageId", messageId);
                    matchResponse.setSubject(endpointManager.getReplyTo(messageId));
                    LOG.debug("Match response message: " + matchResponse + "; " + matchResponse.getText() + "|" + matchResponse.getStringProperty("messageId") + "|" + matchResponse.getSubject());
                    endpointManager.interruptRequestThread(messageId);
                    endpointManager.cleanupResponse(messageId);
                    LOG.info("Sending to ep: " + matchResponse);
                    sendToEndpointsProducer(matchResponse, resultString.toString());
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            } else {
                // ... explain mode, compare data ...
            }
        }
    }

    class GetResponseThread extends ResponseThread {
        private final String className;

        public GetResponseThread(String transactionId, String messageId, String originalClassName) {
            super(transactionId, messageId);
            this.className = originalClassName;
        }

        public void run() {
            //response animation
            endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("get"), endpointManager.getResponseAnimation(messageId));
            endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("get"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
            LOG.debug("Wow! XML is built");
            ArrayList<String> responses = endpointManager.getAllResponses(messageId);

            for (String response : responses) {
                LOG.debug("Response: " + response);
            }
            try {
                ResponseMerger responseMerger = new ResponseMerger(responses);
                Document result = responseMerger.getResponseXML(this.className);
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer = transformerFactory.newTransformer();
                StringWriter resultString = new StringWriter();
                transformer.transform(new DOMSource(result), new StreamResult(resultString));
                LOG.debug("Merged response: " + resultString);
                TextMessage getResponse = session.createTextMessage();
                getResponse.setStringProperty("transactionId", transactionId);
                getResponse.setStringProperty("messageId", messageId);
                getResponse.setSubject(endpointManager.getReplyTo(messageId));
                LOG.debug("Get response message: " + getResponse + "; " + getResponse.getText() + "|" + getResponse.getStringProperty("messageId") + "|" + getResponse.getSubject());
                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
                LOG.info("Sending to ep: " + getResponse);
                sendToEndpointsProducer(getResponse, resultString.toString());
            } catch (Exception e) {
                LOG.error("Error when sending response");
            }
        }
    }

    class UnifyResponseThread extends ResponseThread {

        public UnifyResponseThread(String transactionId, String messageId) {
            super(transactionId, messageId);
        }

        public void run() {
            //response animation
            try {
                LOG.info("Unify response thread activated");
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), endpointManager.getResponseAnimation(messageId));
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                LOG.info("Sendanim");
                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                //check for nonuniqueness
                String response = "";
                for (int i = 0; i < responses.size(); i++) {
                    if (responses.get(i).trim().length() != 0) {
                        response = responses.get(i);
                        break;
                    }
                }
                TextMessage unifyResponse = session.createTextMessage();
                unifyResponse.setStringProperty("messageId", messageId);
                unifyResponse.setStringProperty("transactionId", transactionId);
                unifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                endpointManager.interruptRequestThread(messageId);
                LOG.info("presync");
                sendToEndpointsProducer(unifyResponse, response);
                endpointManager.cleanupResponse(messageId);
                LOG.info("Response sent");
//                }

            } catch (Exception e) {
                LOG.error("Error when sending unify response");
            }
        }
    }


    class IdentifyResponseThread extends ResponseThread {

        public IdentifyResponseThread(String transactionId, String messageId) {
            super(transactionId, messageId);
        }

        public void run() {
            //response animation
            try {
                String identifyReplyTo = endpointManager.getReplyTo(messageId);
                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                String responseString = "";
                for (int i = 0; i < responses.size(); i++) {
                    if (responses.get(i).trim().length() != 0) {
                        responseString = responses.get(i);
                        break;
                    }
                }

                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("identify"), endpointManager.getResponseAnimation(messageId));
                if (identifyReplyTo != null) {

                    endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("identify"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                    LOG.debug("Sending identify response to requester " + endpointManager.getReplyTo(messageId));

                    //check for nonuniqueness
                    TextMessage identifyResponse = session.createTextMessage();
                    identifyResponse.setStringProperty("messageId", messageId);
                    identifyResponse.setStringProperty("transactionId", transactionId);
                    identifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                    LOG.debug("Data: " + responseString + " message id: " + messageId);
                    endpointManager.interruptRequestThread(messageId);
                    endpointManager.cleanupResponse(messageId);
                    LOG.info("Sending to ep: " + identifyResponse);
                    sendToEndpointsProducer(identifyResponse, responseString);
//                    }
                } else {
                    LOG.info("Receiver: Got identify response " + responseString + " and proceed to next action");
                    endpointManager.setResponse(messageId, responseString);
                    endpointManager.interruptRequestThread(messageId);
                    endpointManager.cleanupResponse(messageId);
                }

            } catch (Exception e) {
                LOG.error("Error when sending identify response " + e.getMessage());
            }
        }
    }

    class SpecifyResponseThread extends ResponseThread {
        private final String defaultValue;

        public SpecifyResponseThread(String transactionId, String messageId, String defaultValue) {
            super(transactionId, messageId);
            this.defaultValue = defaultValue;
        }

        public void run() {
            try {
                //response animation
                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                LOG.debug("Receiver: Specify responses " + responses);
                //get an unique collection

                String responseString = defaultValue;            //default is original classname
                ArrayList<String> specifications = new ArrayList<>();
                for (int i = 0; i < responses.size(); i++) {
                    String response = responses.get(i).trim();
                    String[] lines = response.split("\n");
                    for (String line : lines) {
                        if (line.trim().length() != 0 && !specifications.contains(line.trim()))
                            specifications.add(line.trim());
                    }
                }
                //todo: reorder by ACL priorities
                if (specifications.size() > 0) responseString = endpointManager.join(specifications, ",");

                String specifyReplyTo = endpointManager.getReplyTo(messageId);
                if (specifyReplyTo != null) {

                    endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("specify"), endpointManager.getResponseAnimation(messageId));
                    endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("specify"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));

                    LOG.info("Receiver: Got specify response " + responseString + " and send it to endpoint " + specifyReplyTo);

                    TextMessage specifyResponse = session.createTextMessage();
                    specifyResponse.setStringProperty("messageId", messageId);
                    specifyResponse.setStringProperty("transactionId", transactionId);
                    specifyResponse.setSubject(specifyReplyTo);
                    endpointManager.interruptRequestThread(messageId);
                    endpointManager.cleanupResponse(messageId);
                    LOG.info("Sending to ep: " + specifyResponse);
                    sendToEndpointsProducer(specifyResponse, responseString);
                } else {
                    LOG.info("Receiver: Got specify response " + responseString + " and proceed to next action");
                    endpointManager.interruptRequestThread(messageId);
                    endpointManager.cleanupResponse(messageId);
                    endpointManager.setResponse(messageId, responseString);
                }

            } catch (Exception e) {
                LOG.error("Error when sending specify response " + e.getMessage());
            }
        }
    }

    class CheckResponseThread extends ResponseThread {

        public CheckResponseThread(String transactionId, String messageId) {
            super(transactionId, messageId);
        }

        public void run() {
            try {
                //response animation
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("check"), endpointManager.getResponseAnimation(messageId));
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("check"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                String response = "false";
                for (int i = 0; i < responses.size(); i++) {
                    if (responses.get(i).trim().length() != 0) {
                        response = responses.get(i);
                        break;
                    }
                }
                String result = "false";
                if (response.equalsIgnoreCase("true") || response.equalsIgnoreCase("1"))
                    result = "true";
                TextMessage checkResponse = session.createTextMessage();
                checkResponse.setStringProperty("messageId", messageId);
                checkResponse.setStringProperty("transactionId", transactionId);
                checkResponse.setSubject(endpointManager.getReplyTo(messageId));
                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
                LOG.info("Sending to ep: " + checkResponse);
                sendToEndpointsProducer(checkResponse, result);
//                }

            } catch (Exception e) {
                LOG.error("Error when sending check response " + e.getMessage());
            }
        }

    }

    class RequestThread extends Thread {

        protected String identifierName;
        protected String identifier;
        protected String messageId;
        protected String transactionId;
        protected String replyTo;
        protected String action;
        protected String routingKey;
        protected String className;
        protected String content;
        protected ResponseThread responseThread;
        protected HashMap<String, String> headers;
        protected String overridedClasses = null;

        protected ArrayList<RequestThread> chainThreads = new ArrayList<>();

        protected String originalRoutingKey;
        protected String originalClassName;

        public RequestThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content, HashMap<String, String> headers, ResponseThread responseThread) {
            this.identifier = identifier;
            this.messageId = messageId;
            this.transactionId = transactionId;
            this.replyTo = replyTo;
            this.routingKey = routingKey;
            String[] keySplitted = routingKey.split("\\.");
            this.identifierName = keySplitted.length > 2 ? keySplitted[2] : "";
            this.action = keySplitted[0];
            this.className = keySplitted[1];
            this.content = content;
            this.headers = headers;
            this.responseThread = responseThread;
            LOG.info(action + " Thread initialized");
        }

        @Override
        public void run() {
            try {
                LOG.info("Run thread for " + action + ", " + className + ", " + identifier + " (replyTo: " + replyTo + ", messageId: " + messageId + ", routingKey:" + routingKey);

                TextMessage request = session.createTextMessage();
                request.setStringProperty("messageId", messageId);
                request.setStringProperty("transactionId", transactionId);
                request.setStringProperty((uuidCommands.contains(action.toLowerCase()) ? "uuid" : "id"), identifier);

                for (String headerName : this.headers.keySet()) {
                    request.setStringProperty(headerName, this.headers.get(headerName));
                }
                if (overridedClasses != null) {
                    request.setStringProperty("target", overridedClasses);
                }
                LOG.debug("Content: [" + content + "]");
                LOG.debug("TransactionID: " + transactionId);
                LOG.debug("MessageID: " + messageId);
                LOG.debug("Identifier: " + identifier);
                LOG.debug("Thread: " + responseThread);
                request.setText(content);

                request.setSubject(routingKey);
                action = action.toLowerCase();
                switch (action) {
                    case "get":
                        endpointManager.addGetRequest(routingKey, messageId, className, identifier, replyTo, this, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "specify":
                        endpointManager.addSpecifyRequest(routingKey, messageId, className, identifier, replyTo, this, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "check":
                        endpointManager.addCheckRequest(routingKey, messageId, className, identifierName, identifier, replyTo, this, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "match":
                        endpointManager.addMatchRequest(routingKey, messageId, className, replyTo, this, headers, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "identify":
                    case "unify":
                        endpointManager.addIdentifierRequest(routingKey, messageId, className, identifierName, identifier, replyTo, this, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className + "." + identifierName, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "update":
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToUpdateEndpoints(className));
                        sendToProducer(request);
                        return;

                    case "remove":
                        //get all IDs!
                        String getIdsReplyTo = null;

                        ArrayList<String> as = endpointManager.getIdentifierRequests(className);
                        for (String identifierName : as) {
                            String getIdsMessageId = UUID.randomUUID().toString();
                            String getIdsRoutingKey = "identify." + identifierName;
                            Thread th = new RequestThread(transactionId, getIdsMessageId, identifier, getIdsReplyTo, getIdsRoutingKey, content, new HashMap<>(), new IdentifyResponseThread(transactionId, getIdsMessageId));
                            th.start();
                            th.join();

                            String identifierValue = endpointManager.getResponse(getIdsMessageId);
                            if (identifierValue != null) {
                                LOG.debug("Found value for identifier " + identifierName + " is " + identifierValue);
                                //store identifier to database!!!!
                                endpointManager.storeIdentifier(identifier, identifierName, identifierValue);
                            }
                        }
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToUpdateEndpoints(className));
                        sendToProducer(request);
                        return;
                }

                sendToProducer(request);
                Thread.sleep(3000);
                endpointManager.timeoutResponse(messageId);
                LOG.info(action + " response timeout");

            } catch (JMSException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                LOG.info("Response arrived");
            }
        }

        public void setRequestClassName(String classNames) {
            String[] splitted = classNames.split(",");
            ArrayList<String> result = new ArrayList<>();
            for (String part : splitted) {
                result.add(endpointManager.getTrack(part));
            }
            overridedClasses = endpointManager.join(result, ",");
        }
    }

    class SpecifyThread extends RequestThread {

        public SpecifyThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content, ResponseThread responseThread) {
            super(transactionId, messageId, identifier, replyTo, routingKey, content, new HashMap<>(), responseThread);
        }

        public void run() {
            String specifyMessageId = UUID.randomUUID().toString();
            String[] sm = routingKey.split("\\.");
            String clName = sm[1];
            String specificClassName = clName;      //origin class
            if (endpointManager.isClassNameExtendable(clName)) {

                String specifyRoutingKey = "specify." + className; // "specify.<className>
                String specifyReplyTo = null;

                LOG.info("SpecifyThread: Create request for specify class " + this.className + " with messageId " + specifyMessageId + " (next messageId is " + messageId + ")");

                // identifier is uuid
                // replyTo is null


                Thread th = new RequestThread(transactionId, specifyMessageId, identifier, specifyReplyTo, specifyRoutingKey, content, new HashMap<>(), new SpecifyResponseThread(transactionId, specifyMessageId, clName));
                th.start();
                try {
                    th.join();

                    specificClassName = endpointManager.getResponse(specifyMessageId);

                    if (specificClassName == null) {
                        specificClassName = className;
                    }
                    LOG.info("SpecifyThread: Specify response for class " + className + " with messageId " + specifyMessageId + " is " + specificClassName);
                } catch (InterruptedException e) {

                }
            }

            for (RequestThread chainThread : chainThreads) {
                chainThread.setRequestClassName(specificClassName);
                chainThread.start();
            }
        }

        public void addChainThread(RequestThread th) {
            chainThreads.add(th);
        }
    }

    class ReceiverThreadShutdownHook extends Thread {

        public ReceiverThreadShutdownHook() {
        }

        @Override
        public void run() {
            endpointManager.disconnect();
            LOG.info("Receiver thread gracefully shutdown");
        }
    }
}
