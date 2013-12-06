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
import org.w3c.dom.Document;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.UUID;

public class ReceiverThread extends Thread {

    Queue dispatcher;
    Logger LOG = Logger.getLogger(this.getClass());
    org.gathe.integration.EndpointManager endpointManager;
    org.apache.qpid.amqp_1_0.jms.MessageProducer producer;
    org.apache.qpid.amqp_1_0.jms.MessageProducer endpointsProducer;
    org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer;
    org.apache.qpid.amqp_1_0.jms.MessageConsumer messageConsumer;
    org.apache.qpid.amqp_1_0.jms.Connection connection;
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
    }

    ArrayList<String> uuidCommands = new ArrayList<>();
    org.apache.qpid.amqp_1_0.jms.Session session;

    public ReceiverThread(EndpointManager endpointManager) {
        LOG.info("Receiver thread initialized");
        this.endpointManager = endpointManager;

        LOG.info("Receiver ready");
        System.setProperty("max_prefetch", "1");
        try {
            Class.forName("org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
            Hashtable<String, String> properties = new Hashtable<String, String>();
            String path = new File("queue.properties").getAbsolutePath();
            properties.put("java.naming.provider.url", path);
            properties.put("java.naming.factory.initial", "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
            Context context = new InitialContext(properties);
            javax.jms.ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
            connection = (org.apache.qpid.amqp_1_0.jms.Connection) connectionFactory.createConnection();
            connection.setClientID("dispatcher");
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.dispatcher = (org.apache.qpid.amqp_1_0.jms.Queue) context.lookup("dispatcher");
            messageConsumer = session.createConsumer(dispatcher);
            org.apache.qpid.amqp_1_0.jms.Queue integration = (Queue) context.lookup("integration");
            producer = session.createProducer(integration);
            org.apache.qpid.amqp_1_0.jms.Queue endpoints = (Queue) context.lookup("endpoints");
            endpointsProducer = session.createProducer(endpoints);
            org.apache.qpid.amqp_1_0.jms.Queue selfProducerQueue = (Queue) context.lookup("uno");
            selfProducer = session.createProducer(selfProducerQueue);

            Runtime.getRuntime().addShutdownHook(new ReceiverThreadShutdownHook());
            endpointManager.setSession(session);
            endpointManager.setMessageProducer(producer);
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
            boolean isDisconnected = false;

            while (!endpointManager.isDisconnected()) {
                try {
                    Object message = null;
                    if (activated) {
                        message = messageConsumer.receive();
                    } else {
                        message = messageConsumer.receive(100);
                    }
                    LOG.debug("Receiver is waiting for a message");
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

                    if (!(message instanceof TextMessage)) continue;
                    TextMessage textMessage = (TextMessage) message;
                    if (!activated && checkForEchoResponse && echoMessageId.equals(textMessage.getStringProperty("messageId"))) {
                        LOG.info("Echo message found: this dispatcher is only one");
                        TextMessage discoverMessage = session.createTextMessage();
                        discoverMessage.setSubject("discover");
                        discoverMessage.setStringProperty("messageId", UUID.randomUUID().toString());
                        synchronized (producer) {
                            producer.send(discoverMessage);            //send discover message to acquire connected endpoints
                        }
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
                    LOG.info("Receiver: Message arrived: " + routingKey + " with " + content + " (transaction: " + transactionId + ")");
                    String[] keyParts = routingKey.split("\\.");

                    String action = keyParts[0].toLowerCase();

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
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, null);
                                    break;

                                case "remove":
                                    LOG.info("Receiver: Remove notification " + className + " uuid: " + objectUuid + " Content: " + content);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, null);
                                    break;

                                case "get":
                                    //add animation for get

                                    LOG.info("Receiver: Requesting class " + className + " uuid: " + objectUuid);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new GetResponseThread(transactionId, messageId, className));
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
                                th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new IdentifyResponseThread(transactionId, messageId));
                                th.start();
                            } else {
                                TextMessage identifyResponse = session.createTextMessage();
                                identifyResponse.setStringProperty("messageId", messageId);
                                identifyResponse.setStringProperty("transactionId", transactionId);
                                identifyResponse.setText(result);
                                identifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                                LOG.debug("Data: " + result + " message id: " + messageId);
                                synchronized (endpointsProducer) {
                                    endpointsProducer.send(identifyResponse);
                                }
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
                                th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content, new UnifyResponseThread(transactionId, messageId));
                                th.start();
                            } else {
                                TextMessage unifyResponse = session.createTextMessage();
                                unifyResponse.setStringProperty("messageId", messageId);
                                unifyResponse.setStringProperty("transactionId", transactionId);
                                unifyResponse.setText(result);
                                unifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                                LOG.debug("Data: " + result + " message id: " + messageId);
                                synchronized (endpointsProducer) {
                                    endpointsProducer.send(unifyResponse);
                                }

                            }
                            break;
                        case "specify":
                            if (!activated) continue;
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from); //get request
                            LOG.info("Receiver: Specifing class " + keyParts[1] + " uuid: " + uuid);
                            th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content, new SpecifyResponseThread(transactionId, messageId, keyParts[1]));
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
                            th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content, new CheckResponseThread(transactionId, messageId));
                            th.start();
                            break;

                        case "got":
                            if (!activated) continue;
                            LOG.info("Receiver: Got response from " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "unifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Unify response " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "identifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Identify response " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "checkresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Check response: " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            endpointManager.gotResponse(messageId, keyParts[1], content);
                            break;
                        case "specifyresponse":
                            if (!activated) continue;
                            LOG.info("Receiver: Specify response from " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
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

    class GetResponseThread extends Thread {
        private final String transactionId;
        private final String messageId;
        private final String className;

        public GetResponseThread(String transactionId, String messageId, String originalClassName) {
            this.transactionId = transactionId;
            this.messageId = messageId;
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
                getResponse.setText(resultString.toString());
                getResponse.setSubject(endpointManager.getReplyTo(messageId));
                LOG.debug("Get response message: " + getResponse + "; " + getResponse.getText() + "|" + getResponse.getStringProperty("messageId") + "|" + getResponse.getSubject());
                synchronized (endpointsProducer) {
                    endpointsProducer.send(getResponse);
                }
                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
            } catch (Exception e) {
                LOG.error("Error when sending response");
            }
        }
    }

    class UnifyResponseThread extends Thread {
        private final String transactionId;
        private final String messageId;

        public UnifyResponseThread(String transactionId, String messageId) {
            this.transactionId = transactionId;
            this.messageId = messageId;
        }

        public void run() {
            //response animation
            try {
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), endpointManager.getResponseAnimation(messageId));
                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
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
                unifyResponse.setText(response);
                unifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                synchronized (endpointsProducer) {
                    endpointsProducer.send(unifyResponse);
                }

                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
            } catch (Exception e) {
                LOG.error("Error when sending unify response");
            }
        }
    }


    class IdentifyResponseThread extends Thread {
        private final String transactionId;
        private final String messageId;

        public IdentifyResponseThread(String transactionId, String messageId) {
            this.transactionId = transactionId;
            this.messageId = messageId;
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
                    identifyResponse.setText(responseString);
                    identifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                    LOG.debug("Data: " + responseString + " message id: " + messageId);
                    synchronized (endpointsProducer) {
                        endpointsProducer.send(identifyResponse);
                    }
                } else {
                    LOG.info("Receiver: Got identify response " + responseString + " and proceed to next action");
                    endpointManager.setResponse(messageId, responseString);
                }

                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
            } catch (Exception e) {
                LOG.error("Error when sending identify response " + e.getMessage());
            }
        }
    }

    class SpecifyResponseThread extends Thread {
        private final String transactionId;
        private final String messageId;
        private final String defaultValue;

        public SpecifyResponseThread(String transactionId, String messageId, String defaultValue) {
            this.transactionId = transactionId;
            this.messageId = messageId;
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
                    specifyResponse.setText(responseString);
                    specifyResponse.setSubject(specifyReplyTo);
                    synchronized (endpointsProducer) {
                        endpointsProducer.send(specifyResponse);
                    }
                } else {
                    LOG.info("Receiver: Got specify response " + responseString + " and proceed to next action");
                    endpointManager.setResponse(messageId, responseString);
                }
                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);

            } catch (Exception e) {
                LOG.error("Error when sending specify response " + e.getMessage());
            }
        }
    }

    class CheckResponseThread extends Thread {

        private final String transactionId;
        private final String messageId;

        public CheckResponseThread(String transactionId, String messageId) {
            this.transactionId = transactionId;
            this.messageId = messageId;
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
                checkResponse.setText(result);
                checkResponse.setSubject(endpointManager.getReplyTo(messageId));
                synchronized (endpointsProducer) {
                    endpointsProducer.send(checkResponse);
                }

                endpointManager.interruptRequestThread(messageId);
                endpointManager.cleanupResponse(messageId);
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
        protected Thread responseThread;
        protected String overridedClasses = null;

        protected ArrayList<RequestThread> chainThreads = new ArrayList<>();

        protected String originalRoutingKey;
        protected String originalClassName;

        public RequestThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content, Thread responseThread) {
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

                    case "identify":
                    case "unify":
                        endpointManager.addIdentifierRequest(routingKey, messageId, className, identifierName, identifier, replyTo, this, responseThread);
                        endpointManager.sendAnimation(transactionId, action + "." + className + "." + identifierName, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "update":
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToUpdateEndpoints(className));
                        synchronized (producer) {
                            producer.send(request);
                        }
                        return;

                    case "remove":
                        //get all IDs!
                        String getIdsReplyTo = null;

                        ArrayList<String> as = endpointManager.getIdentifierRequests(className);
                        for (String identifierName : as) {
                            String getIdsMessageId = UUID.randomUUID().toString();
                            String getIdsRoutingKey = "identify." + identifierName;
                            Thread th = new RequestThread(transactionId, getIdsMessageId, identifier, getIdsReplyTo, getIdsRoutingKey, content, new IdentifyResponseThread(transactionId, getIdsMessageId));
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
                        synchronized (producer) {
                            producer.send(request);
                        }
                        return;
                }

                synchronized (producer) {
                    producer.send(request);
                }
                Thread.sleep(2000);
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

        public SpecifyThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content, Thread responseThread) {
            super(transactionId, messageId, identifier, replyTo, routingKey, content, responseThread);
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


                Thread th = new RequestThread(transactionId, specifyMessageId, identifier, specifyReplyTo, specifyRoutingKey, content, new SpecifyResponseThread(transactionId, specifyMessageId, clName));
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
