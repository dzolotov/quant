package org.gathe.integration;

import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.Queue;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.apache.qpid.amqp_1_0.jms.impl.AmqpMessageImpl;
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

;

/**
 * Created by zolotov on 31.10.13.
 */
public class ReceiverThread extends Thread {

    Queue dispatcher;
    Logger LOG = Logger.getLogger(this.getClass());
    org.gathe.integration.EndpointManager endpointManager;
    org.apache.qpid.amqp_1_0.jms.MessageProducer producer;
    org.apache.qpid.amqp_1_0.jms.MessageProducer endpointsProducer;
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

        uuidCommands.add("get");
        uuidCommands.add("update");
        uuidCommands.add("remove");
        uuidCommands.add("identify");
        uuidCommands.add("specify");

    }

    @Override
    public void run() {
        try {
            LOG.info("Receiver ready");
            System.setProperty("max_prefetch", "1");
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
            // org.apache.qpid.amqp_1_0.jms.Session sessionQ = connection.createSession(false, Session.AUTO_...);
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.dispatcher = (org.apache.qpid.amqp_1_0.jms.Queue) context.lookup("dispatcher");
            messageConsumer = session.createConsumer(dispatcher);
            org.apache.qpid.amqp_1_0.jms.Queue integration = (Queue) context.lookup("integration");
            producer = session.createProducer(integration);
            org.apache.qpid.amqp_1_0.jms.Queue endpoints = (Queue) context.lookup("endpoints");
            endpointsProducer = session.createProducer(endpoints);

            Runtime.getRuntime().addShutdownHook(new ReceiverThreadShutdownHook());

            while (true) {
                try {
                    LOG.debug("Receiver is waiting for a message");
                    Object message = messageConsumer.receive();
                    String content = null;
                    String messageId = "";
                    String routingKey = "";
                    String replyTo = "";
                    String transactionId = "";
                    String uuid = null;
                    String headers_id = null;
                    if (message instanceof AmqpMessageImpl) {
                        messageId = ((AmqpMessageImpl) message).getStringProperty("messageId");
                        routingKey = ((AmqpMessageImpl) message).getSubject();
                        replyTo = ((AmqpMessageImpl) message).getReplyTo();
                        uuid = ((AmqpMessageImpl) message).getStringProperty("uuid");
                        transactionId = ((AmqpMessageImpl) message).getStringProperty("transactionId");
                        headers_id = ((AmqpMessageImpl) message).getStringProperty("id");
                        content = "";
                        LOG.debug("replyTo: " + replyTo);
                    } else if (message instanceof TextMessage) {
                        content = ((TextMessage) message).getText();
                        messageId = ((TextMessage) message).getStringProperty("messageId");
                        routingKey = ((TextMessage) message).getSubject();
                        uuid = ((TextMessage) message).getStringProperty("uuid");
                        transactionId = ((TextMessage) message).getStringProperty("transactionId");
                        headers_id = ((TextMessage) message).getStringProperty("id");
                        replyTo = ((TextMessage) message).getReplyTo();
                        LOG.debug("replyTo: " + replyTo);
                    }
//                    org.apache.qpid.amqp_1_0.jms.TextMessage textMessage = (org.apache.qpid.amqp_1_0.jms.TextMessage) message;
//                    Enumeration en = textMessage.getPropertyNames();
//                    while (en.hasMoreElements()) {
//                        String propertyName = "" + en.nextElement();
//                        LOG.debug(propertyName + " : " + textMessage.getObjectProperty(propertyName));
//                    }
                    if (transactionId == null) transactionId = "";
                    LOG.info("Receiver: Message arrived: " + routingKey + " with " + content + " (transaction: " + transactionId + ")");
                    String[] keyParts = routingKey.split("\\.");

                    String action = keyParts[0].toLowerCase();

                    switch (action) {

                        case "hello":
                            endpointManager.register(keyParts[1], content);
                            break;
                        //extract metadata from message body announcement
                        case "bye":
                            //disable endpoint
                            LOG.info("Receiver: Endpoint " + keyParts[1] + " is disconnected");
                            endpointManager.unregister(keyParts[1]);
                            break;
                        case "pong":
                            LOG.info("Receiver: Endpoint " + keyParts[1] + " is confirmed");
                            endpointManager.doPong(keyParts[1]);
                            break;
                        //confirm endpoint
                        case "update":
                        case "remove":
                        case "get":
                            // specify className if need
                            String className = (keyParts.length > 1) ? keyParts[1] : null;
                            String objectUuid = uuid;

                            LOG.info("Receiver: Request for '" + keyParts[0] + "' with class " + className + " and uuid " + objectUuid);

                            RequestThread th = null;

                            int from = endpointManager.getEndpointIndex(replyTo);

                            endpointManager.sendAnimation(transactionId, action + "." + className, objectUuid, colors.get(action), "-" + from); //get request

                            switch (action) {
                                case "update":
                                    LOG.info("Receiver: Update notification " + className + " uuid: " + objectUuid + " Content: " + content);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content);
                                    break;

                                case "remove":
                                    LOG.info("Receiver: Remove notification " + className + " uuid: " + objectUuid + " Content: " + content);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content);
                                    break;

                                case "get":
                                    //add animation for get

                                    LOG.info("Receiver: Requesting class " + className + " uuid: " + objectUuid);
                                    th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content);
                            }

                            if (th != null) {

                                if (endpointManager.isClassNameExtendable(className)) {

                                    LOG.info("Receiver: specify class " + className + " before action " + keyParts[0]);

                                    SpecifyThread specifyThread = new SpecifyThread(transactionId, messageId, objectUuid, replyTo, routingKey, content);
                                    specifyThread.addChainThread(th);
                                    specifyThread.start();


                                } else {
                                    LOG.info("Receiver: Do not specify class " + className + " before " + keyParts[0]);

                                    th.start();
                                }
                            }
                            break;

                        case "identify":
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from); //get request
//                        endpointManager.sendAnimation(transactionId, routingKey, uuid, "yellow", endpointManager.getAnimationToEndpointIndexes(messageId,keyParts[1]));
                            LOG.info("Receiver: Identify request for " + keyParts[1] + " " + uuid);
                            th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content);
                            th.start();
                            break;

                        case "unify":

                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, headers_id, colors.get(keyParts[0]), "-" + from); //get request
//                        endpointManager.sendAnimation(transactionId, routingKey, headers_id, "yellow", endpointManager.getAnimationToEndpointIndexes(messageId,keyParts[1]));
                            LOG.info("Receiver: Unify request for " + keyParts[1] + " " + headers_id);
                            th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content);
                            th.start();
                            break;
                        case "specify":
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, uuid, colors.get(keyParts[0]), "-" + from); //get request
//                        endpointManager.sendAnimation(transactionId, routingKey, uuid, "yellow", endpointManager.getAnimationToEndpointIndexes(messageId,keyParts[1]));
                            LOG.info("Receiver: Specifing class " + keyParts[1] + " uuid: " + uuid);
                            th = new RequestThread(transactionId, messageId, uuid, replyTo, routingKey, content);
                            th.start();
                            break;
                        case "check":
                            from = endpointManager.getEndpointIndex(replyTo);
                            endpointManager.sendAnimation(transactionId, routingKey, headers_id, colors.get(keyParts[0]), "-" + from); //get request
//                        endpointManager.sendAnimation(transactionId, routingKey, headers_id, "yellow", endpointManager.getAnimationToEndpointIndexes(messageId,keyParts[1]));
                            LOG.info("Receiver: Check request for " + keyParts[1] + " " + headers_id);
                            th = new RequestThread(transactionId, messageId, headers_id, replyTo, routingKey, content);
                            th.start();
                            break;

                        case "got":
                            LOG.info("Receiver: Got response from " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            if (endpointManager.gotResponse(messageId, keyParts[1], content)) {
                                //response animation
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("get"), endpointManager.getResponseAnimation(messageId));
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("get"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                                LOG.debug("Wow! XML is built");
                                ArrayList<String> responses = endpointManager.getAllResponses(messageId);

                                for (String response : responses) {
                                    LOG.debug("Response: " + response);
                                }
                                ResponseMerger responseMerger = new ResponseMerger(responses);
                                Document result = responseMerger.getResponseXML();
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
                                endpointsProducer.send(getResponse);
                                endpointManager.interruptRequestThread(messageId);
                                endpointManager.cleanupResponse(messageId);
                            }
                            break;
                        case "unifyresponse":
                            LOG.info("Receiver: Unify response " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            if (endpointManager.gotResponse(messageId, keyParts[1], content)) {
                                //response animation
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), endpointManager.getResponseAnimation(messageId));
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("unify"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                                //check for nonuniqueness
                                String response = responses.get(0);
                                TextMessage unifyResponse = session.createTextMessage();
                                unifyResponse.setStringProperty("messageId", messageId);
                                unifyResponse.setStringProperty("transactionId", transactionId);
                                unifyResponse.setText(response);
                                unifyResponse.setSubject(endpointManager.getReplyTo(messageId));
                                endpointsProducer.send(unifyResponse);


                                endpointManager.interruptRequestThread(messageId);
                                endpointManager.cleanupResponse(messageId);
                            }
                            break;
                        case "identifyresponse":
                            LOG.info("Receiver: Identify response " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            if (endpointManager.gotResponse(messageId, keyParts[1], content)) {
                                //response animation

                                String identifyReplyTo = endpointManager.getReplyTo(messageId);
                                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                                String responseString = responses.get(0);

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
                                    endpointsProducer.send(identifyResponse);
                                } else {
                                    LOG.info("Receiver: Got identify response " + responseString + " and proceed to next action");
                                    endpointManager.setResponse(messageId, responseString);
                                }

                                endpointManager.interruptRequestThread(messageId);
                                endpointManager.cleanupResponse(messageId);

                            }
                            break;
                        case "checkresponse":
                            LOG.info("Receiver: Check response: " + keyParts[1]);
                            endpointManager.doPing(keyParts[1]);
                            if (endpointManager.gotResponse(messageId, keyParts[1], content)) {
                                //response animation
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("check"), endpointManager.getResponseAnimation(messageId));
                                endpointManager.sendAnimation(transactionId, endpointManager.getRequestCommonAction(messageId), endpointManager.getRequestIdentifier(messageId), colors.get("check"), "+" + endpointManager.getEndpointIndex(endpointManager.getReplyTo(messageId)));
                                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                                String response = responses.get(0);
                                String result = "false";
                                if (response.equalsIgnoreCase("true") || response.equalsIgnoreCase("1"))
                                    result = "true";
                                TextMessage checkResponse = session.createTextMessage();
                                checkResponse.setStringProperty("messageId", messageId);
                                checkResponse.setStringProperty("transactionId", transactionId);
                                checkResponse.setText(result);
                                checkResponse.setSubject(endpointManager.getReplyTo(messageId));
                                endpointsProducer.send(checkResponse);

                                endpointManager.interruptRequestThread(messageId);
                                endpointManager.cleanupResponse(messageId);
                            }
                            break;
                        case "specifyresponse":
                            LOG.info("Receiver: Specify response from " + keyParts[1]);
                            endpointManager.doPong(keyParts[1]);
                            if (endpointManager.gotResponse(messageId, keyParts[1], content)) {
                                //response animation
                                ArrayList<String> responses = endpointManager.getAllResponses(messageId);
                                LOG.debug("Receiver: Specify responses " + responses);
                                //check for nonuniqueness

                                String responseString = responses.get(0);

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
                                    endpointsProducer.send(specifyResponse);
                                } else {
                                    LOG.info("Receiver: Got specify response " + responseString + " and proceed to next action");
                                    endpointManager.setResponse(messageId, responseString);
                                }
                                endpointManager.interruptRequestThread(messageId);
                                endpointManager.cleanupResponse(messageId);
                            }
                            break;
                    }
                } catch (Exception e) {
                    LOG.error("Receiver: Error in message loop " + e.getMessage());
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            LOG.error("Error in receiver initialization " + e.getMessage());
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

        protected ArrayList<RequestThread> chainThreads = new ArrayList<>();

        protected String originalRoutingKey;
        protected String originalClassName;

        public RequestThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content) {
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
                request.setText(content);

                request.setSubject(routingKey);
                action = action.toLowerCase();
                switch (action) {
                    case "get":
                        endpointManager.addGetRequest(routingKey, messageId, className, identifier, replyTo, this);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "specify":
                        endpointManager.addSpecifyRequest(routingKey, messageId, className, identifier, replyTo, this);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "check":
                        endpointManager.addCheckRequest(routingKey, messageId, className, identifierName, identifier, replyTo, this);
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "identify":
                    case "unify":
                        endpointManager.addIdentifierRequest(routingKey, messageId, className, identifierName, identifier, replyTo, this);
                        endpointManager.sendAnimation(transactionId, action + "." + className + "." +identifierName, identifier, colors.get(action), endpointManager.getAnimationToEndpointIndexes(messageId));
                        break;

                    case "update":
                        endpointManager.sendAnimation(transactionId, action + "." + className, identifier, colors.get(action), endpointManager.getAnimationToUpdateEndpoints(className));
                        break;

                    case "remove":
                        //get all IDs!
                        String getIdsReplyTo = null;

                        ArrayList<String> as = endpointManager.getIdentifierRequests(className);
                        for (String identifierName : as) {
                            String getIdsMessageId = UUID.randomUUID().toString();
                            String getIdsRoutingKey = "identify." + identifierName;
                            Thread th = new RequestThread(transactionId, getIdsMessageId, identifier, getIdsReplyTo, getIdsRoutingKey, content);
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
                        break;
                }

                producer.send(request);
                LOG.debug(action + " request sent");
                Thread.sleep(5000);
                LOG.debug(action + " response timeout");
            } catch (JMSException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                LOG.info("Response arrived");
            }
        }

        public void setRequestClassName(String className) {
            this.originalRoutingKey = this.routingKey;
            this.originalClassName = this.className;

            String[] routingKeyParts = this.routingKey.split("\\.");
            routingKeyParts[1] = className;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < routingKeyParts.length; i++) {
                if (sb.length() == 0) {
                    sb.append(routingKeyParts[i]);
                } else {
                    sb.append("." + routingKeyParts[i]);
                }
            }

            this.routingKey = sb.toString();
            this.className = className;
        }

    }

    class SpecifyThread extends RequestThread {

        public SpecifyThread(String transactionId, String messageId, String identifier, String replyTo, String routingKey, String content) {
            super(transactionId, messageId, identifier, replyTo, routingKey, content);
        }

        public void run() {
            String specifyMessageId = UUID.randomUUID().toString();
            String specifyRoutingKey = "specify." + className; // "specify.<className>
            String specifyReplyTo = null;

            LOG.info("SpecifyThread: Create request for specify class " + this.className + " with messageId " + specifyMessageId + " (next messageId is " + messageId + ")");

            // identifier is uuid
            // replyTo is null

            Thread th = new RequestThread(transactionId, specifyMessageId, identifier, specifyReplyTo, specifyRoutingKey, content);
            th.start();
            try {
                th.join();

                String specificClassName = endpointManager.getResponse(specifyMessageId);

                if (specificClassName == null) {
                    specificClassName = className;
                }
                LOG.info("SpecifyThread: Specify response for class " + className + " with messageId " + specifyMessageId + " is " + specificClassName);

                for (RequestThread chainThread : chainThreads) {
                    chainThread.setRequestClassName(specificClassName);
                    chainThread.start();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();

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
            try {
                messageConsumer.close();
            } catch (JMSException e) {
            }
            try {
                producer.close();
            } catch (JMSException e) {
            }
            try {
                endpointsProducer.close();
            } catch (JMSException e) {
            }
            try {
                session.close();
            } catch (JMSException e) {
            }
            try {
                connection.stop();
                connection.close();
            } catch (JMSException e) {

            }
            System.out.println("Receiver thread gracefully shutdown");
        }
    }
}
