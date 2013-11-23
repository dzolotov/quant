package org.gathe.integration;

import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.Message;
import org.apache.qpid.amqp_1_0.jms.Queue;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.UUID;

/**
 * Created by zolotov on 01.11.13.
 */
public class Connector extends Thread {

    //Destination queue to echo selfdiagnostics
    org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer;
    //Destination queue to exchange messages with dispatcher
    org.apache.qpid.amqp_1_0.jms.MessageProducer uno;
    //Source queue for direct commands and data response
    org.apache.qpid.amqp_1_0.jms.MessageConsumer consumer;
    org.apache.qpid.amqp_1_0.jms.Session session;
    private boolean activated = false;
    private String id;
    private Accessor accessor;
    private Logger LOG = Logger.getLogger(this.getClass());
    private HashMap<String, String> getResponse = new HashMap<String, String>();
    private HashMap<String, Thread> responseThreads = new HashMap<String, Thread>();


    ArrayList<String> uuidCommands = new ArrayList<String>();


    public Connector(String id, Accessor accessor) throws ClassNotFoundException, NamingException, JMSException {
        this.id = id;
        this.accessor = accessor;
        System.setProperty("max_prefetch", "1");
        Class.forName("org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
        Hashtable<String, String> properties = new Hashtable<String, String>();
        String path = new File("queue.properties").getAbsolutePath();
        properties.put("java.naming.provider.url", path);
        properties.put("java.naming.factory.initial", "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
        Context context = new InitialContext(properties);
        javax.jms.ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
        org.apache.qpid.amqp_1_0.jms.Connection connection = (org.apache.qpid.amqp_1_0.jms.Connection) connectionFactory.createConnection();
        connection.setClientID(id);
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        org.apache.qpid.amqp_1_0.jms.Queue inbound = (Queue) context.lookup("inbound");
        consumer = session.createConsumer(inbound);

        org.apache.qpid.amqp_1_0.jms.Queue selfQueue = (Queue) context.lookup("endpoints");
        selfProducer = session.createProducer(selfQueue);

        org.apache.qpid.amqp_1_0.jms.Queue outbound = (Queue) context.lookup("uno");
        uno = session.createProducer(outbound);


        uuidCommands.add("get");
        uuidCommands.add("specify");
        uuidCommands.add("identify");
        uuidCommands.add("update");
        uuidCommands.add("remove");

        Runtime.getRuntime().addShutdownHook(new ConnectorShutdownHook(id, session, connection, uno, consumer, selfProducer));
        this.start();
    }

    //aux method - send text message to dispatcher with specified subject and content
    public void sendTextMessage(String subject, String content) throws JMSException {
        TextMessage textMessage = this.session.createTextMessage(content);
        LOG.info("Sending message with subject " + subject + " and content " + content);
        textMessage.setSubject(subject);
        uno.send(textMessage);
    }

    //prepare schema announce from HashMap description
    public Document prepareSchema(HashMap<String, HashMap<String, String>> schemaDescription) throws ParserConfigurationException {

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document schema = dBuilder.newDocument();

        Element rootElement = schema.createElement("schema");
        schema.appendChild(rootElement);

        for (String schemaKey : schemaDescription.keySet()) {
            HashMap<String, String> schemaEntries = schemaDescription.get(schemaKey);
            Element classElement = schema.createElement("class");
            classElement.setAttribute("id", schemaKey);
            for (String schemaEntryKey : schemaEntries.keySet()) {
                Element classAttribute = null;
                if (schemaEntryKey.startsWith("@")) {
                    String schemaAttributeName = schemaEntryKey.substring(1);
                    classElement.setAttribute(schemaAttributeName, schemaEntries.get(schemaEntryKey));
                } else if (schemaEntryKey.startsWith("#")) {
                    classAttribute = schema.createElement("identifier");
                    classAttribute.setAttribute("name", schemaEntryKey.substring(1));
                    classAttribute.setAttribute("description", schemaEntries.get(schemaEntryKey));
                } else if (schemaEntryKey.startsWith("?")) {
                    classAttribute = schema.createElement("check");
                    classAttribute.setAttribute("name", schemaEntryKey.substring(1));
                    classAttribute.setAttribute("description", schemaEntries.get(schemaEntryKey));
                } else {
                    classAttribute = schema.createElement("attribute");
                    classAttribute.setAttribute("path", schemaEntryKey);
                    classAttribute.setAttribute("description", schemaEntries.get(schemaEntryKey));
                }
                if (classAttribute != null) {
                    classElement.appendChild(classAttribute);
                }
            }
            rootElement.appendChild(classElement);
        }
        return schema;
    }

    public String get(String transactionId, String className, String uuid, boolean async) throws JMSException {
        return this.doAction("get", transactionId, className, uuid, null, async);
    }

    public String unify(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException {
        return this.doAction("unify", transactionId, className, identifierValue, identifier, async);
    }

    public String identify(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException {
        return this.doAction("identify", transactionId, className, identifierValue, identifier, async);
    }

    public String check(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException {
        return this.doAction("check", transactionId, className, identifierValue, identifier, async);
    }

    public String specify(String transactionId, String className, String uuid, boolean async) throws JMSException {
        return this.doAction("specify", transactionId, className, uuid, null, async);
    }

    public void remove(String transactionId, String className, String uuid) throws JMSException {
        this.doActionWithoutResponse("remove", transactionId, className, uuid, null, "");
    }

    public void update(String transactionId, String className, String uuid, String content) throws  JMSException {
        this.doActionWithoutResponse("update", transactionId, className, uuid, null, content);
    }

//end todo

    private void doActionWithoutResponse(String action, String transactionId, String className, String identifierValue, String suffix, String content) throws JMSException {
        while (!activated) {
            try {
                LOG.debug("Waiting for activation");
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        if (transactionId == null) transactionId = UUID.randomUUID().toString();
        String messageId = UUID.randomUUID().toString();
        TextMessage actionMessage = session.createTextMessage();
        actionMessage.setReplyTo(this.id);
        actionMessage.setStringProperty("messageId", messageId);
        actionMessage.setStringProperty("transactionId", transactionId);
        actionMessage.setSubject(action + "." + className + ((suffix != null) ? "." + suffix : ""));
        actionMessage.setStringProperty((uuidCommands.contains(action.toLowerCase()) ? "uuid" : "id"), identifierValue);
        actionMessage.setText(content);
        uno.send(actionMessage);
    }

    private String doAction(String action, String transactionId, String className, String identifierValue, String suffix, boolean async) throws JMSException {
        while (!activated) {
            try {
                LOG.debug("Waiting for activation");
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        if (transactionId == null) transactionId = UUID.randomUUID().toString();
        String messageId = UUID.randomUUID().toString();
        TextMessage getMessage = session.createTextMessage();
        getMessage.setReplyTo(this.id);
        getMessage.setStringProperty("messageId", messageId);
        getMessage.setStringProperty("transactionId", transactionId);

        getMessage.setSubject(action + "." + className + ((suffix != null) ? "." + suffix : ""));
        getMessage.setStringProperty((uuidCommands.contains(action.toLowerCase()) ? "uuid" : "id"), identifierValue);
        LOG.debug("Message sent. Reply to "+this.id+" messageId="+messageId+" transaction="+transactionId+" subject:"+action + "." + className + ((suffix != null) ? "." + suffix : "")+" identifier: "+identifierValue);
        uno.send(getMessage);
        if (!async) {
            ActionThread actionThread = new ActionThread(action, 5);
            responseThreads.put(messageId, actionThread);
            actionThread.start();
            boolean accepted = true;
            try {
                actionThread.join();
            } catch (InterruptedException e) {
                LOG.debug("Thread interrupted");
            }
            accepted = actionThread.isAccepted();
            LOG.debug("Response accepted: " + accepted);
            if (accepted) {
                String result = getResponse.get(messageId);
                getResponse.remove(messageId);
                responseThreads.remove(messageId);
                return result;
            }
        } else {
            responseThreads.put(messageId, null);
            return messageId;
        }
        return null;
    }

    public void run() {

        LOG.info("Primary message loop initialized");

        Boolean checkForEchoResponse = false;
        String echoMessageId = UUID.randomUUID().toString();
        activated = false;

        while (true) {
            try {
                LOG.debug("Check existing messages before sending hello message");
                Message consumerMessage = consumer.receive(100);

                LOG.debug("Accepted existing message " + consumerMessage);

                if (consumerMessage == null) {
                    if (checkForEchoResponse) {
                        LOG.error("Echo message not found: there are another consumers or message routing issue");
                        return;
                    } else {
                        // no old messages
                        // send echo message to check presence of a single consumer


                        TextMessage echoMessage = session.createTextMessage();
                        echoMessage.setText("echoing " + id);
                        echoMessage.setStringProperty("messageId", echoMessageId);
                        echoMessage.setSubject(id);
                        LOG.debug("Send echo message " + echoMessage.getText() + "|" + echoMessage.getSubject() + "|" + echoMessage.getStringProperty("messageId"));

                        selfProducer.send(echoMessage);


                        LOG.info("Sleep to let our doppelgänger retrieve echo message");

                        Thread.sleep(500);
                        LOG.info("Wake up to check echo message presence");

                        checkForEchoResponse = true;
//                        return;

                    }
                } else {
                    if (!(consumerMessage instanceof TextMessage)) continue;      //skip any non-text messages
                    TextMessage textMessage = (TextMessage) consumerMessage;

                    LOG.info("Found existing message with subject: " + textMessage.getSubject() + "|" + textMessage.getStringProperty("messageId"));
                    String keyParts[] = textMessage.getSubject().split("\\.");
                    // ignore everything except update messages
                    if (keyParts[0].equalsIgnoreCase("update") || keyParts[0].equalsIgnoreCase("remove")) {
                        LOG.debug("Update/remove request for class: " + keyParts[1] + " with uuid: " + keyParts[2]);
                        // do update here
                    } else if (echoMessageId.equals(textMessage.getStringProperty("messageId"))) {
                        LOG.debug("Echo message found: this consumer is only one");

                        break;
                    } else {
                        LOG.debug("Ignore old message " + textMessage.getSubject());
                    }
                }
            } catch (Exception e) {
                LOG.error("Exception in pre-hello message loop");
                e.printStackTrace();
                return;
            }
        }
        activated = true;

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer;
        StringWriter resultString = new StringWriter();

        try {
            transformer = transformerFactory.newTransformer();

            //send hello message with schema announce

            transformer.transform(new DOMSource(this.prepareSchema(accessor.getSchema())), new StreamResult(resultString));
            LOG.debug("Schema XML is " + resultString.toString());

            // send hello message if we've receieved our own message
            sendTextMessage("hello." + this.id, resultString.toString());      //send hello message to dispatcher

            // proceed to actual message loop


            //message loop
            while (true) {
                try {
                    LOG.debug("Waiting for message");
                    TextMessage textMessage = (TextMessage) consumer.receive();
                    LOG.debug("Accepted message " + textMessage);
                    if (textMessage == null) continue;
                    LOG.info("Arrived message " + textMessage.getSubject());
                    String keyParts[] = textMessage.getSubject().split("\\.");
                    String action = keyParts[0].toLowerCase();

                    switch (action) {
                        case "ping":
                            TextMessage pingResponse = session.createTextMessage();
                            pingResponse.setStringProperty("transactionId", textMessage.getStringProperty("transactionId"));
                            pingResponse.setSubject("pong." + id);
                            uno.send(pingResponse);
                            textMessage.acknowledge();
                            break;
                        case "get":
                            textMessage.acknowledge();

                            String className = keyParts[1];
                            String uuid = textMessage.getStringProperty("uuid");
                            LOG.debug("Get request for class: " + className + " with uuid: " + uuid);

                            new GetThread(textMessage, className, uuid).start();
                            LOG.debug("Thread released");
                            break;

                        case "update":
                            className = keyParts[1];
                            uuid = textMessage.getStringProperty("uuid");
                            String data = textMessage.getText();
                            LOG.debug("Update request for class: " + className + " with uuid: " + uuid);
                            textMessage.acknowledge();

                            new UpdateThread(textMessage, className, uuid, data).start();
                            LOG.debug("Thread released");
                            break;

                        case "remove":
                            textMessage.acknowledge();

                            className = keyParts[1];
                            uuid = textMessage.getStringProperty("uuid");
                            LOG.debug("Remove request for class: " + className + " with uuid: " + keyParts[2]);

                            new RemoveThread(textMessage, className, uuid).start();
                            LOG.debug("Thread released");
                            break;

                        case "unify":
                            className = keyParts[1];
                            String identifierName = keyParts[2];
                            String identifierValue = textMessage.getStringProperty("id");

                            new UnifyThread(textMessage, className, identifierName, identifierValue).start();
                            LOG.debug("Thread released");
                            break;

                        case "check":
                            className = keyParts[1];
                            identifierName = keyParts[2];
                            identifierValue = textMessage.getStringProperty("id");

                            new CheckThread(textMessage, className, identifierName, identifierValue).start();
                            LOG.debug("Thread released");
                            break;

                        case "identify":
                            className = keyParts[1];
                            identifierName = keyParts[2];
                            String uuidValue = textMessage.getStringProperty("uuid");

                            new IdentifyThread(textMessage, className, identifierName, uuidValue).start();
                            LOG.debug("Identify thread released");
                            break;

                        case "specify":

                            LOG.debug("Request for specify");

                            String generalClassName = keyParts[1];
                            uuid = textMessage.getStringProperty("uuid");

                            new SpecifyThread(textMessage, generalClassName, uuid).start();

                        default:
                            if (keyParts[0].equalsIgnoreCase(id)) {
                                LOG.debug("WOW. We have response!");
                                LOG.debug("Response content: " + textMessage.getText());
                                String messageId = textMessage.getStringProperty("messageId");
                                if (responseThreads.containsKey(messageId)) {
                                    if (responseThreads.get(messageId) != null) {
                                        getResponse.put(messageId, "" + textMessage.getText());
                                        responseThreads.get(messageId).interrupt();
                                    }
                                    // else {
                                    //                            new ResponseThread((AsyncAccessor) accessor, messageId, ""+textMessage.getText()).start();
                                    //                        }
                                }
                            }
                    }
                } catch (javax.jms.IllegalStateException e) {
                    LOG.debug("Connection lost");
                    break;
                } catch (Exception e) {
                    LOG.debug("Error in connector main message loop");
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            LOG.debug("Error in connector pre-main message loop");
            e.printStackTrace();
        }
        //send bye
    }

    private class ConnectorShutdownHook extends Thread {

        org.apache.qpid.amqp_1_0.jms.Connection connection;
        org.apache.qpid.amqp_1_0.jms.MessageProducer uno;
        org.apache.qpid.amqp_1_0.jms.MessageConsumer consumer;
        org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer;
        org.apache.qpid.amqp_1_0.jms.Session session;
        String id;

        public ConnectorShutdownHook(String id, org.apache.qpid.amqp_1_0.jms.Session session, org.apache.qpid.amqp_1_0.jms.Connection connection, org.apache.qpid.amqp_1_0.jms.MessageProducer uno, org.apache.qpid.amqp_1_0.jms.MessageConsumer consumer, org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer) {
            this.id = id;
            this.connection = connection;
            this.uno = uno;
            this.consumer = consumer;
            this.session = session;
            this.selfProducer = selfProducer;
        }

        public void run() {
            try {
                TextMessage textMessage = session.createTextMessage();
                textMessage.setSubject("bye." + id);
                uno.send(textMessage);
                uno.close();

            } catch (Exception e) {
            }

            try {
                selfProducer.close();
            } catch (JMSException e) {

            }

            try {
                consumer.close();
            } catch (Exception e) {
            }
            try {
                connection.stop();
                connection.close();
            } catch (Exception e) {
            }
            System.out.println("Graceful shutdown");
        }
    }

    private class AsyncThread extends Thread {

        protected TextMessage sourceMessage;

        protected AsyncThread(TextMessage sourceMessage) {
            this.sourceMessage = sourceMessage;
        }
    }

    class GetThread extends AsyncThread {
        String className;
        String uuid;

        public GetThread(TextMessage sourceMessage, String className, String uuid) {
            super(sourceMessage);
            this.className = className;
            this.uuid = uuid;
        }

        public void run() {

            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                Document xml = accessor.get(transactionId, className, uuid);
                TransformerFactory transformerFactory = TransformerFactory.newInstance();
                Transformer transformer;
                transformer = transformerFactory.newTransformer();
                StringWriter sw = new StringWriter();
                transformer.transform(new DOMSource(xml), new StreamResult(sw));
                String response = sw.toString();

                TextMessage responseMessage = null;
                responseMessage = session.createTextMessage();
                responseMessage.setSubject("got." + id);
                responseMessage.setText(response);
                responseMessage.setStringProperty("transactionId", transactionId);
                responseMessage.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                uno.send(responseMessage);
            } catch (TransformerException e) {
                LOG.error("Document transform error");
            } catch (JMSException e) {
                LOG.error("Message error");
            }
        }
    }

    class UpdateThread extends AsyncThread {
        String className;
        String uuid;
        String data;

        public UpdateThread(TextMessage sourceMessage, String className, String uuid, String data) {
            super(sourceMessage);
            this.className = className;
            this.uuid = uuid;
            this.data = data;
        }

        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");

                accessor.update(transactionId, className, uuid, data);
            } catch (JMSException e) {
            }
            ;
        }
    }

    class RemoveThread extends AsyncThread {
        String className;
        String uuid;

        public RemoveThread(TextMessage sourceMessage, String className, String uuid) {
            super(sourceMessage);
            this.className = className;
            this.uuid = uuid;
        }

        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");

                accessor.remove(transactionId, className, uuid);
            } catch (JMSException e) {
            }
            ;
        }
    }

    class SpecifyThread extends AsyncThread {
        String genericClassName;
        String uuid;

        public SpecifyThread(TextMessage sourceMessage, String genericClassName, String uuid) {
            super(sourceMessage);
            this.genericClassName = genericClassName;
            this.uuid = uuid;
        }

        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");

                String specifiedClassName = accessor.specify(transactionId, genericClassName, uuid);
                TextMessage response = null;
                response = session.createTextMessage(specifiedClassName);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("specifyResponse." + id);
                LOG.debug("Response is " + specifiedClassName);
                uno.send(response);
            } catch (JMSException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

        }
    }

    private class IdentifyThread extends AsyncThread {
        String className;
        String uuidValue;
        String identifierName;

        public IdentifyThread(TextMessage sourceMessage, String className, String identifierName, String uuidValue) {
            super(sourceMessage);
            this.className = className;
            this.identifierName = identifierName;
            this.uuidValue = uuidValue;
        }

        @Override
        public void run() {

            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                String result = accessor.getIdentifierByUuid(transactionId, className, identifierName, uuidValue);
                TextMessage response = null;
                response = session.createTextMessage(result);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("identifyResponse." + id);
                LOG.debug("Response is " + result);
                uno.send(response);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    private class UnifyThread extends AsyncThread {
        String className;
        String identifierValue;
        String identifierName;

        public UnifyThread(TextMessage sourceMessage, String className, String identifierName, String identifierValue) {
            super(sourceMessage);
            this.className = className;
            this.identifierName = identifierName;
            this.identifierValue = identifierValue;
        }

        @Override
        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                String result = accessor.getUuidByIdentifier(transactionId, className, identifierName, identifierValue);
                TextMessage response = null;
                response = session.createTextMessage(result);
                response.setStringProperty("transactionId", transactionId);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("unifyResponse." + id);
                LOG.debug("Response is " + result);
                uno.send(response);


            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }


    private class CheckThread extends AsyncThread {
        String className;
        String identifierValue;
        String identifierName;

        public CheckThread(TextMessage sourceMessage, String className, String identifierName, String identifierValue) {

            super(sourceMessage);
            this.className = className;
            this.identifierName = identifierName;
            this.identifierValue = identifierValue;
        }

        @Override
        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                Boolean result = accessor.checkByIdentifier(transactionId, className, identifierName, identifierValue);
                TextMessage response = null;
                response = session.createTextMessage((result ? "true" : "false"));
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("checkResponse." + id);
                response.setStringProperty("transactionId", transactionId);
                LOG.debug("Response to is " + id + " " + result);
                uno.send(response);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
