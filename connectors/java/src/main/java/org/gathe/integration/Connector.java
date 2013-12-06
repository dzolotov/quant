package org.gathe.integration;

import org.apache.log4j.Logger;
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
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
public class Connector extends Thread {

    //Destination queue to echo selfdiagnostics
    final org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer;
    //Destination queue to exchange messages with dispatcher
    final org.apache.qpid.amqp_1_0.jms.MessageProducer uno;
    //Source queue for direct commands and data response
    final org.apache.qpid.amqp_1_0.jms.MessageConsumer consumer;
    //Source queue for modification commands
    final org.apache.qpid.amqp_1_0.jms.MessageConsumer modification;

    final org.apache.qpid.amqp_1_0.jms.Session session;
    private boolean activated = false;
    private String id;
    private Accessor accessor;
    private Logger LOG = Logger.getLogger(this.getClass());
    private ConcurrentHashMap<String, String> getResponse = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Thread> responseThreads = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashMap<String,String>> updatePatches = new ConcurrentHashMap<>();

    private boolean isDisconnected;

    List<DataClass> schema = new ArrayList<>();

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
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);    //???

        org.apache.qpid.amqp_1_0.jms.Queue inbound = (Queue) context.lookup("inbound");
        consumer = session.createConsumer(inbound);

        org.apache.qpid.amqp_1_0.jms.Queue modificationQueue = (Queue) context.lookup("modification");
        modification = session.createConsumer(modificationQueue);

        org.apache.qpid.amqp_1_0.jms.Queue selfQueue = (Queue) context.lookup("endpoints");
        selfProducer = session.createProducer(selfQueue);

        org.apache.qpid.amqp_1_0.jms.Queue outbound = (Queue) context.lookup("uno");
        uno = session.createProducer(outbound);

        Thread modificationThread = new ModificationThread();
        modificationThread.start();        //running parallel modification thread

        uuidCommands.add("get");
        uuidCommands.add("specify");
        uuidCommands.add("identify");
        uuidCommands.add("update");
        uuidCommands.add("remove");

        Runtime.getRuntime().addShutdownHook(new ConnectorShutdownHook());
        this.start();
    }

    //aux method - send text message to dispatcher with specified subject and content
    public void sendTextMessage(String subject, String content) throws JMSException {
        TextMessage textMessage = this.session.createTextMessage(content);
        LOG.info("Sending message with subject " + subject + " and content " + content);
        textMessage.setSubject(subject);
        synchronized (uno) {
            uno.send(textMessage);
        }
    }

    //prepare schema announce from description
    private Document prepareSchema(List<DataClass> schemaDescription) throws ParserConfigurationException {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document schema = dBuilder.newDocument();

        Element rootElement = schema.createElement("schema");
        schema.appendChild(rootElement);

        for (DataClass schemaClass : schemaDescription) {

            String schemaKey = schemaClass.getClassName();
            LOG.info("Parsing schema: " + schemaKey);

            Element classElement = schema.createElement("class");
            classElement.setAttribute("id", schemaKey);
            if (schemaClass.getExtendClassName() != null) {
                classElement.setAttribute("extends", schemaClass.getExtendClassName());
                LOG.info("Extends: " + schemaClass.getExtendClassName());
            }
            if (schemaClass.isReadOnly()) classElement.setAttribute("readonly", "true");
            if (schemaClass.isSpecifiability()) classElement.setAttribute("specifiable", "true");

            Iterator<DataElement> dataElement = schemaClass.getElements();
            while (dataElement.hasNext()) {
                DataElement entry = dataElement.next();
                Element classAttribute = schema.createElement("attribute");
                classAttribute.setAttribute("path", entry.getXPath());
                classAttribute.setAttribute("description", entry.getDescription());
                classElement.appendChild(classAttribute);
            }

            Iterator<String> identifiers = schemaClass.getIdentifiers();
            while (identifiers.hasNext()) {
                String identifier = identifiers.next();
                Element classAttribute = schema.createElement("identifier");
                classAttribute.setAttribute("name", identifier);
                classAttribute.setAttribute("description", identifier);
                classElement.appendChild(classAttribute);
            }

            Iterator<String> checks = schemaClass.getChecks();
            while (checks.hasNext()) {
                String check = checks.next();
                Element classAttribute = schema.createElement("check");
                classAttribute.setAttribute("name", check);
                classAttribute.setAttribute("description", check);
                classElement.appendChild(classAttribute);
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

    public boolean check(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException {
        return (this.doAction("check", transactionId, className, identifierValue, identifier, async).equalsIgnoreCase("true"));
    }

    public String specify(String transactionId, String className, String uuid, boolean async) throws JMSException {
        return this.doAction("specify", transactionId, className, uuid, null, async);
    }

    public void remove(String transactionId, String className, String uuid) throws JMSException {
        this.doActionWithoutResponse("remove", transactionId, className, uuid, null, "");
    }

    public void update(String transactionId, String className, String uuid, String content) throws JMSException {
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
        synchronized (uno) {
            uno.send(actionMessage);
        }
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
        LOG.debug("Message sent. Reply to " + this.id + " messageId=" + messageId + " transaction=" + transactionId + " subject:" + action + "." + className + ((suffix != null) ? "." + suffix : "") + " identifier: " + identifierValue);
        synchronized (uno) {
            uno.send(getMessage);
        }
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
        return "";
    }

    public void run() {

        LOG.info("Primary message loop initialized");

        isDisconnected = false;

        Boolean checkForEchoResponse = false;
        String echoMessageId = UUID.randomUUID().toString();
        activated = false;

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer;
        StringWriter resultString = new StringWriter();

        try {
            transformer = transformerFactory.newTransformer();
            schema = accessor.getSchema();
            LOG.debug(schema);
            transformer.transform(new DOMSource(this.prepareSchema(schema)), new StreamResult(resultString));

            //message loop
            checkForEchoResponse = false;

            while (!isDisconnected) {
                try {
                    LOG.debug("Waiting for message");
                    Object message = null;
                    if (activated) {
                        message = consumer.receive();
                    } else {
                        message = consumer.receive(100);
                    }

                    if (message == null && !activated) {
                        if (checkForEchoResponse) {
                            LOG.error("Echo message not found: there are another consumers or message routing issue");
                            return;
                        }
                        TextMessage echoMessage = session.createTextMessage();
                        echoMessage.setText("echoing " + id);
                        echoMessage.setStringProperty("messageId", echoMessageId);
                        echoMessage.setSubject(id);
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
                        textMessage.acknowledge();
                        LOG.debug("Echo message found: this consumer is only one");
                        //send schema
                        sendTextMessage("hello." + this.id, resultString.toString());      //send hello message to dispatcher

                        activated = true;
                        continue;
                    }

                    LOG.debug("Accepted message " + textMessage);
                    if (textMessage == null || textMessage.getSubject() == null) {
                        textMessage.acknowledge();
                        continue;
                    }
                    LOG.info("Arrived message " + textMessage.getSubject());
                    String keyParts[] = textMessage.getSubject().split("\\.");
                    String action = keyParts[0].toLowerCase();
                    if (!activated) {
                        textMessage.acknowledge();
                        continue;
                    }

                    switch (action) {
                        case "discover":
                            sendTextMessage("hello." + this.id, resultString.toString());
                            textMessage.acknowledge();
                            break;

                        case "ping":
                            TextMessage pingResponse = session.createTextMessage();
                            pingResponse.setStringProperty("transactionId", textMessage.getStringProperty("transactionId"));
                            pingResponse.setSubject("pong." + id);
                            synchronized (uno) {
                                uno.send(pingResponse);
                            }
                            textMessage.acknowledge();
                            break;
                        case "get":

                            String className = keyParts[1];
                            String uuid = textMessage.getStringProperty("uuid");
                            String target = textMessage.getStringProperty("target");
                            LOG.debug("Get request for class: " + className + " with uuid: " + uuid + " (target: " + target + ")");

                            new GetThread(textMessage, className, uuid).start();
                            textMessage.acknowledge();
                            break;

                        case "unify":
                            className = keyParts[1];
                            String identifierName = keyParts[2];
                            String identifierValue = textMessage.getStringProperty("id");

                            new UnifyThread(textMessage, className, identifierName, identifierValue).start();
                            textMessage.acknowledge();
                            break;

                        case "check":
                            className = keyParts[1];
                            identifierName = keyParts[2];
                            identifierValue = textMessage.getStringProperty("id");

                            new CheckThread(textMessage, className, identifierName, identifierValue).start();
                            textMessage.acknowledge();
                            break;

                        case "identify":
                            className = keyParts[1];
                            identifierName = keyParts[2];
                            String uuidValue = textMessage.getStringProperty("uuid");

                            new IdentifyThread(textMessage, className, identifierName, uuidValue).start();
                            textMessage.acknowledge();
                            break;

                        case "specify":

                            LOG.debug("Request for specify");

                            String generalClassName = keyParts[1];
                            uuid = textMessage.getStringProperty("uuid");

                            new SpecifyThread(textMessage, generalClassName, uuid).start();
                            textMessage.acknowledge();
                            break;

                        case "validate":
                            LOG.debug("Request for validate");
                            className = keyParts[1];
                            String data = textMessage.getText();
                            uuid = textMessage.getStringProperty("uuid");
                            LOG.info("Validating "+uuid);
                            new ValidateThread(textMessage, className, uuid, data).start();
                            textMessage.acknowledge();
                            break;

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
                                 }
                            }
                            textMessage.acknowledge();
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

    private class ModificationThread extends Thread {
        public ModificationThread() {
            super();
        }

        private void buildSubSchema(String[] classNames, int minIndex, int maxIndex, UpdateHelper helper, List<DataClass> schema) {
            helper.resetSchema();
            for (int i = minIndex; i <= maxIndex; i++) {
                String clName = classNames[i];
                for (DataClass schemaElement : schema) {
                    if (schemaElement.getClassName().equalsIgnoreCase(clName)) {
                        //add all the elements of class to subschema
                        Iterator<DataElement> ide = schemaElement.getElements();
                        while (ide.hasNext()) {
                            helper.addElementToSchema(ide.next());
                        }
                    }
                }
            }
        }


        public void run() {
            while (!isDisconnected) {
                try {
                    while (!activated) {
                        //waiting for endpoint activation
                        try {
                            Thread.sleep(100);
                        } catch (Exception e) {
                        }
                    }
                    LOG.debug("Waiting for modification message");
                    Object message = null;
                    message = modification.receive();
                    if (!(message instanceof TextMessage)) continue;    //skip empty
                    TextMessage textMessage = (TextMessage) message;
                    LOG.info("Arrived modification message " + textMessage.getSubject());
                    String keyParts[] = textMessage.getSubject().split("\\.");
                    String action = keyParts[0].toLowerCase();
                    switch (action) {
                        case "update":

                            String data = textMessage.getText();
                            String transactionId = textMessage.getStringProperty("transactionId");
                            String uuid = textMessage.getStringProperty("uuid");
                            LOG.info("Updating "+uuid);
                            UpdateHelper updateHelper = new UpdateHelper(uuid, transactionId);
                            updateHelper.transformFromXML(data);
                            //store patch
                            updatePatches.put(uuid, updateHelper.getPatch());

                            List<DataClass> schema = accessor.getSchema();        //todo: optimize!!!
                            String target = textMessage.getStringProperty("target");    //target classes
                            LOG.info("Target: " + target);
                            String targets[] = target.split(",");
                            ArrayList<String> appliedClasses = new ArrayList<>();
                            for (int i = targets.length - 1; i >= 0; i--) {
                                LOG.info("i=" + i);
                                LOG.info("TargetLength: " + targets.length);
                                LOG.info("Checking entry " + targets[i]);
                                //reverse order - to high priority
                                String[] classNames = targets[i].split("-");        //parse from top to bottom (specialize)

                                LOG.info("ClassNames: " + classNames);

                                //search in reverse order to last monolithic class
                                int k = -1;
                                for (int j = classNames.length - 1; j >= 0; j--) {
                                    String clName = classNames[j];
                                    LOG.info("ClName: " + clName);
                                    for (DataClass schemaElement : schema) {
                                        LOG.info("Checking class path: " + schemaElement.getClassName());
                                        if (schemaElement.getClassName().equalsIgnoreCase(clName) && schemaElement.isMonolithic() == true) {
                                            k = j;        //store monolithic entry
                                            break;
                                        }
                                    }
                                    if (k >= 0) break;
                                }

                                if (k >= 0) {
                                    LOG.info("Found monolithic entry " + classNames[k]);
                                    if (!appliedClasses.contains(classNames[k])) {
                                        this.buildSubSchema(classNames, 0, k, updateHelper, schema);    //schema contains all inherited properties
                                        for (int l = 0; l <= k; l++) appliedClasses.add(classNames[l]);
                                        if (!accessor.update(classNames[k], updateHelper)) {
                                            //found an error when update
                                            LOG.error("Found an error when updating " + updateHelper.getUuid() + " class: " + classNames[k]);
                                            continue;
                                        }
                                    }
                                }

                                if (k < classNames.length - 1) {
                                    //need to overlay some inherited classes
                                    for (int j = k + 1; j < classNames.length; j++) {
                                        LOG.info("Pass through overlay " + classNames[j]);
                                        if (!appliedClasses.contains(classNames[j])) {
                                            this.buildSubSchema(classNames, j, j, updateHelper, schema);
                                            if (!accessor.update(classNames[j], updateHelper)) {
                                                LOG.error("Found an error when updating " + updateHelper.getUuid() + " class: " + classNames[j]);
                                                continue;
                                                //overlay by single instances
                                            }
                                        }
                                    }
                                }
                            }            //and go to most prioritied values

                            updatePatches.remove(uuid);
                            textMessage.acknowledge();
                            break;

                        case "remove":
                            textMessage.acknowledge();
                            transactionId = textMessage.getStringProperty("transactionId");
                            String className = keyParts[1];        //add specify
                            uuid = textMessage.getStringProperty("uuid");
                            if (accessor.backup(transactionId, className, uuid)) {
                                if (accessor.remove(transactionId, className, uuid)) {
                                    textMessage.acknowledge();
                                } else {
                                    LOG.info("Problem when removing " + className + "." + uuid);
                                }
                            } else {
                                LOG.info("Problem when backing up "+className+"."+uuid);
                            }
                    }
                } catch (Exception e) {
                    e.printStackTrace();        //todo: granularity
                }
            }
        }
    }

    private class ConnectorShutdownHook extends Thread {


        public ConnectorShutdownHook() {
        }

        public void run() {
            LOG.info("Terminating endpoint");
            try {
                TextMessage textMessage = session.createTextMessage();
                textMessage.setSubject("bye." + id);
                synchronized (uno) {
                    uno.send(textMessage);
                }
            } catch (Exception e) {
            }
            LOG.info("Disconnecting");
            isDisconnected = true;

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

        private void buildSubSchema(String[] classNames, int minIndex, int maxIndex, GetHelper helper, List<DataClass> schema) {
            helper.resetSchema();
            for (int i = minIndex; i <= maxIndex; i++) {
                String clName = classNames[i];
                for (DataClass schemaElement : schema) {
                    if (schemaElement.getClassName().equalsIgnoreCase(clName)) {
                        //add all the elements of class to subschema
                        Iterator<DataElement> ide = schemaElement.getElements();
                        while (ide.hasNext()) {
                            helper.addElementToSchema(ide.next());
                        }
                    }
                }
            }
        }

        public void run() {

            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                GetHelper getHelper = new GetHelper(uuid, transactionId, className);
                List<DataClass> schema = accessor.getSchema();        //todo: optimize!!!
                String target = sourceMessage.getStringProperty("target");    //target classes
                LOG.info("Target: " + target);
                String targets[] = target.split(",");
                ArrayList<String> appliedClasses = new ArrayList<>();
                for (int i = targets.length - 1; i >= 0; i--) {
                    //reverse order - to high priority
                    String[] classNames = targets[i].split("-");        //parse from top to bottom (specialize)

                    //search in reverse order to last monolithic class
                    int k = -1;
                    for (int j = classNames.length - 1; j >= 0; j--) {
                        String clName = classNames[j];
                        LOG.debug("ClName: " + clName);
                        for (DataClass schemaElement : schema) {
                            LOG.debug("Checking class path: " + schemaElement.getClassName());
                            if (schemaElement.getClassName().equalsIgnoreCase(clName) && schemaElement.isMonolithic() == true) {
                                k = j;        //store monolithic entry
                                break;
                            }
                        }
                        if (k >= 0) break;
                    }

                    if (k >= 0) {
                        LOG.debug("Found monolithic entry " + classNames[k]);
                        if (!appliedClasses.contains(classNames[k])) {
                            this.buildSubSchema(classNames, 0, k, getHelper, schema);    //schema contains all inherited properties
                            for (int l = 0; l <= k; l++) appliedClasses.add(classNames[l]);
                            accessor.get(classNames[k], getHelper);     //fill monolithic part
                        }
                    }

                    if (k < classNames.length - 1) {
                        //need to overlay some inherited classes
                        for (int j = k + 1; j < classNames.length; j++) {
                            LOG.debug("Pass through overlay " + classNames[j]);
                            if (!appliedClasses.contains(classNames[j])) {
                                this.buildSubSchema(classNames, j, j, getHelper, schema);
                                accessor.get(classNames[j], getHelper);        //overlay by single instances
                            }
                        }
                    }
                }            //and go to most priority values

                //apply any patches
                if (updatePatches.containsKey(uuid)) {
                    getHelper.applyPatch(updatePatches.get(uuid));
                }

                String response = getHelper.transformToXml();

                TextMessage responseMessage;
                responseMessage = session.createTextMessage();
                responseMessage.setSubject("got." + id);
                responseMessage.setText(response);
                responseMessage.setStringProperty("transactionId", transactionId);
                responseMessage.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                synchronized (uno) {
                    uno.send(responseMessage);
                }
            } catch (JMSException e) {
                LOG.error("Get message error "+e.getMessage());
            }
        }
    }

    class ValidateThread extends AsyncThread {
        String uuid;
        String className;
        String data;

        public ValidateThread(TextMessage sourceMessage, String className, String uuid, String data) {
            super(sourceMessage);
            this.uuid = uuid;
            this.className = className;
            this.data = data;
        }

        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                UpdateHelper uh = new UpdateHelper(uuid, transactionId);
                String[] validationResults = accessor.validate(className, uh);
                String result = "";
                for (String row : validationResults) {
                    result+=row+"\n";
                }
                result = result.trim();
                TextMessage response;
                response = session.createTextMessage(result);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("validateresponse."+id);
                LOG.debug("Response is "+result);
                synchronized (uno) {
                    uno.send(response);
                }
            } catch (JMSException e) {
                LOG.error("Error when validating entry");
            }
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
                if (specifiedClassName == null) specifiedClassName = "";
                TextMessage response;
                response = session.createTextMessage(specifiedClassName);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("specifyResponse." + id);
                LOG.debug("Response is " + specifiedClassName);
                synchronized (uno) {
                    uno.send(response);
                }
            } catch (JMSException e) {
                LOG.error("Error when specifying: "+e.getMessage());
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
                LOG.debug("Identifying " + className + "." + identifierName + " for " + uuidValue);
                String result = accessor.getIdentifierByUuid(transactionId, className, identifierName, uuidValue);
                LOG.debug("Result is " + result);
                if (result == null) result = "";
                TextMessage response;
                response = session.createTextMessage(result);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("identifyResponse." + id);
                LOG.debug("Response is " + result);
                synchronized (uno) {
                    uno.send(response);
                }
            } catch (JMSException e) {
                LOG.error("Error when identifying "+e.getMessage());
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
                LOG.debug("Unifying " + className + "." + identifierName + " for " + identifierValue);
                String result = accessor.getUuidByIdentifier(transactionId, className, identifierName, identifierValue);
                LOG.debug("Result is " + result);
                if (result == null) result = "";
                TextMessage response;
                response = session.createTextMessage(result);
                response.setStringProperty("transactionId", transactionId);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("unifyResponse." + id);
                LOG.debug("Response is " + result);
                synchronized (uno) {
                    uno.send(response);
                }
            } catch (JMSException e) {
                LOG.error("Error when unifying: "+e.getMessage());
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
                if (result == null) result = new Boolean(false);
                TextMessage response;
                response = session.createTextMessage((result ? "true" : "false"));
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("checkResponse." + id);
                response.setStringProperty("transactionId", transactionId);
                LOG.debug("Response to is " + id + " " + result);
                synchronized (uno) {
                    uno.send(response);
                }
            } catch (JMSException e) {
                LOG.error("Error when checking: "+e.getMessage());
            }
        }
    }
}
