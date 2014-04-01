package org.gathe.integration;

import org.apache.log4j.Logger;
import org.apache.qpid.amqp_1_0.jms.Queue;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
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
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>
 */
public class BasicConnector extends Thread implements Connector {

    //Destination queue to echo selfdiagnostics
    org.apache.qpid.amqp_1_0.jms.MessageProducer selfProducer;
    //Destination queue to exchange messages with dispatcher
    org.apache.qpid.amqp_1_0.jms.MessageProducer uno;
    //Source queue for direct commands and data response
    org.apache.qpid.amqp_1_0.jms.MessageConsumer consumer;
    //Source queue for modification commands
    org.apache.qpid.amqp_1_0.jms.MessageConsumer modification;

    protected ArrayList<String> selfTransactions = new ArrayList<>();

    private String semaphore = "";

    org.apache.qpid.amqp_1_0.jms.Session session;
    private boolean activated = false;
    private boolean readOnly = false;

    private String id;
    //    private Accessor accessor;
    private Logger LOG = Logger.getLogger(this.getClass());
    private ConcurrentHashMap<String, String> getResponse = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Thread> responseThreads = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashMap<String, String>> updatePatches = new ConcurrentHashMap<>();

    private boolean isDisconnected;

    HashMap<DataClass, Accessor> schema = new HashMap<>();
//    List<DataClass> schema = new ArrayList<>();

    ArrayList<String> uuidCommands = new ArrayList<String>();

    protected String joinStrings(String glue, String[] array) {
        String line = "";
        for (String s : array) line += s + glue;
        return (array.length == 0) ? line : line.substring(0, line.length() - glue.length());
    }

    @Override
    public void connectESB(boolean readOnly) throws JMSException {
        System.setProperty("max_prefetch", "1");
        try {
            Class.forName("org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
            Hashtable<String, String> properties = new Hashtable<String, String>();
            String path = new File(id + ".properties").getAbsolutePath();
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
//        ((org.apache.qpid.amqp_1_0.jms.impl.MessageConsumerImpl) consumer).setMaxPrefetch(1);

            if (!readOnly) {
                org.apache.qpid.amqp_1_0.jms.Queue modificationQueue = (Queue) context.lookup("modification");
                modification = session.createConsumer(modificationQueue);
            }
//        ((org.apache.qpid.amqp_1_0.jms.impl.MessageConsumerImpl) modification).setMaxPrefetch(1);

            org.apache.qpid.amqp_1_0.jms.Queue selfQueue = (Queue) context.lookup("endpoints");
            selfProducer = session.createProducer(selfQueue);

            org.apache.qpid.amqp_1_0.jms.Queue outbound = (Queue) context.lookup("uno");
            uno = session.createProducer(outbound);
            uno.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (ClassNotFoundException | NamingException e) {
            e.printStackTrace();
        }
    }

    public BasicConnector(String id, boolean readOnly) throws ClassNotFoundException, NamingException, JMSException {
        this.id = id;

        this.readOnly = readOnly;
        connectESB(readOnly);

        if (!readOnly) {
            Thread modificationThread = new ModificationThread();
            modificationThread.start();        //running parallel modification thread
        }

        uuidCommands.add("get");
        uuidCommands.add("specify");
        uuidCommands.add("identify");
        uuidCommands.add("update");
        uuidCommands.add("remove");

        Runtime.getRuntime().addShutdownHook(new ConnectorShutdownHook());
//        this.start();
    }

    public void appendAccessor(Accessor accessor) {
        List<DataClass> accessorSchema = accessor.getSchema();

        for (DataClass schemaEntry : accessorSchema) {
            LOG.debug("Appending schema " + schemaEntry.getClassName() + " with " + accessor);
            schema.put(schemaEntry, accessor);
        }

        accessor.setConnector(this);
    }

    public void connect() {
        this.start();
    }

    private void sendChunk(TextMessage textMessage, String chunk, int number, int count) {

        LOG.debug("Sending chunk for " + textMessage + " data length: " + chunk.length() + " " + number + "/" + count);
        synchronized (uno) {
            try {
                textMessage.setIntProperty("number", number);
                textMessage.setIntProperty("count", count);
                textMessage.setText(chunk);
                textMessage.setDurable(true);
                uno.send(textMessage);
                LOG.debug("Sent");
                textMessage = null;
            } catch (JMSException e) {
                try {
                    connectESB(this.readOnly);
                    textMessage.setIntProperty("number", number);
                    textMessage.setIntProperty("count", count);
                    textMessage.setText(chunk);
                    LOG.debug("Twin send");
                } catch (JMSException e2) {
                    LOG.error("Twin error " + e.getLocalizedMessage());
                }
            }
        }
    }

    public void sendToUno(TextMessage textMessage, String content) throws JMSException {

        LOG.debug("Content: [" + content + "]");
        if (content.isEmpty()) {
            sendChunk(textMessage, "", 0, 1);
        }

        int chunkSize = 16384;          //todo: define as parameter

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
            LOG.info("Sending chunk " + number + "/" + count + " Subject: " + subject + " Length: " + (maxLimit - (number * chunkSize)));
            sendChunk(tm, content.substring(number * chunkSize, maxLimit), number, count);
            tm = null;
        }
        LOG.info("Message sent");
    }

    //aux method - send text message to dispatcher with specified subject and content
    public void sendTextMessage(String subject, String content) throws JMSException {
        TextMessage textMessage = this.session.createTextMessage();
        LOG.info("Sending message with subject " + subject + " and content " + content);
        textMessage.setSubject(subject);
        sendToUno(textMessage, content);
        textMessage = null;
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
            if (schemaClass.isMatchable()) classElement.setAttribute("matchable", "true");
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

    @Override
    public String get(String transactionId, String className, String uuid, boolean async, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        String result;
        if (!isLocalRequest) {
            result = this.doAction("get", transactionId, className, uuid, null, async);
        } else {
            Accessor accessor = this.getAccessor(className);
            GetHelper getHelper = new GetHelper(uuid, transactionId, className);

            for (DataClass cls : this.schema.keySet()) {
                if (cls.getClassName().equalsIgnoreCase(className)) {
                    Iterator<DataElement> delIterator = cls.getElements();
                    while (delIterator.hasNext()) {
                        DataElement del = delIterator.next();
                        getHelper.addElementToSchema(del);
                    }
                }
            }

            if (accessor.get(className, getHelper)) {
                result = getHelper.transformToXml();
            } else {
                result = "";
            }
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
    }


    @Override
    public String unify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException {
        return this.unify(transactionId, className, identifier, identifierValue, async, isLocalRequest, false);
    }

    public String unify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest, boolean forcedCreation) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        String result;
        if (!isLocalRequest) {
            result = this.doAction("unify", transactionId, className, identifierValue, identifier, async);
        } else {
            Accessor accessor = this.getAccessor(className);
            result = accessor.getUuidByIdentifier(null, className, identifier, identifierValue, forcedCreation);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
    }

    @Override
    public String identify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        String result;
        if (!isLocalRequest) {
            result = this.doAction("identify", transactionId, className, identifierValue, identifier, async);
        } else {
            Accessor accessor = this.getAccessor(className);
            result = accessor.getIdentifierByUuid(null, className, identifier, identifierValue);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
//        return this.doAction("identify", transactionId, className, identifierValue, identifier, async);
    }

    @Override
    public String identify(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException {
        return this.identify(transactionId, className, identifier, identifierValue, async, false);
    }

    @Override
    public boolean check(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        boolean result;
        if (!isLocalRequest) {
            result = this.doAction("check", transactionId, className, identifierValue, identifier, async).equalsIgnoreCase("true");
        } else {
            Accessor accessor = this.getAccessor(className);
            result = accessor.checkByIdentifier(transactionId, className, identifier, identifierValue);
        }

        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
    }

    @Override
    public String specify(String transactionId, String className, String uuid, boolean async, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        String result;
        if (!isLocalRequest) {
            result = this.doAction("specify", transactionId, className, uuid, null, async);
        } else {
            Accessor accessor = this.getAccessor(className);
            result = accessor.specify(transactionId, className, uuid);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
    }

    @Override
    public void remove(String transactionId, String className, String uuid, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        if (!isLocalRequest) {
            this.doActionWithoutResponse("remove", transactionId, className, uuid, null, "");
        } else {
            Accessor accessor = this.getAccessor(className);
            accessor.remove(transactionId, className, uuid);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
    }

    @Override
    public void update(String transactionId, String className, String uuid, String content, boolean isLocalRequest) throws JMSException {
        LOG.debug("Updating for " + className + ": " + uuid + " content: " + content);
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        if (!isLocalRequest) {
            this.doActionWithoutResponse("update", transactionId, className, uuid, null, content);
        } else {
            Accessor accessor = this.getAccessor(className);
            UpdateHelper uh = new UpdateHelper(uuid, transactionId);
            for (DataClass cls : this.schema.keySet()) {
                if (cls.getClassName().equalsIgnoreCase(className)) {
                    Iterator<DataElement> delIterator = cls.getElements();
                    while (delIterator.hasNext()) {
                        DataElement del = delIterator.next();
                        uh.addElementToSchema(del);
                    }
                }
            }
            uh.transformFromXML(content);
            accessor.update(className, uh);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
    }

    @Override
    public String matchAll(String transactionId, String className, HashMap<String, String> filters, boolean async, boolean isLocalRequest) throws JMSException {
        boolean stored = !selfTransactions.contains(transactionId + ":" + className) && transactionId != null;
        if (stored) selfTransactions.add(transactionId + ":" + className);
        String result;
        if (!isLocalRequest) {
            result = this.doMatchAction("matchAll", transactionId, className, filters, async);
        } else {
            Accessor accessor = this.getAccessor(className);
            String[] results = accessor.match(transactionId, className, filters, false);
            result = this.joinStrings(",", results);
        }
        if (stored) selfTransactions.remove(transactionId + ":" + className);
        return result;
    }

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
        sendToUno(actionMessage, content);
        actionMessage = null;
    }

    @Override
    public String get(String className, String uuid) throws JMSException {
        return this.get(null, className, uuid, false, false);
    }

    @Override
    public String get(String className, String uuid, boolean isLocalRequest) throws JMSException {
        return this.get(null, className, uuid, false, isLocalRequest);
    }

    @Override
    public String unify(String className, String identifier, String identifierValue, boolean isLocalRequest) throws JMSException {
        return this.unify(null, className, identifier, identifierValue, false, isLocalRequest, false);
    }

    @Override
    public String unify(String className, String identifier, String identifierValue, boolean isLocalRequest, boolean forcedCreation) throws JMSException {
        return this.unify(null, className, identifier, identifierValue, false, isLocalRequest, forcedCreation);
    }

    @Override
    public String unify(String className, String identifier, String identifierValue) throws JMSException {
        return this.unify(null, className, identifier, identifierValue, false, false, false);
    }

    @Override
    public String identify(String className, String identifier, String uuid) throws JMSException {
        return this.identify(null, className, identifier, uuid, false);
    }

    @Override
    public boolean check(String className, String identifier, String identifierValue, boolean isLocalRequest) throws JMSException {
        return this.check(null, className, identifier, identifierValue, false, isLocalRequest);
    }

    @Override
    public boolean check(String className, String identifier, String identifierValue) throws JMSException {
        return this.check(null, className, identifier, identifierValue, false, false);
    }

    @Override
    public String specify(String className, String uuid, boolean isLocalRequest) throws JMSException {
        return this.specify(null, className, uuid, false, isLocalRequest);
    }

    @Override
    public String specify(String className, String uuid) throws JMSException {
        return this.specify(null, className, uuid, false, false);
    }

    @Override
    public void remove(String className, String uuid, boolean isLocalRequest) throws JMSException {
        this.remove(null, className, uuid, isLocalRequest);
    }

    @Override
    public void remove(String className, String uuid) throws JMSException {
        this.remove(null, className, uuid, false);
    }

    @Override
    public void update(String className, String uuid, String content, boolean isLocalRequest) throws JMSException {
        this.update(null, className, uuid, content, isLocalRequest);
    }

    @Override
    public void update(String className, String uuid, String content) throws JMSException {
        this.update(null, className, uuid, content, false);
    }

    @Override
    public String matchAll(String className, HashMap<String, String> filters, boolean isLocalRequest) throws JMSException {
        return this.matchAll(null, className, filters, false, isLocalRequest);
    }

    @Override
    public String matchAll(String className, HashMap<String, String> filters) throws JMSException {
        return this.matchAll(null, className, filters, false, false);
    }

    @Override
    public boolean isSelfRequest(String transactionId, String className) {
        LOG.debug("Self Request pool: " + this.selfTransactions);
        return this.selfTransactions.contains(transactionId + ":" + className);
    }

    private String doMatchAction(String action, String transactionId, String className, HashMap<String, String> filters, boolean async) throws JMSException {
        while (!activated) {
            try {
                LOG.debug("Waiting for activation");
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        if (transactionId == null) transactionId = UUID.randomUUID().toString();
        String messageId = UUID.randomUUID().toString();
        TextMessage matchMessage = session.createTextMessage();
        matchMessage.setReplyTo(this.id);
        matchMessage.setStringProperty("messageId", messageId);
        matchMessage.setStringProperty("transactionId", transactionId);

        for (String filterKey : filters.keySet()) {
            matchMessage.setStringProperty("filter-" + filterKey, filters.get(filterKey));
        }

        matchMessage.setSubject(action + "." + className);
        matchMessage.setStringProperty("uuid", "");
        LOG.debug("Message sent. Reply to " + this.id + " messageId=" + messageId + " transaction=" + transactionId + " subject:" + action + "." + className);

//        if (true) {
        ActionThread actionThread = new ActionThread(action, this, matchMessage, 5);
        LOG.debug("Stored to " + messageId + " ActionThread: " + actionThread);
        responseThreads.put(messageId, actionThread);
        boolean accepted;

        try {
            actionThread.start();
            actionThread.join();
        } catch (InterruptedException e) {
        }
        accepted = actionThread.isAccepted();
        LOG.debug("Response accepted: " + accepted);
        if (accepted) {
            String result = getResponse.get(messageId);
            getResponse.remove(messageId);
            responseThreads.remove(messageId);
            return result;
        }
//        } else {
//            responseThreads.put(messageId, null);
//            return messageId;
//        }
        return "";
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

        ActionThread actionThread = new ActionThread(action, this, getMessage, 5);
        LOG.debug("Stored to " + messageId + " ActionThread: " + actionThread);
        responseThreads.put(messageId, actionThread);
        boolean accepted;

        try {
            actionThread.start();
            actionThread.join();
        } catch (InterruptedException e) {
        }

        accepted = actionThread.isAccepted();
        LOG.debug("Response accepted: " + accepted);
        if (accepted) {
            String result = getResponse.get(messageId);
            getResponse.remove(messageId);
            responseThreads.remove(messageId);
            return result;
        }
//        } else {
//            responseThreads.put(messageId, null);
//            return messageId;
//        }
        return "";
    }

    public void run() {

        LOG.info("Primary message loop initialized");

        isDisconnected = false;
        HashMap<String, String> chunks = new HashMap<>();

        Boolean checkForEchoResponse = false;
        String echoMessageId = UUID.randomUUID().toString();
        activated = false;

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer;
        StringWriter resultString = new StringWriter();

        try {
            transformer = transformerFactory.newTransformer();

            ArrayList<DataClass> totalSchema = new ArrayList<>();
            totalSchema.addAll(schema.keySet());

//            schema = accessor.getSchema();
            LOG.debug(totalSchema);
            transformer.transform(new DOMSource(this.prepareSchema(totalSchema)), new StreamResult(resultString));

            //message loop
            checkForEchoResponse = false;

            while (!isDisconnected) {
                try {
//                    LOG.debug("Waiting for message");
                    Object message = null;
//                    try {
                    if (activated) {
                        message = consumer.receive(50);
                    } else {
                        message = consumer.receive(100);
                    }
//                    } catch (JMSException e) {
//                        try {
//                            connect();
//                            if (activated) {
//                                message = consumer.receive();
//                            } else {
//                                message = consumer.receive(100);
//                            }
//                        } catch (JMSException e2) {
//                            LOG.error("Twin error "+e.getLocalizedMessage());
//                        }
//                    }

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
                            sendToUno(pingResponse, "");
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
                            LOG.info("Validating " + uuid);
                            new ValidateThread(textMessage, className, uuid, data).start();
                            textMessage.acknowledge();
                            break;

                        case "match":
                            LOG.debug("Match query");
                            className = keyParts[1];
                            Enumeration<String> filters = textMessage.getPropertyNames();

                            String mode = textMessage.getStringProperty("mode");
                            String explain = ""+textMessage.getStringProperty("explain");
                            boolean needToExplain = (""+explain).equalsIgnoreCase("true");

                            HashMap<String, String> filterData = new HashMap<>();
                            while (filters.hasMoreElements()) {
                                String filterName = filters.nextElement();
                                if (filterName.startsWith("filter-")) {
                                    String filterAttribute = filterName.substring(7);       //skip prefix
                                    String filterCondition = textMessage.getStringProperty(filterName);
                                    filterData.put(filterAttribute, filterCondition);
                                }
                            }
                            new MatchThread(textMessage, className, filterData, mode, needToExplain).start();
                            textMessage.acknowledge();
                            break;

                        default:
                            if (keyParts[0].equalsIgnoreCase(id)) {

                                LOG.info("Accepted direct message");
                                String messageId = textMessage.getStringProperty("messageId");
                                LOG.info("MessageID: " + messageId);
                                if (textMessage.getStringProperty("waiting") != null) {
                                    LOG.info("Waiting notify");
//                                    synchronized (semaphore) {
//                                        this.semaphore = "5";
                                    ((ActionThread) (responseThreads.get(messageId))).needContinue(true);
//                                        responseThreads.get(messageId).interrupt();
//                                    }
                                    textMessage.acknowledge();
                                    continue;
                                }

                                LOG.debug("WOW. We have response!");
                                LOG.debug("Response content: " + textMessage.getText());
                                LOG.debug("MessageID: " + textMessage.getStringProperty("messageId"));

                                int number = textMessage.getIntProperty("number");
                                int count = textMessage.getIntProperty("count");
                                if (!chunks.containsKey(messageId)) {
                                    chunks.put(messageId, "");
                                }

                                LOG.debug("Accepted " + number + "/" + count);
                                chunks.put(messageId, chunks.get(messageId) + textMessage.getText());
                                LOG.info("Chunk length for " + messageId + " is " + chunks.get(messageId).length());
                                if (number < count - 1) {
//                                    synchronized (semaphore) {
//                                        this.semaphore = "3";
                                    ((ActionThread) (responseThreads.get(messageId))).needContinue(true);
//                                        responseThreads.get(messageId).interrupt();
//                                    }
                                    textMessage.acknowledge();
                                    continue;
                                }
                                LOG.info("Mission completed");
                                String content = chunks.get(messageId);
                                chunks.remove(messageId);

                                if (responseThreads.containsKey(messageId)) {
                                    if (responseThreads.get(messageId) != null) {
//                                        synchronized (semaphore) {
//                                            this.semaphore = "4";
                                        getResponse.put(messageId, content);
                                        ((ActionThread) (responseThreads.get(messageId))).needContinue(false);
                                        LOG.debug("Terminating thread " + responseThreads.get(messageId));
                                        responseThreads.get(messageId).interrupt();
//                                        }
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

    protected Accessor getAccessor(String className) {
//        LOG.info("Getting accessor from " + schema.keySet().size());
        for (DataClass dc : schema.keySet()) {
//            LOG.info("Iterating: " + dc + " classname: " + dc.getClassName() + " with " + className);
            if (dc.getClassName().equalsIgnoreCase(className)) return schema.get(dc);
        }
        return null;
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
//                    LOG.debug("Waiting for modification message");
                    Object message = null;
//                    try {
                    message = modification.receive();
/*
                    } catch (JMSException e) {
                        try {
                            connect();
                            message = modification.receive();
                        } catch (JMSException e2) {
                            LOG.error("Twin error "+e2.getLocalizedMessage());
                        }
                    }
*/
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
                            LOG.info("Updating " + uuid);
                            UpdateHelper updateHelper = new UpdateHelper(uuid, transactionId);
                            updateHelper.transformFromXML(data);
                            //store patch
                            updatePatches.put(uuid, updateHelper.getPatch());


                            List<DataClass> allSchema = new ArrayList<>(schema.keySet());
                            //List<DataClass> schema = accessor.getSchema();        //todo: optimize!!!
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
                                    for (DataClass schemaElement : allSchema) {
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
                                        this.buildSubSchema(classNames, 0, k, updateHelper, allSchema);    //schema contains all inherited properties
                                        for (int l = 0; l <= k; l++) appliedClasses.add(classNames[l]);
                                        Accessor accessor = getAccessor(classNames[k]);
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
                                            this.buildSubSchema(classNames, j, j, updateHelper, allSchema);
                                            Accessor accessor = getAccessor(classNames[j]);
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
                            Accessor accessor = getAccessor(className);
                            if (accessor.backup(transactionId, className, uuid)) {
                                if (accessor.remove(transactionId, className, uuid)) {
                                    textMessage.acknowledge();
                                } else {
                                    LOG.info("Problem when removing " + className + "." + uuid);
                                }
                            } else {
                                LOG.info("Problem when backing up " + className + "." + uuid);
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
                sendToUno(textMessage, "");
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
                Accessor accessor = getAccessor(className);
                List<DataClass> allSchema = new ArrayList<>(schema.keySet());        //todo: optimize!!!
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
                        for (DataClass schemaElement : allSchema) {
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
                            this.buildSubSchema(classNames, 0, k, getHelper, allSchema);    //schema contains all inherited properties
                            for (int l = 0; l <= k; l++) appliedClasses.add(classNames[l]);
                            accessor.get(classNames[k], getHelper);     //fill monolithic part
                        }
                    }

                    if (k < classNames.length - 1) {
                        //need to overlay some inherited classes
                        for (int j = k + 1; j < classNames.length; j++) {
                            LOG.debug("Pass through overlay " + classNames[j]);
                            if (!appliedClasses.contains(classNames[j])) {
                                this.buildSubSchema(classNames, j, j, getHelper, allSchema);
                                Accessor accessor2 = getAccessor(classNames[j]);
                                accessor2.get(classNames[j], getHelper);        //overlay by single instances
                            }
                        }
                    }
                }            //and go to most priority values

                //apply any patches
                if (updatePatches.containsKey(uuid)) {
                    getHelper.applyPatch(updatePatches.get(uuid));
                }

                String response = getHelper.transformToXml();
                getHelper = null;

                TextMessage responseMessage;
                responseMessage = session.createTextMessage();
                responseMessage.setSubject("got." + id);
                responseMessage.setStringProperty("transactionId", transactionId);
                responseMessage.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                sendToUno(responseMessage, response);
                System.gc();

            } catch (JMSException e) {
                LOG.error("Get message error " + e.getMessage());
            }
        }
    }

    class MatchThread extends AsyncThread {
        String className;
        HashMap<String, String> filters;
        String mode;
        boolean explain;

        public MatchThread(TextMessage sourceMessage, String className, HashMap<String, String> filters, String mode, boolean explain) {
            super(sourceMessage);
            this.className = className;
            this.filters = filters;
            this.mode = mode;
            this.explain = explain;
        }

        public void run() {
            try {
                String transactionId = sourceMessage.getStringProperty("transactionId");
                Accessor accessor = getAccessor(className);
                TextMessage response;
                switch (mode) {
                    case "scan":
                        String scanResult = accessor.countMatches(transactionId, className, filters);
                        response = session.createTextMessage();
                        response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                        response.setStringProperty("transactionId", transactionId);
                        response.setSubject("matchResponse." + id);
                        LOG.debug("Sending countmatch response. Subject: " + "matchResponse." + id + " TR: " + transactionId);
                        sendToUno(response, scanResult);
                        break;
                    case "seek":
                        String[] result = accessor.match(transactionId, className, filters, explain);
                        String stringResult = joinStrings(",", result);
                        response = session.createTextMessage();
                        response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                        response.setStringProperty("transactionId", transactionId);
                        response.setSubject("matchResponse." + id);
                        LOG.debug("Sending response. Subject: " + "matchResponse." + id + " TR: " + transactionId);
//                LOG.debug("Response is "+stringResult);
                        sendToUno(response, stringResult);
                        break;
                }
            } catch (JMSException e) {
                e.printStackTrace();
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
                Accessor accessor = getAccessor(className);
                String[] validationResults = accessor.validate(className, uh);
                String result = "";
                for (String row : validationResults) {
                    result += row + "\n";
                }
                result = result.trim();
                TextMessage response;
                response = session.createTextMessage();
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("validateresponse." + id);
                LOG.debug("Response is " + result);
                sendToUno(response, result);
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
                Accessor accessor = getAccessor(genericClassName);
                String specifiedClassName = accessor.specify(transactionId, genericClassName, uuid);
                if (specifiedClassName == null) specifiedClassName = "";
                TextMessage response;
                response = session.createTextMessage();
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("specifyResponse." + id);
                LOG.debug("Response is " + specifiedClassName);
                sendToUno(response, specifiedClassName);
            } catch (JMSException e) {
                LOG.error("Error when specifying: " + e.getMessage());
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
                Accessor accessor = getAccessor(className);
                String result = accessor.getIdentifierByUuid(transactionId, className, identifierName, uuidValue);
                LOG.debug("Result is " + result);
                if (result == null) result = "";
                TextMessage response;
                response = session.createTextMessage();
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setStringProperty("transactionId", transactionId);
                response.setSubject("identifyResponse." + id);
                LOG.debug("Response is " + result);
                sendToUno(response, result);
            } catch (JMSException e) {
                LOG.error("Error when identifying " + e.getMessage());
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
                Accessor accessor = getAccessor(className);
                String result = accessor.getUuidByIdentifier(transactionId, className, identifierName, identifierValue, true);
                LOG.debug("Result is " + result);
                if (result == null) result = "";
                TextMessage response;
                response = session.createTextMessage();
                response.setStringProperty("transactionId", transactionId);
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("unifyResponse." + id);
                LOG.debug("Response is " + result);
                sendToUno(response, result);
            } catch (JMSException e) {
                LOG.error("Error when unifying: " + e.getMessage());
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
                Accessor accessor = getAccessor(this.className);
                Boolean result = accessor.checkByIdentifier(transactionId, className, identifierName, identifierValue);
                if (result == null) result = new Boolean(false);
                TextMessage response;
                response = session.createTextMessage();
                response.setStringProperty("messageId", sourceMessage.getStringProperty("messageId"));
                response.setSubject("checkResponse." + id);
                response.setStringProperty("transactionId", transactionId);
                LOG.debug("Response to is " + id + " " + result);
                sendToUno(response, (result ? "true" : "false"));
            } catch (JMSException e) {
                LOG.error("Error when checking: " + e.getMessage());
            }
        }
    }
}
