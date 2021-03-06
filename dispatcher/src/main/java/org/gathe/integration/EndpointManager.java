package org.gathe.integration;

import org.apache.log4j.Logger;
import org.hsqldb.Server;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.NamingException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
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
public class EndpointManager extends Thread {

    private static int ringLength = 256;

    Connection operations;
    Connection history;

    private MonitorThread mt;

    private static Logger LOG = Logger.getLogger("EndpointManager");
    private ArrayList<String> endpointNames = new ArrayList<>();
    private HashMap<String, HashMap<String, Object>> endpoints = new HashMap<>();
    private HashMap<String, HashMap<String, String>> extendsData = new HashMap<>();
    private ConcurrentHashMap<String, String> requests = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Thread> threads = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> identifiers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> replies = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> classNames = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashMap<String, String>> headers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ResponseThread> responseThreads = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, HashMap<String, String>> waitingList = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ArrayList<String>> responses = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ArrayList<String>> responders = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> chainResponses = new ConcurrentHashMap<>();

    private ReceiverThread rt;

    private static String ring[] = new String[ringLength];
    private static int ringHead = 0;

    private Session session;
    private MessageProducer producer;
    private boolean isDisconnected;

    private Response sync;

    public void reconnect() throws JMSException {
        try {
            rt.connect();
        } catch (JMSException | NamingException e) {
            LOG.error("Error when reconnecting to MQ Broker");
        }
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Session getSession() {
        return this.session;
    }

    public void setMessageProducer(MessageProducer messageProducer) {
        this.producer = messageProducer;
    }

    public MessageProducer getMessageProducer() {
        return this.producer;
    }

    public void disconnect() {
        //kill all the threads
        for (String thKey : threads.keySet()) {
            Thread th = threads.get(thKey);
            try {
                th.interrupt();
            } catch (Exception e) {
            }
        }
        isDisconnected = true;
    }

    public boolean isDisconnected() {
        return isDisconnected;
    }

    public EndpointManager() {
        LOG.info("Endpoint Manager initialized");
        isDisconnected = false;

        Server embedded = new Server();
        embedded.setPort(8000);
        embedded.setLogWriter(null);
        embedded.setSilent(true);
        embedded.setDatabaseName(0, "history");
        embedded.setDatabasePath(0, "file:history");
        embedded.setDatabaseName(1, "operations");
        embedded.setDatabasePath(1, "file:operations");
        embedded.start();

        try {
            Class.forName("org.hsqldb.jdbcDriver");
            operations = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:8000/operations", "sa", "");
            history = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:8000/history", "sa", "");

            Statement st = operations.createStatement();
            try {
                if (!st.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='log'").next()) {
                    //define table
                    st.executeUpdate("CREATE TABLE log (id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL, action varchar(100), source varchar(100), dtime timestamp default now)");
                }
            } catch (SQLException e) {
                //just ignore
            }

            Statement sth = history.createStatement();
            try {
                if (!sth.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='identifiers'").next()) {
                    //define table
                    sth.executeUpdate("CREATE TABLE identifiers (id INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL, uuid varchar(36), identifier_name varchar(100), identifier_value varchar(255), dtime timestamp default now)");
                }
            } catch (SQLException e) {
                //just ignore
            }
        } catch (ClassNotFoundException e) {
            LOG.error("Error when connecting to operations database");
            e.printStackTrace();
        } catch (SQLException e) {
            LOG.error("Error when connecting to operations database");
            e.printStackTrace();
        }

    }

    public void run() {
        rt = new ReceiverThread(this);
        mt = new MonitorThread(this);
        mt.start();
        rt.start();

        try {
            rt.join();          //message loop
        } catch (Exception e) {
        }

        mt.interrupt();
    }

    public int getRingHead() {
        return ringHead;
    }

    public String[] getMessages(int tail) {
        Integer head = new Integer(ringHead);
        synchronized (head) {
            ArrayList<String> messages = new ArrayList<>();
            messages.add("" + head);
            int index = tail;
            while (index != head) {
                messages.add(ring[index]);
                index++;
                if (index >= ringLength) index = 0;
            }


            for (String message : messages) {
                LOG.debug(message);
            }

            return messages.toArray(new String[0]);
        }
    }

    public static void sendMessage(String message) {

        ring[ringHead] = message;
        ringHead++;
        if (ringHead >= ringLength) ringHead = 0;

        ConcurrentHashMap<String, Response> sem = WebHandler.semaphores;

        try {
            synchronized (sem) {
                for (String threadId : sem.keySet()) {
                    Response response = sem.get(threadId);
                    synchronized (response) {
                        LOG.debug("Sending to " + threadId);
                        response.setResponse(message);
                        response.notify();
                    }
                }
            }
        } catch (ConcurrentModificationException e) {
            LOG.error("ConcurrentModificationException in sendMessage, continue to message loop");
            e.printStackTrace();
        }
    }

    public void doInit() {
        sendEndpointNames();
    }

    public void setSyncObject(Response sync) {
        this.sync = sync;
    }

    public void doPing(String endpointName) {
        if (endpoints.containsKey(endpointName)) {
            LOG.info("Pinging system " + endpointName);
            endpoints.get(endpointName).put("confirmed", "0");
        }
    }

    public boolean checkEndpointUpdatable(String endpointName, String className) {
        HashMap<String, Object> schema = (HashMap<String, Object>) endpoints.get(endpointName);
        ArrayList<String> updatable = (ArrayList<String>) schema.get("updatable");
        if (schema.containsKey(className) && updatable.contains(className)) {
            return true;         //found top class match
        }
        HashMap<String, String> extData = extendsData.get(endpointName);
        if (extData != null) {
            for (String subclass : extData.keySet()) {
                String extClass = extData.get(subclass);
                if (extClass.trim().equalsIgnoreCase(className)) {
                    if (checkEndpointUpdatable(endpointName, subclass)) return true;
                }
            }
        }
        return false;
    }

    public boolean checkEndpointMatchable(String endpointName, String className) {
        HashMap<String, Object> schema = (HashMap<String, Object>) endpoints.get(endpointName);
        ArrayList<String> matchable = (ArrayList<String>) schema.get("matchable");
        if (schema.containsKey(className) && matchable.contains(className)) {
            return true;         //found top class match
        }
        HashMap<String, String> extData = extendsData.get(endpointName);
        if (extData != null) {
            for (String subclass : extData.keySet()) {
                String extClass = extData.get(subclass);
                if (extClass.trim().equalsIgnoreCase(className)) {
                    if (checkEndpointMatchable(endpointName, subclass)) return true;
                }
            }
        }
        return false;
    }

    //Traverse to most abstract type
    public String traverseToAbstract(String className) {
        LOG.debug("Traversing to abstract via " + className);
        String topClass = this.searchAncestor(className);
        if (topClass != null) return traverseToAbstract(topClass);
        return className;
    }

    public String searchAncestor(String className) {
        String topClass = null;
        for (String endpointName : endpoints.keySet()) {
            HashMap<String, String> extData = extendsData.get(endpointName);
            if (extData != null) {
                if (extData.containsKey(className)) topClass = extData.get(className);
            }
        }
        return topClass;
    }

    public String getTrack(String className) {
        LOG.info("Traversing via " + className);
        String topClass = this.searchAncestor(className);
        if (topClass != null) return getTrack(topClass) + "-" + className;
        return className;
    }

    public String searchNearestIdentification(String className, String identifier) {
        LOG.info("Searching for nearest identification");
        for (String endpointName : endpoints.keySet()) {
            LOG.debug("Endpoint: " + endpointName);
            HashMap<String, Object> epRef = (HashMap<String, Object>) endpoints.get(endpointName).get("identifiers");
            HashMap<String, String> idRef = (HashMap<String, String>) epRef.get(className);
            if (idRef != null && idRef.containsKey(identifier))
                return className;        //find identification possibility
        }
        String topClass = searchAncestor(className);        //traverse up to root
        if (topClass != null) return searchNearestIdentification(topClass, identifier);
        return null;        //if not found
    }

    public String searchNearestCheckpoint(String className, String check) {
        for (String endpointName : endpoints.keySet()) {
            HashMap<String, Object> epRef = (HashMap<String, Object>) endpoints.get(endpointName).get("checks");
            HashMap<String, String> idRef = (HashMap<String, String>) epRef.get(className);
            if (idRef != null && idRef.containsKey(check)) return className;        //find identification possibility
        }
        String topClass = searchAncestor(className);        //traverse up to root
        if (topClass != null) return searchNearestCheckpoint(topClass, check);
        return null;        //if not found
    }

    //get all declared subclasses for specified class
    public ArrayList<String> getSubclasses(String className) {
        ArrayList<String> result = new ArrayList<>();
        for (String endpointName : extendsData.keySet()) {
            HashMap<String, String> extendInfo = extendsData.get(endpointName);
            if (extendInfo.containsValue(className)) {
                for (String subclass : extendInfo.keySet()) {
                    if (extendInfo.get(subclass).equalsIgnoreCase(className)) {
                        result.add(subclass);           //add direct derivative class
                        result.addAll(getSubclasses(subclass));     //and all subclasses
                    }
                }
            }
        }
        ArrayList<String> unique = new ArrayList<>();
        for (String res : result) {
            if (!unique.contains(res)) unique.add(res);
        }
        return unique;
    }

    private ArrayList<String> getIdentifierByEndpoint(String endpointName, String className, ArrayList<String> allSubclasses) {
        ArrayList<String> result = new ArrayList<>();
        if (!endpoints.containsKey(endpointName)) return result;
        HashMap<String, HashMap<String, HashMap<String, String>>> endpointIdentifiers = (HashMap<String, HashMap<String, HashMap<String, String>>>) endpoints.get(endpointName).get("identifiers");

        for (String clName : allSubclasses) {
            if (endpointIdentifiers.containsKey(clName)) {
                //class present - extract identifiers
                for (String ks : endpointIdentifiers.get(clName).keySet()) {
                    result.add(clName + "." + ks);           //add full identifier path
                }
            }
        }
        return result;
    }

    public void storeIdentifier(String uuid, String identifierName, String identifierValue) {
        try {
            Statement sth = history.createStatement();
            String qr = "INSERT INTO identifiers (uuid,identifier_name,identifier_value) VALUES ('" + uuid + "','" + identifierName + "','" + identifierValue + "')";
            LOG.debug("Query to store: " + qr);
            sth.executeUpdate(qr);
            sth.executeUpdate("COMMIT");
        } catch (SQLException e) {
            LOG.error("Error occured when storing identifier");
        }
    }

    public ArrayList<String> getIdentifierRequests(String className) {
        ArrayList<String> result = new ArrayList<>();
        ArrayList<String> allClasses = this.getSubclasses(className);
        allClasses.add(className);
        for (String endpointName : endpoints.keySet()) {
            result.addAll(getIdentifierByEndpoint(endpointName, className, allClasses));
        }

        ArrayList<String> unique = new ArrayList<>();
        for (String res : result) {
            if (!unique.contains(res)) unique.add(res);
        }
        return unique;
    }

    public String getAnimationToEndpointIndexes(String messageId) {
        LOG.debug("To Endpoints");
        ArrayList<String> endpointIndexes = new ArrayList<>();
        for (String key : waitingList.get(messageId).keySet()) {
            int id = this.getEndpointIndex(key);
            if (id >= 0) endpointIndexes.add("+" + id);
        }

        return this.join(endpointIndexes, ",");
    }

    public String getAnimationToUpdateEndpoints(String className) {
        ArrayList<String> endpointIndexes = new ArrayList<>();
        for (String endpointName : endpoints.keySet()) {
            if (checkEndpointUpdatable(endpointName, className)) {
                int id = this.getEndpointIndex(endpointName);
                if (id >= 0) endpointIndexes.add("+" + id);
            }
        }
        return this.join(endpointIndexes, ",");
    }

    public String fetchStoredUuid(String identifierName, String id) {
        try {
            Statement st = history.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM identifiers WHERE identifier_name='" + identifierName + "' AND identifier_value='" + id + "'");
            if (rs.next()) return rs.getString("uuid");
        } catch (Exception e) {
        }
        return null;
    }

    public String fetchStoredId(String identifierName, String uuid) {
        try {
            Statement st = history.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM identifiers WHERE uuid='" + uuid + "' AND identifier_name='" + identifierName + "'");
            if (rs.next()) return rs.getString("identifier_value");
        } catch (Exception e) {
        }
        return null;
    }

    private boolean allSystemsConfirmed() {
        boolean result = true;
        for (String key : endpoints.keySet()) {
            if (endpoints.get(key).get("confirmed").equals("0")) {
                result = false;
                break;
            }
        }
        return result;
    }

    public void reactivateSystem(String endpointName) {
        endpoints.get(endpointName).put("confirmed", "1");
    }

    public void doPong(String endpointName) {
        if (endpoints.containsKey(endpointName)) {
            LOG.info("System confirmed " + endpointName);
            endpoints.get(endpointName).put("confirmed", "1");
            if (allSystemsConfirmed()) {
                synchronized (sync) {
                    sync.setResponse(this.getPongAnimation());
                    sync.notify();
                }
            }
        }
    }

    private String getPongAnimation() {
        ArrayList<String> as = this.getConfirmedSystems();
        ArrayList<String> as2 = new ArrayList<>();
        for (String ax : as) {
            as2.add("-" + this.getEndpointIndex(ax));

        }
        return this.join(as2, ",");
    }

    public ArrayList<String> getConfirmedSystems() {
        ArrayList<String> result = new ArrayList<>();
        for (String key : endpoints.keySet()) {
            if (endpoints.get(key).get("confirmed").equals("1")) {
                result.add(key);
            }
        }
        return result;
    }

    public List<String> getTimeouted() {
        List<String> timeouted = new ArrayList<String>();
        Set<String> st = this.enumerateEndpoints();
        for (String endpointName : st) {
            if (endpoints.get(endpointName).containsKey("confirmed") && ("" + endpoints.get(endpointName).get("confirmed")).equalsIgnoreCase("0")) {
                timeouted.add(endpointName);
                LOG.info("System is dead " + endpointName);
            }
        }
        return timeouted;
    }

    public String join(String r[], String d) {
        if (r.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        int i;
        for (i = 0; i < r.length - 1; i++)
            sb.append(r[i] + d);
        return sb.toString() + r[i];
    }

    public String join(ArrayList<String> r, String d) {
        return join(r.toArray(new String[0]), d);
    }


    public Set<String> enumerateEndpoints() {
        return endpoints.keySet();
    }

    public void unregister(String endpointName) {
        if (endpointNames.contains("+" + endpointName)) {
            int index = endpointNames.indexOf("+" + endpointName);
            endpointNames.set(index, "-" + endpointName);
            sendEndpointNames();
        }
        endpoints.remove(endpointName);
    }

    public void sendEndpointNames() {
        sendMessage("endpoints:" + join(endpointNames.toArray(new String[0]), ","));
    }

    public void sendAnimation(String transactionId, String commonAction, String identifier, String color, String actions) {
        if (identifier == null) return;
        LOG.debug("Sending animation for " + transactionId + " ca: " + commonAction + " id:" + identifier + " script " + color + ":" + actions);
        identifier = identifier.replace(":", "-");
        sendMessage("animation:" + transactionId + ":" + commonAction + ":" + identifier + ":" + color + ":" + actions);

    }

    public int getEndpointIndex(String name) {
        LOG.debug("Resolving endpoint name: " + name);
        int id = endpointNames.indexOf("+" + name);
        if (id < 0) {
            id = endpointNames.indexOf("-" + name);
        }
        return id;
    }

    public void register(String endpointName, String xml) {
        LOG.info("Registering new endpoint: " + endpointName + " with schema: " + xml);
        if (endpointNames.contains("-" + endpointName)) {
            int index = endpointNames.indexOf("-" + endpointName);
            endpointNames.set(index, "+" + endpointName);
            sendEndpointNames();
        }
        if (!endpointNames.contains("+" + endpointName)) {
            endpointNames.add("+" + endpointName);
            sendEndpointNames();
        }
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new InputSource(new StringReader(xml)));
            Element root = doc.getDocumentElement();
            NodeList classes = root.getChildNodes();
            HashMap<String, HashMap<String, String>> classDescription = new HashMap<>();
            HashMap<String, HashMap<String, String>> identifierDescription = new HashMap<>();
            HashMap<String, HashMap<String, String>> checkDescription = new HashMap<>();
            ArrayList<String> specifiables = new ArrayList<>();
            ArrayList<String> updatable = new ArrayList<>();
            ArrayList<String> matchable = new ArrayList<>();

            for (int classId = 0; classId < classes.getLength(); classId++) {
                Element classTag = (Element) classes.item(classId);
                if (!classTag.getNodeName().equalsIgnoreCase("class")) continue;
                if (classTag.hasAttribute("specifiable") && classTag.getAttribute("specifiable").equals("true")) {
                    specifiables.add(classTag.getAttribute("id"));
                }
                if (!classTag.hasAttribute("readonly") || !classTag.getAttribute("readonly").equalsIgnoreCase("true")) {
                    updatable.add(classTag.getAttribute("id"));
                }
                if (classTag.hasAttribute("matchable") && classTag.getAttribute("matchable").equals("true")) {
                    matchable.add(classTag.getAttribute("id"));
                }

                NodeList attributes = classTag.getChildNodes();
                HashMap<String, String> attributesHM = new HashMap<>();
                HashMap<String, String> identifierHM = new HashMap<>();
                HashMap<String, String> checkHM = new HashMap<>();
                for (int attributeId = 0; attributeId < attributes.getLength(); attributeId++) {
                    Element attribute = (Element) attributes.item(attributeId);
                    if (attribute.getNodeName().equalsIgnoreCase("attribute")) {
                        String xmlPath = attribute.getAttribute("path");
                        String description = attribute.getAttribute("description");
                        if (description == null) description = "";
                        LOG.debug("Found new attribute " + xmlPath + " (" + description + ")");
                        attributesHM.put(xmlPath, description);
                    } else if (attribute.getNodeName().equalsIgnoreCase("identifier")) {
                        String name = attribute.getAttribute("name");
                        String description = attribute.getAttribute("description");
                        LOG.debug("Found new identifier " + name + " (" + description + ")");
                        identifierHM.put(name, description);
                    } else if (attribute.getNodeName().equalsIgnoreCase("check")) {
                        String name = attribute.getAttribute("name");
                        String description = attribute.getAttribute("description");
                        LOG.debug("Found new check " + name + " (" + description + ")");
                        checkHM.put(name, description);
                    }
                }

                if (classTag.hasAttribute("extends")) {
                    String extendsClass = classTag.getAttribute("extends");
                    if (extendsClass != null && extendsClass.trim() != "") {

                        String className = classTag.getAttribute("id");
                        if (!extendsData.containsKey(endpointName)) {
                            HashMap hx = new HashMap();
                            extendsData.put(endpointName, hx);
                            LOG.debug("Found extend for " + endpointName + " source class: " + className + " extends " + extendsClass);
                        }
                        extendsData.get(endpointName).put(className, extendsClass);

                    }
                }

                classDescription.put(classTag.getAttribute("id"), attributesHM);
                identifierDescription.put(classTag.getAttribute("id"), identifierHM);
                checkDescription.put(classTag.getAttribute("id"), checkHM);
            }
            HashMap systemDescription = new HashMap();
            systemDescription.put("schema", classDescription);
            systemDescription.put("identifiers", identifierDescription);
            systemDescription.put("checks", checkDescription);
            systemDescription.put("matchable", matchable);
            systemDescription.put("specifiables", specifiables);
            systemDescription.put("updatable", updatable);
            systemDescription.put("confirmed", "1");
            endpoints.put(endpointName, systemDescription);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addGetRequest(String commonAction, String messageId, String className, String uuid, String replyTo, Thread thread, ResponseThread responseThread) {
        LOG.debug("Add get request for " + className + ":" + uuid + ". Message ID: " + messageId);
        responders.put(messageId, new ArrayList<String>());
        requests.put(messageId, commonAction);
        threads.put(messageId, thread);
        replies.put(messageId, replyTo);
        identifiers.put(messageId, uuid);
        responseThreads.put(messageId, responseThread);
        HashMap<String, String> waitingData = new HashMap<>();
        for (String endpointName : endpoints.keySet()) {
            HashMap<String, Object> schema = (HashMap<String, Object>) endpoints.get(endpointName).get("schema");
            for (String schemaClassName : schema.keySet()) {
                if (schemaClassName.equalsIgnoreCase(className)) {
                    LOG.debug("Resolved to endpoint " + endpointName);
                    waitingData.put(endpointName, new Timestamp(new java.util.Date().getTime()).toString());
                }
            }
        }
        waitingList.put(messageId, waitingData);
    }

    public void addMatchRequest(String commonAction, String messageId, String className, String replyTo, Thread thread, HashMap<String, String> headers, ResponseThread responseThread) {
        LOG.debug("Add match request for " + className + ". Message ID: " + messageId);
        responders.put(messageId, new ArrayList<String>());
        requests.put(messageId, commonAction);
        threads.put(messageId, thread);
        if (replyTo != null) replies.put(messageId, replyTo);
        classNames.put(messageId, className);
        if (headers != null) this.headers.put(messageId, headers);
        identifiers.put(messageId, "");
        responseThreads.put(messageId, responseThread);
        HashMap<String, String> waitingData = new HashMap<>();
        for (String endpointName : endpoints.keySet()) {
            ArrayList<String> matches = (ArrayList<String>) endpoints.get(endpointName).get("matchable");
            if (matches.contains(className)) {
                LOG.debug("Resolved to endpoint " + endpointName);
                waitingData.put(endpointName, new Timestamp(new java.util.Date().getTime()).toString());
            }
        }
        waitingList.put(messageId, waitingData);
    }

    public void addCheckRequest(String commonAction, String messageId, String className, String identifierName, String identifier, String replyTo, Thread thread, ResponseThread responseThread) {
        LOG.debug("Add check request for " + className + "." + identifierName + ": " + identifier + ". Message ID: " + messageId);
        responders.put(messageId, new ArrayList<String>());
        requests.put(messageId, commonAction);
        threads.put(messageId, thread);
        if (replyTo != null) replies.put(messageId, replyTo);
        identifiers.put(messageId, identifier);
        responseThreads.put(messageId, responseThread);
        HashMap<String, String> waitingData = new HashMap<>();
        for (String endpointName : endpoints.keySet()) {
            HashMap<String, HashMap<String, String>> checks = (HashMap<String, HashMap<String, String>>) endpoints.get(endpointName).get("checks");
            for (String schemaClassName : checks.keySet()) {
                if (schemaClassName.equalsIgnoreCase(className)) {
                    if (checks.get(schemaClassName).containsKey(identifierName)) {
                        LOG.debug("Resolved to endpoint " + endpointName);
                        waitingData.put(endpointName, new Timestamp(new java.util.Date().getTime()).toString());
                    }
                }
            }
        }
        waitingList.put(messageId, waitingData);
    }

    public void addIdentifierRequest(String commonAction, String messageId, String className, String identifierName, String identifier, String replyTo, Thread thread, ResponseThread responseThread) {
        LOG.debug("Add resolve request for " + className + "." + identifierName + ": " + identifier + ". Message ID: " + messageId);
        responders.put(messageId, new ArrayList<String>());
        requests.put(messageId, commonAction);
        threads.put(messageId, thread);
        if (replyTo != null) replies.put(messageId, replyTo);
        identifiers.put(messageId, identifier);
        responseThreads.put(messageId, responseThread);

        HashMap<String, String> waitingData = new HashMap<>();
        for (String endpointName : endpoints.keySet()) {
            HashMap<String, HashMap<String, String>> identifiers = (HashMap<String, HashMap<String, String>>) endpoints.get(endpointName).get("identifiers");
            for (String schemaClassName : identifiers.keySet()) {
                LOG.debug("addIdentifierRequest: @" + endpointName + ": className: " + className + ":" + schemaClassName + " -> " + identifierName + ":" + identifiers.get(schemaClassName));
                if (schemaClassName.equalsIgnoreCase(className)) {
                    if (identifiers.get(schemaClassName).containsKey(identifierName)) {
                        LOG.debug("Resolved to endpoint: " + endpointName);
                        waitingData.put(endpointName, new Timestamp(new java.util.Date().getTime()).toString());
                    }
                }
            }
        }
        waitingList.put(messageId, waitingData);
    }

    public void addSpecifyRequest(String commonAction, String messageId, String className, String uuid, String replyTo, Thread thread, ResponseThread responseThread) {
        LOG.debug("Add specify request for " + className + ":" + uuid + ". Message ID: " + messageId);
        responders.put(messageId, new ArrayList<String>());
        requests.put(messageId, commonAction);
        threads.put(messageId, thread);
        if (replyTo != null) replies.put(messageId, replyTo);
        identifiers.put(messageId, uuid);
        if (responseThread != null) responseThreads.put(messageId, responseThread);
        HashMap<String, String> waitingData = new HashMap<>();
        for (String endpointName : endpoints.keySet()) {
            if (isClassNameSpecifiableByEndpoint(endpointName, className)) {
                LOG.debug("Resolved to endpoint: " + endpointName);
                waitingData.put(endpointName, new Timestamp(new java.util.Date().getTime()).toString());
            }
        }
        waitingList.put(messageId, waitingData);
    }

    public String getReplyTo(String messageId) {
        return replies.get(messageId);
    }

    public HashMap<String, String> getHeaders(String messageId) {
        return new HashMap<>(headers.get(messageId));
    }

    public String getClassNames(String messageId) {
        return classNames.get(messageId);
    }

    public void timeoutResponse(String messageId) {
        if (threads.containsKey(messageId)) {
            responseThreads.get(messageId).start();
        }
    }

    public void gotResponse(String messageId, String from, String content) {
//        LOG.debug("Got response from remote system " + from + " with " + content);
        if (threads.containsKey(messageId)) {

            HashMap<String, String> waitingData = waitingList.get(messageId);
            waitingData.remove(from);
            responders.get(messageId).add(from);
            waitingList.put(messageId, waitingData);
            if (!responses.containsKey(messageId)) {
                responses.put(messageId, new ArrayList<String>());
            }
            LOG.debug("waiting data length for " + messageId + " is " + waitingData.size());
            responses.get(messageId).add(content);

            if (waitingData.isEmpty()) {
                responseThreads.get(messageId).start();
            }
        }
    }

    public String getResponseAnimation(String messageId) {
        ArrayList<String> animationElements = new ArrayList<>();
        LOG.debug("Retrieving response animation for " + messageId);
        for (String systemId : responders.get(messageId)) {
            LOG.debug("Getting endpoint: " + systemId);
            animationElements.add("-" + this.getEndpointIndex(systemId));
        }
        return this.join(animationElements, ",");
    }

    public void interruptRequestThread(String messageId) {
        if (threads.containsKey(messageId) && threads.get(messageId) != null && !threads.get(messageId).isInterrupted()) {
            threads.get(messageId).interrupt();
        }
    }

    public String getRequestCommonAction(String messageId) {
        return requests.get(messageId);
    }

    public String getRequestIdentifier(String messageId) {
        return identifiers.get(messageId);
    }

    public void cleanupResponse(String messageId) {
        replies.remove(messageId);
        threads.remove(messageId);
        responses.remove(messageId);
        requests.remove(messageId);
        if (headers.contains(messageId)) headers.remove(messageId);
        if (classNames.contains(messageId)) classNames.remove(messageId);
        responders.remove(messageId);
        identifiers.remove(messageId);
        responseThreads.remove(messageId);
    }

    public ResponseThread getResponseThread(String messageId) {
        return responseThreads.get(messageId);
    }

    public ArrayList<String> getAllResponses(String messageId) {
        return responses.get(messageId);
    }

    public boolean isClassNameExtendable(String generalClassName) {

        for (String endpointName : extendsData.keySet()) {
            if (extendsData.containsKey(endpointName) && extendsData.get(endpointName).containsValue(generalClassName))
                return true;
        }
        return false;

    }

    public boolean isClassNameSpecifiableByEndpoint(String endpointName, String className) {
//
        ArrayList<String> specifiables = (ArrayList<String>) endpoints.get(endpointName).get("specifiables");

        if (specifiables.contains(className)) {
            return true;
        }

        ArrayList<String> checkedClassNames = new ArrayList<>();

        for (String epName : endpoints.keySet()) {
            if (extendsData.containsKey(epName)) {
                HashMap<String, String> endpointExtends = extendsData.get(epName);
                if (endpointExtends.containsKey(className)) {
                    String parentClassName = endpointExtends.get(className);

                    if (!checkedClassNames.contains(parentClassName)) {
                        if (isClassNameSpecifiableByEndpoint(endpointName, parentClassName)) {
                            return true;
                        }
                        checkedClassNames.add(parentClassName);
                    }
                }
            }
        }

        return false;
    }

    public void setResponse(String messageId, String response) {
        chainResponses.put(messageId, response);
    }

    public String getResponse(String messageId) {
        String response = chainResponses.get(messageId);
        chainResponses.remove(messageId);

        return response;
    }
}
