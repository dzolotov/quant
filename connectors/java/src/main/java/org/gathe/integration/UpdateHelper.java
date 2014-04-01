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
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

public class UpdateHelper {
    private final String uuid;
    private final String transactionId;
    private Logger LOG = Logger.getLogger(this.getClass());

    private ArrayList<DataElement> schema = new ArrayList<>();

    private HashMap<String, String> result = new HashMap<>();

    public UpdateHelper(String uuid, String transactionId) {
        this.uuid = uuid;
        this.transactionId = transactionId;
    }

    public HashMap<String, String> getPatch() {
        return result;
    }

    public String getUuid() {
        return this.uuid;
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    public void resetSchema() {
        schema = new ArrayList<>();
    }

    public void addElementToSchema(DataElement del) {
        schema.add(del);
    }

    public String get(String path) {
        //todo: path normalization
        boolean found = false;
        for (DataElement element : schema) {
            if (element.getXPath().equalsIgnoreCase(path)) {
                found = true;
                break;
            }
        }
        return (found ? result.get(path) : null);
    }

    private int countSubnodes(Node node) {
        int count = 0;
        NodeList nl = node.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node subnode = nl.item(i);
            if (subnode.getNodeType() == Node.ELEMENT_NODE) count++;
        }
        return count;
    }

    private void parseLevel(Node level, String basePath) {
        LOG.debug("Node: " + level.getNodeName());
        NodeList nodeList = level.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;
            LOG.debug("Check subnode: " + node.getNodeName());
            if (countSubnodes(node) > 0) {
                LOG.debug("Parsing subnode");
                parseLevel(node, basePath + "/" + node.getNodeName());
            } else {
                LOG.debug("Put " + node.getTextContent() + " to " + basePath + "/" + node.getNodeName());
                result.put(basePath + "/" + node.getNodeName(), node.getTextContent());
            }
        }
    }

    public void transformFromXML(String data) {
        try {
            LOG.debug("Parsing XML: " + data);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new InputSource(new StringReader(data)));
            Element root = doc.getDocumentElement();
            parseLevel(root, "");
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//todo: convert from JSON
}
