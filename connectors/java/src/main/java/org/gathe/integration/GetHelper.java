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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;

public class GetHelper {
    private final String uuid;
    private final String transactionId;
    private final String rootNode;
    private Logger LOG = Logger.getLogger(this.getClass());

    private ArrayList<DataElement> schema = new ArrayList<>();

    private HashMap<String, String> result = new HashMap<>();

    public GetHelper(String uuid, String transactionId, String rootNode) {
        this.uuid = uuid;
        this.transactionId = transactionId;
        this.rootNode = rootNode;
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

    public void put(String path, String value) {
        //todo: path normalization
        LOG.info("Put to "+path+" value="+value);
        boolean found = false;
        for (DataElement element : schema) {
            if (element.getXPath().equalsIgnoreCase(path)) {
                found = true;
                break;
            }
        }
        if (found) result.put(path, value);
    }

    public void applyPatch(HashMap<String, String> patch) {
        for (String key : result.keySet()) {
            if (patch.containsKey(key)) result.put(key, patch.get(key));
        }
    }

    public HashMap<String, String> getResult() {
        return this.result;
    }
//todo: convert to XML

    public void fillXML(Document doc, Element parent, String topPath, int pos) {
        ArrayList<String> level = new ArrayList<>();
        LOG.info("TopPath: "+topPath);
        LOG.info("Pos: "+pos);
        String[] top = topPath.split("/");
        for (String resultKey : result.keySet()) {
            String[] split = resultKey.split("/");
            if (pos < split.length) {
                boolean match = true;
                for (int i = 1; i < top.length; i++)
                    if (!split[i].equalsIgnoreCase(top[i])) {
                        match = false;
                        break;        //skip any other paths
                    }
                if (!match) continue;
                String key = split[pos];
                LOG.info("Checking for key "+key);
                if (!level.contains(key)) {
                    level.add(key);
                    LOG.info("Added subelement of level " + pos + " " + key);
                    Element subElement = doc.createElement(key);
                    parent.appendChild(subElement);
                    LOG.info("Pos: " + (pos + 1) + " LN: " + split.length);
                    if (pos + 1 == split.length) {
                        LOG.info(key+" is a leaf");
                        LOG.info(topPath + "/" + key);
                        subElement.setTextContent(result.get(topPath + "/" + key));
                    } else {
                        LOG.info("Deeping to "+key+" from "+topPath);
                        fillXML(doc, subElement, topPath + "/" + key, pos + 1);
                    }
                }
            }
        }
    }

    public String transformToXml() {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        StringWriter sw = null;
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document newDocument = db.newDocument();
            Element root = newDocument.createElement(this.rootNode);
            newDocument.appendChild(root);
            fillXML(newDocument, root, "", 1);
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer;
            transformer = transformerFactory.newTransformer();
            sw = new StringWriter();
            transformer.transform(new DOMSource(root), new StreamResult(sw));
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        } catch (TransformerException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        return sw.toString();
    }
//todo: convert to JSON
}
