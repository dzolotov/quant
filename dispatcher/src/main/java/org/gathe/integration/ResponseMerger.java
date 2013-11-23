package org.gathe.integration;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by zolotov on 01.11.13.
 */
public class ResponseMerger {
    private Logger LOG = Logger.getLogger(this.getClass());
    ArrayList<String> responses = new ArrayList<>();

    public ResponseMerger(ArrayList<String> responses) {
        this.responses = responses;
    }

    //todo: ACL, Priority
    public void parseXMLOneLevel(Element rootEntry, Document newDocument, Element newDocumentParentElement, String newPath) {
        NodeList subNodes = rootEntry.getChildNodes();

        HashMap<String, Element> newSubNodesEntries = new HashMap<>();
        NodeList newSubNodes = newDocumentParentElement.getChildNodes();
        for (int i = 0; i < newSubNodes.getLength(); i++) {
            if (newSubNodes.item(i).getNodeType() == Node.ELEMENT_NODE)
                newSubNodesEntries.put(newSubNodes.item(i).getNodeName(), (Element) newSubNodes.item(i));
        }

        String textContent = null;
        for (int i = 0; i < subNodes.getLength(); i++) {
            if (subNodes.item(i).getNodeType() != Node.TEXT_NODE) continue;
            Node item = (Node) subNodes.item(i);
            LOG.debug("Node: " + item.getNodeName() + " text: " + item.getTextContent());
            if (item.getTextContent().trim().length() != 0) textContent = item.getTextContent();
        }

        if (textContent != null) {
            newDocumentParentElement.appendChild(newDocument.createTextNode(textContent));
        }
        for (int i = 0; i < subNodes.getLength(); i++) {
            Element newParent = null;
            String path = newPath + "/" + subNodes.item(i).getNodeName();
            if (subNodes.item(i).getNodeType() != Node.ELEMENT_NODE) continue;
            Element oldElement = (Element) subNodes.item(i);
            if (newSubNodesEntries.keySet().contains(oldElement.getNodeName())) {
                //overriding xml subtree
                newParent = newSubNodesEntries.get(oldElement.getNodeName());
            } else {
                Element newElement = newDocument.createElement(oldElement.getNodeName());
                newDocumentParentElement.appendChild(newElement);
                newParent = newElement;
            }
            parseXMLOneLevel((Element) subNodes.item(i), newDocument, newParent, path);
        }
    }

    public Document getResponseXML() {
        DocumentBuilderFactory responseFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder responseBuilder = null;
        org.w3c.dom.Document mergedResponse = null;
        try {
            responseBuilder = responseFactory.newDocumentBuilder();
            mergedResponse = responseBuilder.newDocument();

            for (String response : responses) {
                try {
                    DocumentBuilder responseParserBuilder = responseFactory.newDocumentBuilder();
                    Document doc = responseParserBuilder.parse(new InputSource(new StringReader(response)));
                    Element root = doc.getDocumentElement();
                    Element newRoot = mergedResponse.createElement(root.getTagName());
                    mergedResponse.appendChild(newRoot);
                    parseXMLOneLevel(root, mergedResponse, newRoot, "/");
                } catch (Exception e) {
                    LOG.debug("Warning. Error when merging");
                }
            }

        } catch (Exception e) {
            LOG.error("Error occured when parsing response XML " + e.getMessage());
        }

        return mergedResponse;
    }

    public String getResponseJSON() {
        //TODO
        String json = "";
        return json;
    }
}
