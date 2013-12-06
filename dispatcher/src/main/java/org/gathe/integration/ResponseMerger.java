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
import com.sun.corba.se.spi.activation._RepositoryImplBase;
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

public class ResponseMerger {
    private Logger LOG = Logger.getLogger(this.getClass());
    ArrayList<String> responses = new ArrayList<>();

    public ResponseMerger(ArrayList<String> responses) {
        this.responses = responses;

        LOG.info("Merging xml response");
        LOG.debug("Length="+responses.size());
        for (int i=0;i<responses.size();i++) {
            LOG.debug("Data["+i+"]="+responses.get(i));
        }
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

        String previousTextContent = null;
        for (int i = 0; i < newSubNodes.getLength(); i++) {
            if (newSubNodes.item(i).getNodeType() != Node.TEXT_NODE) continue;
            Node item = (Node) newSubNodes.item(i);
            LOG.debug("Node: " + item.getNodeName() + " text: " + item.getTextContent());
            if (item.getTextContent().trim().length() != 0) previousTextContent = item.getTextContent();
        }

        if (textContent != null && previousTextContent==null) {
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
            LOG.info("Diving deep to "+subNodes.item(i).getNodeName()+" (newpath: "+path+")");
            parseXMLOneLevel((Element) subNodes.item(i), newDocument, newParent, path);
        }
    }

    public Document getResponseXML(String className) {
        DocumentBuilderFactory responseFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder responseBuilder = null;
        org.w3c.dom.Document mergedResponse = null;
        try {
            responseBuilder = responseFactory.newDocumentBuilder();
            mergedResponse = responseBuilder.newDocument();

            String firstResponse = "";
            for (String response : responses) {
                if (response==null || response.trim().length()==0) continue;
                firstResponse = response;
            }
            DocumentBuilder responseParserBuilder = responseFactory.newDocumentBuilder();
            Document doc = responseParserBuilder.parse(new InputSource(new StringReader(firstResponse)));
            if (firstResponse.trim().length()==0) return mergedResponse;

            Element newRoot = mergedResponse.createElement(className);
            mergedResponse.appendChild(newRoot);

            for (String response : responses) {
                Document parse = responseParserBuilder.parse(new InputSource(new StringReader(response)));
                Element root = parse.getDocumentElement();
                if (response==null || response.trim().length()==0) continue;      //skip empty
                try {
                    parseXMLOneLevel(root, mergedResponse, newRoot, "");
                } catch (Exception e) {
                    LOG.info("Warning. Error when merging");
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            LOG.error("Error occured when parsing response XML " + e.getMessage());
        }

        return mergedResponse;
    }

    //todo
    public String getResponseJSON() {
        String json = "";
        return json;
    }
}
