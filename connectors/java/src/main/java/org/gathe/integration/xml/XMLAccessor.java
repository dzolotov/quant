package org.gathe.integration.xml;

import org.gathe.integration.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
 * @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
 */
public class XMLAccessor extends DatasetAccessor {

    private String schemaName;

    public XMLAccessor(String systemId, String schemaName) {
        super("XML", systemId);
        this.schemaName = schemaName;
        JAXBContext jc = null;
        try {
            jc = JAXBContext.newInstance(XMLSchemaJAXB.class);
            Unmarshaller u = jc.createUnmarshaller();
            schema = (XMLSchemaJAXB) u.unmarshal(new FileReader(this.schemaName));
            for (AccessorField field : schema.getSchemaFields()) {
                LOG.debug("Schema entry: " + field);
            }
//            if (((XMLSchemaJAXB) schema).getSource() != null) parseSourceFile();
//            System.out.println(schema.getSchemaFields().size());
            bindingDB = DSBindingDatabase.getDatabase("XML", ((XMLSchemaJAXB) schema).getDataClass());
        } catch (JAXBException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<DataClass> getSchema() {
        LOG.info("Extracting schema");
        return this.getClassSchema(((XMLSchemaJAXB) schema).getDataClass());
    }

    @Override
    protected HashMap<String, String> getRow(String identifierName, String identifierValue, boolean applyTransform) {
        //transform xml to row
        return new HashMap<String, String>();
    }

    @Override
    protected ArrayList<HashMap<String, String>> getDataset(String transactionId, String className) {
        //transform xml to dataset
        return new ArrayList<>();
    }


    @Override
    protected String getPath(String key) {
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.getKey().equalsIgnoreCase(key)) {
//                if (field.isIdentifier()) return
                return field.getPath();
            }
        }
        return null;
        //transform key (xpath) -> transport xml path
    }

    @Override
    public boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> row) {

        LOG.debug("Update");
        for (String key : row.keySet()) {
            LOG.debug(key + " : " + row.get(key));
        }

        String filenamePath = ((XMLSchemaJAXB) schema).getFilename();
        String filename;
        if (identifierValue != null) {
            filename = ((XMLSchemaJAXB) schema).getDir() + "/" + identifierValue + ".xml";
        } else {
            filename = ((XMLSchemaJAXB) schema).getDir() + "/" + row.get(filenamePath) + ".xml";
        }

        List<DataClass> sch = this.getSchema();
        GetHelper gh = new GetHelper(null, null, className);
        gh.setEncoding(((XMLSchemaJAXB) schema).getEncoding());
        for (DataClass sc : sch) {
            Iterator<DataElement> de = sc.getElements();
            while (de.hasNext()) {
                gh.addElementToSchema(de.next());
            }
        }
        for (String key : row.keySet()) {
            gh.put(key, row.get(key));
        }
        String res = gh.transformToXml();

        File result = new File(filename);
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(result), ((XMLSchemaJAXB) schema).getEncoding()));
            bw.write(res);
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public HashMap<String, String> insertData(String className, String identifierName, String identifierValue, HashMap<String, String> row) {
        //create new xml
        //impossible function
//        HashMap<String,String> identifier

        this.updateData(className, identifierName, identifierValue, row);


        LOG.debug("Insert");
        for (String key : row.keySet()) {
            LOG.debug(key + " : " + row.get(key));
        }

        String filenamePath = ((XMLSchemaJAXB) schema).getFilename();
        HashMap<String, String> identifiers = new HashMap<>();
        identifiers.put(identifierName, row.get(filenamePath));


        return identifiers;
    }

    @Override
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue) {
        //check existence
        return true;
//        String dir = ((XMLSchemaJAXB) schema).getDir();
//        return (new File(dir+"/"+identifierValue+".xml").exists());
    }
}
