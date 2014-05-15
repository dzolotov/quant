package org.gathe.integration.csv;

import au.com.bytecode.opencsv.CSVReader;
import org.gathe.integration.AccessorField;
import org.gathe.integration.DSBindingDatabase;
import org.gathe.integration.DataClass;
import org.gathe.integration.DatasetAccessor;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
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

public class CSVAccessor extends DatasetAccessor {

    ArrayList<HashMap<String, String>> data = new ArrayList<>();
    private char separator;
    private char quote;
    private String schemaName;

    public CSVAccessor(String systemId, String schemaName, char separator, char quote) throws IOException {
        super("DS", systemId);
        this.schemaName = schemaName;
        JAXBContext jc = null;
        this.separator = separator;
        this.quote = quote;
        try {
            jc = JAXBContext.newInstance(CSVSchemaJAXB.class);
            Unmarshaller u = jc.createUnmarshaller();
            schema = (CSVSchemaJAXB) u.unmarshal(new FileReader(this.schemaName));

            for (AccessorField field : schema.getSchemaFields()) {
                LOG.debug("Schema entry: " + field);
            }
            if (((CSVSchemaJAXB) schema).getSource() != null) parseSourceFile();
            System.out.println(schema.getSchemaFields().size());
            bindingDB = DSBindingDatabase.getDatabase("DS", ((CSVSchemaJAXB) schema).getDataClass());
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    private void parseSourceFile() throws IOException {
        if (((CSVSchemaJAXB) (this.schema)).getSource() == null) throw new FileNotFoundException();

        CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(((CSVSchemaJAXB) (this.schema)).getSource()), ((CSVSchemaJAXB) (this.schema)).getEncoding()), this.separator, this.quote, false);
        LOG.info("Parsing source file: " + ((CSVSchemaJAXB) (this.schema)).getSource());

        List<String[]> entries = reader.readAll();
        int i = 0;
        if (((CSVSchemaJAXB) (this.schema)).getHeader().equalsIgnoreCase("true")) i++;       //skip header
        for (; i < entries.size(); i++) {
            HashMap<String, String> row = new HashMap<>();
            String[] entry = entries.get(i);
            for (int j = 0; j < entry.length; j++) {

                int quotePos = entry[j].indexOf('"');
                if (quotePos >= 0) {
                    LOG.debug("Entry: " + entry[j]);
                    LOG.debug("POS: " + entry[j].indexOf('"', quotePos + 1));

                    if (entry[j].indexOf('"', quotePos + 1) < 0) {
                        entry[j] += '"';        //workaround
                    }
                    LOG.debug("Fixed: " + entry[j]);
                }

                //todo: check field type
//                String type = ""+this.getType(""+(j+1));
                if ("date".equalsIgnoreCase(this.getType("" + (j + 1))) && !entry[j].isEmpty()) {
                    try {
//                        LOG.debug("Source date is "+entry[j]);

                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy");
                        LocalDateTime ldt = LocalDate.parse(entry[j], dtf).atStartOfDay();
//                        LocalDateTime ld = LocalDateTime.parse(entry[j]+" 11:00:00", dtf);
                        Instant instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
//                        LOG.debug("Transformed date is "+instant.toString());
                        row.put(this.getPath("" + (j + 1)), instant.toString());
                    } catch (DateTimeParseException e) {
//                        LOG.error("Parse exception: "+e.getLocalizedMessage());
                        row.put(this.getPath("" + (j + 1)), "");
                    }
                } else {
                    row.put(this.getPath("" + (j + 1)), entry[j]);
                }
            }
            data.add(row);
            //process entry
        }
    }

    public void setSource(String source) throws IOException {
        ((CSVSchemaJAXB) schema).setSource(source);
        this.parseSourceFile();
    }

    public void setEncoding(String encoding) {
        ((CSVSchemaJAXB) schema).setEncoding(encoding);
    }

    @Override
    protected HashMap<String, String> getRow(String identifierName, String identifierValue, boolean applyTransform) {
        for (HashMap<String, String> row : data) {
            if (applyTransform) row = this.transform(row);
            if (row.containsKey("#" + identifierName) && row.get("#" + identifierName).equalsIgnoreCase(identifierValue)) {
                return row;
            }
        }
        return new HashMap<>();
    }

    @Override
    protected ArrayList<HashMap<String, String>> getDataset(String transactionId, String className) {
        return data;
    }

    @Override
    protected String getPath(String key) {
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.getKey().equalsIgnoreCase(key)) {
                if (field.isIdentifier()) {
                    String ident = field.getId();
                    if (!field.getScope().equalsIgnoreCase("global")) ident = this.systemId + ":" + ident;
                    return "#" + ident;
                } else {
                    return field.getPath();
                }
            }
        }
        return null;
    }

    protected String getType(String key) {
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.getKey().equalsIgnoreCase(key)) {
                if (field.isIdentifier()) return null;
                return field.getType();
            }
        }
        return null;
    }


    @Override
    public boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        return false;
    }

    @Override
    public HashMap<String, String> insertData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        return null;
    }

    @Override
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue) {
        String position = "";
//        for (String identifier : identifiers.keySet()) {
//            if (identifier.equalsIgnoreCase(identifierName)) position = identifiers.get(identifierName);
//        }
        return this.getRow(identifierName, identifierValue, true) != null;
    }

    @Override
    public List<DataClass> getSchema() {
        LOG.info("Extracting schema");
        return this.getClassSchema(((CSVSchemaJAXB) schema).getDataClass());
    }
}
