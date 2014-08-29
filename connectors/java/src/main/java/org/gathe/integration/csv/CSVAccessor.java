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
 * CSV Accessor for CSV-based data source (read-only)
 *
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

    /**
     * CSV parser
     * @param systemId System identifier
     * @param schemaName Schema filepath
     * @param separator CSV field separator
     * @param quote CSV quote character
     * @throws IOException
     */
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

    /**
     * Parse data
     * @throws IOException
     */
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
                if ("date".equalsIgnoreCase(this.getType("" + (j + 1))) && !entry[j].isEmpty()) {
                    try {
                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy");
                        LocalDateTime ldt = LocalDate.parse(entry[j], dtf).atStartOfDay();
                        Instant instant = ldt.atZone(ZoneId.systemDefault()).toInstant();
                        row.put(this.getPath("" + (j + 1)), instant.toString());
                    } catch (DateTimeParseException e) {
                        row.put(this.getPath("" + (j + 1)), "");
                    }
                } else {
                    row.put(this.getPath("" + (j + 1)), entry[j]);
                }
            }
            data.add(row);
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

    /**
     * Get path (by key name)
     * @param key Key name
     * @return Path
     */
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

    /**
     * Get field type (by key name)
     * @param key Key name
     * @return Field type
     */
    protected String getType(String key) {
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.getKey().equalsIgnoreCase(key)) {
                if (field.isIdentifier()) return null;
                return field.getType();
            }
        }
        return null;
    }

    /**
     * Empty implementation for data updating method
     * @param className       class name (ignored)
     * @param identifierName  identifier name (ignored)
     * @param identifierValue identifier value (ignored)
     * @param newData         data (ignored)
     * @return                always false
     */
    @Override
    public boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        return false;
    }

    /**
     * Empty implementation for data insertion method
     * @param className       class name (ignored)
     * @param identifierName  identifier name (ignored)
     * @param identifierValue identifier value (ignored)
     * @param newData         data (ignored)
     * @return                always null
     */
    @Override
    public HashMap<String, String> insertData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        return null;
    }

    /**
     * Check entry presence by identifier
     * @param transactionId Quant transaction Id
     * @param className Data class
     * @param identifierName Identifier
     * @param identifierValue Identifier value
     * @return true if presence
     */
    @Override
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue) {
        String position = "";
        return this.getRow(identifierName, identifierValue, true) != null;
    }

    @Override
    public List<DataClass> getSchema() {
        LOG.info("Extracting schema...");
        return this.getClassSchema(((CSVSchemaJAXB) schema).getDataClass());
    }
}
