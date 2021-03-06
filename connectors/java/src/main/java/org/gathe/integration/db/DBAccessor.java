package org.gathe.integration.db;

import org.gathe.integration.AccessorField;
import org.gathe.integration.DSBindingDatabase;
import org.gathe.integration.DataClass;
import org.gathe.integration.DatasetAccessor;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
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

public class DBAccessor extends DatasetAccessor {

    private String schemaName;
    private PooledConnectionWrapper ds;
    private Connection connection = null;
//    ArrayList<HashMap<String, String>> data = new ArrayList<>();

    public DBAccessor(String systemId, String schemaName, PooledConnectionWrapper ds) {
        super("DS", systemId);
        this.ds = ds;
        this.schemaName = schemaName;
        JAXBContext jc = null;
        try {
            jc = JAXBContext.newInstance(DBSchemaJAXB.class);
            Unmarshaller u = jc.createUnmarshaller();
            schema = (DBSchemaJAXB) u.unmarshal(new FileReader(this.schemaName));
            getConnection();
            System.out.println(schema.getSchemaFields().size());
            bindingDB = DSBindingDatabase.getDatabase("DS", ((DBSchemaJAXB) schema).getDataClass());

        } catch (JAXBException | IOException e) {
            e.printStackTrace();
        }

        for (AccessorField field : schema.getSchemaFields()) {
            LOG.debug("Schema entry: " + field);
        }

    }

    private Connection getConnection() {
        try {
            if (connection != null && !connection.isClosed()) return connection;
        } catch (Exception e) {
            try {
                LOG.info("Reconnecting...");
                connection = ds.getPooledConnection().getConnection();
                return connection;
            } catch (SQLException e3) {
                LOG.error("Exception when connecting: " + e3.getMessage());
            }
        }
        try {
            connection = ds.getPooledConnection().getConnection();
            return connection;
        } catch (Exception e2) {
            LOG.error("Exception when connecting: " + e2.getMessage());
            return null;
        }
    }

    public void setSource(String source) throws IOException {
        ((DBSchemaJAXB) schema).setSource(source);
        getConnection();
    }

    private String getIdentifierField(String identifierName) {
        if (identifierName.indexOf(":") > 0) {
            identifierName = identifierName.substring(identifierName.indexOf(":") + 1);
            LOG.debug("Scanning for identifier: " + identifierName);
            List<AccessorField> fields = schema.getSchemaFields();
            for (AccessorField field : fields) {
                LOG.debug("Scanning field: " + field);
                if (!field.isIdentifier()) continue;
                if (field.getId().equalsIgnoreCase(identifierName)) {
                    return ((DBFieldJAXB) field).getName();
                }
            }
        } else {
            //todo: resolve external identifier value
            for (AccessorField field : schema.getSchemaFields()) {
                if (field.isIdentifier() && field.getScope().equalsIgnoreCase("global") && field.getId().equalsIgnoreCase(identifierName))
                    return field.getKey();
            }
        }
        return null;
    }

    @Override
    protected HashMap<String, String> getRow(String identifierName, String identifierValue, boolean applyTransform) {
        LOG.debug("Getting row for " + identifierName + "=" + identifierValue);

        identifierValue = this.transformIdentifier(identifierName, identifierValue);

        String tableName = ((DBSchemaJAXB) schema).getTable();
        String fieldName = this.getIdentifierField(identifierName);
        LOG.debug("Field name: " + fieldName);
        if (fieldName == null) return null;
        try {

            String names = "";
            for (AccessorField field : schema.getSchemaFields()) {
                String expression = ((DBFieldJAXB) field).getExpression();
                if (field.isIdentifier()) continue;
                String name = ((DBFieldJAXB) field).getName();
                if (!names.isEmpty()) names += ",";
                if (expression == null && name.indexOf(".") >= 0) name = name + " AS " + name.replace(".", "_");
                if (expression != null) name = expression + " AS " + name;
                names += name;
            }

            String q = "SELECT " + tableName + "." + fieldName + "," + names + " FROM " + tableName;

            for (DBJoinJAXB dbjoin : ((DBSchemaJAXB) schema).getJoin()) {
                if ("left".equalsIgnoreCase(dbjoin.getType())) q += " LEFT";
                String joinFrom = dbjoin.getFrom();
                if (joinFrom.indexOf(".") < 0) joinFrom = tableName + "." + joinFrom;
                q += " JOIN " + dbjoin.getWith() + " ON " + joinFrom + "=" + dbjoin.getWith() + "." + dbjoin.getTo() + "";
            }
            q += " WHERE " + tableName + "." + fieldName + "=?";

            LOG.debug(q);
            Connection connection = this.getConnection();
            PreparedStatement ps = connection.prepareStatement(q);
            ps.setString(1, identifierValue);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;

            HashMap<String, String> result = new HashMap<>();

            for (AccessorField field : schema.getSchemaFields()) {

                String name = ((DBFieldJAXB) field).getName();
                String path = this.getPath(name);
                name = name.replace("[", "");
                name = name.replace("]", "");
                name = name.replace("`", "");
                name = name.replace("\"", "");

                name = name.replace(".", "_");
                if (name.startsWith("@")) {
                    String value = ((DBFieldJAXB) field).getValue();
                    if (value == null) continue;
                    result.put(path, value);
                    continue;
                }
                LOG.debug("Path: " + path + " data: '" + rs.getString(name) + "'");

                //todo: check field type
                String fieldType = field.getType();
                String value = "";
                if (fieldType.equalsIgnoreCase("date")) {
                    if (rs.getDate(name) == null) {
                        value = null;
                    } else {
                        LocalDate ld = rs.getDate(name).toLocalDate();
                        LocalDateTime ldt = ld.atStartOfDay();
                        value = ldt.atZone(ZoneId.systemDefault()).toInstant().toString();
                    }
                } else {
                    value = rs.getString(name);
                }

                if (value != null) result.put(path, value);
            }
            LOG.debug("Applying transformation " + applyTransform);
            if (applyTransform) result = this.transform(result);
            return result;

        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    @Override
    protected ArrayList<HashMap<String, String>> getDataset(String transactionId, String className) {
        String tableName = ((DBSchemaJAXB) schema).getTable();
        try {
            String activeField = ((DBSchemaJAXB) schema).getActive();
            String q = "SELECT * FROM " + tableName;
            for (DBJoinJAXB dbjoin : ((DBSchemaJAXB) schema).getJoin()) {
                q += " JOIN " + dbjoin.getWith() + " ON " + tableName + "." + dbjoin.getFrom() + "=" + dbjoin.getWith() + "." + dbjoin.getTo() + "";
            }
            q += " ORDER BY " + tableName + ".ID";
            LOG.debug("*************************************** " + q);

            Connection connection = this.getConnection();
            PreparedStatement ps = connection.prepareStatement(q);
            //+(activeField!=null?" WHERE `"+activeField+"` IS NOT NULL AND `"+activeField+"`<>0 AND `"+activeField+"`<>''":""));
            ResultSet rs = ps.executeQuery();
            ArrayList<HashMap<String, String>> result = new ArrayList<>();

            while (rs.next()) {
                HashMap<String, String> row = new HashMap<>();

                for (AccessorField field : schema.getSchemaFields()) {
                    String name = ((DBFieldJAXB) field).getName();
                    String path = this.getPath(name);

                    name = name.replace("[", "");
                    name = name.replace("]", "");
                    name = name.replace("`", "");
                    name = name.replace("\"", "");

                    if (name.startsWith("@")) {
                        String value = ((DBFieldJAXB) field).getValue();
                        if (value == null) continue;
                        row.put(path, value);
                        continue;
                    }

                    String fieldType = field.getType();
                    String value = "";
                    if (fieldType.equalsIgnoreCase("date")) {
                        LOG.debug("Parsing " + rs.getDate(name));
                        if (rs.getDate(name) == null) {
                            value = null;
                        } else {
                            LocalDate ld = rs.getDate(name).toLocalDate();
                            LocalDateTime ldt = ld.atStartOfDay();
                            value = ldt.atZone(ZoneId.systemDefault()).toInstant().toString();
                        }
                    } else {
                        value = rs.getString(name);
                    }

                    if (value != null) row.put(path, value);
                }
                result.add(row);
            }
            return result;

        } catch (SQLException se) {
            se.printStackTrace();
        }
        return null;
    }

    @Override
    protected String getPath(String key) {
        for (AccessorField field : schema.getSchemaFields()) {
//	    LOG.debug("Getting path for "+key);
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

    public String transformIdentifier(String identifierName, String identifierValue) {
        for (AccessorField field : schema.getSchemaFields()) {
            LOG.debug("Field: " + field.getKey());
            if (field.isIdentifier()) {
                LOG.debug("Scanning identifier: " + field.getId());
                if (field.getId().equalsIgnoreCase(identifierName)) {
                    LOG.debug("Reverse replace from " + identifierValue);
                    identifierValue = this.reverseReplace(field, identifierValue);
                    LOG.debug("New value is " + identifierValue);
                    break;
                }
            }
        }
        return identifierValue;
    }

    @Override
    public boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        LOG.debug("Updating data in database (class: " + className + ") for identifier:" + identifierName + "=" + identifierValue);

        identifierValue = this.transformIdentifier(identifierName, identifierValue);

        String tableName = ((DBSchemaJAXB) schema).getTable();

        for (String key : newData.keySet()) {
            LOG.debug("=== " + key + " = " + newData.get(key));
        }
        LOG.debug("=================================");
        identifierName = identifierName.substring(identifierName.indexOf(":") + 1);

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();

        String identifierField = "";

        for (String fieldPath : newData.keySet()) {
            for (AccessorField field : schema.getSchemaFields()) {


                if (field.isIdentifier() && field.getId().equalsIgnoreCase(identifierName)) {
                    identifierField = ((DBFieldJAXB) field).getName();
                }

                String key = field.getKey();
                String path = this.getPath(key);
                if (fieldPath.equalsIgnoreCase(path)) {
                    String fieldValue = newData.get(fieldPath);
                    if (field.getType().equalsIgnoreCase("date")) {
                        names.add(((DBFieldJAXB) field).getName());
                        if (fieldValue.isEmpty()) {
                            values.add("null");
                        } else {
                            values.add(Instant.parse(fieldValue));
                        }
                    } else {
                        names.add(((DBFieldJAXB) field).getName());
                        values.add(fieldValue);
                    }
                }
            }
        }

        String activeField = ((DBSchemaJAXB) schema).getActive();
        if (activeField != null) {
            names.add(activeField);
            values.add("1");
        }

        String query = "UPDATE " + tableName + " SET ";
        boolean first = true;
        for (String name : names) {
            if (!first) query += ",";
            first = false;
            query += (name + "=?");
        }
        query += " WHERE " + identifierField + "=?";
        LOG.debug("Query: " + query);

        try {
            Connection connection = this.getConnection();
            PreparedStatement ps = connection.prepareStatement(query);
            int index = 1;
            for (Object value : values) {
                if (value == null) {
                    ps.setObject(index, value);
                } else if (value instanceof Instant) {
                    Instant instant = (Instant) value;
                    java.util.Date date = Date.from(instant);
                    ps.setDate(index, new java.sql.Date(date.getTime()));
                } else {
                    ps.setString(index, "" + value);
                }
                index++;
            }
            ps.setString(index, identifierValue);

            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            LOG.error("Error when updating: " + e.getLocalizedMessage());
            return false;
        }
    }

    @Override
    public HashMap<String, String> insertData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        //todo

        identifierValue = this.transformIdentifier(identifierName, identifierValue);

        LOG.debug("Inserting data into database (class: " + className + ") for identifier:" + identifierName + "=" + identifierValue);
        for (String key : newData.keySet()) {
            LOG.debug("=== " + key + " = " + newData.get(key));
        }
        LOG.debug("=================================");

        String tableName = ((DBSchemaJAXB) schema).getTable();

        for (String key : newData.keySet()) {
            LOG.debug("=== " + key + " = " + newData.get(key));
        }

        LOG.debug("=================================");
        identifierName = identifierName.substring(identifierName.indexOf(":") + 1);

        ArrayList<Object> values = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();

        String identifierField = "";

        for (String fieldPath : newData.keySet()) {
            for (AccessorField field : schema.getSchemaFields()) {

                if (field.isIdentifier() && field.getId().equalsIgnoreCase(identifierName)) {
                    identifierField = ((DBFieldJAXB) field).getName();
                }

                String key = field.getKey();
                String path = this.getPath(key);
                if (fieldPath.equalsIgnoreCase(path)) {

                    String fieldValue = newData.get(fieldPath);

                    if (field.getType().equalsIgnoreCase("date")) {
                        names.add(((DBFieldJAXB) field).getName());
                        if (fieldValue.isEmpty()) {
                            values.add("null");
                        } else {
                            values.add(Instant.parse(fieldValue));
                        }
                    } else {
                        names.add(((DBFieldJAXB) field).getName());
                        values.add(fieldValue);
                    }

                }
            }
        }

        String activeField = ((DBSchemaJAXB) schema).getActive();
        if (activeField != null) {
            names.add(activeField);
            values.add("1");
        }

        //register identifier!
        if (identifierValue != null) {
            names.add(identifierField);
            values.add(identifierValue);
        }

        String query = "INSERT INTO " + tableName + " (";
        boolean first = true;
        for (String name : names) {
            if (!first) query += ",";
            first = false;
            query += name;
        }

        query += ") VALUES (";
        first = true;
        for (int i = 0; i < names.size(); i++) {
            if (!first) query += ",";
            first = false;
            query += "?";
        }
        query += ")";
        LOG.debug("Query: " + query);
        HashMap<String, String> keyResult = new HashMap<>();
        try {
            Connection connection = this.getConnection();
            PreparedStatement ps = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            int index = 1;
            for (Object value : values) {
                if (value == null) {
                    ps.setObject(index, value);
                } else if (value instanceof Instant) {
                    //todo
                    Instant instant = (Instant) value;
//                    LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                    java.util.Date date = Date.from(instant);
                    ps.setDate(index, new java.sql.Date(date.getTime()));
                    //set date
                } else {
                    ps.setString(index, "" + value);
                }
                index++;
            }
            ps.executeUpdate();
            ResultSet keys = ps.getGeneratedKeys();
            LOG.debug("IDNames: " + identifierName);

            String[] idNames = identifierName.split(",");
            List<String> idList = new ArrayList<>(Arrays.asList(idNames));

            while (keys.next()) {
                ResultSetMetaData rsMetaData = keys.getMetaData();
                int columnCount = rsMetaData.getColumnCount();

                String key = keys.getString(1);
                LOG.debug("Key: " + key + " identifier: " + identifierName);
                keyResult.put(this.systemId + ":" + identifierName, key);
            }
            return keyResult;
        } catch (SQLException se) {
            LOG.error("Error when inserting: " + se.getLocalizedMessage());
        }
        return null;
    }

    @Override
    public boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue) {
        String position = "";
        identifierValue = this.transformIdentifier(identifierName, identifierValue);

        String tableName = ((DBSchemaJAXB) schema).getTable();
        String fieldName = this.getIdentifierField(identifierName);
        try {
            Connection connection = this.getConnection();
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + tableName + " WHERE " + fieldName + "=?");
            ps.setString(1, identifierValue);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public List<DataClass> getSchema() {
        LOG.info("Extracting schema...");
        return this.getClassSchema(((DBSchemaJAXB) schema).getDataClass());
    }

    @Override
    protected void updateUuid(String identifierName, String identifierValue, String uuidValue) {
        String uuid = (((DBSchemaJAXB) schema).getUuid());
        if (uuid != null) {
            Connection connection = this.getConnection();
            String tableName = ((DBSchemaJAXB) schema).getTable();
            String fieldName = this.getIdentifierField(identifierName);
            try {
                PreparedStatement ps = connection.prepareStatement("UPDATE " + tableName + " SET " + uuid + "=? WHERE " + fieldName + "=?");
                ps.setString(1, uuidValue);
                ps.setString(2, identifierValue);
                ps.executeUpdate();
            } catch (SQLException se) {
                LOG.error("Error when updating uuid!");
            }

        }
    }
}
