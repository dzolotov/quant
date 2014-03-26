package org.gathe.integration.db;

import org.gathe.integration.AccessorField;
import org.gathe.integration.DSBindingDatabase;
import org.gathe.integration.DataClass;
import org.gathe.integration.DatasetAccessor;

import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by dmitrii on 25.03.14.
 */
public class DBAccessor extends DatasetAccessor {

    private Connection connection;

    private String schemaName;
    private DataSource ds;
//    ArrayList<HashMap<String, String>> data = new ArrayList<>();

    private void getSourceData() throws IOException {
        if (((DBSchemaJAXB) (this.schema)).getSource() == null) throw new FileNotFoundException();

        //connect to source
        String source = ((DBSchemaJAXB) this.schema).getSource();
        LOG.info("Connecting to database: " + source);

        connection = null;
        try {
//            Context ctx = new InitialContext();
//            DataSource ds = (DataSource) ctx.lookup(source);
            this.connection = ds.getConnection();
            this.connection.createStatement().executeUpdate("SET NAMES 'utf8'");
        } catch (SQLException ne) {
            ne.printStackTrace();
        }
        //todo: parse data
    }

    public void setSource(String source) throws IOException {
        ((DBSchemaJAXB) schema).setSource(source);
        this.getSourceData();
    }

    public DBAccessor(String systemId, String schemaName, DataSource ds) {
        super("DB", systemId);
        this.ds = ds;
        this.schemaName = schemaName;
        JAXBContext jc = null;
        try {
            jc = JAXBContext.newInstance(DBSchemaJAXB.class);
            Unmarshaller u = jc.createUnmarshaller();
            schema = (DBSchemaJAXB) u.unmarshal(new FileReader(this.schemaName));
            if (((DBSchemaJAXB) schema).getSource() != null) getSourceData();
            System.out.println(schema.getSchemaFields().size());
            bindingDB = DSBindingDatabase.getDatabase("DB", ((DBSchemaJAXB) schema).getDataClass());

        } catch (JAXBException | IOException e) {
            e.printStackTrace();
        }

        for (AccessorField field : schema.getSchemaFields()) {
            LOG.debug("Schema entry: " + field);
        }

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
        }
        //todo: resolve external identifier value
        return null;
    }

    @Override
    protected HashMap<String, String> getRow(String identifierName, String identifierValue, boolean applyTransform) {
        LOG.debug("Getting row for " + identifierName + "=" + identifierValue);
        String tableName = ((DBSchemaJAXB) schema).getTable();
        String fieldName = this.getIdentifierField(identifierName);
        if (fieldName == null) return null;
        try {
            String q = "SELECT * FROM " + tableName + " WHERE " + fieldName + "=?";
            LOG.debug(q);
            PreparedStatement ps = connection.prepareStatement(q);
            ps.setString(1, identifierValue);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) return null;
            HashMap<String, String> result = new HashMap<>();

            for (AccessorField field : schema.getSchemaFields()) {
                String name = ((DBFieldJAXB) field).getName();
                String path = this.getPath(name);
                LOG.debug("Path: " + path + " data: '" + rs.getString(name) + "'");
                result.put(path, rs.getString(name));
            }
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
            PreparedStatement ps = connection.prepareStatement("SELECT * FROM " + tableName);
            ResultSet rs = ps.executeQuery();
            ArrayList<HashMap<String, String>> result = new ArrayList<>();

            while (rs.next()) {
                HashMap<String, String> row = new HashMap<>();

                for (AccessorField field : schema.getSchemaFields()) {
                    String name = ((DBFieldJAXB) field).getName();
                    String path = this.getPath(name);
//        		    LOG.debug("Extracting field: "+name+" path: "+path+" value: "+rs.getString(name));
                    row.put(path, rs.getString(name));
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

    @Override
    public boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> newData) {
        LOG.debug("Updating data in database (class: " + className + ") for identifier:" + identifierName + "=" + identifierValue);

        String tableName = ((DBSchemaJAXB) schema).getTable();

        for (String key : newData.keySet()) {
            LOG.debug("=== " + key + " = " + newData.get(key));
        }
        LOG.debug("=================================");
        identifierName = identifierName.substring(identifierName.indexOf(":") + 1);

        ArrayList<String> values = new ArrayList<>();
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
                    names.add(((DBFieldJAXB) field).getName());
                    values.add(newData.get(fieldPath));
                }
            }
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
            PreparedStatement ps = this.connection.prepareStatement(query);
            int index = 1;
            for (String value : values) {
                ps.setString(index, value);
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

        ArrayList<String> values = new ArrayList<>();
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
                    names.add(((DBFieldJAXB) field).getName());
                    values.add(newData.get(fieldPath));
                }
            }
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
            query += "?";
        }
        query += ")";
        LOG.debug("Query: " + query);
        HashMap<String, String> keyResult = new HashMap<>();
        try {
            PreparedStatement ps = this.connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            int index = 1;
            for (String value : values) {
                ps.setString(index, value);
                index++;
            }
            ps.executeUpdate();
            ResultSet keys = ps.getGeneratedKeys();

            String[] idNames = identifierName.split(",");
            List<String> idList = new ArrayList<>(Arrays.asList(idNames));

            while (keys.next()) {
                ResultSetMetaData rsMetaData = keys.getMetaData();
                int columnCount = rsMetaData.getColumnCount();

//                for (int i = 1; i <= columnCount; i++) {
                String key = keys.getString(1);
                keyResult.put(this.systemId + ":" + identifierName, key);
//                    LOG.debug("Key found: "+i+": "+rsMetaData.getColumnName(i));
//                    String keyName = rsMetaData.getColumnName(i);
//                    String key = keys.getString(i);
//                    if (idList.contains(keyName)) {
//                        LOG.debug("Associate key "+keyName+" with "+key);
//                        keyResult.put(keyName, key);
//                    }
//                    System.out.println("key " + i + " is " + key);
//                }
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
//        for (String identifier : identifiers.keySet()) {
//            if (identifier.equalsIgnoreCase(identifierName)) position = identifiers.get(identifierName);
//        }

        String tableName = ((DBSchemaJAXB) schema).getTable();
        String fieldName = this.getIdentifierField(identifierName);
        try {
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
        LOG.info("Extracting schema");
        return this.getClassSchema(((DBSchemaJAXB) schema).getDataClass());
    }

}
