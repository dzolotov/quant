package org.gathe.integration;

import org.apache.log4j.Logger;

import javax.jms.JMSException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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

public abstract class DatasetAccessor extends BaseAccessor {
    private static String digits = "0123456789abcdef";
    protected AccessorSchema schema;
    protected Logger LOG = Logger.getLogger(this.getClass());
    protected String systemId;
    protected Connection bindingDB;
    protected String bindingPrefix;
    protected HashMap<String, String> identifiers = new HashMap<>();

    public DatasetAccessor(String bindingPrefix, String systemId) {
        this.bindingPrefix = bindingPrefix;
        this.systemId = systemId;
    }

    public static String toHex(byte[] data) {
        StringBuffer buf = new StringBuffer();

        for (int i = 0; i != data.length; i++) {
            int v = data[i] & 0xff;

            buf.append(digits.charAt(v >> 4));
            buf.append(digits.charAt(v & 0xf));
        }

        return buf.toString();
    }

    /**
     * Calculate hash for dataset row
     *
     * @param row Dataset Row
     * @return Hash String (sha1)
     */
    protected String getHash(HashMap<String, String> row) {
        String hashRow = "";
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.isIdentifier()) continue; //skip identifiers
            if (!hashRow.isEmpty()) hashRow += "~";
            hashRow += row.get(field.getPath());
        }

        String hashResult = "";

        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            hashResult = toHex(md.digest(hashRow.getBytes("utf-8")));


        } catch (NoSuchAlgorithmException | UnsupportedEncodingException nsae) {
            LOG.error("Hash generation error: " + nsae.getLocalizedMessage());
        }
        return hashResult;
    }


    public abstract List<DataClass> getSchema();

    public List<DataClass> getClassSchema(String dataClass) {

        DataClass dc = new DataClass(dataClass);
        dc.setMatchable(true);
        for (AccessorField field : schema.getSchemaFields()) {
            if (field.isIdentifier()) {
                String identifier = field.getId();
                if (!field.getScope().equalsIgnoreCase("global")) identifier = this.systemId + ":" + identifier;
                dc.addCheck(identifier);
                dc.addIdentifier(identifier);
                identifiers.put(identifier, field.getKey());
            } else {
                DataElement de = new DataElement(field.getPath(), field.getDescription());
                dc.addElement(de);
            }
        }

        List<DataClass> schemaEntries = new ArrayList<>();
        schemaEntries.add(dc);
        return schemaEntries;
    }

    protected abstract HashMap<String, String> getRow(String identifierName, String identifierValue, boolean applyTransform);

    protected abstract ArrayList<HashMap<String, String>> getDataset(String transactionId, String className);

    /**
     * Get Hash by UUID value
     *
     * @param transactionId transaction identifier
     * @param className     className
     * @param uuid          uuid
     * @return Hash String (sha1)
     */
    private String getHashByUuid(String transactionId, String className, String uuid) {
        String[] idents = identifiers.keySet().toArray(new String[0]);
        String identifierName = idents[0];
        String id = this.getIdentifierByUuid(transactionId, className, identifierName, uuid);
        HashMap<String, String> row = this.getRow(identifierName, id, false);
        if (row.isEmpty()) return null;
        else return this.getHash(row);
//        for (int i = 0; i < data.size(); i++) {
//            HashMap<String, String> row = data.get(i);
//            if (row.containsKey("#" + identifierName) && row.get("#" + identifierName).equalsIgnoreCase(id)) {
//                return this.getHash(row);
//            }
//        }
//        return null;
    }

    /**
     * Check for row modification
     *
     * @param transactionId transaction identifier
     * @param className     classname
     * @param uuid          uuid
     * @return true if modified
     */

    public boolean isModified(String transactionId, String className, String uuid) {
        String actualHash = getHashByUuid(transactionId, className, uuid);
        try {
            PreparedStatement ps = bindingDB.prepareStatement("SELECT hash FROM " + this.bindingPrefix + "_" + className + " WHERE uuid=? AND disabled=0 AND source=?");
            ps.setString(1, uuid);
            ps.setString(2, this.systemId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return (!rs.getString("hash").equalsIgnoreCase(actualHash));
            } else {
                return true;    //no record
            }
        } catch (SQLException e) {
            LOG.error("SQL Exception: " + e.getLocalizedMessage());
        }
        return false;
    }

    private String getKeyByPath(String path) {
        for (AccessorField field : schema.getSchemaFields()) {
            if (this.getPath(field.getKey()).equalsIgnoreCase(path)) return field.getKey();
        }
        return null;
    }

    /**
     * Apply transformation rules
     *
     * @param row datarow
     * @return transformed datarow
     */
    public HashMap<String, String> transform(HashMap<String, String> row) {

        for (String path : row.keySet()) {
            List<AccessorField> fields = schema.getSchemaFields();
            for (AccessorField field : fields) {

                boolean matchedField = false;
                if (field.isIdentifier()) {
                    matchedField = (field.getId().equalsIgnoreCase(path.substring(1)));
                } else {
                    matchedField = (field.getPath().equalsIgnoreCase(path));
                }

                if (matchedField) {
                    List<ReplaceJAXB> replaces = field.getReplaces();
                    if (replaces != null) {
                        for (ReplaceJAXB replaceRule : replaces) {
                            if (replaceRule.getFrom().trim().equalsIgnoreCase(row.get(path).trim())) {
                                row.put(path, replaceRule.getTo().trim());
                                break;
                            }
                        }
                    }
                    List<AppendJAXB> appends = field.getAppends();
                    if (appends != null) {
                        for (AppendJAXB appendRule : appends) {
                            LOG.debug("Applying append rule: " + appendRule);
                            //check for attributes

                            if (appendRule.getPath() != null) {
                                LOG.debug("APPENDING RULE for " + appendRule.getPath() + " old value: " + row.get(path) + " add value: " + row.get(appendRule.getPath()));
                                row.put(path, row.get(path) + row.get(appendRule.getPath()));

                            } else if (appendRule.getValue() != null) {
                                row.put(path, row.get(path) + appendRule.getValue());
                            }
                        }
                    }
                }
            }
        }

        for (String path : row.keySet()) {
            if (path.startsWith("@")) row.remove(row.get(path));
        }

        return row;
    }


    /**
     * Count matching records
     */

    @Override
    public String countMatches(String transactionId, String className, HashMap<String, String> filters) {
        String[] idents = identifiers.keySet().toArray(new String[0]);
        String identifierName = idents[0];
        LOG.debug("Identifier: " + identifierName);
        ArrayList<String> uuids = new ArrayList<>();
        ArrayList<HashMap<String, String>> data = this.getDataset(transactionId, className);
        //todo: apply filters
        int count = data.size();
        int unbinded = 0;
        for (int i = 0; i < data.size(); i++) {
            HashMap<String, String> row = transform(data.get(i));
            String identifierValue = row.get("#" + identifierName);
            try {
                PreparedStatement ps = bindingDB.prepareStatement("SELECT * FROM " + this.bindingPrefix + "_" + className + " WHERE name=? AND id=? AND disabled=0 AND source=?");
                ps.setString(1, identifierName);
                ps.setString(2, identifierValue);
                ps.setString(3, systemId);
                if (!ps.executeQuery().next()) {
                    LOG.debug("Found unbinded record");
                    unbinded++;
                }
            } catch (SQLException e) {
                LOG.error("Error when seeking: " + e.getLocalizedMessage());
            }
        }
        return count + "/" + unbinded;
    }

    /**
     * Scanning for matched entries
     *
     * @param transactionId transaction identifier
     * @param className     classname
     * @param filters       filters set
     * @return comma separated uuid list
     */
    @Override
    public String[] match(String transactionId, String className, HashMap<String, String> filters, boolean explain) {

        //search for all entries
        //data[identifier] -> uuid
        String[] idents = identifiers.keySet().toArray(new String[0]);
        String identifierName = idents[0];
        LOG.debug("Identifier: " + identifierName);
        ArrayList<String> uuids = new ArrayList<>();
        ArrayList<HashMap<String, String>> data = this.getDataset(transactionId, className);
        for (int i = 0; i < data.size(); i++) {
            HashMap<String, String> row = transform(data.get(i));
            String uuid = getUuidByIdentifier(transactionId, className, identifierName, row.get("#" + identifierName), true);
            uuids.add(uuid);
        }
        return uuids.toArray(new String[0]);
    }

    /**
     * Get path (or identifier) by key
     *
     * @param key
     * @return Path (identifier)
     */
    protected abstract String getPath(String key);

    /**
     * Compare records
     *
     * @param datasetRow data row
     * @param xmlData    update helper object
     * @return true if identical
     */

    public boolean equals(HashMap<String, String> datasetRow, HashMap<String, String> xmlData) {
        for (String rowKey : datasetRow.keySet()) {
            if (rowKey.startsWith("#")) continue;          //skip identifiers
            String value = "";
            if (!xmlData.containsKey(rowKey)) continue;         //skip

            boolean skipable = false;
            for (AccessorField f : schema.getSchemaFields()) {
                if (this.getPath(f.getKey()).equalsIgnoreCase(rowKey)) {
                    if (!f.getMatchIgnore().equalsIgnoreCase("false")) skipable = true;
                }
            }
            if (skipable) continue;
            //todo: null conventions

            if (datasetRow.get(rowKey) == null) {
                String defaultValue = null;
                boolean found = false;
                for (AccessorField f : schema.getSchemaFields()) {
                    if (this.getPath(f.getKey()).equalsIgnoreCase(rowKey)) {
                        if (f.getDefault() != null) defaultValue = f.getDefault();
                        found = true;
                    }
                }
                if (!found || defaultValue == null) continue;     //skip empty and not defaulted value
                value = defaultValue;
            } else {
                value = datasetRow.get(rowKey);
            }
            //todo: check scope
            if (!xmlData.get(rowKey).trim().equalsIgnoreCase(value.trim())) return false;
        }
        return true;
    }

    /**
     * Update data in real dataset
     *
     * @param className       class name
     * @param identifierName  identifier name
     * @param identifierValue identifier value
     * @param row             Helper class for row data
     * @return true if success
     */

    public abstract boolean updateData(String className, String identifierName, String identifierValue, HashMap<String, String> row);

    /**
     * Insert new data row to real dataset
     *
     * @param className       class name
     * @param identifierName  identifier name
     * @param identifierValue identifier value
     * @param row             Helper class for row data
     * @return Hashmap for new keys data
     */

    public abstract HashMap<String, String> insertData(String className, String identifierName, String identifierValue, HashMap<String, String> row);

    protected String reverseReplace(AccessorField field, String value) {
        if (field.getReplaces() == null || field.getReplaces().size() == 0) return value;
        LOG.debug("Reverse replace");
        List<ReplaceJAXB> replaces = field.getReplaces();
        for (ReplaceJAXB replace : replaces) {
            if (replace.getTo().equalsIgnoreCase(value)) {
                return replace.getFrom();
            }
        }
        return value;
    }


    private void fillDefaultValues(HashMap<String, String> newData, boolean update) {
        LOG.debug("Filling default values");
        //set default value (when update)
        for (AccessorField field : schema.getSchemaFields()) {
            String path = this.getPath(field.getKey());
            LOG.debug("For path: " + path + " newData: " + newData.containsKey(path) + " field:" + field.getDefault());

            String oldValue;
            String newValue;
            if (update && !newData.containsKey(path)) continue;
            if (!newData.containsKey(path) || newData.get(path).equals("\\N")) newValue = null;
            else newValue = newData.get(path);
            oldValue = newValue;
            String defaultValue = field.getDefault();
            if (newValue == null) {
                if (field.getNullBehavior().equalsIgnoreCase("DEFAULT")) newValue = defaultValue;
            } else if (newValue.isEmpty()) {
                LOG.debug("Value for " + path + " is empty");
                if (field.getEmptyBehavior().equalsIgnoreCase("DEFAULT")) newValue = defaultValue;
                else if (field.getEmptyBehavior().equalsIgnoreCase("NULL")) newValue = null;
                LOG.debug("New value is " + newValue);
            }

            if (!("" + newValue).equals("" + oldValue)) {
                newData.put(path, newValue);
            }
        }
    }

    /**
     * Update record!
     *
     * @param className
     * @param helper
     * @return
     */
    @Override
    public boolean update(String className, UpdateHelper helper) {
        String uuid = helper.getUuid();
        LOG.debug("DS Update: " + uuid);

        //preprocessing helper!
        HashMap<String, String> newData = helper.getPatch();
        for (String key : newData.keySet()) {
            LOG.debug("Old value for " + key + " is " + newData.get(key));
            for (AccessorField field : schema.getSchemaFields()) {
                String path = this.getPath(field.getKey());
                if (!path.equalsIgnoreCase(key)) continue;
                if (field.getRef() != null) {
                    String ref = field.getRef();
                    int classDelimiter = ref.indexOf(".");
                    String refClass = ref.substring(0, classDelimiter);
                    String refId = ref.substring(classDelimiter + 1);
                    LOG.debug("Resolving ref of " + refClass + " to " + refId);
                    try {
                        String resolvedId = connector.identify(helper.getTransactionId(), refClass, refId, newData.get(key), false, false);
                        newData.put(key, resolvedId);
                    } catch (JMSException e) {
                    }
                    ;
                } else {
                    String value = this.reverseReplace(field, newData.get(key));
                    newData.put(key, value);
                }
            }
            LOG.debug("Value for " + key + " is " + newData.get(key));
        }

        try {
            PreparedStatement ps = bindingDB.prepareStatement("SELECT * FROM " + this.bindingPrefix + "_" + className + " WHERE uuid=? AND disabled=0 AND source=?");
            ps.setString(1, uuid);
            ps.setString(2, systemId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String id = rs.getString("id");
                String identifier = rs.getString("name");

                LOG.debug("Binding found for " + identifier + ":" + id);

                if (this.checkByIdentifier(helper.getTransactionId(), className, identifier, id)) {
                    fillDefaultValues(newData, true);
                    return updateData(className, identifier, id, newData);           //update existing!
                } else {
                    fillDefaultValues(newData, false);
                    return (insertData(className, identifier, id, newData) != null);      //recreate!
                }
            }

            ArrayList<HashMap<String, String>> data = this.getDataset(helper.getTransactionId(), className);

            LOG.debug("*********************** COMPARING " + data.size() + " with " + newData);

            for (int i = 0; i < data.size(); i++) {
                HashMap<String, String> row = data.get(i);
                row = this.transform(row);
                //searching for full match
                if (this.equals(row, newData)) {

                    LOG.debug("Found full match!");
                    String[] idents = identifiers.keySet().toArray(new String[0]);
                    String hash = this.getHash(row);
                    //bind with all identifiers
                    for (String identifierName : idents) {
                        String id = row.get("#" + identifierName);
                        PreparedStatement uuidUpdate = this.bindingDB.prepareStatement("INSERT INTO " + this.bindingPrefix + "_" + className + " (uuid,name,id,disabled,actual,value,hash,source) VALUES (?,?,?,0,NOW(),'',?,?)");
                        uuidUpdate.setString(1, uuid);
                        uuidUpdate.setString(2, identifierName);
                        uuidUpdate.setString(3, id);
                        uuidUpdate.setString(4, hash);
                        uuidUpdate.setString(5, systemId);
                        uuidUpdate.executeUpdate();
                    }
                    String id0 = idents[0];
                    String val0 = row.get("#" + id0);
                    LOG.debug("ID: " + id0 + " val:" + val0);
                    fillDefaultValues(newData, true);
                    //if (this.checkByIdentifier(helper.getTransactionId(), className, identifier, id)) {
                    return updateData(className, id0, val0, newData);           //update existing!
                    //} else {
                    //   return (insertData(className, identifier, id, helper)!=null);      //rebind existing!
                    //}
                }
            }

            //add new record!
            LOG.debug("Adding new record");
            String newUuid = UUID.randomUUID().toString();
            String classIdentifiers = "";
            for (String ks : identifiers.keySet()) {
                if (!classIdentifiers.isEmpty()) classIdentifiers += ",";
                classIdentifiers += ks;
            }

            fillDefaultValues(newData, false);
            HashMap<String, String> keys = insertData(className, classIdentifiers, null, newData);
            if (keys == null) return false;
            //register bindings
            for (String key : keys.keySet()) {
                String value = keys.get(key);
                PreparedStatement uuidUpdate = this.bindingDB.prepareStatement("INSERT INTO " + this.bindingPrefix + "_" + className + " (uuid,name,id,disabled,actual,value,hash,source) VALUES (?,?,?,0,NOW(),'',?,?)");
                uuidUpdate.setString(1, uuid);
                uuidUpdate.setString(2, key);
                uuidUpdate.setString(3, value);
                uuidUpdate.setString(4, "");        //todo: hash
                uuidUpdate.setString(5, systemId);
                uuidUpdate.executeUpdate();
            }
            return true;
        } catch (SQLException e) {
            LOG.error("SQL Exception: " + e.getLocalizedMessage());
            return false;
        }
    }

    protected void updateUuid(String identifier, String identifierValue, String uuidvalue) {
    }

    /**
     * Translate local identifier to global uuid
     *
     * @param transactionId   transaction identifier
     * @param className       classname
     * @param identifierName  identifier name
     * @param identifierValue identifier value
     * @return Global UUID
     */
    @Override
    public String getUuidByIdentifier(String transactionId, String className, String identifierName, String identifierValue, boolean forcedCreation) {
        try {
            PreparedStatement st = this.bindingDB.prepareStatement("SELECT uuid FROM " + this.bindingPrefix + "_" + className + " WHERE id=? AND name=? AND disabled=0 AND source=?");
            LOG.info("Searching for " + identifierName + " = " + identifierValue);
            st.setString(1, identifierValue);
            st.setString(2, identifierName);
            st.setString(3, systemId);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                //record exists
                LOG.debug("Record exists");
                String uuidValue = rs.getString("uuid");
                LOG.debug(uuidValue);
                updateUuid(identifierName, identifierValue, uuidValue);
                return uuidValue;
            } else if (!forcedCreation) {
                LOG.debug("Search by indirect ids");
                //поиск совпадений с другими идентификаторами строки
                ArrayList<HashMap<String, String>> data = this.getDataset(transactionId, className);
                for (int i = 0; i < data.size(); i++) {
                    HashMap<String, String> row = transform(data.get(i));
                    if (!row.containsKey("#" + identifierName) || !row.get("#" + identifierName).equalsIgnoreCase(identifierValue))
                        continue;
                    LOG.debug("Found matched record");
                    for (String key : row.keySet()) {
                        if (!key.startsWith("#") || key.equalsIgnoreCase("#" + identifierName)) continue;
                        String name = key.substring(1);
                        String value = row.get(key);
                        try {
                            st = this.bindingDB.prepareStatement("SELECT uuid FROM " + this.bindingPrefix + "_" + className + " WHERE id=? AND name=? AND disabled=0 AND source=?");
                            st.setString(1, value);
                            st.setString(2, name);
                            st.setString(3, systemId);
                            rs = st.executeQuery();
                            if (rs.next()) {
                                LOG.debug("Found!!!");
                                String uuid = rs.getString("uuid");
                                PreparedStatement uuidUpdate = this.bindingDB.prepareStatement("INSERT INTO " + this.bindingPrefix + "_" + className + " (uuid,id,disabled,name,actual,value,source) VALUES (?,?,0,?,NOW(),'',?)");
                                uuidUpdate.setString(1, uuid);
                                uuidUpdate.setString(2, identifierValue);
                                uuidUpdate.setString(3, identifierName);
                                uuidUpdate.setString(4, systemId);
                                uuidUpdate.executeUpdate();
                                this.updateUuid(identifierName, identifierValue, uuid);
                                return uuid;
                            }
                        } catch (SQLException se) {
                            LOG.error("SQL Exception in unify: " + se.getLocalizedMessage());
                        }
                    }

                    //todo: choice scenario
                    //request ESB with global scope identifiers

                    if (!connector.isSelfRequest(transactionId, className)) {

                        LOG.debug("Getting row: " + identifierName + "=" + identifierValue);
                        row = this.getRow(identifierName, identifierValue, true);
                        LOG.debug("Row is " + row);
                        for (AccessorField field : schema.getSchemaFields()) {
                            if (field.isIdentifier() && field.getScope().equalsIgnoreCase("global")) {
                                LOG.info("Searching in outer world for " + field.getId());
                                String identifier = field.getId();
                                String uuidGlobal = null;
                                try {
                                    LOG.debug("Connector is " + connector);
                                    LOG.debug("Run signature: " + transactionId + "," + className + "," + identifier + "," + row.get("#" + identifier));
                                    uuidGlobal = connector.unify(transactionId, className, identifier, row.get("#" + identifier), false, false);
                                } catch (JMSException e) {
                                }
                                LOG.debug("Found in global scope: " + uuidGlobal);
                                if (uuidGlobal != null) {
                                    //object found in outer world!

                                    for (AccessorField identifierField : schema.getSchemaFields()) {

                                        if (identifierField.isIdentifier()) {
                                            String identifierNameOther = identifierField.getId();
                                            if (!"global".equalsIgnoreCase(identifierField.getScope())) {
                                                identifierNameOther = this.systemId + ':' + identifierNameOther;
                                            }

                                            String identifierFieldName = this.getPath(identifierField.getKey());
                                            String identifierFieldValue = row.get(identifierFieldName);
                                            PreparedStatement uuidUpdate = this.bindingDB.prepareStatement("INSERT INTO " + this.bindingPrefix + "_" + className + " (uuid,id,disabled,name,actual,value,source) VALUES (?,?,0,?,NOW(),'',?)");
                                            LOG.debug("Adding to " + uuidGlobal + " " + identifierNameOther + ":" + identifierFieldValue);
                                            uuidUpdate.setString(1, uuidGlobal);
                                            uuidUpdate.setString(2, identifierFieldValue);
                                            uuidUpdate.setString(3, identifierNameOther);
                                            uuidUpdate.setString(4, systemId);
                                            uuidUpdate.executeUpdate();
                                        }


                                    }


                                    return uuidGlobal;
                                }
                            }
                        }
                    }
                }
            } else {

                LOG.info("Object not found!!!!");
                //todo: binding via match

                String newUuid = UUID.randomUUID().toString();
                LOG.info("Registering new record");
                PreparedStatement uuidUpdate = this.bindingDB.prepareStatement("INSERT INTO " + this.bindingPrefix + "_" + className + " (uuid,id,disabled,name,actual,value,source) VALUES (?,?,0,?,NOW(),'',?)");
                uuidUpdate.setString(1, newUuid);
                uuidUpdate.setString(2, identifierValue);
                uuidUpdate.setString(3, identifierName);
                uuidUpdate.setString(4, systemId);
                uuidUpdate.executeUpdate();
                this.updateUuid(identifierName, identifierValue, newUuid);
                return newUuid;
            }

        } catch (SQLException e) {
            LOG.error("SQL Exception (in unify): " + e.getLocalizedMessage());
        }
        return super.getUuidByIdentifier(transactionId, className, identifierName, identifierValue, forcedCreation);
    }

    @Override
    public boolean get(String className, GetHelper helper) {
        String uuid = helper.getUuid();
        //todo: check identifiers
        String[] idents = identifiers.keySet().toArray(new String[0]);
        String id = this.getIdentifierByUuid(helper.getTransactionId(), className, idents[0], uuid);
        LOG.debug("Resolved identifier: " + id);
        if (id == null) return true;
        String identifierName = idents[0];
        HashMap<String, String> row = getRow(identifierName, id, true);

        //fill default values
        for (AccessorField field : schema.getSchemaFields()) {
            String path = this.getPath(field.getKey());
            if (field.getDefault() != null && (!row.containsKey(path) || row.get(path) == null)) {
                row.put(path, field.getDefault());
            }
        }

        if (row == null) return true;
        else {
            //fill the object
            String hash = this.getHash(row);
            try {
                PreparedStatement ps = bindingDB.prepareStatement("UPDATE " + bindingPrefix + "_" + className + " SET ACTUAL=NOW(),hash=? WHERE uuid=? AND disabled=0 AND source=?");
                ps.setString(1, hash);
                ps.setString(2, uuid);
                ps.setString(3, systemId);
                ps.executeUpdate();
            } catch (SQLException e) {
                LOG.error("SQL Exception (in get) " + e.getLocalizedMessage());
            }
            for (String rowKey : row.keySet()) {
                if (rowKey.startsWith("#")) continue;
                String referenced = null;
                for (AccessorField field : this.schema.getSchemaFields()) {
                    if (field.getPath().equalsIgnoreCase(rowKey)) {
                        referenced = field.getRef();
                    }
                }
                if (referenced != null) {
                    String referencedClass = referenced.substring(0, referenced.indexOf("."));
                    String referencedId = referenced.substring(referenced.indexOf(".") + 1);
                    try {
                        helper.put(rowKey, connector.unify(helper.getTransactionId(), referencedClass, referencedId, row.get(rowKey), false, false));
                    } catch (JMSException e) {
                        LOG.error("Error when resolving: " + e.getLocalizedMessage());
                    }
                } else {
                    helper.put(rowKey, row.get(rowKey));
                }
            }
            return true;
        }
    }

    /**
     * Translate global uuid to local identifier
     *
     * @param transactionId  transaction identifier
     * @param className      classname
     * @param identifierName identifier name
     * @param uuidValue      uuid
     * @return Local identifier
     */
    @Override
    public String getIdentifierByUuid(String transactionId, String className, String identifierName, String uuidValue) {
        LOG.debug("Identifying: " + transactionId + " class: " + className + " name: " + identifierName + " uuidValue: " + uuidValue);
        try {
            String qr = "SELECT id FROM " + this.bindingPrefix + "_" + className + " WHERE uuid=? AND name=? AND disabled=0 AND source=?";
            LOG.debug(qr);
            PreparedStatement st = this.bindingDB.prepareStatement(qr);
            st.setString(1, uuidValue);
            st.setString(2, identifierName);
            st.setString(3, systemId);
            ResultSet rs = st.executeQuery();
            if (rs.next()) {
                this.updateUuid(identifierName, rs.getString("id"), uuidValue);
                return rs.getString("id");
            }
        } catch (SQLException e) {
            LOG.error("SQL Exception (in identify): " + e.getLocalizedMessage());
        }
        return null;
    }

    public abstract boolean checkByIdentifier(String transactionId, String className, String identifierName, String identifierValue);
}
