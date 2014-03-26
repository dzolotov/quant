package org.gathe.integration.db;

/**
 * Created by dmitrii on 25.03.14.
 */

import org.gathe.integration.AccessorField;
import org.gathe.integration.AccessorSchema;
import org.gathe.integration.ReplaceJAXB;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dmitrii on 24.03.14.
 */

@XmlRootElement(name = "db")
@XmlAccessorType(XmlAccessType.FIELD)
class DBSchemaJAXB extends AccessorSchema {

    @XmlElement(name = "field")
    private List<DBFieldJAXB> fields;

    @XmlAttribute(name = "class")
    private String dataClass;

    @XmlAttribute(name = "source")
    private String source = null;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @XmlAttribute(name = "table")
    private String table = null;

    public String getDataClass() {
        return dataClass;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setDataClass(String dataClass) {
        this.dataClass = dataClass;
    }

    public void addField(DBFieldJAXB field) {
        fields.add(field);
    }

    @Override
    public List<AccessorField> getSchemaFields() {
        List<AccessorField> af = new ArrayList<AccessorField>();
        af.addAll(fields);
        return af;
    }
}

@XmlRootElement(name = "field")
class DBFieldJAXB extends AccessorField {

    private String name = null;

    private String type = "text";

    String id = "false";

    String path = "";

    String description = "";

    String scope = "local";

    String ref = null;

    String defaultValue = null;

    @XmlElement(name = "replace")
    private List<ReplaceJAXB> replaces;

    @XmlAttribute
    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    @XmlAttribute
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @XmlAttribute
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @XmlAttribute
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @XmlAttribute
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    @XmlAttribute
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @XmlAttribute
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @XmlAttribute
    public String getDefault() {
        return this.defaultValue;
    }

    public void setDefault(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String toString() {
        return this.getName() + (this.isIdentifier() ? " [I:" + this.getId() + "]" : "") + ": " + this.getType() + " -> " + this.getPath() + (this.getDescription().isEmpty() ? "" : " (" + this.getDescription() + ")");
    }

    public List<ReplaceJAXB> getReplaces() {
        return replaces;
    }

    public void addReplace(ReplaceJAXB replace) {
        replaces.add(replace);
    }

    @Override
    public boolean isIdentifier() {
//        System.out.println("Checking for identifier: "+this.getId());
        return !(this.getId().equalsIgnoreCase("false"));
    }

    @Override
    public String getKey() {
        return this.getName();
    }
}
