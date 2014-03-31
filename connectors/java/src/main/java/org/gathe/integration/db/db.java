package org.gathe.integration.db;

/**
 * Created by dmitrii on 25.03.14.
 */

import org.gathe.integration.AccessorField;
import org.gathe.integration.AccessorSchema;
import org.gathe.integration.AppendJAXB;
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

    @XmlElement(name = "join")
    private List<DBJoinJAXB> joins = new ArrayList<DBJoinJAXB>();

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

    @XmlAttribute(name = "active")
    private String active = null;

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
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

    public void addJoin(DBJoinJAXB join) { joins.add(join); }

    public List<DBJoinJAXB> getJoin() { return joins; }

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

    String value = null;

    String defaultValue = null;

    String nullBehavior = "stay";

    String emptyBehavior = "stay";

    @XmlAttribute(name="value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @XmlElement(name = "replace")
    private List<ReplaceJAXB> replaces;

    @XmlElement(name = "append")
    private List<AppendJAXB> appends;

    @XmlAttribute
    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @XmlAttribute
    public String getNullBehavior() {
        return nullBehavior;
    }

    public void setNullBehavior(String nullBehavior) {
        this.nullBehavior = nullBehavior;
    }

    @XmlAttribute
    public String getEmptyBehavior() {
        return emptyBehavior;
    }

    public void setEmptyBehavior(String emptyBehavior) {
        this.emptyBehavior = emptyBehavior;
    }

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

    public void addAppend(AppendJAXB append) { appends.add(append); }

    public List<AppendJAXB> getAppends() { return appends; }

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
