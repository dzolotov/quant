package org.gathe.integration.csv;

import javax.xml.bind.annotation.*;
import org.gathe.integration.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dmitrii on 24.03.14.
 */

@XmlRootElement(name = "csv")
@XmlAccessorType(XmlAccessType.FIELD)
class CSVSchemaJAXB extends AccessorSchema {

    @XmlElement(name = "field")
    private List<CSVFieldJAXB> fields;

    @XmlAttribute(name="class")
    private String dataClass;

    @XmlAttribute(name="source")
    private String source = null;

    @XmlAttribute(name="encoding")
    private String encoding = "utf-8";

    @XmlAttribute(name="header")
    private String header = "false";

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getDataClass() {
        return dataClass;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setDataClass(String dataClass) {
        this.dataClass = dataClass;
    }

    public void addField(CSVFieldJAXB field) {
        fields.add(field);
    }

//    public List<CSVFieldJAXB> getFields() {
//        return this.fields;
//    }

    @Override
    public List<AccessorField> getSchemaFields() {
        //AccessorField[] f = fields.toArray(new AccessorField[0]);
        List<AccessorField> af = new ArrayList<AccessorField>();
        af.addAll(fields);
        return af;
//
//        return new ArrayList<AccessorField>(fields.toArray(new AccessorField[0]));
    }
}

@XmlRootElement(name = "field")
class CSVFieldJAXB extends AccessorField {

    private String order = null;

    private String type = "text";

    String id = "false";

    String path = "";

    String description = "";

    String scope = "local";

    String ref = null;

    String defaultValue = null;

    String nullBehavior;

    String emptyBehavior;

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

    public String getEmptyBehavior() {
        return emptyBehavior;
    }

    public void setEmptyBehavior(String emptyBehavior) {
        this.emptyBehavior = emptyBehavior;
    }

    public void setNullBehavior(String nullBehavior) {
        this.nullBehavior = nullBehavior;
    }


    @XmlElement(name="replace")
    private List<ReplaceJAXB> replaces;

    @XmlElement(name="append")
    private List<AppendJAXB> appends;

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
    public String getOrder() {
        return this.order;
    }

    public void setOrder(String order) {
        this.order = order;
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
    public String getDefault() { return this.defaultValue; }

    public void setDefault(String defaultValue) { this.defaultValue = defaultValue; }

    public String toString() {
        return this.getOrder()+(this.isIdentifier()?" [I:"+this.getId()+"]":"")+": "+this.getType()+" -> "+this.getPath()+(this.getDescription().isEmpty()?"":" ("+this.getDescription()+")");
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
        return this.getOrder();
    }
}