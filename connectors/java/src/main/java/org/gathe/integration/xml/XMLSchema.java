package org.gathe.integration.xml;

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

 @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
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

@XmlRootElement(name = "xml")
@XmlAccessorType(XmlAccessType.FIELD)
class XMLSchemaJAXB extends AccessorSchema {

    @XmlElement(name = "field")
    private List<XMLFieldJAXB> fields;

    @XmlAttribute(name = "dir")
    private String dir = null;

    @XmlAttribute(name = "filename")
    private String filename = null;

    @XmlAttribute(name = "class")
    private String dataClass;

    @XmlAttribute(name = "encoding")
    private String encoding = "utf-8";

    private String uuid;

    public String getUuid() {
        return uuid;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getDataClass() {
        return this.dataClass;
    }

    public void setDataClass(String dataClass) {
        this.dataClass = dataClass;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void addField(XMLFieldJAXB field) {
        fields.add(field);
    }

    @Override
    public List<AccessorField> getSchemaFields() {
        //AccessorField[] f = fields.toArray(new AccessorField[0]);
        List<AccessorField> af = new ArrayList<AccessorField>();
        af.addAll(fields);
        return af;
    }
}

@XmlRootElement(name = "field")
class XMLFieldJAXB extends AccessorField {

    String id = "false";
    String path = "";
    String description = "";
    String scope = "local";
    String ref = null;
    String defaultValue = null;
    String nullBehavior = "stay";
    String emptyBehavior = "stay";
    String matchIgnore = "false";
    private String xpath = null;
    private String type = "text";
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
    public String getMatchIgnore() {
        return matchIgnore;
    }

    public void setMatchIgnore(String matchIgnore) {
        this.matchIgnore = matchIgnore;
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

    @XmlAttribute(name = "xpath")
    public String getXPath() {
        return this.xpath;
    }

    public void setXPath(String xpath) {
        this.xpath = xpath;
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
        return this.getXPath() + (this.isIdentifier() ? " [I:" + this.getId() + "]" : "") + ": " + this.getType() + " -> " + this.getPath() + (this.getDescription().isEmpty() ? "" : " (" + this.getDescription() + ")");
    }

    public List<ReplaceJAXB> getReplaces() {
        return replaces;
    }

    public void addReplace(ReplaceJAXB replace) {
        replaces.add(replace);
    }

    public void addAppend(AppendJAXB append) {
        appends.add(append);
    }

    public List<AppendJAXB> getAppends() {
        return appends;
    }

    @Override
    public boolean isIdentifier() {
//        System.out.println("Checking for identifier: "+this.getId());
        return !(this.getId().equalsIgnoreCase("false"));
    }

    @Override
    public String getKey() {
        return this.getXPath();
    }
}
