package org.gathe.integration;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "append")
public class AppendJAXB {
    private String path;
    private String value;

    @XmlAttribute(name = "path")
    public String getPath() {
        return path;
    }

    public void setName(String path) {
        this.path = path;
    }

    @XmlAttribute(name = "value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
