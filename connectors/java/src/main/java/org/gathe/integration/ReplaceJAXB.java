package org.gathe.integration;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "replace")
public class ReplaceJAXB {
    private String from;
    private String to;

    @XmlAttribute(name = "from")
    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    @XmlAttribute(name = "to")
    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }
}
