package org.gathe.integration.db;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "join")
public class DBJoinJAXB {
    private String with;
    private String from;
    private String to;

    @XmlAttribute(name = "with")
    public String getWith() { return with; }

    public void setWith(String with) { this.with = with; }

    @XmlAttribute(name = "from")
    public String getFrom() { return from; }

    public void setFrom(String from) { this.from = from; }

    @XmlAttribute(name = "to")
    public String getTo() { return to; }

    public void setTo(String to) { this.to = to; }
}
