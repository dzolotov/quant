package org.gathe.integration;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseAccessor extends Thread implements Accessor {
    protected Connector connector;

    public void setConnector(Connector connector) {

        this.connector = connector;
    }
}