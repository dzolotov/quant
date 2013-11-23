package org.gathe.integration;

public abstract class BaseAccessor extends Thread implements Accessor {
    protected Connector connector;

    public void setConnector(Connector connector) {

        this.connector = connector;
    }
}