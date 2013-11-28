package org.gathe.integration;

/**
 * Created by zolotov on 28.11.13.
 */
public class DataElement {
    private final String path;
    private final String description;
    private final boolean required;

    public DataElement(String path,String description) {
        this.path = path;
        this.description = description;
        required = false;
    }

    public DataElement(String path,String description, boolean required) {
        this.path = path;
        this.description = description;
        this.required = required;
    }

    public String getXPath() {
        return this.path;
    }

    public String getDescription() {
        return this.description;
    }

    public boolean isRequired() {
        return this.required;
    }
}

