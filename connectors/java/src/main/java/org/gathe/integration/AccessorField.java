package org.gathe.integration;

import java.util.List;

public abstract class AccessorField {
    public abstract boolean isIdentifier();

    public abstract String getPath();

    public abstract String getId();

    public abstract String getDefault();

    public abstract List<ReplaceJAXB> getReplaces();

    public abstract String getScope();

    public abstract String getDescription();

    public abstract String getRef();

    public abstract String getKey();

    public abstract String getType();

    public abstract String getNullBehavior();

    public abstract String getEmptyBehavior();

    public abstract List<AppendJAXB> getAppends();
}

