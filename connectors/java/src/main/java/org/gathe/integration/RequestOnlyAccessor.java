package org.gathe.integration;

/**
 * Created by dmitrii on 21.03.14.
 */
public class RequestOnlyAccessor extends BaseAccessor {

    public boolean isReadOnly() {
        return true;
    }
}
