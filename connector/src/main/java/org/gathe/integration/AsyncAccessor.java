package org.gathe.integration;

/**
 * Created by zolotov on 01.11.13.
 */
public interface AsyncAccessor extends Accessor {

    public void processResult(String messageId, String result);

}
