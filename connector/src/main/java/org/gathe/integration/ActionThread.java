package org.gathe.integration;

import org.apache.log4j.Logger;

/**
 * Created by zolotov on 01.11.13.
 */
public class ActionThread extends Thread {

    private final Logger LOG = Logger.getLogger(this.getClass());
    private final int timeout;
    private boolean accepted;
    private final String action;

    public ActionThread(String action, int timeout) {
        this.timeout = timeout;
        this.action = action;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void run() {
        try {
            accepted = false;
            Thread.sleep(timeout * 1000);
        } catch (InterruptedException e) {
            LOG.debug("Action thread terminated");
            accepted = true;
        }
    }
}
