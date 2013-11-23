package org.gathe.integration;

import org.apache.log4j.Logger;

/**
 * Created by zolotov on 01.11.13.
 */
public class ActionThread extends Thread {

    private Logger LOG = Logger.getLogger(this.getClass());
    private int timeout;
    private boolean accepted;
    private String action;

    public ActionThread(String action, int timeout) {
        this.timeout = timeout;
        this.action = action;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public String getAction() {
        return action;
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
