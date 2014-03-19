package org.gathe.integration;

import org.apache.log4j.Logger;

/**
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>
*/

 public class ActionThread extends Thread {

    private final Logger LOG = Logger.getLogger(this.getClass());
    private final int timeout;
    private boolean accepted;
    private final String action;
    private boolean flags;

    public ActionThread(String action, int timeout) {
        this.timeout = timeout;
        this.action = action;
        this.accepted = false;
        this.flags = false;
    }

    public void reset() {
        this.flags = false;
    }

    public void needContinue(boolean accepted) {
        if (accepted) LOG.debug("Need to continue..."); else LOG.debug("Finished");
        this.flags = accepted;
        this.interrupt();
    }

    public boolean isNeedContinue() {
        return this.flags;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void run() {
        try {
            accepted = false;
            LOG.info("Thread reloaded");
            Thread.sleep(timeout * 1000);
        } catch (InterruptedException e) {
            LOG.debug("Action thread terminated");
            accepted = true;
        }
    }
}
