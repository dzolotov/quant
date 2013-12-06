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
