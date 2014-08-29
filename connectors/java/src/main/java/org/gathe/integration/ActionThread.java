package org.gathe.integration;

import org.apache.log4j.Logger;

import javax.jms.JMSException;

/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
 */

public class ActionThread extends Thread {

    private final Logger LOG = Logger.getLogger(this.getClass());
    private final int timeout;
    private boolean accepted;
    private final String action;
    private boolean flags;
    private Connector connector;
    private org.apache.qpid.amqp_1_0.jms.TextMessage textMessage;

    public ActionThread(String action, Connector connector, org.apache.qpid.amqp_1_0.jms.TextMessage tm, int timeout) {
        this.timeout = timeout;
        this.action = action;
        this.accepted = false;
        this.flags = false;
        this.textMessage = tm;
        this.connector = connector;
    }

    public void needContinue(boolean flags) {
        if (flags) LOG.debug("Need to continue...");
        else LOG.debug("Finished");
        this.flags = flags;
        if (!flags) {
            LOG.debug("Interrupting...");
        }
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void run() {
        try {
            LOG.debug("Thread reloaded");
            accepted = false;
            this.flags = true;
            try {
                LOG.debug("Sending message");
                connector.sendToUno(textMessage, "");
            } catch (JMSException e) {
            }
            while (this.flags) {
                LOG.debug("Sleeping for " + timeout + " seconds");
                this.flags = false;
                Thread.sleep(timeout * 1000);
            }
        } catch (InterruptedException e) {
            LOG.debug("Action thread terminated");
            accepted = true;
        }
    }
}
