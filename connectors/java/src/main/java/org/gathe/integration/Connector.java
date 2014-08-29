package org.gathe.integration;

import org.apache.qpid.amqp_1_0.jms.TextMessage;

import javax.jms.JMSException;
import java.util.HashMap;

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

public interface Connector extends Runnable {
    void connect();

    void connectESB() throws JMSException;

    void appendAccessor(Accessor accessor);

    void sendToUno(TextMessage textMessage, String content) throws JMSException;

    String get(String transactionId, String className, String uuid, boolean async, boolean isLocalRequest) throws JMSException;

    String get(String className, String uuid, boolean isLocalRequest) throws JMSException;

    String get(String className, String uuid) throws JMSException;

    String unify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException;

    String unify(String className, String identifier, String identifierValue) throws JMSException;

    String unify(String className, String identifier, String identifierValue, boolean isLocalRequest) throws JMSException;

    String unify(String className, String identifier, String identifierValue, boolean isLocalRequest, boolean forcedCreation) throws JMSException;

    String identify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException;

    String identify(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException;

    String identify(String className, String identifier, String uuid) throws JMSException;

    String identify(String className, String identifier, String uuid, boolean isLocalRequest) throws JMSException;

    boolean check(String className, String identifier, String identifierValue, boolean isLocalRequest) throws JMSException;

    boolean check(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException;

    boolean check(String className, String identifier, String identifierValue) throws JMSException;

    String specify(String className, String uuid, boolean isLocalRequest) throws JMSException;

    String specify(String transactionId, String className, String uuid, boolean async, boolean isLocalRequest) throws JMSException;

    String specify(String className, String uuid) throws JMSException;

    void remove(String className, String uuid, boolean isLocalRequest) throws JMSException;

    void remove(String transactionId, String className, String uuid, boolean isLocalRequest) throws JMSException;

    void remove(String className, String uuid) throws JMSException;

    void update(String className, String uuid, String content, boolean isLocalRequest) throws JMSException;

    void update(String transactionId, String className, String uuid, String content, boolean isLocalRequest) throws JMSException;

    void update(String className, String uuid, String content) throws JMSException;

    String matchAll(String className, HashMap<String, String> filters, boolean isLocalRequest) throws JMSException;

    String matchAll(String transactionId, String className, HashMap<String, String> filters, boolean async, boolean isLocalRequest) throws JMSException;

    String matchAll(String className, HashMap<String, String> filters) throws JMSException;

    boolean isSelfRequest(String transactionId, String className);
}
