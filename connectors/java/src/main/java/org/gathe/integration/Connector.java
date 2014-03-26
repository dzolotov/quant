package org.gathe.integration;

import org.apache.qpid.amqp_1_0.jms.TextMessage;

import javax.jms.JMSException;
import java.util.HashMap;

/**
 * Created by dmitrii on 22.03.14.
 */
public interface Connector extends Runnable {
    void connect();

    void connectESB(boolean readOnly) throws JMSException;

    void appendAccessor(Accessor accessor);

    void sendToUno(TextMessage textMessage, String content) throws JMSException;

    String get(String transactionId, String className, String uuid, boolean async, boolean isLocalRequest) throws JMSException;

    String get(String className, String uuid, boolean isLocalRequest) throws JMSException;

    String get(String className, String uuid) throws JMSException;

    String unify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException;

    String unify(String className, String identifier, String identifierValue) throws JMSException;

    String unify(String className, String identifier, String identifierValue, boolean isLocalRequest) throws JMSException;

    String identify(String transactionId, String className, String identifier, String identifierValue, boolean async, boolean isLocalRequest) throws JMSException;

    String identify(String transactionId, String className, String identifier, String identifierValue, boolean async) throws JMSException;

    String identify(String className, String identifier, String uuid) throws JMSException;

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
