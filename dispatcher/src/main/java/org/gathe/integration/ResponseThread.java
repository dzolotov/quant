package org.gathe.integration;

/**
 * Created by dmitrii on 19.03.14.
 */
public class ResponseThread extends Thread {
    protected final String transactionId;
    protected final String messageId;

    public ResponseThread(String transactionId, String messageId) {
        this.transactionId = transactionId;
        this.messageId = messageId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getMessageId() {
        return messageId;
    }
}

