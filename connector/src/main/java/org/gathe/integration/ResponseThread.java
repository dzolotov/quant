package org.gathe.integration;

/**
 * Created by zolotov on 01.11.13.
 */
public class ResponseThread extends Thread {
    private AsyncAccessor accessor;
    private String messageId;
    private String result;

    public ResponseThread(AsyncAccessor accessor, String messageId, String result) {
        this.accessor = accessor;
        this.messageId = messageId;
        this.result = result;
    }

    public void run() {
        this.accessor.processResult(this.messageId, this.result);
    }
}
