package org.gathe.integration;

import org.apache.qpid.amqp_1_0.jms.Session;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;


public class Main {

    private static EndpointManager endpointManager = new EndpointManager();

    public static void main(String[] args) throws Exception {
//        System.out.println(new File("log4j.xml").getAbsolutePath());
//        System.setProperty("log4j.configuration", "file:"+new File("log4j.xml").getAbsolutePath());

        ReceiverThread rt = new ReceiverThread(endpointManager);
        MonitorThread mt = new MonitorThread(endpointManager);
        mt.start();
        rt.start();

        Server webServer = new Server(6080);
        webServer.setHandler(new WebHandler(endpointManager));
        webServer.start();

        rt.join();          //message loop
    }
}
