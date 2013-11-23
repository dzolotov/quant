package org.gathe.integration;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

public class WebHandler extends AbstractHandler {

    public static HashMap<String,Response> semaphores = new HashMap<>();
    private EndpointManager endpointManager;

    private Logger LOG = Logger.getLogger(this.getClass());

    public WebHandler(EndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        LOG.info("Web Server thread initialized");
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException, ServletException {
        LOG.info("Handling target: "+target);

        if (target.equalsIgnoreCase("/init")) {
            String threadName = Thread.currentThread().getName();
            endpointManager.doInit();
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            return;
        }

        if (target.startsWith("/static")) {
            String filename = target.substring("/static".length());

            URL url = getClass().getResource(filename);
            InputStream stream = getClass().getResourceAsStream(filename);
            if (stream==null) {
                //resource not found
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().println("Resource not found");
                baseRequest.setHandled(true);
                return;
            }
            String contentType = URLConnection.guessContentTypeFromStream(stream);
            response.setContentType(contentType);
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            IOUtils.copy(stream, response.getOutputStream());
            return;
        }

        if (target.startsWith("/status")) {
            int next;
            try {
                next = Integer.parseInt(target.substring(8));
            } catch (Exception e) {
                ;
                next = -1;
            }
            if (next<0) {
                next = endpointManager.getRingHead();
            }
            String threadName = Thread.currentThread().getName();
            Response responseObj = new Response();
            semaphores.put(threadName,responseObj);
            synchronized (responseObj) {
                if (endpointManager.getRingHead() == next) {
                    try {
                        LOG.debug("Waiting for semaphone " + threadName);
                        responseObj.wait(60000);
                    } catch (InterruptedException e) {
                        LOG.debug("Timeout");
                    }
                }
                //LOG.debug("Result is "+responseObj.getResponse());
                LOG.debug("Messages from "+next+" to "+endpointManager.getRingHead());
                String[] responses = endpointManager.getMessages(next);
                for (String resp : responses) {
                    LOG.debug(resp);
                }
                String responseStr = endpointManager.join(responses, "\n");
                response.setStatus(HttpServletResponse.SC_OK);
//                response.getWriter().println("");
                response.getWriter().println(responseStr);
                semaphores.remove(threadName);
                baseRequest.setHandled(true);
                return;
            }
        }

        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        response.getWriter().println("Hello world!");
    }

}