package org.gathe.integration;
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

        new EndpointManager();
        System.out.println("Leaving main loop");
    }
}
