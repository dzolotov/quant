package org.gathe.integration;

import org.hsqldb.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

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

 @Author Dmitrii Zolotov <zolotov@gathe.org>, Tikhon Tagunov <tagunov@gathe.org>, Nataliya Sorokina <nv@gathe.org>
 */


public class DSBindingDatabase {

    private static Server embedded = null;
    private static Connection connection = null;
    protected AccessorSchema schema;

    private DSBindingDatabase() {
    }

    public static Connection getDatabase(String bindingPrefix, String className) {
        try {
            if (embedded == null) {
                embedded = new Server();
                embedded.setLogWriter(null);
                embedded.setSilent(true);
                embedded.setPort(10946);            //todo: check
                embedded.setDatabaseName(0, "csv");
                embedded.setDatabasePath(0, "file:csv");
                embedded.start();

                Class.forName("org.hsqldb.jdbcDriver");
                System.out.println("Accessing DB for " + className);
                connection = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:10946/csv", "sa", ""); // can through sql exception
            }
        } catch (Exception e) {
        }
        ;
        try {
            Statement st = connection.createStatement();
            String query = "CREATE TABLE " + bindingPrefix.toUpperCase() + "_" + className + " (uuid varchar(36), name varchar(255), id varchar(255), hash varchar(128), actual timestamp, disabled int, source varchar(128), value varchar(4096))";
            System.out.println(query);
            st.executeUpdate(query);
            connection.commit();
        } catch (Exception e) {
        }
        return connection;
    }
}
