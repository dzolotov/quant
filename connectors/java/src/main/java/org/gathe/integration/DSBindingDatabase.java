package org.gathe.integration;

import org.hsqldb.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by dmitrii on 25.03.14.
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
            String query = "CREATE TABLE " + bindingPrefix.toUpperCase() + "_" + className + " (uuid varchar(36), name varchar(255), id varchar(255), hash varchar(128), actual timestamp, disabled int, value varchar(4096))";
            System.out.println(query);
            st.executeUpdate(query);
            connection.commit();
        } catch (Exception e) {
//                    e.printStackTrace();
        }
        return connection;
    }
}
