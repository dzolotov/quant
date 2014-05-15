package org.gathe.integration.db;

import javax.sql.PooledConnection;

public interface PooledConnectionWrapper {

    public PooledConnection getPooledConnection();
}
