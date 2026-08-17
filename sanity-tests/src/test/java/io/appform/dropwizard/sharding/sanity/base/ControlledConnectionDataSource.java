package io.appform.dropwizard.sharding.sanity.base;

import io.dropwizard.db.ManagedDataSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A {@link ManagedDataSource} that returns pre-created JDBC connections in a controlled
 * order, instead of borrowing from a pool.
 *
 * <p>Usage:
 * <pre>
 *   Connection conn1 = DriverManager.getConnection(url, user, pass);
 *   Connection conn2 = DriverManager.getConnection(url, user, pass);
 *   var ds = new ControlledConnectionDataSource(List.of(conn1, conn2));
 *
 *   ds.useConnection(0);  // subsequent getConnection() calls return conn1
 *   ds.useConnection(1);  // subsequent getConnection() calls return conn2
 * </pre>
 *
 * <p>Connections returned by {@link #getConnection()} are wrapped in a proxy that
 * makes {@code close()} a no-op. This prevents Hibernate's session cleanup from
 * physically closing the connection, so it can be reused across operations.
 *
 * <p>The actual connections must be closed by the test in {@code @AfterAll}.
 */
@Slf4j
public class ControlledConnectionDataSource implements ManagedDataSource {

    private final List<Connection> connections;
    private final AtomicInteger selector = new AtomicInteger(0);

    public ControlledConnectionDataSource(List<Connection> connections) {
        if (connections == null || connections.isEmpty()) {
            throw new IllegalArgumentException("Must provide at least one connection");
        }
        this.connections = connections;
    }

    /**
     * Select which pre-created connection index to return on the next
     * {@link #getConnection()} call.
     *
     * @param index 0-based index into the connections list
     */
    public void useConnection(int index) {
        if (index < 0 || index >= connections.size()) {
            throw new IndexOutOfBoundsException(
                    "Index " + index + " out of range [0, " + connections.size() + ")");
        }
        selector.set(index);
    }

    /**
     * Returns the currently selected connection index.
     */
    public int currentIndex() {
        return selector.get();
    }

    @Override
    public Connection getConnection() throws SQLException {
        int idx = selector.get();
        Connection real = connections.get(idx);
        log.debug("ControlledConnectionDataSource: returning connection index={}, conn={}",
                idx, System.identityHashCode(real));
        return wrapWithNoOpClose(real);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    /**
     * Wraps a real connection in a proxy where {@code close()} is a no-op.
     * All other method calls are forwarded to the real connection.
     */
    private Connection wrapWithNoOpClose(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class[]{Connection.class},
                new NoOpCloseHandler(real)
        );
    }

    private static class NoOpCloseHandler implements InvocationHandler {
        private final Connection delegate;

        NoOpCloseHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("close".equals(method.getName())) {
                // No-op: don't close the real connection
                return null;
            }
            if ("isClosed".equals(method.getName())) {
                // Always report open so Hibernate doesn't think the connection is dead
                return false;
            }
            try {
                return method.invoke(delegate, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Managed lifecycle — no-ops since we manage connections ourselves
    // -------------------------------------------------------------------------

    @Override
    public void start() throws Exception {
        // No pool to start
    }

    @Override
    public void stop() throws Exception {
        // Don't close connections here — test owns their lifecycle
    }

    // -------------------------------------------------------------------------
    // DataSource boilerplate — not used by Hibernate/bundle
    // -------------------------------------------------------------------------

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Not supported");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
