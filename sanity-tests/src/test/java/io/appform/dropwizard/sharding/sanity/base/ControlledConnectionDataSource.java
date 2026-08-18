package io.appform.dropwizard.sharding.sanity.base;

import io.dropwizard.db.ManagedDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;

/**
 * A {@link ManagedDataSource} that returns pre-created JDBC connections in a controlled
 * order, instead of borrowing from a pool.
 *
 * <p>Connection selection is governed by a pluggable {@link ConnectionSelectionStrategy}:
 * <ul>
 *   <li>{@link FixedConnectionStrategy} (default) — returns the same connection until
 *       the caller explicitly changes it via {@link #useConnection(int)}.</li>
 *   <li>{@link RoundRobinConnectionStrategy} — returns the current connection and
 *       automatically advances to the next one on each {@code getConnection()} call,
 *       wrapping around when the end is reached.</li>
 * </ul>
 *
 * <p>The strategy can be changed at any time via {@link #setStrategy(ConnectionSelectionStrategy)}.
 *
 * <p>Usage:
 * <pre>
 *   var ds = new ControlledConnectionDataSource(List.of(conn1, conn2));
 *
 *   // Fixed (default): explicit control
 *   ds.useConnection(0);  // all getConnection() calls return conn1
 *   ds.useConnection(1);  // all getConnection() calls return conn2
 *
 *   // Round-robin: auto-advancing
 *   ds.setStrategy(new RoundRobinConnectionStrategy());
 *   ds.getConnection();   // → conn1 (advances to 1)
 *   ds.getConnection();   // → conn2 (advances to 0)
 *   ds.getConnection();   // → conn1 (advances to 1)
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
    private volatile ConnectionSelectionStrategy strategy;

    public ControlledConnectionDataSource(List<Connection> connections) {
        this(connections, new FixedConnectionStrategy());
    }

    public ControlledConnectionDataSource(List<Connection> connections,
                                          ConnectionSelectionStrategy strategy) {
        if (connections == null || connections.isEmpty()) {
            throw new IllegalArgumentException("Must provide at least one connection");
        }
        this.connections = connections;
        this.strategy = strategy;
    }

    /**
     * Replaces the connection selection strategy.
     * The new strategy starts from index 0 unless you call {@link #useConnection(int)} after.
     */
    public void setStrategy(ConnectionSelectionStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Returns the current strategy.
     */
    public ConnectionSelectionStrategy getStrategy() {
        return strategy;
    }

    /**
     * Select which pre-created connection index to return on the next
     * {@link #getConnection()} call. Delegates to the current strategy's
     * {@link ConnectionSelectionStrategy#reset(int)}.
     *
     * @param index 0-based index into the connections list
     */
    public void useConnection(int index) {
        if (index < 0 || index >= connections.size()) {
            throw new IndexOutOfBoundsException(
                    "Index " + index + " out of range [0, " + connections.size() + ")");
        }
        strategy.reset(index);
    }

    /**
     * Returns the index that was last returned (or will be returned for fixed strategy).
     */
    public int currentIndex() {
        return strategy.currentIndex();
    }

    @Override
    public Connection getConnection() throws SQLException {
        int idx = strategy.nextIndex(connections.size());
        Connection real = connections.get(idx);
        log.info("ControlledConnectionDataSource: returning connection index={}, conn={}",
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
