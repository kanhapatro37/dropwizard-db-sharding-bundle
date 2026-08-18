package io.appform.dropwizard.sharding.sanity.base;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.dropwizard.sharding.BalancedDBShardingBundle;
import io.appform.dropwizard.sharding.DBShardingBundleBase;
import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.dao.LookupDao;
import io.appform.dropwizard.sharding.dao.RelationalDao;
import io.appform.dropwizard.sharding.sanity.entities.SanityOrder;
import io.appform.dropwizard.sharding.sanity.entities.SanityOrderItem;
import io.appform.dropwizard.sharding.sharding.InMemoryLocalShardBlacklistingStore;
import io.appform.dropwizard.sharding.sharding.ShardBlacklistingStore;
import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.AdminEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MariaDBContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for all sanity tests.
 *
 * <h3>Architecture</h3>
 * <pre>
 *  ┌──────────────────────────────────────────────────────────────┐
 *  │                    MariaDB Container                         │
 *  │              (singleton — shared across all tests)           │
 *  │                                                              │
 *  │   ┌─────────────┐              ┌─────────────┐               │
 *  │   │   shard_0   │              │   shard_1   │               │
 *  │   └──────┬──────┘              └──────┬──────┘               │
 *  └──────────┼────────────────────────────┼──────────────────────┘
 *             │                            │
 *    ┌────────┴────────┐          ┌────────┴────────┐
 *    │  Writer Bundle  │          │  Reader Bundle  │
 *    │  (pool A)       │          │  (pool B)       │
 *    │  autoCommit=T   │          │  autoCommit=F   │
 *    │  hbm2ddl=create │          │  hbm2ddl=none   │
 *    └────────┬────────┘          └────────┬────────┘
 *             │                            │
 *      writerOrderItemDao           readerOrderItemDao
 *      writerOrderLookupDao         readerOrderLookupDao
 * </pre>
 *
 * <h3>Container lifecycle</h3>
 * The MariaDB container is started <b>once</b> for the entire JVM via the singleton pattern
 * (static initializer). All test classes share the same container. The container is stopped
 * automatically when the JVM exits (Testcontainers registers a shutdown hook).
 *
 * <h3>Why two bundles</h3>
 * Two bundles = two separate connection pools = guaranteed different physical connections.
 * Writing via writer DAOs and reading via reader DAOs ensures cross-connection verification.
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SanityTestBase {

    // -------------------------------------------------------------------------
    // MariaDB container — singleton, shared across ALL test classes in the JVM
    // -------------------------------------------------------------------------

    private static final String SHARD_0_DB = "shard_0";
    private static final String SHARD_1_DB = "shard_1";

    /**
     * Singleton container. Started once when this class is first loaded.
     * Stopped automatically via JVM shutdown hook (Testcontainers default behavior).
     *
     * <p>We do NOT use {@code @Container} + {@code @Testcontainers} because those
     * annotations manage lifecycle per-test-class, meaning a new container would start
     * for every test class that extends this base. The singleton pattern starts it once.
     */
    protected static final MariaDBContainer<?> MARIADB;

    static {
        MARIADB = new MariaDBContainer<>("mariadb:10.11")
                .withDatabaseName(SHARD_0_DB)
                .withUsername("test")
                .withPassword("test")
                .withCommand("--transaction-isolation=REPEATABLE-READ");
        MARIADB.start();
    }

    // -------------------------------------------------------------------------
    // Config wrapper
    // -------------------------------------------------------------------------

    protected static class SanityTestConfig extends Configuration {
        @Getter
        private final ShardedHibernateFactory shards;

        SanityTestConfig(ShardedHibernateFactory shards) {
            this.shards = shards;
        }
    }

    // -------------------------------------------------------------------------
    // Mocked Dropwizard infrastructure
    //
    // These are "sink" mocks — bundle.run() calls methods on them (register
    // health checks, lifecycle managed objects, admin tasks) and the mocks
    // silently accept the calls. We never assert on them.
    // -------------------------------------------------------------------------

    private final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    private final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    private final LifecycleEnvironment lifecycleEnvironment = mock(LifecycleEnvironment.class);
    private final Environment environment = mock(Environment.class);
    private final AdminEnvironment adminEnvironment = mock(AdminEnvironment.class);
    private final Bootstrap<?> bootstrap = mock(Bootstrap.class);

    // -------------------------------------------------------------------------
    // Bundles and DAOs — available to subclasses
    // -------------------------------------------------------------------------

    protected DBShardingBundleBase<SanityTestConfig> writerBundle;
    protected DBShardingBundleBase<SanityTestConfig> readerBundle;

    protected LookupDao<SanityOrder> writerOrderLookupDao;
    protected LookupDao<SanityOrder> readerOrderLookupDao;

    protected RelationalDao<SanityOrderItem> writerOrderItemDao;
    protected RelationalDao<SanityOrderItem> readerOrderItemDao;

    // -------------------------------------------------------------------------
    // Controlled connections for the reader bundle
    //
    // Instead of a Tomcat pool, the reader bundle uses pre-created JDBC
    // connections. Tests can call readerShard0Ds.useConnection(n) to select
    // which connection index is returned by getConnection().
    // -------------------------------------------------------------------------

    /** Controlled DataSource for reader shard 0 */
    protected ControlledConnectionDataSource readerShard0Ds;
    /** Controlled DataSource for reader shard 1 */
    protected ControlledConnectionDataSource readerShard1Ds;

    /** Raw JDBC connections backing the reader shard 0 controlled data source */
    private final List<Connection> readerShard0Connections = new ArrayList<>();
    /** Raw JDBC connections backing the reader shard 1 controlled data source */
    private final List<Connection> readerShard1Connections = new ArrayList<>();

    // -------------------------------------------------------------------------
    // Setup — runs once before all tests in the subclass
    // -------------------------------------------------------------------------

    @BeforeAll
    void setupBundles() throws SQLException {
        wireDropwizardMocks();
        createDatabase(SHARD_1_DB);

        String shard0Url = MARIADB.getJdbcUrl();
        String shard1Url = shard0Url.replace("/" + SHARD_0_DB, "/" + SHARD_1_DB);

        // Writer bundle: creates schema, standard Tomcat pool (unchanged)
        val writerConfig = buildShardConfig(shard0Url, shard1Url, true);
        writerBundle = initBundle(writerConfig);
        writerOrderLookupDao = writerBundle.createParentObjectDao(SanityOrder.class);
        writerOrderItemDao = writerBundle.createRelatedObjectDao(SanityOrderItem.class);

        // Reader bundle: controlled connections instead of a pool.
        // Create 2 pre-made connections per shard (conn_0 and conn_1 for each shard).
        // Tests can switch between them via readerShard0Ds.useConnection(n).
//        int numConnectionsPerShard = getReaderConnectionsPerShard();
//        for (int i = 0; i < numConnectionsPerShard; i++) {
//            readerShard0Connections.add(createReaderConnection(shard0Url));
//            readerShard1Connections.add(createReaderConnection(shard1Url));
//        }
//
//        readerShard0Ds = new ControlledConnectionDataSource(readerShard0Connections);
//        readerShard1Ds = new ControlledConnectionDataSource(readerShard1Connections);
//
//        val readerConfig = buildControlledShardConfig(shard0Url, shard1Url);
//        readerBundle = initBundle(readerConfig);
//        readerOrderLookupDao = readerBundle.createParentObjectDao(SanityOrder.class);
//        readerOrderItemDao = readerBundle.createRelatedObjectDao(SanityOrderItem.class);
        val readerConfig = buildShardConfig(shard0Url, shard1Url, true);
        readerBundle = initBundle(readerConfig);
        readerOrderLookupDao = readerBundle.createParentObjectDao(SanityOrder.class);
        readerOrderItemDao = readerBundle.createRelatedObjectDao(SanityOrderItem.class);
    }

    @AfterAll
    void closeControlledConnections() {
        for (Connection conn : readerShard0Connections) {
            try { conn.close(); } catch (SQLException e) { log.warn("Failed to close shard0 connection", e); }
        }
        for (Connection conn : readerShard1Connections) {
            try { conn.close(); } catch (SQLException e) { log.warn("Failed to close shard1 connection", e); }
        }
    }

    /**
     * Number of pre-created connections per reader shard. Override in subclass
     * if you need more than 2.
     */
    protected int getReaderConnectionsPerShard() {
        return 2;
    }

    /**
     * Selects which connection index the reader bundle will use for BOTH shards.
     * Call this before a reader DAO operation to control which connection is used.
     * Only meaningful when using {@link FixedConnectionStrategy} (the default).
     *
     * @param index 0-based index (default is 0)
     */
    protected void useReaderConnection(int index) {
        readerShard0Ds.useConnection(index);
        readerShard1Ds.useConnection(index);
        log.info("Reader bundle now using connection index={}", index);
    }

    /**
     * Switches the connection selection strategy on BOTH reader shard DataSources.
     * <p>Example usage in a test:
     * <pre>
     *   // Switch to round-robin: each getConnection() auto-advances
     *   setReaderStrategy(new RoundRobinConnectionStrategy());
     *
     *   // Switch back to fixed: caller controls which connection via useReaderConnection()
     *   setReaderStrategy(new FixedConnectionStrategy());
     * </pre>
     *
     * @param strategy the new strategy to apply to both reader shard DataSources
     */
    protected void setReaderStrategy(ConnectionSelectionStrategy strategy) {
        readerShard0Ds.setStrategy(strategy);
        readerShard1Ds.setStrategy(strategy);
        log.info("Reader bundle strategy changed to {}", strategy.getClass().getSimpleName());
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void wireDropwizardMocks() {
        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.admin()).thenReturn(adminEnvironment);
        when(bootstrap.getHealthCheckRegistry()).thenReturn(mock(HealthCheckRegistry.class));
        when(bootstrap.getObjectMapper()).thenReturn(new ObjectMapper());
    }

    private SanityTestConfig buildShardConfig(String shard0Url, String shard1Url, boolean isWriter) {
        return new SanityTestConfig(ShardedHibernateFactory.builder()
                .shards(List.of(
                        buildDataSource(shard0Url, isWriter),
                        buildDataSource(shard1Url, isWriter)))
                .shardingOptions(ShardingBundleOptions.builder().build())
                .build());
    }

    /**
     * Builds a ShardConfig for the reader bundle that uses {@link ControlledDataSourceFactory}
     * instead of real DataSourceFactory. This ensures the bundle's SessionFactoryFactory.build()
     * receives our ControlledConnectionDataSource instead of a Tomcat pool.
     */
    private SanityTestConfig buildControlledShardConfig(String shard0Url, String shard1Url) {
        return new SanityTestConfig(ShardedHibernateFactory.builder()
                .shards(List.of(
                        buildControlledDataSource(shard0Url, readerShard0Ds),
                        buildControlledDataSource(shard1Url, readerShard1Ds)))
                .shardingOptions(ShardingBundleOptions.builder().build())
                .build());
    }

    /**
     * Creates a ControlledDataSourceFactory that carries all the Hibernate properties
     * (dialect, hbm2ddl=none) but returns the given ControlledConnectionDataSource
     * when build() is called.
     */
    private ControlledDataSourceFactory buildControlledDataSource(String jdbcUrl,
                                                                   ControlledConnectionDataSource controlledDs) {
        val ds = new ControlledDataSourceFactory(controlledDs);
        ds.setDriverClass("org.mariadb.jdbc.Driver");
        ds.setUrl(jdbcUrl);
        ds.setUser(MARIADB.getUsername());
        ds.setPassword(MARIADB.getPassword());
        ds.setValidationQuery("SELECT 1");

        val props = new HashMap<>(Map.of(
                "hibernate.dialect", "org.hibernate.dialect.MariaDBDialect",
                "hibernate.hbm2ddl.auto", "none"));
        ds.setProperties(props);

        ds.setAutoCommitByDefault(false);
        ds.setDefaultTransactionIsolation(DataSourceFactory.TransactionIsolation.REPEATABLE_READ);
        return ds;
    }

    /**
     * Creates a raw JDBC connection configured like the reader bundle would:
     * autoCommit=false, REPEATABLE_READ isolation.
     */
    private Connection createReaderConnection(String jdbcUrl) throws SQLException {
        Connection conn = DriverManager.getConnection(
                jdbcUrl, MARIADB.getUsername(), MARIADB.getPassword());
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        log.info("Created controlled reader connection: url={}, identityHash={}",
                jdbcUrl, System.identityHashCode(conn));
        return conn;
    }

    private DBShardingBundleBase<SanityTestConfig> initBundle(SanityTestConfig config) {
        DBShardingBundleBase<SanityTestConfig> bundle =
                new BalancedDBShardingBundle<SanityTestConfig>(
                        SanityOrder.class, SanityOrderItem.class) {
                    @Override
                    protected ShardedHibernateFactory getConfig(SanityTestConfig c) {
                        return c.getShards();
                    }

                    @Override
                    protected ShardBlacklistingStore getBlacklistingStore() {
                        return new InMemoryLocalShardBlacklistingStore();
                    }
                };
        bundle.initialize(bootstrap);
        bundle.run(config, environment);
        return bundle;
    }

    private DataSourceFactory buildDataSource(String jdbcUrl, boolean isWriter) {
        val ds = new DataSourceFactory();
        ds.setDriverClass("org.mariadb.jdbc.Driver");
        ds.setUrl(jdbcUrl);
        ds.setUser(MARIADB.getUsername());
        ds.setPassword(MARIADB.getPassword());
        ds.setValidationQuery("SELECT 1");
        ds.setMinSize(2);
        ds.setMaxSize(4);
        ds.setInitialSize(2);
        ds.setUseFairQueue(false);
        ds.setMaxConnectionAge(Duration.seconds(600));

        val props = new HashMap<>(Map.of(
                "hibernate.dialect", "org.hibernate.dialect.MariaDBDialect",
                "hibernate.hbm2ddl.auto", isWriter ? "create" : "none"));
        ds.setProperties(props);

//        if (!isWriter) {
            ds.setAutoCommitByDefault(false);
            ds.setDefaultTransactionIsolation(DataSourceFactory.TransactionIsolation.REPEATABLE_READ);
//        }
        return ds;
    }

    /**
     * Creates a database and grants the test user full access.
     * Uses root because the test user only has privileges on shard_0 (created by the container).
     */
    private void createDatabase(String dbName) throws SQLException {
        String rootUrl = MARIADB.getJdbcUrl().replace("/" + SHARD_0_DB, "/");
        try (Connection conn = DriverManager.getConnection(rootUrl, "root", MARIADB.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + dbName);
            conn.createStatement().execute(
                    "GRANT ALL PRIVILEGES ON " + dbName + ".* TO '" + MARIADB.getUsername() + "'@'%'");
            conn.createStatement().execute("FLUSH PRIVILEGES");
        }
    }

    /**
     * Truncates all sanity test tables across both shards.
     * Call in {@code @BeforeEach} if tests need a clean slate between test methods.
     */
    protected void truncateAllTables() throws SQLException {
        for (String db : List.of(SHARD_0_DB, SHARD_1_DB)) {
            String url = MARIADB.getJdbcUrl().replace("/" + SHARD_0_DB, "/" + db);
            try (Connection conn = DriverManager.getConnection(
                    url, MARIADB.getUsername(), MARIADB.getPassword())) {
                conn.createStatement().execute("SET FOREIGN_KEY_CHECKS = 0");
                conn.createStatement().execute("TRUNCATE TABLE sanity_order_items");
                conn.createStatement().execute("TRUNCATE TABLE sanity_orders");
                conn.createStatement().execute("SET FOREIGN_KEY_CHECKS = 1");
            }
        }
    }
}
