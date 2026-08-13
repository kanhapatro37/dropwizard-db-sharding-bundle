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
import lombok.Getter;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.MariaDBContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    // Setup — runs once before all tests in the subclass
    // -------------------------------------------------------------------------

    @BeforeAll
    void setupBundles() throws SQLException {
        wireDropwizardMocks();
        createDatabase(SHARD_1_DB);

        String shard0Url = MARIADB.getJdbcUrl();
        String shard1Url = shard0Url.replace("/" + SHARD_0_DB, "/" + SHARD_1_DB);

        // Writer bundle: creates schema, standard pool
        val writerConfig = buildShardConfig(shard0Url, shard1Url, true);
        writerBundle = initBundle(writerConfig);
        writerOrderLookupDao = writerBundle.createParentObjectDao(SanityOrder.class);
        writerOrderItemDao = writerBundle.createRelatedObjectDao(SanityOrderItem.class);

        // Reader bundle: autoCommit=false, REPEATABLE_READ, separate pool
        val readerConfig = buildShardConfig(shard0Url, shard1Url, false);
        readerBundle = initBundle(readerConfig);
        readerOrderLookupDao = readerBundle.createParentObjectDao(SanityOrder.class);
        readerOrderItemDao = readerBundle.createRelatedObjectDao(SanityOrderItem.class);
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

        val props = new HashMap<>(Map.of(
                "hibernate.dialect", "org.hibernate.dialect.MariaDBDialect",
                "hibernate.hbm2ddl.auto", isWriter ? "create" : "none"));
        ds.setProperties(props);

        if (!isWriter) {
            ds.setAutoCommitByDefault(false);
            ds.setDefaultTransactionIsolation(DataSourceFactory.TransactionIsolation.REPEATABLE_READ);
        }
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
