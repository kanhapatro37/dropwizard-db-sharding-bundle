package io.appform.dropwizard.sharding.sanity.base;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.ManagedDataSource;
import lombok.Getter;

/**
 * A {@link DataSourceFactory} subclass that overrides {@link #build(MetricRegistry, String)}
 * to return a pre-configured {@link ControlledConnectionDataSource} instead of creating
 * a real Tomcat JDBC connection pool.
 *
 * <p>All other DataSourceFactory properties (driver, URL, Hibernate properties like dialect)
 * are still set normally so that Hibernate configuration picks them up correctly. Only the
 * actual pool creation is bypassed.
 *
 * <p>Usage:
 * <pre>
 *   var conn1 = DriverManager.getConnection(url, user, pass);
 *   var conn2 = DriverManager.getConnection(url, user, pass);
 *   var controlledDs = new ControlledConnectionDataSource(List.of(conn1, conn2));
 *
 *   var factory = new ControlledDataSourceFactory(controlledDs);
 *   factory.setDriverClass("org.mariadb.jdbc.Driver");
 *   factory.setUrl(url);
 *   // ... set other properties as normal
 *
 *   // When bundle calls factory.build(metrics, name), it gets controlledDs back
 * </pre>
 */
public class ControlledDataSourceFactory extends DataSourceFactory {

    @Getter
    private final ControlledConnectionDataSource controlledDataSource;

    public ControlledDataSourceFactory(ControlledConnectionDataSource controlledDataSource) {
        this.controlledDataSource = controlledDataSource;
    }

    /**
     * Overrides the pool creation to return our controlled data source.
     * The bundle's {@code SessionFactoryFactory.build()} calls this method;
     * it will receive our stub instead of a real Tomcat pool.
     */
    @Override
    public ManagedDataSource build(MetricRegistry metricRegistry, String name) {
        return controlledDataSource;
    }
}
