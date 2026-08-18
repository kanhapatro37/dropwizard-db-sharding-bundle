package io.appform.dropwizard.sharding.sanity.base;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Returns the same connection index on every {@code getConnection()} call
 * until the caller explicitly changes it via {@link #reset(int)}.
 *
 * <p>This is the original behavior: the test calls {@code useConnection(n)}
 * and all subsequent DAO operations use connection {@code n} until changed.
 */
public class FixedConnectionStrategy implements ConnectionSelectionStrategy {

    private final AtomicInteger selector = new AtomicInteger(0);

    @Override
    public int nextIndex(int connectionCount) {
        return selector.get();
    }

    @Override
    public int currentIndex() {
        return selector.get();
    }

    @Override
    public void reset(int index) {
        selector.set(index);
    }
}
