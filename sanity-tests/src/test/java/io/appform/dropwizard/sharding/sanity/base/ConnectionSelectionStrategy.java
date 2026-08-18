package io.appform.dropwizard.sharding.sanity.base;

/**
 * Strategy for selecting which connection index to return from
 * {@link ControlledConnectionDataSource#getConnection()}.
 *
 * <p>Implementations control how the connection index advances (or doesn't)
 * across successive {@code getConnection()} calls.
 *
 * @see FixedConnectionStrategy
 * @see RoundRobinConnectionStrategy
 */
public interface ConnectionSelectionStrategy {

    /**
     * Returns the connection index to use for the current {@code getConnection()} call.
     * May advance internal state (e.g., round-robin increments after each call).
     *
     * @param connectionCount total number of available connections
     * @return 0-based index into the connections list
     */
    int nextIndex(int connectionCount);

    /**
     * Returns the index that was last returned by {@link #nextIndex(int)},
     * without advancing state.
     */
    int currentIndex();

    /**
     * Resets the strategy's internal selector to the given index.
     * For fixed strategy, this sets the connection to use.
     * For round-robin, this sets the starting point for the next call.
     *
     * @param index 0-based index
     */
    void reset(int index);
}
