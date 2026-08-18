package io.appform.dropwizard.sharding.sanity.base;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Returns the current connection index and then advances to the next one.
 * Wraps around to 0 when the end of the connections list is reached.
 *
 * <p>Example with 3 connections:
 * <pre>
 *   call 1 → index 0  (advances to 1)
 *   call 2 → index 1  (advances to 2)
 *   call 3 → index 2  (advances to 0, wraps around)
 *   call 4 → index 0  (advances to 1)
 *   ...
 * </pre>
 *
 * <p>Use {@link #reset(int)} to set the starting index.
 */
public class RoundRobinConnectionStrategy implements ConnectionSelectionStrategy {

    private final AtomicInteger selector = new AtomicInteger(0);
    private final AtomicInteger lastReturned = new AtomicInteger(0);

    @Override
    public int nextIndex(int connectionCount) {
        int idx = selector.getAndUpdate(current -> (current + 1) % connectionCount);
        lastReturned.set(idx);
        return idx;
    }

    @Override
    public int currentIndex() {
        return lastReturned.get();
    }

    @Override
    public void reset(int index) {
        selector.set(index);
        lastReturned.set(index);
    }
}
