package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * Independent, bounded retention for finalized empty ("no data") windows with
 * exact counters.
 *
 * <p>Empty windows are kept apart from real windows so an arbitrarily long
 * idle period can never evict a window that actually held data. Retention is
 * bounded independently; when it fills up, the oldest empty windows are
 * evicted and counted in {@link #evictedWindowCount()}, so the capacity
 * boundary is visible rather than silent. The cumulative counters
 * ({@link #totalEmptyWindowCount()}, {@link #skippedGapCount()}) are exact
 * regardless of how long traffic stays idle.
 */
public class EmptyWindowRegistry {

    /** Default bound on retained empty windows (independent of real windows). */
    public static final int DEFAULT_MAX_RETAINED = 1_000;

    private final int maxRetained;
    private final ConcurrentLinkedDeque<WindowSnapshot> empty =
            new ConcurrentLinkedDeque<>();
    private final LongAdder total = new LongAdder();
    private final LongAdder evicted = new LongAdder();
    private final LongAdder skippedGaps = new LongAdder();

    public EmptyWindowRegistry() {
        this(DEFAULT_MAX_RETAINED);
    }

    public EmptyWindowRegistry(int maxRetained) {
        if (maxRetained < 1) {
            throw new IllegalArgumentException("maxRetained must be >= 1");
        }
        this.maxRetained = maxRetained;
    }

    /**
     * Records one finalized empty window (the snapshot is expected to carry
     * requestCount=0). Evicts from bounded retention oldest-first and counts
     * each eviction. Never throws.
     */
    public void record(WindowSnapshot snapshot) {
        total.increment();
        empty.addLast(snapshot);
        while (empty.size() > maxRetained) {
            if (empty.pollFirst() != null) {
                evicted.increment();
            }
        }
    }

    /**
     * Accounts for empty window indices skipped wholesale during a large idle
     * time jump (older than the bounded synthesis span). They are never
     * synthesized one by one, but they are counted so empty-window totals stay
     * exact and the skipped capacity is visible.
     */
    public void addSkippedGap(long count) {
        if (count > 0) {
            total.add(count);
            skippedGaps.add(count);
        }
    }

    /** Retained empty windows, oldest first. */
    public List<WindowSnapshot> snapshots() {
        return List.copyOf(empty);
    }

    /** Exact number of empty windows finalized since start (never evicted). */
    public long totalEmptyWindowCount() {
        return total.sum();
    }

    /** Empty windows evicted from bounded retention (observable boundary). */
    public long evictedWindowCount() {
        return evicted.sum();
    }

    /** Empty window indices skipped wholesale during large idle jumps. */
    public long skippedGapCount() {
        return skippedGaps.sum();
    }

    /** Retention bound for empty windows. */
    public int maxRetained() {
        return maxRetained;
    }
}
