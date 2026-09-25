package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bounds the number of active dimensions. Once the limit is reached, events for
 * unseen dimensions are deterministically folded into {@link DimensionKey#OVERFLOW}
 * (never dropped) and counted in {@link #overflowedEventCount()} as an
 * observable signal. Already-known dimensions keep their own buckets.
 */
public class CardinalityLimiter {

    private final int maxDimensions;
    private final Set<DimensionKey> known = ConcurrentHashMap.newKeySet();
    private final LongAdder overflowedEvents = new LongAdder();

    public CardinalityLimiter(int maxDimensions) {
        if (maxDimensions < 1) {
            throw new IllegalArgumentException("maxDimensions must be >= 1");
        }
        this.maxDimensions = maxDimensions;
    }

    /** Resolves the effective dimension; may return {@link DimensionKey#OVERFLOW}. */
    public DimensionKey resolve(DimensionKey requested) {
        if (DimensionKey.OVERFLOW.equals(requested)) {
            return requested;
        }
        if (known.contains(requested)) {
            return requested;
        }
        // Admission is serialized so the bound holds strictly under concurrency.
        synchronized (known) {
            if (known.contains(requested)) {
                return requested;
            }
            if (known.size() < maxDimensions) {
                known.add(requested);
                return requested;
            }
        }
        overflowedEvents.increment();
        return DimensionKey.OVERFLOW;
    }

    /** Number of events folded into the overflow bucket so far. */
    public long overflowedEventCount() {
        return overflowedEvents.sum();
    }

    public int activeDimensionCount() {
        return known.size();
    }

    public int maxDimensions() {
        return maxDimensions;
    }
}
