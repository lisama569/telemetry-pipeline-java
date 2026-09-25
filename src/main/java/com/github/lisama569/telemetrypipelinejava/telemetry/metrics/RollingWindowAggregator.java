package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Thread-safe rolling-window aggregator.
 *
 * <p>Each dimension owns a ring of fixed-size, time-aligned buckets. Writes to
 * the current bucket only ever increase counters (monotonic). When an event
 * arrives for a newer window, all older non-empty buckets are snapshotted and
 * emitted to listeners exactly once before reuse, so window rotation never
 * loses data. Events too old for any retained bucket are counted in
 * {@link #lateEventCount()} instead of being silently dropped. Listener
 * failures are counted in {@link #listenerErrorCount()} and never propagate.
 */
public class RollingWindowAggregator {

    private static final int MAX_COMPLETED_RETAINED = 1_000;

    private final long windowSizeMillis;
    private final int windowCount;
    private final CardinalityLimiter limiter;
    private final ConcurrentHashMap<DimensionKey, DimensionWindows> windows =
            new ConcurrentHashMap<>();
    private final List<Consumer<WindowSnapshot>> listeners = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedDeque<WindowSnapshot> completed =
            new ConcurrentLinkedDeque<>();
    private final LongAdder lateEvents = new LongAdder();
    private final LongAdder listenerErrors = new LongAdder();

    public RollingWindowAggregator(long windowSizeMillis, int windowCount,
            CardinalityLimiter limiter) {
        if (windowSizeMillis <= 0 || windowCount <= 0) {
            throw new IllegalArgumentException("windowSizeMillis and windowCount must be > 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.windowCount = windowCount;
        this.limiter = limiter;
    }

    /** Records one request event. Never throws. */
    public void record(DimensionKey dimension, long latencyMillis, boolean error,
            long nowEpochMillis) {
        DimensionKey effective = limiter.resolve(dimension);
        DimensionWindows dw = windows.computeIfAbsent(effective,
                k -> new DimensionWindows(k, windowCount));
        dw.record(latencyMillis, error, nowEpochMillis);
    }

    /** Registers a listener for completed windows. */
    public void addWindowListener(Consumer<WindowSnapshot> listener) {
        listeners.add(listener);
    }

    /** Snapshots of currently active (in-progress, non-empty) windows. */
    public List<WindowSnapshot> activeSnapshots(long nowEpochMillis) {
        List<WindowSnapshot> out = new ArrayList<>();
        for (DimensionWindows dw : windows.values()) {
            dw.collectActive(out);
        }
        return List.copyOf(out);
    }

    /** Snapshots of completed windows, in emission order (bounded retention). */
    public List<WindowSnapshot> completedSnapshots() {
        return List.copyOf(completed);
    }

    /** Events that arrived too late for any retained window. */
    public long lateEventCount() {
        return lateEvents.sum();
    }

    /** Number of listener invocations that failed. */
    public long listenerErrorCount() {
        return listenerErrors.sum();
    }

    private void emit(WindowSnapshot snapshot) {
        completed.addLast(snapshot);
        while (completed.size() > MAX_COMPLETED_RETAINED) {
            completed.pollFirst();
        }
        for (Consumer<WindowSnapshot> listener : listeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException e) {
                listenerErrors.increment();
            }
        }
    }

    /** Mutable counters for one time bucket. */
    private static final class Bucket {
        long windowIndex = Long.MIN_VALUE;
        long count;
        long errors;
        long totalLatency;
        long maxLatency;

        void reset(long index) {
            windowIndex = index;
            count = 0;
            errors = 0;
            totalLatency = 0;
            maxLatency = 0;
        }

        void add(long latencyMillis, boolean error) {
            count++;
            if (error) {
                errors++;
            }
            totalLatency += latencyMillis;
            maxLatency = Math.max(maxLatency, latencyMillis);
        }
    }

    /** Per-dimension ring of buckets; all mutations serialized per dimension. */
    private final class DimensionWindows {
        private final DimensionKey dimension;
        private final Bucket[] buckets;

        DimensionWindows(DimensionKey dimension, int size) {
            this.dimension = dimension;
            this.buckets = new Bucket[size];
            for (int i = 0; i < size; i++) {
                buckets[i] = new Bucket();
            }
        }

        synchronized void record(long latencyMillis, boolean error, long nowEpochMillis) {
            long index = nowEpochMillis / windowSizeMillis;
            Bucket bucket = buckets[(int) Math.floorMod(index, buckets.length)];
            if (bucket.windowIndex == index) {
                bucket.add(latencyMillis, error);
                return;
            }
            if (bucket.windowIndex > index) {
                // Too old for its ring slot: observable, not silently dropped.
                lateEvents.increment();
                return;
            }
            // Rotate: emit every older non-empty bucket exactly once.
            for (Bucket b : buckets) {
                if (b.windowIndex != Long.MIN_VALUE && b.windowIndex < index && b.count > 0) {
                    emit(snapshotOf(b));
                    b.reset(Long.MIN_VALUE);
                }
            }
            bucket.reset(index);
            bucket.add(latencyMillis, error);
        }

        synchronized void collectActive(List<WindowSnapshot> out) {
            for (Bucket b : buckets) {
                if (b.windowIndex != Long.MIN_VALUE && b.count > 0) {
                    out.add(snapshotOf(b));
                }
            }
        }

        private WindowSnapshot snapshotOf(Bucket b) {
            return new WindowSnapshot(dimension, b.windowIndex * windowSizeMillis,
                    windowSizeMillis, b.count, b.errors, b.totalLatency, b.maxLatency);
        }
    }
}
