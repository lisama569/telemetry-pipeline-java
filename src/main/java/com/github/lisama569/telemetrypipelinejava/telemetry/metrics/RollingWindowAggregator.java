package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Thread-safe rolling-window aggregator with delayable, timer-drivable window
 * finalization.
 *
 * <p>Window lifecycle per dimension:
 * <pre>
 *   OPEN (accepting on-time events)
 *     -&gt; CLOSED (rotated out or past its end; late events within
 *        {@code lateToleranceMillis} are still merged and classified)
 *     -&gt; FINALIZED (emitted to listeners exactly once, at
 *        windowEnd + {@code windowCloseDelayMillis}, driven by new events
 *        or by {@link #finalizeDue(long)} when traffic stops)
 * </pre>
 *
 * <p>Determinism: each window is finalized exactly once, in increasing window
 * index per dimension, including every on-time and tolerated-late event.
 * Windows with no events at all are emitted as empty snapshots
 * ({@code requestCount == 0}) so downstream alerts can recover during silent
 * periods; windows closed but not yet due are visible via
 * {@link #pendingCloseSnapshots()} so "no data" is never confused with
 * "not yet finalized".
 *
 * <p>Late events beyond tolerance (or for already-finalized windows) are
 * counted in {@link #lateEventCount()} and classified in the
 * {@link LateEventRegistry}; nothing is silently dropped and retention is
 * bounded.
 */
public class RollingWindowAggregator {

    private static final int MAX_COMPLETED_RETAINED = 1_000;
    private static final int MAX_EMPTY_RETAINED = 100;
    private static final int MAX_EMPTY_EMISSIONS_PER_PASS = 1_000;
    private static final int MAX_LATE_RECORDS_RETAINED = 1_000;

    private final long windowSizeMillis;
    private final int windowCount;
    private final long closeDelayMillis;
    private final long lateToleranceMillis;
    private final CardinalityLimiter limiter;
    private final LateEventRegistry lateRegistry;
    private final ConcurrentHashMap<DimensionKey, DimensionWindows> windows =
            new ConcurrentHashMap<>();
    private final List<Consumer<WindowSnapshot>> listeners = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedDeque<WindowSnapshot> completed =
            new ConcurrentLinkedDeque<>();
    /** Recent empty windows, retained separately so silence cannot evict data. */
    private final ConcurrentLinkedDeque<WindowSnapshot> completedEmpty =
            new ConcurrentLinkedDeque<>();
    private final LongAdder emptyWindowsFinalized = new LongAdder();
    private final LongAdder lateEvents = new LongAdder();
    private final LongAdder listenerErrors = new LongAdder();

    /** Backwards-compatible: immediate finalization, no late tolerance. */
    public RollingWindowAggregator(long windowSizeMillis, int windowCount,
            CardinalityLimiter limiter) {
        this(windowSizeMillis, windowCount, limiter, 0, 0,
                new LateEventRegistry(MAX_LATE_RECORDS_RETAINED));
    }

    public RollingWindowAggregator(long windowSizeMillis, int windowCount,
            CardinalityLimiter limiter, long closeDelayMillis, long lateToleranceMillis,
            LateEventRegistry lateRegistry) {
        if (windowSizeMillis <= 0 || windowCount <= 0) {
            throw new IllegalArgumentException("windowSizeMillis and windowCount must be > 0");
        }
        if (closeDelayMillis < 0 || lateToleranceMillis < 0) {
            throw new IllegalArgumentException("closeDelayMillis and lateToleranceMillis must be >= 0");
        }
        if (lateToleranceMillis > closeDelayMillis) {
            throw new IllegalArgumentException(
                    "lateToleranceMillis must be <= windowCloseDelayMillis so tolerated "
                            + "events always land before finalization");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.windowCount = windowCount;
        this.closeDelayMillis = closeDelayMillis;
        this.lateToleranceMillis = lateToleranceMillis;
        this.limiter = limiter;
        this.lateRegistry = lateRegistry;
    }

    /** Records one request event. Never throws. */
    public void record(DimensionKey dimension, long latencyMillis, boolean error,
            long nowEpochMillis) {
        DimensionKey effective = limiter.resolve(dimension);
        DimensionWindows dw = windows.computeIfAbsent(effective,
                k -> new DimensionWindows(k, windowCount));
        dw.record(latencyMillis, error, nowEpochMillis);
    }

    /**
     * Finalizes every window whose close deadline
     * ({@code windowEnd + closeDelayMillis}) has passed as of
     * {@code nowEpochMillis}, emitting empty snapshots for windows that saw
     * no events. Safe to call concurrently with {@link #record}; each window
     * is still emitted exactly once per dimension, in index order.
     */
    public void finalizeDue(long nowEpochMillis) {
        for (DimensionWindows dw : windows.values()) {
            dw.finalizeDue(nowEpochMillis);
        }
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

    /**
     * Snapshots of windows that are closed (past their end) but not yet due
     * for finalization: "not yet finalized", as opposed to "no data".
     */
    public List<WindowSnapshot> pendingCloseSnapshots() {
        List<WindowSnapshot> out = new ArrayList<>();
        for (DimensionWindows dw : windows.values()) {
            dw.collectPendingClose(out);
        }
        return List.copyOf(out);
    }

    /** Snapshots of completed windows, in emission order (bounded retention). */
    public List<WindowSnapshot> completedSnapshots() {
        return List.copyOf(completed);
    }

    /**
     * Recent finalized windows that saw no events at all ("no data this
     * period"), retained separately from data windows (bounded).
     */
    public List<WindowSnapshot> emptyWindowSnapshots() {
        return List.copyOf(completedEmpty);
    }

    /** Exact count of empty windows finalized so far. */
    public long emptyWindowCount() {
        return emptyWindowsFinalized.sum();
    }

    /** Events that arrived too late for any retained window. */
    public long lateEventCount() {
        return lateEvents.sum();
    }

    /** Number of listener invocations that failed. */
    public long listenerErrorCount() {
        return listenerErrors.sum();
    }

    /** Classification log for late arrivals (merged vs dropped). */
    public LateEventRegistry lateEventRegistry() {
        return lateRegistry;
    }

    public long windowCloseDelayMillis() {
        return closeDelayMillis;
    }

    public long lateToleranceMillis() {
        return lateToleranceMillis;
    }

    private void emit(WindowSnapshot snapshot) {
        if (snapshot.requestCount() > 0) {
            completed.addLast(snapshot);
            while (completed.size() > MAX_COMPLETED_RETAINED) {
                completed.pollFirst();
            }
        } else {
            // Empty windows prove "no data this period"; they are retained
            // separately (and counted exactly) so a long silence can never
            // evict real data windows from the queryable history.
            emptyWindowsFinalized.increment();
            completedEmpty.addLast(snapshot);
            while (completedEmpty.size() > MAX_EMPTY_RETAINED) {
                completedEmpty.pollFirst();
            }
        }
        for (Consumer<WindowSnapshot> listener : listeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException e) {
                listenerErrors.increment();
            }
        }
    }

    private void classifyLate(DimensionKey dimension, long windowIndex,
            long eventTimestamp, LateEventRegistry.Classification classification) {
        lateRegistry.record(new LateEventRegistry.LateEventRecord(dimension,
                windowIndex * windowSizeMillis, eventTimestamp,
                System.currentTimeMillis(), classification));
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
        /** Closed (past-end) windows awaiting their finalization deadline. */
        private final TreeMap<Long, Bucket> closedWindows = new TreeMap<>();
        /**
         * Finalization horizon: every window below this index has been
         * emitted. Distinct from {@link #firstSeenIndex}: the anchor only
         * says where data began, it does not finalize anything, so
         * out-of-order events for earlier windows are still mergeable.
         */
        private long nextFinalizeIndex = Long.MIN_VALUE;
        /** Lowest window index with accepted data; anchors empty emission. */
        private long firstSeenIndex = Long.MIN_VALUE;
        /** Highest window index ever opened; events below it are late. */
        private long newestIndex = Long.MIN_VALUE;
        /** Event-time watermark: highest event timestamp observed. */
        private long maxSeenEpochMillis = Long.MIN_VALUE;

        DimensionWindows(DimensionKey dimension, int size) {
            this.dimension = dimension;
            this.buckets = new Bucket[size];
            for (int i = 0; i < size; i++) {
                buckets[i] = new Bucket();
            }
        }

        synchronized void record(long latencyMillis, boolean error, long nowEpochMillis) {
            long index = nowEpochMillis / windowSizeMillis;
            maxSeenEpochMillis = Math.max(maxSeenEpochMillis, nowEpochMillis);
            Bucket bucket = buckets[(int) Math.floorMod(index, buckets.length)];
            if (bucket.windowIndex == index) {
                // On-time event for the currently open window.
                bucket.add(latencyMillis, error);
                anchorFirstSeen(index);
            } else if (nextFinalizeIndex != Long.MIN_VALUE && index < nextFinalizeIndex) {
                // Window already finalized (by rotation or the timer):
                // observable drop, never a silent re-open.
                lateEvents.increment();
                classifyLate(dimension, index, nowEpochMillis,
                        LateEventRegistry.Classification.DROPPED_BEYOND_TOLERANCE);
            } else if (index > newestIndex) {
                // Newest unfinalized window so far: rotate. Close every older
                // non-empty bucket; finalization happens at each window's
                // close deadline.
                for (Bucket b : buckets) {
                    if (b.windowIndex != Long.MIN_VALUE && b.windowIndex < index
                            && b.count > 0) {
                        closedWindows.put(b.windowIndex, copyOf(b));
                        b.reset(Long.MIN_VALUE);
                    }
                }
                bucket.reset(index);
                bucket.add(latencyMillis, error);
                newestIndex = index;
                anchorFirstSeen(index);
            } else {
                // Event for an unfinalized window older than the newest one.
                recordLate(index, latencyMillis, error, nowEpochMillis);
            }
            finalizeDueLocked(nowEpochMillis);
        }

        private void recordLate(long index, long latencyMillis, boolean error,
                long nowEpochMillis) {
            if (nextFinalizeIndex != Long.MIN_VALUE && index < nextFinalizeIndex) {
                // Window already finalized: observable drop, never silent.
                lateEvents.increment();
                classifyLate(dimension, index, nowEpochMillis,
                        LateEventRegistry.Classification.DROPPED_BEYOND_TOLERANCE);
                return;
            }
            // Lateness is measured against the event-time watermark so the
            // outcome depends only on the data, not on wall-clock scheduling.
            if (maxSeenEpochMillis - windowEnd(index) <= lateToleranceMillis) {
                // Within tolerance: merge into the (not yet finalized) window.
                Bucket closed = closedWindows.computeIfAbsent(index, i -> {
                    Bucket b = new Bucket();
                    b.reset(i);
                    return b;
                });
                closed.add(latencyMillis, error);
                anchorFirstSeen(index);
                classifyLate(dimension, index, nowEpochMillis,
                        LateEventRegistry.Classification.MERGED_WITHIN_TOLERANCE);
                return;
            }
            lateEvents.increment();
            classifyLate(dimension, index, nowEpochMillis,
                    LateEventRegistry.Classification.DROPPED_BEYOND_TOLERANCE);
        }

        synchronized void finalizeDue(long nowEpochMillis) {
            finalizeDueLocked(nowEpochMillis);
        }

        private void finalizeDueLocked(long nowEpochMillis) {
            if (firstSeenIndex == Long.MIN_VALUE) {
                return;
            }
            long maxDue = Math.floorDiv(nowEpochMillis - closeDelayMillis, windowSizeMillis) - 1;
            if (nextFinalizeIndex == Long.MIN_VALUE) {
                // Anchor the horizon only once a window is actually due;
                // anchoring earlier would wrongly reject out-of-order events
                // for windows before the current firstSeenIndex.
                if (maxDue < firstSeenIndex) {
                    return;
                }
                nextFinalizeIndex = firstSeenIndex;
            }
            if (maxDue < nextFinalizeIndex) {
                return;
            }
            // Close open buckets whose window is due (covers the no-traffic case).
            for (Bucket b : buckets) {
                if (b.windowIndex != Long.MIN_VALUE && b.windowIndex <= maxDue
                        && b.count > 0) {
                    closedWindows.put(b.windowIndex, copyOf(b));
                    b.reset(Long.MIN_VALUE);
                }
            }
            // Emit every due window holding data (bounded set), filling the
            // gaps between them with empty snapshots.
            while (!closedWindows.isEmpty() && closedWindows.firstKey() <= maxDue) {
                long idx = closedWindows.firstKey();
                emitEmptyRange(nextFinalizeIndex, idx - 1);
                emit(snapshotOf(closedWindows.remove(idx)));
                nextFinalizeIndex = Math.max(nextFinalizeIndex, idx + 1);
            }
            // Trailing empty windows up to the due horizon.
            emitEmptyRange(nextFinalizeIndex, maxDue);
            nextFinalizeIndex = maxDue + 1;
        }

        /**
         * Emits empty snapshots for [from, to]. Ancient empty windows carry no
         * information and would only flood the pipeline after a long silence
         * or clock jump, so emission is bounded to the most recent
         * {@code MAX_EMPTY_EMISSIONS_PER_PASS} windows per pass; windows with
         * actual data are never skipped.
         */
        private void emitEmptyRange(long from, long to) {
            if (to < from) {
                return;
            }
            long start = from;
            if (to - from + 1 > MAX_EMPTY_EMISSIONS_PER_PASS) {
                start = to - MAX_EMPTY_EMISSIONS_PER_PASS + 1;
            }
            for (long i = start; i <= to; i++) {
                emit(new WindowSnapshot(dimension, i * windowSizeMillis,
                        windowSizeMillis, 0, 0, 0, 0));
            }
        }

        synchronized void collectActive(List<WindowSnapshot> out) {
            for (Bucket b : buckets) {
                if (b.windowIndex != Long.MIN_VALUE && b.count > 0) {
                    out.add(snapshotOf(b));
                }
            }
        }

        synchronized void collectPendingClose(List<WindowSnapshot> out) {
            for (Map.Entry<Long, Bucket> e : closedWindows.entrySet()) {
                out.add(snapshotOf(e.getValue()));
            }
        }

        private long windowEnd(long index) {
            return (index + 1) * windowSizeMillis;
        }

        private void anchorFirstSeen(long index) {
            firstSeenIndex = firstSeenIndex == Long.MIN_VALUE
                    ? index : Math.min(firstSeenIndex, index);
        }

        private Bucket copyOf(Bucket src) {
            Bucket copy = new Bucket();
            copy.windowIndex = src.windowIndex;
            copy.count = src.count;
            copy.errors = src.errors;
            copy.totalLatency = src.totalLatency;
            copy.maxLatency = src.maxLatency;
            return copy;
        }

        private WindowSnapshot snapshotOf(Bucket b) {
            return new WindowSnapshot(dimension, b.windowIndex * windowSizeMillis,
                    windowSizeMillis, b.count, b.errors, b.totalLatency, b.maxLatency);
        }
    }
}
