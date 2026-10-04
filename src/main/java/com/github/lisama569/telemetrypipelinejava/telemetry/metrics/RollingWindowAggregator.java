package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry.Classification;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Thread-safe rolling-window aggregator with a three-stage window lifecycle:
 *
 * <pre>
 *   ACTIVE --(window end + finalization delay passed)--> OPEN
 *   OPEN   --(grace bound passed)----------------------> CLOSED
 * </pre>
 *
 * <p>ACTIVE buckets live in a per-dimension ring and only ever increase
 * (monotonic). A window is finalized either by rotation (a newer event
 * arrives) or by {@link #finalizeDue(long)} (idle traffic), whichever comes
 * first; finalization moves it to the grace-open set and notifies
 * {@link #addWindowListener(Consumer) window listeners} exactly once. While
 * OPEN, late events still merge into the window and are audited in the
 * {@link LateEventRegistry}; once the grace bound passes the window is
 * CLOSED, appended to the bounded completed retention and released to
 * {@link #addCloseListener(Consumer) close listeners}.
 *
 * <p>Closed windows are released in canonical (windowStart, dimension) order
 * via a watermark: a closed window is released only when every known
 * dimension has closed all older windows, so the stream seen by close
 * listeners is independent of arrival order and thread scheduling. Events
 * too old for any retained or grace-open window are counted in
 * {@link #lateEventCount()} and audited as rejected instead of being silently
 * dropped. Listener failures are counted in {@link #listenerErrorCount()} and
 * never propagate.
 */
public class RollingWindowAggregator {

    private static final int MAX_COMPLETED_RETAINED = 1_000;

    private final long windowSizeMillis;
    private final int windowCount;
    private final CardinalityLimiter limiter;
    private final long finalizationDelayMillis;
    private final long lateGraceMillis;
    private final LateEventRegistry lateEventRegistry;
    private final ConcurrentHashMap<DimensionKey, DimensionWindows> windows =
            new ConcurrentHashMap<>();
    private final List<Consumer<WindowSnapshot>> listeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<WindowSnapshot>> closeListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedDeque<WindowSnapshot> completed =
            new ConcurrentLinkedDeque<>();
    private final LongAdder lateEvents = new LongAdder();
    private final LongAdder listenerErrors = new LongAdder();

    /** Closed windows waiting for the release watermark; guarded by releaseLock. */
    private final Object releaseLock = new Object();
    private final TreeMap<Long, List<WindowSnapshot>> releaseBuffer = new TreeMap<>();
    private long releaseHorizon = Long.MIN_VALUE;

    public RollingWindowAggregator(long windowSizeMillis, int windowCount,
            CardinalityLimiter limiter) {
        this(windowSizeMillis, windowCount, limiter, 0, 0, new LateEventRegistry());
    }

    public RollingWindowAggregator(long windowSizeMillis, int windowCount,
            CardinalityLimiter limiter, long finalizationDelayMillis,
            long lateGraceMillis, LateEventRegistry lateEventRegistry) {
        if (windowSizeMillis <= 0 || windowCount <= 0) {
            throw new IllegalArgumentException("windowSizeMillis and windowCount must be > 0");
        }
        if (finalizationDelayMillis < 0 || lateGraceMillis < 0) {
            throw new IllegalArgumentException("finalizationDelayMillis and lateGraceMillis must be >= 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.windowCount = windowCount;
        this.limiter = limiter;
        this.finalizationDelayMillis = finalizationDelayMillis;
        this.lateGraceMillis = lateGraceMillis;
        this.lateEventRegistry = lateEventRegistry;
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
     * Finalizes every window whose end plus the configured finalization delay
     * has passed as of {@code nowEpochMillis} — including synthesized empty
     * windows for idle gaps — and closes grace-open windows whose grace bound
     * has passed. Driven periodically so idle windows complete without traffic.
     */
    public void finalizeDue(long nowEpochMillis) {
        long maxDueIndex = Math.floorDiv(nowEpochMillis - finalizationDelayMillis,
                windowSizeMillis) - 1;
        for (DimensionWindows dw : windows.values()) {
            dw.finalizeUpTo(maxDueIndex, nowEpochMillis);
        }
    }

    /** Registers a listener for finalized windows (emitted exactly once each). */
    public void addWindowListener(Consumer<WindowSnapshot> listener) {
        listeners.add(listener);
    }

    /**
     * Registers a listener for closed windows. Closed windows are released in
     * canonical (windowStart, dimension) order, independent of arrival order.
     */
    public void addCloseListener(Consumer<WindowSnapshot> listener) {
        closeListeners.add(listener);
    }

    /** Snapshots of currently active (in-progress, non-empty) windows. */
    public List<WindowSnapshot> activeSnapshots(long nowEpochMillis) {
        List<WindowSnapshot> out = new ArrayList<>();
        for (DimensionWindows dw : windows.values()) {
            dw.collectActive(out);
        }
        return List.copyOf(out);
    }

    /** Snapshots of finalized-but-still-grace-open windows (late events may still merge). */
    public List<WindowSnapshot> openSnapshots() {
        List<WindowSnapshot> out = new ArrayList<>();
        for (DimensionWindows dw : windows.values()) {
            dw.collectOpen(out);
        }
        return List.copyOf(out);
    }

    /** Snapshots of closed windows, in release order (bounded retention). */
    public List<WindowSnapshot> completedSnapshots() {
        return List.copyOf(completed);
    }

    /** Events that arrived too late for any retained or grace-open window. */
    public long lateEventCount() {
        return lateEvents.sum();
    }

    /** Audit trail and counters for late-event classifications. */
    public LateEventRegistry lateEventRegistry() {
        return lateEventRegistry;
    }

    /** Number of listener invocations that failed. */
    public long listenerErrorCount() {
        return listenerErrors.sum();
    }

    private void emitFinalized(WindowSnapshot snapshot) {
        for (Consumer<WindowSnapshot> listener : listeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException e) {
                listenerErrors.increment();
            }
        }
    }

    private void emitClosed(WindowSnapshot snapshot) {
        completed.addLast(snapshot);
        while (completed.size() > MAX_COMPLETED_RETAINED) {
            completed.pollFirst();
        }
        for (Consumer<WindowSnapshot> listener : closeListeners) {
            try {
                listener.accept(snapshot);
            } catch (RuntimeException e) {
                listenerErrors.increment();
            }
        }
    }

    /** Buffers a closed window and releases everything below the watermark. */
    private void enqueueClosed(WindowSnapshot snapshot) {
        long index = snapshot.windowStartEpochMillis() / windowSizeMillis;
        List<WindowSnapshot> releasable = new ArrayList<>();
        synchronized (releaseLock) {
            releaseBuffer.computeIfAbsent(index, k -> new ArrayList<>()).add(snapshot);
            long minFrontier = Long.MAX_VALUE;
            for (DimensionWindows dw : windows.values()) {
                minFrontier = Math.min(minFrontier, dw.closedFrontier());
            }
            if (minFrontier != Long.MAX_VALUE && minFrontier > releaseHorizon) {
                releaseHorizon = minFrontier;
            }
            while (!releaseBuffer.isEmpty()
                    && releaseBuffer.firstKey() < releaseHorizon) {
                List<WindowSnapshot> batch = releaseBuffer.pollFirstEntry().getValue();
                batch.sort(Comparator.comparing((WindowSnapshot s) -> s.dimension().route())
                        .thenComparing(s -> s.dimension().statusClass()));
                releasable.addAll(batch);
            }
        }
        for (WindowSnapshot s : releasable) {
            emitClosed(s);
        }
    }

    /** Mutable counters for one active time bucket. */
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

    /** A finalized window still open for late merges until its grace bound. */
    private static final class OpenWindow {
        final long index;
        final long closeTimeEpochMillis;
        long count;
        long errors;
        long totalLatency;
        long maxLatency;

        OpenWindow(long index, long count, long errors, long totalLatency, long maxLatency,
                long closeTimeEpochMillis) {
            this.index = index;
            this.count = count;
            this.errors = errors;
            this.totalLatency = totalLatency;
            this.maxLatency = maxLatency;
            this.closeTimeEpochMillis = closeTimeEpochMillis;
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

    /** Per-dimension ring of buckets plus grace-open windows; mutations serialized. */
    private final class DimensionWindows {
        private final DimensionKey dimension;
        private final Bucket[] buckets;
        private final Map<Long, OpenWindow> open = new HashMap<>();
        /** Indices already finalized (bounded to ring reach + grace span). */
        private final Set<Long> finalized = new HashSet<>();
        private long maxSeenIndex = Long.MIN_VALUE;
        private long nextFinalizeIndex = Long.MIN_VALUE;
        private volatile long closedFrontier = Long.MIN_VALUE;

        DimensionWindows(DimensionKey dimension, int size) {
            this.dimension = dimension;
            this.buckets = new Bucket[size];
            for (int i = 0; i < size; i++) {
                buckets[i] = new Bucket();
            }
        }

        long closedFrontier() {
            return closedFrontier;
        }

        synchronized void record(long latencyMillis, boolean error, long nowEpochMillis) {
            long index = nowEpochMillis / windowSizeMillis;
            if (index > maxSeenIndex) {
                maxSeenIndex = index;
            }
            if (nextFinalizeIndex == Long.MIN_VALUE) {
                // Windows before the first event never exist for this dimension.
                nextFinalizeIndex = index;
                closedFrontier = index;
            }
            // 1. Finalized but still grace-open: merge (audited).
            OpenWindow ow = open.get(index);
            if (ow != null) {
                if (nowEpochMillis <= ow.closeTimeEpochMillis) {
                    ow.add(latencyMillis, error);
                    lateEventRegistry.record(dimension, index * windowSizeMillis,
                            Classification.MERGED_INTO_OPEN_WINDOW, nowEpochMillis,
                            nowEpochMillis);
                } else {
                    rejectLate(index, nowEpochMillis);
                }
                return;
            }
            // 2. Beyond ring reach plus grace span: reject (audited, bounded).
            if (index < reachFloor()) {
                rejectLate(index, nowEpochMillis);
                return;
            }
            // 3. Already finalized and closed: reject (audited).
            if (finalized.contains(index)) {
                rejectLate(index, nowEpochMillis);
                return;
            }
            Bucket bucket = buckets[(int) Math.floorMod(index, buckets.length)];
            if (bucket.windowIndex == index) {
                bucket.add(latencyMillis, error);
                return;
            }
            if (bucket.windowIndex > index) {
                // Ring slot reused by a newer window before this one existed.
                rejectLate(index, nowEpochMillis);
                return;
            }
            // Rotate: finalize every older pending window (content or
            // synthesized empty). An out-of-order event may open a window
            // behind the finalization frontier; pull the frontier back so the
            // window is not forfeited.
            finalizeUpTo(index - 1, nowEpochMillis);
            if (index < nextFinalizeIndex) {
                nextFinalizeIndex = index;
                if (closedFrontier > index) {
                    closedFrontier = index;
                }
            }
            bucket.reset(index);
            bucket.add(latencyMillis, error);
        }

        /** Oldest window index still reachable by the ring plus grace span. */
        private long reachFloor() {
            long graceIndices =
                    (finalizationDelayMillis + lateGraceMillis) / windowSizeMillis + 1;
            return maxSeenIndex - windowCount - graceIndices;
        }

        private void rejectLate(long index, long nowEpochMillis) {
            lateEvents.increment();
            lateEventRegistry.record(dimension, index * windowSizeMillis,
                    Classification.REJECTED_BEYOND_GRACE, nowEpochMillis,
                    nowEpochMillis);
        }

        /**
         * Finalizes all windows up to {@code maxIndex} (synthesizing bounded
         * empty windows for idle gaps) and closes grace-open windows whose
         * grace bound has passed as of {@code nowEpochMillis}.
         */
        synchronized void finalizeUpTo(long maxIndex, long nowEpochMillis) {
            long nowIndex = Math.floorDiv(nowEpochMillis, windowSizeMillis);
            if (nowIndex > maxSeenIndex) {
                maxSeenIndex = nowIndex;
            }
            if (nextFinalizeIndex != Long.MIN_VALUE && nextFinalizeIndex <= maxIndex) {
                // Empty-window synthesis is bounded to the most recent
                // windowCount indices; older idle gaps are skipped, never
                // emitted one by one.
                long emptyFloor = maxIndex - windowCount + 1;
                for (long idx = nextFinalizeIndex; idx <= maxIndex; idx++) {
                    if (finalized.contains(idx)) {
                        continue; // finalized before a frontier pull-back
                    }
                    Bucket b = buckets[(int) Math.floorMod(idx, buckets.length)];
                    if (b.windowIndex == idx) {
                        finalizeWindow(idx, b.count, b.errors, b.totalLatency,
                                b.maxLatency);
                        b.reset(Long.MIN_VALUE);
                    } else if (idx >= emptyFloor) {
                        finalizeWindow(idx, 0, 0, 0, 0);
                    }
                }
                nextFinalizeIndex = maxIndex + 1;
            }
            finalized.removeIf(i -> i < reachFloor());
            closeDueOpen(nowEpochMillis);
        }

        private void finalizeWindow(long index, long count, long errors,
                long totalLatency, long maxLatency) {
            long closeTime = (index + 1) * windowSizeMillis
                    + finalizationDelayMillis + lateGraceMillis;
            OpenWindow ow = new OpenWindow(index, count, errors, totalLatency,
                    maxLatency, closeTime);
            open.put(index, ow);
            finalized.add(index);
            emitFinalized(snapshotOf(ow));
        }

        private void closeDueOpen(long nowEpochMillis) {
            var it = open.entrySet().iterator();
            while (it.hasNext()) {
                OpenWindow ow = it.next().getValue();
                if (ow.closeTimeEpochMillis <= nowEpochMillis) {
                    it.remove();
                    if (closedFrontier < ow.index + 1) {
                        closedFrontier = ow.index + 1;
                    }
                    enqueueClosed(snapshotOf(ow));
                }
            }
        }

        synchronized void collectActive(List<WindowSnapshot> out) {
            for (Bucket b : buckets) {
                if (b.windowIndex != Long.MIN_VALUE && b.count > 0) {
                    out.add(new WindowSnapshot(dimension, b.windowIndex * windowSizeMillis,
                            windowSizeMillis, b.count, b.errors, b.totalLatency,
                            b.maxLatency));
                }
            }
        }

        synchronized void collectOpen(List<WindowSnapshot> out) {
            for (OpenWindow ow : open.values()) {
                out.add(snapshotOf(ow));
            }
        }

        private WindowSnapshot snapshotOf(OpenWindow ow) {
            return new WindowSnapshot(dimension, ow.index * windowSizeMillis,
                    windowSizeMillis, ow.count, ow.errors, ow.totalLatency,
                    ow.maxLatency);
        }
    }
}
