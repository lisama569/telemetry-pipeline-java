package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * Queryable, bounded classification log for events that arrive after their
 * window has closed. Merged-within-tolerance and dropped-beyond-tolerance
 * events each leave a record; per-classification counters are exact, record
 * retention is bounded so nothing accumulates without limit.
 */
public class LateEventRegistry {

    /** How a late-arriving event was classified. */
    public enum Classification {
        /** Arrived within the configured tolerance and was merged into its window. */
        MERGED_WITHIN_TOLERANCE,
        /** Arrived after the tolerance/window finalization; not merged. */
        DROPPED_BEYOND_TOLERANCE
    }

    /** One classified late event. */
    public record LateEventRecord(DimensionKey dimension, long windowStartEpochMillis,
            long eventTimestampEpochMillis, long arrivalTimestampEpochMillis,
            Classification classification) {
    }

    private final int maxRetained;
    private final ConcurrentLinkedDeque<LateEventRecord> records =
            new ConcurrentLinkedDeque<>();
    private final LongAdder merged = new LongAdder();
    private final LongAdder dropped = new LongAdder();

    public LateEventRegistry(int maxRetained) {
        if (maxRetained < 1) {
            throw new IllegalArgumentException("maxRetained must be >= 1");
        }
        this.maxRetained = maxRetained;
    }

    /** Records one classification. Never throws. */
    public void record(LateEventRecord rec) {
        if (rec.classification() == Classification.MERGED_WITHIN_TOLERANCE) {
            merged.increment();
        } else {
            dropped.increment();
        }
        records.addLast(rec);
        while (records.size() > maxRetained) {
            records.pollFirst();
        }
    }

    /** Bounded list of recent classifications, oldest first. */
    public List<LateEventRecord> records() {
        return List.copyOf(records);
    }

    public long mergedCount() {
        return merged.sum();
    }

    public long droppedCount() {
        return dropped.sum();
    }
}
