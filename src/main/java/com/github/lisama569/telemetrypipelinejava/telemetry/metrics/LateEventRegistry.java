package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bounded, queryable audit trail for events that arrived after their window
 * was finalized. Each record is classified as either merged into the still
 * grace-open window or rejected as beyond the grace bound. Retention is
 * bounded; overflow is counted, never silent.
 */
public class LateEventRegistry {

    /** How a late event was disposed of. */
    public enum Classification {
        /** Folded into its finalized-but-still-grace-open window. */
        MERGED_INTO_OPEN_WINDOW,
        /** Window already closed (or never existed): beyond the grace bound. */
        REJECTED_BEYOND_GRACE
    }

    /** One classified late-event decision. */
    public record LateEventRecord(
            DimensionKey dimension,
            long windowStartEpochMillis,
            Classification classification,
            long eventEpochMillis,
            long recordedAtEpochMillis) {
    }

    private static final int MAX_RECORDS = 1_000;

    private final ConcurrentLinkedDeque<LateEventRecord> records =
            new ConcurrentLinkedDeque<>();
    private final LongAdder merged = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder droppedRecords = new LongAdder();

    /** Records one classification decision. Never throws. */
    public void record(DimensionKey dimension, long windowStartEpochMillis,
            Classification classification, long eventEpochMillis,
            long recordedAtEpochMillis) {
        if (classification == Classification.MERGED_INTO_OPEN_WINDOW) {
            merged.increment();
        } else {
            rejected.increment();
        }
        records.addLast(new LateEventRecord(dimension, windowStartEpochMillis,
                classification, eventEpochMillis, recordedAtEpochMillis));
        while (records.size() > MAX_RECORDS) {
            if (records.pollFirst() != null) {
                droppedRecords.increment();
            }
        }
    }

    /** Bounded list of recent classification records, oldest first. */
    public List<LateEventRecord> records() {
        return List.copyOf(records);
    }

    public long mergedCount() {
        return merged.sum();
    }

    public long rejectedCount() {
        return rejected.sum();
    }

    /** Records evicted because retention was full (observable, bounded). */
    public long droppedRecordCount() {
        return droppedRecords.sum();
    }
}
