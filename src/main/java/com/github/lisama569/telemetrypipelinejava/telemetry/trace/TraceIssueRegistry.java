package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bounded, thread-safe registry of {@link TraceHeaderIssue}s so that header
 * handling decisions are observable and locally queryable. Bounded to avoid
 * unbounded memory growth; the dropped count is itself observable.
 */
public class TraceIssueRegistry {

    private static final int MAX_RETAINED = 1_000;

    private final ConcurrentLinkedDeque<TraceHeaderIssue> issues = new ConcurrentLinkedDeque<>();
    private final LongAdder dropped = new LongAdder();
    private final AtomicLong total = new AtomicLong();

    public void record(TraceHeaderIssue issue) {
        total.incrementAndGet();
        issues.addLast(issue);
        while (issues.size() > MAX_RETAINED) {
            if (issues.pollFirst() != null) {
                dropped.increment();
            }
        }
    }

    public List<TraceHeaderIssue> issues() {
        return List.copyOf(issues);
    }

    public long totalCount() {
        return total.get();
    }

    public long droppedCount() {
        return dropped.sum();
    }
}
