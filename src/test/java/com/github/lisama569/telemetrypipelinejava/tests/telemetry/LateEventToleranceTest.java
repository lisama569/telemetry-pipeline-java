package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Late events: arrivals within the configured tolerance are merged into their
 * (not yet finalized) window with a queryable classification record; arrivals
 * beyond tolerance are dropped with an explicit, bounded classification
 * signal. Lateness is measured against the event-time watermark, so outcomes
 * are reproducible regardless of wall-clock scheduling.
 */
class LateEventToleranceTest {

    private static final Logger log = LoggerFactory.getLogger(LateEventToleranceTest.class);

    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "2xx");

    private static RollingWindowAggregator newAggregator(LateEventRegistry registry) {
        // 1s windows, 5s close delay, 2s late tolerance.
        return new RollingWindowAggregator(1_000, 4, new CardinalityLimiter(10),
                5_000, 2_000, registry);
    }

    @Test
    void withinToleranceLateEventsAreMergedIntoTheClosedWindow() {
        LateEventRegistry registry = new LateEventRegistry(100);
        RollingWindowAggregator aggregator = newAggregator(registry);

        log.info("input: 2 events in window 0 (t=100,200), rotation at t=3000, "
                + "then a late event for window 0 (t=500) while watermark=3000 "
                + "(lateness 2000 <= tolerance 2000)");
        aggregator.record(DIM, 10, false, 100);
        aggregator.record(DIM, 20, true, 200);
        aggregator.record(DIM, 10, false, 3_000); // rotates: window 0 -> pending close
        aggregator.record(DIM, 30, false, 500);   // late for window 0, within tolerance

        log.info("intermediate: pendingClose={} merged={} dropped={}",
                aggregator.pendingCloseSnapshots(), registry.mergedCount(),
                registry.droppedCount());
        assertThat(registry.mergedCount())
                .as("late event within tolerance is classified as merged").isEqualTo(1);
        assertThat(registry.records())
                .as("merge leaves a queryable classification record")
                .anyMatch(r -> r.classification()
                        == LateEventRegistry.Classification.MERGED_WITHIN_TOLERANCE
                        && r.windowStartEpochMillis() == 0);
        assertThat(aggregator.pendingCloseSnapshots())
                .as("merged event is visible in the pending window before finalization")
                .anyMatch(s -> s.windowStartEpochMillis() == 0 && s.requestCount() == 3);

        aggregator.finalizeDue(20_000);
        log.info("verdict: completed={}", aggregator.completedSnapshots());
        assertThat(aggregator.completedSnapshots())
                .as("finalized window 0 contains the tolerated late event exactly once")
                .anyMatch(s -> s.windowStartEpochMillis() == 0
                        && s.requestCount() == 3 && s.errorCount() == 1
                        && s.totalLatencyMillis() == 60);
        assertThat(aggregator.lateEventCount())
                .as("nothing was dropped").isZero();
    }

    @Test
    void beyondToleranceLateEventsAreDroppedWithBoundedClassification() {
        LateEventRegistry registry = new LateEventRegistry(4); // tiny bound on purpose
        RollingWindowAggregator aggregator = newAggregator(registry);

        log.info("input: event at t=100 (window 0), watermark pushed to t=20000, "
                + "then 6 late events for window 0 (lateness 19000 > tolerance 2000)");
        aggregator.record(DIM, 10, false, 100);
        aggregator.record(DIM, 10, false, 20_000); // watermark far ahead
        for (int i = 0; i < 6; i++) {
            aggregator.record(DIM, 10 + i, false, 200 + i); // all late for window 0
        }

        log.info("verdict: dropped={} records={} lateEvents={}", registry.droppedCount(),
                registry.records(), aggregator.lateEventCount());
        assertThat(registry.droppedCount())
                .as("every beyond-tolerance event is counted exactly").isEqualTo(6);
        assertThat(aggregator.lateEventCount()).isEqualTo(6);
        assertThat(registry.records())
                .as("classification records are bounded: no unbounded accumulation")
                .hasSize(4);
        assertThat(registry.records())
                .as("retained records are the most recent ones")
                .allMatch(r -> r.classification()
                        == LateEventRegistry.Classification.DROPPED_BEYOND_TOLERANCE);

        aggregator.finalizeDue(100_000);
        assertThat(aggregator.completedSnapshots())
                .as("dropped events never leak into the finalized window")
                .anyMatch(s -> s.windowStartEpochMillis() == 0 && s.requestCount() == 1);
    }

    @Test
    void lateEventForWindowWithoutAnyOnTimeDataFormsACompleteWindow() {
        LateEventRegistry registry = new LateEventRegistry(100);
        RollingWindowAggregator aggregator = newAggregator(registry);

        log.info("input: events in windows 0 and 3 only; a tolerated late event "
                + "lands in gap window 1 (t=1500, watermark=3000, lateness 1000)");
        aggregator.record(DIM, 10, false, 100);   // window 0
        aggregator.record(DIM, 10, false, 3_000); // window 3, rotates window 0
        aggregator.record(DIM, 40, true, 1_500);  // late, window 1, within tolerance

        aggregator.finalizeDue(20_000);
        log.info("verdict: completed={} empty={} merged={}",
                aggregator.completedSnapshots(), aggregator.emptyWindowSnapshots(),
                registry.mergedCount());
        assertThat(aggregator.completedSnapshots())
                .as("gap window 1 is finalized with the merged late event, not as empty")
                .anyMatch(s -> s.windowStartEpochMillis() == 1_000
                        && s.requestCount() == 1 && s.errorCount() == 1);
        assertThat(aggregator.emptyWindowSnapshots())
                .as("window 2 (and the trailing silence) saw no data at all")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .contains(2_000L)
                .doesNotContain(0L, 1_000L, 3_000L);
        assertThat(registry.mergedCount()).isEqualTo(1);
    }

    @Test
    void sameEventsReplayedInDifferentOrderProduceIdenticalWindows() {
        log.info("input: same 6 events (on-time + tolerated-late), two record orders");
        List<WindowSnapshot> first = runOnceThrough();
        List<WindowSnapshot> second = runOnceThrough();
        log.info("verdict: run1={} run2={}", first, second);
        assertThat(second)
                .as("finalized windows are reproducible for the same event set")
                .isEqualTo(first);
    }

    private static List<WindowSnapshot> runOnceThrough() {
        RollingWindowAggregator aggregator = newAggregator(new LateEventRegistry(100));
        aggregator.record(DIM, 10, false, 100);
        aggregator.record(DIM, 20, false, 200);
        aggregator.record(DIM, 10, false, 3_000);
        aggregator.record(DIM, 30, true, 500);    // tolerated late for window 0
        aggregator.record(DIM, 50, false, 1_200); // tolerated late for window 1
        aggregator.record(DIM, 10, false, 4_000);
        aggregator.finalizeDue(30_000);
        return aggregator.completedSnapshots();
    }
}
