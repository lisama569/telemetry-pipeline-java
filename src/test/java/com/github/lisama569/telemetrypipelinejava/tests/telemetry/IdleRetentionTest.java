package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.EmptyWindowRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-idle retention: real windows survive arbitrary empty-window floods in
 * an independent retention; empty windows are counted exactly with a visible
 * capacity boundary; the three in-progress window states are separately
 * queryable.
 */
class IdleRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(IdleRetentionTest.class);

    private static final long WINDOW = 1_000;
    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "5xx");

    @Test
    void realWindowsSurviveArbitrarilyLongIdlePeriod() {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, 0, new LateEventRegistry());

        long idleWindows = 2_500; // far beyond the 1000-slot empty retention
        log.info("input: real traffic in window 0 (2 events) and window 1 (1 event),"
                + " then the scheduler ticks through {} idle windows (empty retention=1000)",
                idleWindows);
        aggregator.record(DIM, 10, true, 100);
        aggregator.record(DIM, 20, false, 200);
        aggregator.record(DIM, 30, true, 1_100);

        // Drive finalization one tick per window boundary, exactly as the
        // scheduler does while the app sits idle: each tick materializes the
        // single due empty window rather than skipping it wholesale.
        for (long w = 2; w < 2 + idleWindows; w++) {
            aggregator.finalizeDue((w + 1) * WINDOW);
        }

        List<WindowSnapshot> real = aggregator.completedSnapshots();
        List<WindowSnapshot> empty = aggregator.emptyWindowSnapshots();
        long emptyTotal = aggregator.emptyWindowRegistry().totalEmptyWindowCount();
        log.info("intermediate: after {} idle ticks realWindows={} emptyRetained={}"
                + " emptyTotal={} emptyEvicted={}", idleWindows, real.size(), empty.size(),
                emptyTotal, aggregator.emptyWindowRegistry().evictedWindowCount());

        assertThat(real)
                .as("both windows that held data are still queryable after a long idle")
                .hasSize(2);
        assertThat(real.get(0).windowStartEpochMillis())
                .as("real windows keep canonical order: window 0 first").isZero();
        assertThat(real.get(0).requestCount())
                .as("window 0 counts are intact (2 events, 1 error)").isEqualTo(2);
        assertThat(real.get(0).errorCount()).isEqualTo(1);
        assertThat(real.get(1).windowStartEpochMillis())
                .as("window 1 is the second retained real window").isEqualTo(1_000);
        assertThat(real.get(1).requestCount())
                .as("window 1 counts are intact (1 event)").isEqualTo(1);
        assertThat(real)
                .as("the real retention is never polluted with empty windows")
                .allSatisfy(s -> assertThat(s.requestCount()).isPositive());

        assertThat(empty)
                .as("empty retention is bounded independently at 1000").hasSize(1_000);
        assertThat(empty)
                .as("every retained empty window really carries no data")
                .allSatisfy(s -> assertThat(s.requestCount()).isZero());
        assertThat(emptyTotal)
                .as("total empty windows since traffic stopped is counted exactly")
                .isEqualTo(idleWindows);
        assertThat(aggregator.emptyWindowRegistry().evictedWindowCount())
                .as("empty windows beyond retention were evicted, never pushed real ones out")
                .isEqualTo(idleWindows - 1_000);
        assertThat(aggregator.completedWindowEvictedCount())
                .as("no real window was ever evicted by the empty-window flood").isZero();
        log.info("verdict: realWindowsSurvived={} emptyEvicted={} realEvicted={}",
                real.size(), aggregator.emptyWindowRegistry().evictedWindowCount(),
                aggregator.completedWindowEvictedCount());
    }

    @Test
    void emptyWindowsAreCountedExactlyAndEvictionBoundaryIsVisible() {
        int emptyBound = 10;
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, 0, new LateEventRegistry(),
                new EmptyWindowRegistry(emptyBound));

        // Phase 1: traffic stops, scheduler ticks 13 windows through. With an
        // independent empty bound of 10, 13 materialized empties leave 10
        // retained and 3 evicted.
        aggregator.record(DIM, 10, true, 100);
        log.info("input: 1 real event in window 0, independent empty retention bound={},"
                + " then 13 idle ticks, then one big 50-window clock jump", emptyBound);
        for (long w = 1; w <= 13; w++) {
            aggregator.finalizeDue((w + 1) * WINDOW);
        }
        log.info("intermediate: after 13 idle ticks emptyRetained={} emptyTotal={}"
                + " evicted={} real={}", aggregator.emptyWindowSnapshots().size(),
                aggregator.emptyWindowRegistry().totalEmptyWindowCount(),
                aggregator.emptyWindowRegistry().evictedWindowCount(),
                aggregator.completedSnapshots().size());

        assertThat(aggregator.emptyWindowSnapshots())
                .as("empty retention never exceeds its own bound").hasSize(emptyBound);
        assertThat(aggregator.emptyWindowRegistry().totalEmptyWindowCount())
                .as("exact cumulative empty count despite eviction").isEqualTo(13);
        assertThat(aggregator.emptyWindowRegistry().evictedWindowCount())
                .as("the 3 oldest empties were evicted at the visible boundary")
                .isEqualTo(3);
        assertThat(aggregator.completedSnapshots())
                .as("the one real window is unaffected by empty eviction").hasSize(1);
        assertThat(aggregator.emptyWindowRegistry().maxRetained())
                .as("capacity boundary itself is queryable").isEqualTo(emptyBound);

        // Phase 2: one giant wall-clock jump (e.g. machine suspended for a long
        // time). Windows older than the bounded synthesis span are not
        // synthesized one by one; they are skipped but counted exactly, so the
        // empty-window total still reconciles and nothing is silent.
        aggregator.finalizeDue((13 + 50) * WINDOW);
        long skipped = aggregator.emptyWindowRegistry().skippedGapCount();
        long totalAfter = aggregator.emptyWindowRegistry().totalEmptyWindowCount();
        log.info("intermediate: after the 50-window jump skippedGaps={} emptyTotal={}"
                + " retained={} evicted={}", skipped, totalAfter,
                aggregator.emptyWindowSnapshots().size(),
                aggregator.emptyWindowRegistry().evictedWindowCount());

        assertThat(skipped)
                .as("wholesale-skipped idle gap windows are counted, not lost silently")
                .isPositive();
        long retained = aggregator.emptyWindowSnapshots().size();
        long evicted = aggregator.emptyWindowRegistry().evictedWindowCount();
        assertThat(retained + evicted + skipped)
                .as("retained + evicted + skipped reconciles to the exact total: %s",
                        totalAfter)
                .isEqualTo(totalAfter);
        assertThat(aggregator.completedSnapshots())
                .as("real window still queryable even after a giant idle jump")
                .hasSize(1);
        assertThat(aggregator.completedSnapshots().get(0).requestCount())
                .as("its counts untouched").isEqualTo(1);
        log.info("verdict: retained={} evicted={} skipped={} total={}",
                retained, evicted, skipped, totalAfter);
    }

    @Test
    void pendingEmptyWindowIsDistinctFromFinalizedEmptyWindow() {
        long delay = 600; // delay < window: pending horizon spans the prior gap
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), delay, 0, new LateEventRegistry());

        // One real event in window 0, then we observe while window 1 has
        // elapsed but not yet reached end+delay (due, waiting its turn), and
        // window 2 is still in progress.
        aggregator.record(DIM, 10, true, 100);
        long observeAt = 2_500; // nowIndex=2
        log.info("input: 1 real event in window 0, finalizationDelay={}ms, observe at {}ms",
                delay, observeAt);

        List<WindowSnapshot> active = aggregator.activeSnapshots(observeAt);
        List<WindowSnapshot> pending = aggregator.pendingSnapshots(observeAt);
        List<WindowSnapshot> open = aggregator.openSnapshots();
        List<WindowSnapshot> finalizedEmpty = aggregator.emptyWindowSnapshots();
        List<WindowSnapshot> finalizedReal = aggregator.completedSnapshots();
        log.info("intermediate: active(start)={} pending(start)={} open={} finalizedEmpty={}"
                + " finalizedReal={}",
                active.stream().map(WindowSnapshot::windowStartEpochMillis).toList(),
                pending.stream().map(WindowSnapshot::windowStartEpochMillis).toList(),
                open.size(), finalizedEmpty.size(), finalizedReal.size());

        assertThat(active)
                .as("window 2 is in progress; no elapsed window is shown as active")
                .isEmpty();
        assertThat(pending)
                .as("windows 0 (real, due) and 1 (empty placeholder) wait their turn")
                .hasSize(2);
        assertThat(pending.stream().filter(s -> s.requestCount() > 0).count())
                .as("one pending entry is the real window 0").isEqualTo(1);
        WindowSnapshot pendingEmpty = pending.stream()
                .filter(s -> s.requestCount() == 0).findFirst().orElseThrow();
        assertThat(pendingEmpty.windowStartEpochMillis())
                .as("the other pending entry is window 1 placeholder").isEqualTo(1_000);
        assertThat(finalizedEmpty)
                .as("window 1 has NOT been finalized: it must not appear here yet")
                .isEmpty();
        assertThat(finalizedReal)
                .as("window 0 has NOT reached end+delay: not finalized yet").isEmpty();

        // A due window must not be mistaken for "no data": a pending
        // placeholder and a finalized empty window never coexist for the same
        // (dimension, windowStart).
        for (WindowSnapshot p : pending) {
            assertThat(finalizedEmpty)
                    .as("pending window %s is not also a finalized empty window",
                            p.windowStartEpochMillis())
                    .noneSatisfy(e -> assertThat(e.windowStartEpochMillis())
                            .isEqualTo(p.windowStartEpochMillis()));
        }

        // Advance the clock past end+delay for windows 0 and 1: window 0
        // finalizes as real, window 1 as a finalized empty window. Observe at
        // t=3000 (nowIndex=3): window 2 has elapsed but its end+delay (3600)
        // has not, so it is the single pending empty placeholder.
        aggregator.finalizeDue(2_700); // maxDue = floor((2700-600)/1000)-1 = 1
        log.info("intermediate: after finalizeDue(2700) open(starts)={} finalizedReal={}"
                + " finalizedEmpty={} pending@3000(starts)={}",
                aggregator.openSnapshots().stream()
                        .map(WindowSnapshot::windowStartEpochMillis).toList(),
                aggregator.completedSnapshots().size(),
                aggregator.emptyWindowSnapshots().size(),
                aggregator.pendingSnapshots(3_000).stream()
                        .map(WindowSnapshot::windowStartEpochMillis).toList());

        assertThat(aggregator.completedSnapshots())
                .as("window 0 finalized as a real window").hasSize(1);
        assertThat(aggregator.completedSnapshots().get(0).windowStartEpochMillis())
                .isZero();
        assertThat(aggregator.emptyWindowSnapshots())
                .as("window 1 now finalized and explicitly recorded as no-data")
                .hasSize(1);
        assertThat(aggregator.emptyWindowSnapshots().get(0).windowStartEpochMillis())
                .isEqualTo(1_000);
        assertThat(aggregator.pendingSnapshots(3_000))
                .as("window 2 elapsed and is now the single pending placeholder")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(2_000L);
        assertThat(aggregator.pendingSnapshots(3_000).get(0).requestCount())
                .as("pending window 2 is a distinct not-yet-finalized placeholder")
                .isZero();
        assertThat(aggregator.activeSnapshots(3_000))
                .as("window 2 has elapsed: nothing is in progress").isEmpty();

        // All three groups stay disjoint after finalization too.
        List<WindowSnapshot> finalPending = aggregator.pendingSnapshots(3_000);
        for (WindowSnapshot p : finalPending) {
            assertThat(aggregator.emptyWindowSnapshots())
                    .noneSatisfy(e -> assertThat(e.windowStartEpochMillis())
                            .isEqualTo(p.windowStartEpochMillis()));
        }
        log.info("verdict: finalizedReal(window0) distinct from finalizedEmpty(window1)"
                + " distinct from pendingEmpty(window2): OK");
    }
}
