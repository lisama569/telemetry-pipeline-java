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
 * Long-idle retention and window-state distinction:
 *
 * <ul>
 *   <li>real (traffic-bearing) windows are never evicted by a flood of
 *       empty windows during traffic silence — empty retention is a separate
 *       bounded space with exact closed/evicted counters;</li>
 *   <li>local queries separate in-progress (ACTIVE), due-but-not-finalized
 *       (PENDING) and finalized-empty (CLOSED with zero requests) windows;</li>
 *   <li>idle gaps beyond the bounded synthesis span are counted, never
 *       skipped silently.</li>
 * </ul>
 */
class IdleWindowRetentionTest {

    private static final Logger log = LoggerFactory.getLogger(IdleWindowRetentionTest.class);

    private static final long WINDOW = 1_000;
    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "5xx");

    @Test
    void realWindowsSurviveLongIdleAndEmptyWindowsHaveOwnBoundedSpace() {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, 0, new LateEventRegistry());

        // Three real windows (window starts 0, 2000, 5000) carry the data the
        // operator wants to look back at after the pipeline goes idle.
        aggregator.record(DIM, 10, true, 100);
        aggregator.record(DIM, 20, false, 2_100);
        aggregator.record(DIM, 30, true, 5_200);
        long emptiesBeforeIdle = aggregator.emptyWindowTotal();
        log.info("intermediate: before idle, real={} emptyTotal={} active={}",
                aggregator.completedSnapshots().size(), emptiesBeforeIdle,
                aggregator.activeSnapshots(5_200));
        assertThat(emptiesBeforeIdle)
                .as("rotation synthesized idle gaps as empty: index 1 when window 2"
                        + " rotated, indices 3 and 4 when window 5 rotated")
                .isEqualTo(3L);

        // One scheduler tick per window boundary during total silence: each
        // elapsed window finalizes and closes an empty window on its own.
        // The first tick (t=6001) closes real window 5, whose end just
        // passed; the remaining ticks synthesize empty windows 6..1106.
        int idleTicks = 1_102;
        for (int i = 0; i < idleTicks; i++) {
            aggregator.finalizeDue(6_001 + (long) i * WINDOW);
        }

        List<WindowSnapshot> real = aggregator.completedSnapshots();
        List<WindowSnapshot> empty = aggregator.emptySnapshots();
        // First idle tick closes the real window 5; every later tick closes
        // exactly one empty window.
        long expectedEmptyTotal = emptiesBeforeIdle + idleTicks - 1;
        log.info("input: 3 real windows (starts 0,2000,5000), then {} per-window idle ticks",
                idleTicks);
        log.info("intermediate: realRetained={} emptyRetained={} emptyTotal={}"
                + " emptyEvicted={} gapsSkipped={} realEvicted={}", real.size(),
                empty.size(), aggregator.emptyWindowTotal(),
                aggregator.emptyWindowEvictionCount(),
                aggregator.emptyGapSkippedCount(),
                aggregator.realWindowEvictionCount());

        assertThat(real)
                .as("long idle never pushes real windows out: all three still queryable")
                .hasSize(3);
        assertThat(real)
                .as("real windows keep their original starts and payloads")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(0L, 2_000L, 5_000L);
        assertThat(real)
                .as("real windows carry their actual request counts")
                .extracting(WindowSnapshot::requestCount)
                .containsExactly(1L, 1L, 1L);

        assertThat(aggregator.emptyWindowTotal())
                .as("every finalized empty window is counted exactly")
                .isEqualTo(expectedEmptyTotal);
        assertThat(empty)
                .as("empty retention is bounded independently at 1000")
                .hasSize(1_000);
        assertThat(aggregator.emptyWindowEvictionCount())
                .as("overflow past the empty bound is a visible, exact boundary")
                .isEqualTo(expectedEmptyTotal - 1_000L);
        assertThat(aggregator.emptyGapSkippedCount())
                .as("dense per-window ticks never skip the synthesis span").isZero();
        assertThat(aggregator.realWindowEvictionCount())
                .as("empty-window pressure never evicts a single real window")
                .isZero();
        assertThat(empty)
                .as("all retained empty windows really are empty and finalized")
                .allSatisfy(s -> {
                    assertThat(s.requestCount()).isZero();
                    assertThat(s.errorCount()).isZero();
                });
        assertThat(aggregator.activeSnapshots(6_001L + (idleTicks - 1) * WINDOW))
                .as("after the long idle nothing is still in progress").isEmpty();
        log.info("verdict: real windows {} still present; empty {}/{} retained, {} evicted",
                real.stream().map(WindowSnapshot::windowStartEpochMillis).toList(),
                empty.size(), aggregator.emptyWindowTotal(),
                aggregator.emptyWindowEvictionCount());
    }

    @Test
    void idleGapsBeyondTheSynthesisSpanAreCountedNotSkippedSilently() {
        int windowCount = 4;
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW,
                windowCount, new CardinalityLimiter(10), 0, 0, new LateEventRegistry());

        // One real window at index 0, then the next event jumps straight to
        // index 200. Rotation finalizes indices 0..199 in one sweep: window 0
        // is real, the most recent windowCount empty indices 196..199 are
        // synthesized, and the much older idle indices 1..195 fall outside
        // the bounded synthesis span and must be counted as skipped.
        aggregator.record(DIM, 10, true, 100);
        aggregator.record(DIM, 10, false, 200_200);

        long skipped = aggregator.emptyGapSkippedCount();
        List<WindowSnapshot> real = aggregator.completedSnapshots();
        List<WindowSnapshot> empty = aggregator.emptySnapshots();
        log.info("input: 1 event in window 0 then one event in window 200 (count={})",
                windowCount);
        log.info("intermediate: gapsSkipped={} emptyClosed={} realRetained={}",
                skipped, aggregator.emptyWindowTotal(), real.size());

        assertThat(skipped)
                .as("indices 1..195 are beyond the 4-window synthesis span: counted")
                .isEqualTo(195L);
        assertThat(aggregator.emptyWindowTotal())
                .as("the 4 in-span indices 196..199 become empty windows")
                .isEqualTo(4L);
        assertThat(empty)
                .as("the synthesized empty windows are the most recent in-span ones")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(196_000L, 197_000L, 198_000L, 199_000L);
        assertThat(real)
                .as("the real window 0 is retained despite the giant idle jump")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(0L);
        assertThat(aggregator.emptyWindowEvictionCount())
                .as("3 synthesized empties fit the 1000-bound retention").isZero();
        log.info("verdict: {} old idle gaps counted as skipped (not silent), {} empty"
                + " windows synthesized, real window 0 retained", skipped, empty.size());
    }

    @Test
    void activePendingAndFinalizedEmptyWindowsAreSeparatelyQueryable() {
        long delay = 500;
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 8,
                new CardinalityLimiter(10), delay, 0, new LateEventRegistry());

        // One real window with data at index 0. With finalizationDelay=500
        // the window is only due for finalization at t=1500, so the timeline
        // below walks it through all three distinguishable states.
        aggregator.record(DIM, 10, true, 100);

        log.info("input: one event in window 0, finalizationDelay={}ms", delay);

        // Phase 1 (t=999): window 0 is still in progress.
        List<WindowSnapshot> active = aggregator.activeSnapshots(999);
        List<WindowSnapshot> pending = aggregator.pendingSnapshots(999);
        List<WindowSnapshot> empty = aggregator.emptySnapshots();
        log.info("intermediate: t=999  active={} pending={} empty={}",
                starts(active), starts(pending), empty.size());
        assertThat(active)
                .as("state 1 (in progress): window 0 has not ended yet").hasSize(1);
        assertThat(starts(active)).containsExactly(0L);
        assertThat(pending).as("nothing is due while the window is in progress").isEmpty();
        assertThat(empty).as("nothing is finalized yet").isEmpty();

        // Phase 2 (t=1499): window 0 ended at t=1000 but end+delay (1500) has
        // not been reached by any tick, so it is pending finalization — it
        // carries real data and must not look like an "empty" window.
        assertThat(aggregator.activeSnapshots(1_499))
                .as("window 0 has ended: no longer reported as in progress").isEmpty();
        pending = aggregator.pendingSnapshots(1_499);
        empty = aggregator.emptySnapshots();
        log.info("intermediate: t=1499 active={} pending={} empty={}",
                starts(aggregator.activeSnapshots(1_499)), starts(pending), empty.size());
        assertThat(pending)
                .as("state 2 (due, not yet finalized): distinct from confirmed-empty")
                .hasSize(1);
        assertThat(starts(pending)).containsExactly(0L);
        assertThat(pending.get(0).requestCount())
                .as("a pending window keeps its real payload").isEqualTo(1L);
        assertThat(empty)
                .as("a not-yet-finalized window is never reported as confirmed-empty")
                .isEmpty();

        // Phase 3 (t=1500): window 0 finalizes and closes real (grace=0).
        aggregator.finalizeDue(1_500);
        assertThat(aggregator.completedSnapshots())
                .as("the real window closes into the real-only retention space")
                .hasSize(1);
        assertThat(aggregator.emptySnapshots())
                .as("a data-bearing window never enters the empty space").isEmpty();

        // Phase 4 (t=2500): the idle window 1 is finalized with no data — this
        // is the third state, "definitely no data in that interval, finalized".
        aggregator.finalizeDue(2_500);
        List<WindowSnapshot> activeAfter = aggregator.activeSnapshots(2_500);
        List<WindowSnapshot> pendingAfter = aggregator.pendingSnapshots(2_500);
        List<WindowSnapshot> emptyAfter = aggregator.emptySnapshots();
        List<WindowSnapshot> realAfter = aggregator.completedSnapshots();
        log.info("intermediate: t=2500 active={} pending={} empty={} real={}",
                starts(activeAfter), starts(pendingAfter), starts(emptyAfter),
                starts(realAfter));

        assertThat(activeAfter).as("no window is in progress").isEmpty();
        assertThat(pendingAfter).as("every due window has been finalized").isEmpty();
        assertThat(emptyAfter)
                .as("state 3 (finalized, definitely no data): idle window 1 closed empty")
                .hasSize(1);
        assertThat(starts(emptyAfter)).containsExactly(1_000L);
        assertThat(emptyAfter.get(0).requestCount())
                .as("confirmed-empty window reports zero requests").isZero();
        assertThat(starts(realAfter))
                .as("the earlier real window is still queryable on its own")
                .containsExactly(0L);
        assertThat(aggregator.openSnapshots())
                .as("with zero grace, finalized windows close immediately").isEmpty();
        log.info("verdict: the three states never mix — in-progress={},"
                        + " due-not-finalized={}, confirmed-empty={}",
                starts(active), starts(pending), starts(emptyAfter));
    }

    private static List<Long> starts(List<WindowSnapshot> snapshots) {
        return snapshots.stream().map(WindowSnapshot::windowStartEpochMillis).toList();
    }
}
