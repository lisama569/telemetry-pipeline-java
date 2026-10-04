package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry.Classification;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Late events: within the configured grace bound they merge into the
 * finalized-but-open window (audited, counted exactly once); beyond it they
 * are rejected with an explicit, bounded classification signal.
 */
class LateEventHandlingTest {

    private static final Logger log =
            LoggerFactory.getLogger(LateEventHandlingTest.class);

    private static final long WINDOW = 1_000;
    private static final long GRACE = 500;
    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "2xx");

    private static RollingWindowAggregator newAggregator(LateEventRegistry registry,
            long grace) {
        return new RollingWindowAggregator(WINDOW, 4, new CardinalityLimiter(10),
                0, grace, registry);
    }

    @Test
    void lateEventWithinGraceMergesIntoOpenWindowAndIsAudited() {
        LateEventRegistry registry = new LateEventRegistry();
        RollingWindowAggregator aggregator = newAggregator(registry, GRACE);

        log.info("input: 2 events in window 0, rotation at t=1100, then a late"
                + " event timestamped t=900 (window 0, grace={}ms, closeTime=1500)",
                GRACE);
        aggregator.record(DIM, 10, false, 100);
        aggregator.record(DIM, 20, true, 200);
        aggregator.record(DIM, 30, false, 1_100); // rotates: window 0 finalized-open

        log.info("intermediate: open={} completed={}",
                aggregator.openSnapshots(), aggregator.completedSnapshots());
        assertThat(aggregator.openSnapshots())
                .as("window 0 finalized but still grace-open").hasSize(1);
        assertThat(aggregator.completedSnapshots())
                .as("window 0 not closed before its grace bound").isEmpty();

        aggregator.record(DIM, 40, true, 900); // late for window 0, within grace
        log.info("intermediate: after merge open={} merged={} rejected={}",
                aggregator.openSnapshots(), registry.mergedCount(),
                registry.rejectedCount());
        assertThat(registry.mergedCount()).as("late event classified as merged").isEqualTo(1);
        assertThat(registry.records())
                .as("merge decision is queryable with its target window")
                .anySatisfy(r -> {
                    assertThat(r.classification())
                            .isEqualTo(Classification.MERGED_INTO_OPEN_WINDOW);
                    assertThat(r.windowStartEpochMillis()).isEqualTo(0);
                    assertThat(r.dimension()).isEqualTo(DIM);
                });
        WindowSnapshot open = aggregator.openSnapshots().get(0);
        assertThat(open.requestCount()).as("merged event visible in open window")
                .isEqualTo(3);
        assertThat(open.errorCount()).isEqualTo(2);

        aggregator.finalizeDue(1_499);
        assertThat(aggregator.completedSnapshots())
                .as("still open one ms before the grace bound").isEmpty();
        aggregator.finalizeDue(1_500); // grace bound reached: window 0 closes
        log.info("verdict: completed={} lateEvents={}",
                aggregator.completedSnapshots(), aggregator.lateEventCount());
        assertThat(aggregator.completedSnapshots()).hasSize(1);
        WindowSnapshot closed = aggregator.completedSnapshots().get(0);
        assertThat(closed.requestCount())
                .as("2 original + 1 merged, counted exactly once").isEqualTo(3);
        assertThat(closed.errorCount()).isEqualTo(2);
        assertThat(closed.totalLatencyMillis()).isEqualTo(70);
        assertThat(aggregator.lateEventCount()).as("nothing rejected").isZero();
    }

    @Test
    void lateEventBeyondGraceIsRejectedWithBoundedVisibleSignal() {
        LateEventRegistry registry = new LateEventRegistry();
        RollingWindowAggregator aggregator = newAggregator(registry, GRACE);

        log.info("input: window 0 finalized and closed (grace bound passed),"
                + " then two late events for the already-closed window 0");
        aggregator.record(DIM, 10, false, 100);
        aggregator.record(DIM, 20, false, 1_100); // rotate: window 0 open
        aggregator.finalizeDue(1_500);            // window 0 closed
        assertThat(aggregator.completedSnapshots()).hasSize(1);

        aggregator.record(DIM, 30, true, 600);  // window 0 already closed
        aggregator.record(DIM, 30, true, 700);  // window 0 still closed
        log.info("verdict: lateEvents={} rejected={} records={}",
                aggregator.lateEventCount(), registry.rejectedCount(),
                registry.records());
        assertThat(aggregator.lateEventCount())
                .as("rejected late events are counted").isEqualTo(2);
        assertThat(registry.rejectedCount()).isEqualTo(2);
        assertThat(registry.records())
                .as("rejections carry an explicit classification")
                .allSatisfy(r -> assertThat(r.classification())
                        .isEqualTo(Classification.REJECTED_BEYOND_GRACE));
        assertThat(aggregator.completedSnapshots())
                .as("closed window content unchanged by rejected events")
                .allSatisfy(s -> assertThat(s.requestCount()).isEqualTo(1));
    }

    @Test
    void concurrentLateMergesAreCountedExactlyOnce() throws InterruptedException {
        LateEventRegistry registry = new LateEventRegistry();
        long grace = 60_000;
        RollingWindowAggregator aggregator = newAggregator(registry, grace);
        int threads = 8;
        int eventsPerThread = 250;

        aggregator.record(DIM, 10, false, 100);     // window 0: 1 event
        aggregator.record(DIM, 10, false, 1_100);   // rotate: window 0 open
        log.info("input: threads={} eventsPerThread={} all late for window 0"
                + " within grace={}ms", threads, eventsPerThread, grace);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < eventsPerThread; i++) {
                    aggregator.record(DIM, 10 + (i % 5), (threadId + i) % 3 == 0,
                            100 + (i % 900));
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS))
                .as("all late writers finish").isTrue();

        aggregator.finalizeDue(1_000 + grace); // close window 0
        long expectedCount = 1 + (long) threads * eventsPerThread;
        long expectedErrors = 0;
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < eventsPerThread; i++) {
                if ((t + i) % 3 == 0) {
                    expectedErrors++;
                }
            }
        }
        WindowSnapshot closed = aggregator.completedSnapshots().stream()
                .filter(s -> s.windowStartEpochMillis() == 0).findFirst().orElseThrow();
        log.info("verdict: count={} expected={} errors={} expectedErrors={} merged={}",
                closed.requestCount(), expectedCount, closed.errorCount(),
                expectedErrors, registry.mergedCount());
        assertThat(closed.requestCount())
                .as("concurrent merges neither lost nor double-counted")
                .isEqualTo(expectedCount);
        assertThat(closed.errorCount()).isEqualTo(expectedErrors);
        assertThat(registry.mergedCount())
                .isEqualTo((long) threads * eventsPerThread);
        assertThat(aggregator.lateEventCount()).as("nothing rejected").isZero();
    }

    @Test
    void classificationRecordsAreBoundedAndOverflowIsObservable() {
        LateEventRegistry registry = new LateEventRegistry();
        RollingWindowAggregator aggregator = newAggregator(registry, 0);
        int lateEventTotal = 1_100;

        log.info("input: {} late events beyond grace (retention bound=1000)",
                lateEventTotal);
        aggregator.record(DIM, 10, false, 5_000); // window 5 active
        for (int i = 0; i < lateEventTotal; i++) {
            aggregator.record(DIM, 10, false, 1_000); // window 1: rejected
        }
        log.info("verdict: records={} dropped={} rejected={}",
                registry.records().size(), registry.droppedRecordCount(),
                registry.rejectedCount());
        assertThat(registry.records())
                .as("retention is bounded").hasSize(1_000);
        assertThat(registry.droppedRecordCount())
                .as("evicted records are observable, not silent").isEqualTo(100);
        assertThat(registry.rejectedCount())
                .as("the counter itself is exact and unbounded-safe")
                .isEqualTo(lateEventTotal);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
