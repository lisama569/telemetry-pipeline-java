package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertRule;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertState;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertStore;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowFinalizer;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.TelemetryPipeline;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled finalization: with no inbound traffic, due windows are finalized
 * by the timer (close delay configurable), empty windows are emitted
 * explicitly, and alerts fire and recover on schedule. Local queries
 * distinguish "no data this period" from "not yet finalized".
 */
class ScheduledWindowFinalizationTest {

    private static final Logger log =
            LoggerFactory.getLogger(ScheduledWindowFinalizationTest.class);

    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "5xx");

    private static RollingWindowAggregator newAggregator(long closeDelay, long tolerance) {
        return new RollingWindowAggregator(1_000, 6, new CardinalityLimiter(10),
                closeDelay, tolerance, new LateEventRegistry(1_000));
    }

    @Test
    void timerFinalizesWindowsWithoutTrafficAndDrivesAlertFireAndRecover() {
        RollingWindowAggregator aggregator = newAggregator(2_000, 500);
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = new AlertEvaluator(
                new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE, 0.5, 0.3, 2, 2));
        store.register(evaluator);
        TelemetryPipeline pipeline = new TelemetryPipeline(aggregator, List.of(evaluator),
                new PipelineErrorRegistry());
        WindowFinalizer finalizer = new WindowFinalizer(aggregator, 1_000);

        log.info("input: 10 events (6 errors) at t=100, 10 events (7 errors) at t=1200, "
                + "then silence; closeDelay=2000");
        for (int i = 0; i < 10; i++) {
            pipeline.recordRequest(DIM, 10, i < 6, 100);
        }
        for (int i = 0; i < 10; i++) {
            pipeline.recordRequest(DIM, 10, i < 7, 1_200);
        }
        log.info("intermediate: pendingClose={} completed={} states={}",
                aggregator.pendingCloseSnapshots(), aggregator.completedSnapshots(),
                store.currentStates());
        assertThat(aggregator.pendingCloseSnapshots())
                .as("window 0 is closed but not yet due: visible as pending, not as data")
                .hasSize(1);
        assertThat(aggregator.completedSnapshots())
                .as("nothing finalized before the close delay elapses").isEmpty();
        assertThat(store.currentStates().get("error-rate"))
                .as("no evaluation happens before finalization").isEmpty();

        // Traffic stops entirely; only the timer advances finalization.
        log.info("input: timer ticks at t=3000,4000,5000,6000 with no new requests");
        finalizer.runOnce(3_000); // window 0 due (end 1000 + delay 2000)
        finalizer.runOnce(4_000); // window 1 due
        log.info("intermediate: states after windows 0-1 finalized={}",
                store.currentStates());
        finalizer.runOnce(5_000); // window 2: empty
        finalizer.runOnce(6_000); // window 3: empty
        log.info("verdict: states={} history={} completed={}", store.currentStates(),
                store.history(), aggregator.completedSnapshots());

        assertThat(store.history())
                .as("alert fires and recovers on schedule with zero traffic")
                .extracting(AlertStore.Transition::to)
                .containsExactly(AlertState.PENDING, AlertState.FIRING,
                        AlertState.RECOVERING, AlertState.OK);
        assertThat(aggregator.completedSnapshots())
                .as("data windows 0 and 1 finalized exactly once")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(0L, 1_000L);
        assertThat(aggregator.emptyWindowSnapshots())
                .as("windows 2 and 3 are explicit empty windows: 'no data', not 'pending'")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(2_000L, 3_000L);
        assertThat(aggregator.emptyWindowCount())
                .as("empty windows are counted exactly").isEqualTo(2);
        assertThat(aggregator.pendingCloseSnapshots())
                .as("nothing left pending after the ticks").isEmpty();
    }

    @Test
    void noDataAndNotYetFinalizedAreDistinguishableInLocalQueries() {
        RollingWindowAggregator aggregator = newAggregator(2_000, 500);
        WindowFinalizer finalizer = new WindowFinalizer(aggregator, 1_000);

        log.info("input: 1 event at t=100 (window 0), then silence");
        aggregator.record(DIM, 10, false, 100);

        finalizer.runOnce(2_500); // before window 0's deadline (1000+2000)
        log.info("intermediate: active={} pending={} completed={}",
                aggregator.activeSnapshots(2_500), aggregator.pendingCloseSnapshots(),
                aggregator.completedSnapshots());
        assertThat(aggregator.activeSnapshots(2_500))
                .as("window 0 still in progress: 'not yet finalized'").hasSize(1);
        assertThat(aggregator.completedSnapshots())
                .as("no completed window yet").isEmpty();

        finalizer.runOnce(3_100); // window 0 due
        finalizer.runOnce(5_100); // windows 1 and 2 due: both empty
        log.info("verdict: completed={} empty={}", aggregator.completedSnapshots(),
                aggregator.emptyWindowSnapshots());
        assertThat(aggregator.completedSnapshots())
                .as("window 0 finalized with its data")
                .extracting(WindowSnapshot::requestCount)
                .containsExactly(1L);
        assertThat(aggregator.emptyWindowSnapshots())
                .as("windows 1-2 are explicit empty windows: 'no data this period'")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .containsExactly(1_000L, 2_000L);
        assertThat(aggregator.activeSnapshots(5_100))
                .as("nothing left in progress").isEmpty();
        assertThat(aggregator.pendingCloseSnapshots())
                .as("nothing left pending").isEmpty();
    }

    @Test
    void concurrentFinalizeAndRecordLosesNothingAndCountsEveryEventOnce()
            throws InterruptedException {
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(100, 8, new CardinalityLimiter(10),
                        200, 200, new LateEventRegistry(10_000));
        WindowFinalizer finalizer = new WindowFinalizer(aggregator, 1_000);
        // The exact emission stream (listeners) is what conservation is
        // checked against; the query views are intentionally bounded.
        List<WindowSnapshot> emitted = new java.util.concurrent.CopyOnWriteArrayList<>();
        aggregator.addWindowListener(emitted::add);
        int threads = 4;
        int eventsPerThread = 250;
        int total = threads * eventsPerThread;
        AtomicLong recorded = new AtomicLong();

        log.info("input: {} threads x {} events, event k at t=k*10 (windows of 100ms), "
                + "finalizer ticking concurrently", threads, eventsPerThread);
        ExecutorService pool = Executors.newFixedThreadPool(threads + 1);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < eventsPerThread; i++) {
                    long k = (long) i * threads + threadId;
                    aggregator.record(DIM, 10, false, k * 10);
                    recorded.incrementAndGet();
                }
            });
        }
        pool.submit(() -> {
            await(start);
            for (long now = 0; now < 15_000; now += 150) {
                finalizer.runOnce(now);
            }
        });
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS))
                .as("all writers and the finalizer finish").isTrue();
        finalizer.runOnce(1_000_000); // final catch-up

        long inCompleted = emitted.stream().mapToLong(WindowSnapshot::requestCount).sum();
        long inActive = aggregator.activeSnapshots(1_000_000).stream()
                .mapToLong(WindowSnapshot::requestCount).sum();
        long inPending = aggregator.pendingCloseSnapshots().stream()
                .mapToLong(WindowSnapshot::requestCount).sum();
        long dropped = aggregator.lateEventCount();
        log.info("verdict: recorded={} inCompleted={} inActive={} inPending={} dropped={}",
                recorded.get(), inCompleted, inActive, inPending, dropped);

        assertThat(emitted)
                .as("each window finalized at most once: window starts are unique")
                .extracting(WindowSnapshot::windowStartEpochMillis)
                .doesNotHaveDuplicates();
        assertThat(inCompleted + inActive + inPending + dropped)
                .as("conservation: every recorded event is counted exactly once "
                        + "(completed + still open + pending close + observably dropped)")
                .isEqualTo(total);
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
