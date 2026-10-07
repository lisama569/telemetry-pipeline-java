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
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.TelemetryPipeline;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retention determinism: with the same batch of events (real windows with an
 * idle gap and a sparse dimension), the real windows that survive in the
 * independent retention, the empty-window counters and the alert outcome must
 * be identical whether the batch arrives in order, shuffled or concurrently.
 */
class RetentionDeterminismTest {

    private static final Logger log =
            LoggerFactory.getLogger(RetentionDeterminismTest.class);

    private static final long WINDOW = 1_000;
    // Grace must cover the event timestamp span (latest rotation at t=2100 must
    // not close window 0 while its out-of-order siblings may still arrive), so
    // the outcome depends only on the data, not arrival order.
    private static final long GRACE = 1_500;
    private static final DimensionKey DIM_A = new DimensionKey("GET /a", "2xx");
    private static final DimensionKey DIM_B = new DimensionKey("GET /b", "2xx");

    private record Event(DimensionKey dimension, long latency, boolean error,
            long timestampEpochMillis) {
    }

    private record RunResult(List<WindowSnapshot> realWindows,
            List<WindowSnapshot> emptyWindows, long emptyTotal,
            AlertState finalState, List<String> transitions) {
    }

    private static List<Event> buildBatch() {
        List<Event> batch = new ArrayList<>();
        // Dim A: real windows at 0 (breaching), idle gap at 1, real window at 2.
        batch.add(new Event(DIM_A, 10, true, 100));
        batch.add(new Event(DIM_A, 10, true, 200));
        batch.add(new Event(DIM_A, 10, false, 300));
        batch.add(new Event(DIM_A, 10, false, 2_100));
        // Dim B (sparse): one real window at 3, so closing through 3 forces the
        // watermark to wait for dim A's older windows regardless of order.
        batch.add(new Event(DIM_B, 10, false, 3_100));
        return batch;
    }

    private static RunResult run(List<Event> events, int threads)
            throws InterruptedException {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, GRACE, new LateEventRegistry());
        AlertStore store = new AlertStore();
        AlertRule rule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                0.5, 0.3, 1, 1);
        AlertEvaluator evaluator = new AlertEvaluator(rule);
        store.register(evaluator);
        new TelemetryPipeline(aggregator, List.of(evaluator),
                new PipelineErrorRegistry());

        if (threads == 1) {
            events.forEach(e -> aggregator.record(e.dimension(), e.latency(),
                    e.error(), e.timestampEpochMillis()));
        } else {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            for (int t = 0; t < threads; t++) {
                int threadId = t;
                pool.submit(() -> {
                    await(start);
                    for (int i = threadId; i < events.size(); i += threads) {
                        Event e = events.get(i);
                        aggregator.record(e.dimension(), e.latency(), e.error(),
                                e.timestampEpochMillis());
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(30, TimeUnit.SECONDS))
                    .as("all writers finish").isTrue();
        }
        // Tick exactly to window 3's close horizon (4*1000 + grace 1500):
        // closes 0..3 for all dimensions without synthesizing later empty
        // windows, so the retained sets reflect just the batch's gaps.
        aggregator.finalizeDue(4_000 + GRACE);

        List<String> edges = store.history().stream()
                .map(RetentionDeterminismTest::edge).toList();
        return new RunResult(aggregator.completedSnapshots(),
                aggregator.emptyWindowSnapshots(),
                aggregator.emptyWindowRegistry().totalEmptyWindowCount(),
                evaluator.state(), edges);
    }

    private static String edge(AlertStore.Transition t) {
        return t.from() + "->" + t.to() + "@" + extractWindowStart(t.reason());
    }

    private static long extractWindowStart(String reason) {
        int idx = reason.indexOf("windowStart=");
        return Long.parseLong(reason.substring(idx + "windowStart=".length()));
    }

    @Test
    void realRetentionAndEmptyCountersAreIndependentOfArrivalOrder()
            throws InterruptedException {
        List<Event> batch = buildBatch();
        List<Event> shuffled = new ArrayList<>(batch);
        Collections.shuffle(shuffled, new Random(7));

        log.info("input: {} events across 2 dimensions; dimA real at windows 0,2"
                + " (idle gap at 1); dimB real at window 3; grace={}ms; close through"
                + " window 3", batch.size(), GRACE);
        RunResult inOrder = run(batch, 1);
        log.info("intermediate: in-order realWindows(starts)={} emptyWindows(starts)={}"
                + " emptyTotal={} state={} transitions={}",
                inOrder.realWindows().stream()
                        .map(s -> s.dimension().route() + "@" + s.windowStartEpochMillis())
                        .toList(),
                inOrder.emptyWindows().stream()
                        .map(s -> s.dimension().route() + "@" + s.windowStartEpochMillis())
                        .toList(),
                inOrder.emptyTotal(), inOrder.finalState(),
                inOrder.transitions());
        RunResult shuffledRun = run(shuffled, 1);
        log.info("intermediate: shuffled realWindows={} emptyWindows={} emptyTotal={}"
                + " state={}", shuffledRun.realWindows().size(),
                shuffledRun.emptyWindows().size(), shuffledRun.emptyTotal(),
                shuffledRun.finalState());
        RunResult concurrent = run(batch, 4);
        log.info("verdict: concurrent realWindows={} emptyWindows={} emptyTotal={} state={}",
                concurrent.realWindows().size(), concurrent.emptyWindows().size(),
                concurrent.emptyTotal(), concurrent.finalState());

        assertThat(inOrder.realWindows())
                .as("the scenario leaves exactly three real windows: A@0, A@2, B@3")
                .hasSize(3);
        assertThat(inOrder.realWindows())
                .as("real windows are released in canonical order")
                .extracting(s -> s.dimension().route() + "@" + s.windowStartEpochMillis())
                .containsExactly("GET /a@0", "GET /a@2000", "GET /b@3000");
        // Dim B's lifetime starts at its first event (window 3), so it never
        // synthesizes empties 0..2. Dim A finalizes its idle gap A@1 and its
        // post-last-event gap A@3 (up to the watermark at B's frontier) —
        // exactly 2 empty windows, independent of arrival order.
        assertThat(inOrder.emptyTotal())
                .as("A's idle gap at 1 plus A@3 up to B's frontier = 2 empties, exact")
                .isEqualTo(2);
        assertThat(inOrder.emptyWindows())
                .as("both empty windows materialized in canonical order")
                .extracting(s -> s.dimension().route() + "@" + s.windowStartEpochMillis())
                .containsExactly("GET /a@1000", "GET /a@3000");
        assertThat(inOrder.transitions())
                .as("alert fires on the breaching A@0 and recovers on the empty A@1")
                .containsExactly("OK->FIRING@0", "FIRING->OK@1000");

        for (RunResult other : List.of(shuffledRun, concurrent)) {
            assertThat(other.realWindows())
                    .as("surviving real windows are arrival-order independent")
                    .containsExactlyElementsOf(inOrder.realWindows());
            assertThat(other.emptyWindows())
                    .as("finalized empty windows are arrival-order independent")
                    .containsExactlyElementsOf(inOrder.emptyWindows());
            assertThat(other.emptyTotal())
                    .as("empty-window counter is arrival-order independent")
                    .isEqualTo(inOrder.emptyTotal());
            assertThat(other.finalState())
                    .as("alert final state is arrival-order independent")
                    .isEqualTo(inOrder.finalState());
            assertThat(other.transitions())
                    .as("alert transition sequence is arrival-order independent")
                    .containsExactlyElementsOf(inOrder.transitions());
        }
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
