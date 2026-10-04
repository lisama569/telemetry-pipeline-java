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
 * Alert determinism: the same batch of window data must produce the same
 * alert states and the same transition sequence no matter how event arrival
 * is ordered or scheduled. Closed windows are evaluated in canonical
 * (windowStart, dimension) order, so the outcome depends only on the data.
 */
class AlertDeterminismTest {

    private static final Logger log =
            LoggerFactory.getLogger(AlertDeterminismTest.class);

    private static final long WINDOW = 1_000;
    private static final long GRACE = 10_000; // covers the whole shuffle span
    private static final DimensionKey DIM_A = new DimensionKey("GET /a", "2xx");
    private static final DimensionKey DIM_B = new DimensionKey("GET /b", "2xx");

    /** errors out of 10 per window for each dimension. */
    private static final long[] ERRORS_A = {6, 6, 1, 1, 6, 1};
    private static final long[] ERRORS_B = {2, 7, 1, 6, 1, 1};

    private record Event(DimensionKey dimension, long latency, boolean error,
            long timestampEpochMillis) {
    }

    private record RunResult(AlertState finalState, List<String> transitions,
            List<WindowSnapshot> closedWindows) {
    }

    private static List<Event> buildBatch() {
        List<Event> batch = new ArrayList<>();
        for (int w = 0; w < ERRORS_A.length; w++) {
            addWindowEvents(batch, DIM_A, w, ERRORS_A[w]);
            addWindowEvents(batch, DIM_B, w, ERRORS_B[w]);
        }
        return batch;
    }

    private static void addWindowEvents(List<Event> batch, DimensionKey dim,
            int windowIndex, long errors) {
        for (int i = 0; i < 10; i++) {
            batch.add(new Event(dim, 10 + i, i < errors,
                    windowIndex * WINDOW + 100 + i));
        }
    }

    private static RunResult run(List<Event> events, int threads)
            throws InterruptedException {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, GRACE, new LateEventRegistry());
        AlertStore store = new AlertStore();
        AlertRule rule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                0.5, 0.3, 2, 2);
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
        // Close every window; only now does alert evaluation happen.
        aggregator.finalizeDue(20_000);

        List<String> transitions = store.history().stream()
                .map(t -> t.ruleName() + " " + t.from() + "->" + t.to()
                        + " (" + t.reason() + ")")
                .toList();
        return new RunResult(evaluator.state(), transitions,
                aggregator.completedSnapshots());
    }

    @Test
    void alertOutcomeIsIndependentOfArrivalOrderAndScheduling()
            throws InterruptedException {
        List<Event> batch = buildBatch();
        List<Event> shuffled = new ArrayList<>(batch);
        Collections.shuffle(shuffled, new Random(42));

        log.info("input: {} events, dimensions=2 windows={} errorsA={} errorsB={}",
                batch.size(), ERRORS_A.length, ERRORS_A, ERRORS_B);
        RunResult inOrder = run(batch, 1);
        log.info("intermediate: in-order finalState={} transitions={}",
                inOrder.finalState(), inOrder.transitions());
        RunResult shuffledRun = run(shuffled, 1);
        log.info("intermediate: shuffled finalState={} transitions={}",
                shuffledRun.finalState(), shuffledRun.transitions());
        RunResult concurrent = run(batch, 4);
        log.info("verdict: concurrent finalState={} transitions={}",
                concurrent.finalState(), concurrent.transitions());

        assertThat(inOrder.transitions())
                .as("scenario actually exercises fire and recovery")
                .anyMatch(t -> t.contains("->FIRING"))
                .anyMatch(t -> t.contains("->OK"));

        assertThat(shuffledRun.finalState())
                .as("shuffled arrival: same final state as in-order")
                .isEqualTo(inOrder.finalState());
        assertThat(shuffledRun.transitions())
                .as("shuffled arrival: same transition sequence as in-order")
                .containsExactlyElementsOf(inOrder.transitions());
        assertThat(shuffledRun.closedWindows())
                .as("shuffled arrival: same closed windows in canonical order")
                .containsExactlyElementsOf(inOrder.closedWindows());

        assertThat(concurrent.finalState())
                .as("concurrent scheduling: same final state as in-order")
                .isEqualTo(inOrder.finalState());
        assertThat(concurrent.transitions())
                .as("concurrent scheduling: same transition sequence as in-order")
                .containsExactlyElementsOf(inOrder.transitions());
        assertThat(concurrent.closedWindows())
                .as("concurrent scheduling: same closed windows in canonical order")
                .containsExactlyElementsOf(inOrder.closedWindows());
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
