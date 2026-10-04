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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Order independence: for the same batch of window data, the final alert
 * state and the per-dimension transition sequence are unique and
 * reproducible no matter which dimension's windows arrive first, how
 * reports are ordered, or how evaluation is scheduled across threads.
 */
class AlertOrderIndependenceTest {

    private static final Logger log =
            LoggerFactory.getLogger(AlertOrderIndependenceTest.class);

    private static final List<DimensionKey> DIMS = List.of(
            new DimensionKey("GET /a", "5xx"),
            new DimensionKey("GET /b", "5xx"),
            new DimensionKey("GET /c", "5xx"));

    /** Error counts out of 10 per window: fires on windows 0-1, recovers on 2-3. */
    private static final long[] ERRORS_PER_WINDOW = {6, 7, 1, 1};

    private record Outcome(Map<DimensionKey, AlertState> finalStates,
            Map<DimensionKey, List<String>> transitionsByDimension) {
    }

    private static AlertEvaluator newEvaluator(AlertStore store) {
        AlertEvaluator evaluator = new AlertEvaluator(
                new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE, 0.5, 0.3, 2, 2));
        store.register(evaluator);
        return evaluator;
    }

    private static WindowSnapshot window(DimensionKey dim, int windowIndex, long errors) {
        return new WindowSnapshot(dim, windowIndex * 1_000L, 1_000, 10, errors, 100, 10);
    }

    /** Windows for one dimension, in canonical (window-index) order. */
    private static List<WindowSnapshot> sequenceFor(DimensionKey dim) {
        List<WindowSnapshot> out = new ArrayList<>();
        for (int w = 0; w < ERRORS_PER_WINDOW.length; w++) {
            out.add(window(dim, w, ERRORS_PER_WINDOW[w]));
        }
        return out;
    }

    private static Outcome outcomeOf(AlertEvaluator evaluator, AlertStore store) {
        Map<DimensionKey, List<String>> byDim = new java.util.LinkedHashMap<>();
        for (AlertStore.Transition t : store.history()) {
            byDim.computeIfAbsent(t.dimension(), d -> new ArrayList<>())
                    .add(t.from() + "->" + t.to());
        }
        return new Outcome(evaluator.states(), byDim);
    }

    @Test
    void alertOutcomeIsIndependentOfCrossDimensionArrivalOrder() {
        List<WindowSnapshot> canonical = new ArrayList<>();
        DIMS.forEach(d -> canonical.addAll(sequenceFor(d)));

        // Ordering 1: dimension by dimension.
        List<WindowSnapshot> byDimension = new ArrayList<>(canonical);
        // Ordering 2: round-robin across dimensions.
        List<WindowSnapshot> roundRobin = new ArrayList<>();
        for (int w = 0; w < ERRORS_PER_WINDOW.length; w++) {
            for (DimensionKey d : DIMS) {
                roundRobin.add(sequenceFor(d).get(w));
            }
        }
        // Ordering 3: deterministic shuffle (keeps each dimension's order).
        List<WindowSnapshot> shuffled = interleavedShuffle(7L);
        // Ordering 4: concurrent, one thread per dimension.
        List<List<WindowSnapshot>> orderings =
                List.of(byDimension, roundRobin, shuffled);

        log.info("input: {} dimensions x {} windows, {} arrival orderings "
                + "(+ 1 concurrent)", DIMS.size(), ERRORS_PER_WINDOW.length, orderings.size());
        List<Outcome> outcomes = new ArrayList<>();
        for (List<WindowSnapshot> ordering : orderings) {
            outcomes.add(evaluateSequentially(ordering));
        }
        outcomes.add(evaluateConcurrently());
        Outcome reference = outcomes.get(0);
        log.info("intermediate: reference finalStates={} transitions={}",
                reference.finalStates(), reference.transitionsByDimension());
        for (int i = 1; i < outcomes.size(); i++) {
            log.info("verdict: ordering {} finalStates={} transitions={}", i,
                    outcomes.get(i).finalStates(), outcomes.get(i).transitionsByDimension());
            assertThat(outcomes.get(i).finalStates())
                    .as("final alert states must not depend on arrival order "
                            + "(ordering %d vs reference)", i)
                    .isEqualTo(reference.finalStates());
            assertThat(outcomes.get(i).transitionsByDimension())
                    .as("per-dimension transition sequence must not depend on arrival "
                            + "order (ordering %d vs reference)", i)
                    .isEqualTo(reference.transitionsByDimension());
        }
        assertThat(reference.finalStates().values())
                .as("sanity: every dimension fired and recovered (ends OK)")
                .containsOnly(AlertState.OK);
        assertThat(reference.transitionsByDimension().values())
                .as("sanity: full lifecycle happened per dimension")
                .allMatch(seq -> seq.equals(
                        List.of("OK->PENDING", "PENDING->FIRING",
                                "FIRING->RECOVERING", "RECOVERING->OK")));
    }

    private static List<WindowSnapshot> interleavedShuffle(long seed) {
        // Shuffle that preserves each dimension's canonical window order.
        List<List<WindowSnapshot>> perDim = new ArrayList<>();
        DIMS.forEach(d -> perDim.add(new ArrayList<>(sequenceFor(d))));
        List<WindowSnapshot> out = new ArrayList<>();
        Random random = new Random(seed);
        while (perDim.stream().anyMatch(l -> !l.isEmpty())) {
            List<List<WindowSnapshot>> nonEmpty =
                    perDim.stream().filter(l -> !l.isEmpty()).toList();
            out.add(nonEmpty.get(random.nextInt(nonEmpty.size())).remove(0));
        }
        return out;
    }

    private static Outcome evaluateSequentially(List<WindowSnapshot> ordering) {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        ordering.forEach(evaluator::evaluate);
        return outcomeOf(evaluator, store);
    }

    private static Outcome evaluateConcurrently() {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        ExecutorService pool = Executors.newFixedThreadPool(DIMS.size());
        CountDownLatch start = new CountDownLatch(1);
        for (DimensionKey dim : DIMS) {
            pool.submit(() -> {
                await(start);
                sequenceFor(dim).forEach(evaluator::evaluate);
            });
        }
        start.countDown();
        pool.shutdown();
        try {
            if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("concurrent evaluation did not finish");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return outcomeOf(evaluator, store);
    }

    @Test
    void endToEndSameEventsDifferentRecordOrderSameAlertOutcome() {
        log.info("input: same events for {} dimensions, recorded in 3 different "
                + "shuffled orders (tolerance covers the reordering)", DIMS.size());
        List<Outcome> outcomes = new ArrayList<>();
        List<List<WindowSnapshot>> completedPerRun = new ArrayList<>();
        for (long seed : new long[] {1L, 42L, 9_999L}) {
            Outcome outcome = runEndToEnd(seed, completedPerRun);
            outcomes.add(outcome);
            log.info("intermediate: seed={} finalStates={} transitions={}", seed,
                    outcome.finalStates(), outcome.transitionsByDimension());
        }
        Outcome reference = outcomes.get(0);
        for (int i = 1; i < outcomes.size(); i++) {
            int run = i;
            assertThat(completedPerRun.get(i))
                    .as("finalized windows identical for record order %d", run)
                    .isEqualTo(completedPerRun.get(0));
            assertThat(outcomes.get(i))
                    .as("alert outcome identical for record order %d", run)
                    .isEqualTo(reference);
        }
        log.info("verdict: all {} record orders produced identical windows, states "
                + "and transitions", outcomes.size());
    }

    private static Outcome runEndToEnd(long seed, List<List<WindowSnapshot>> completedSink) {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(1_000, 6,
                new CardinalityLimiter(10), 10_000, 10_000, new LateEventRegistry(1_000));
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        TelemetryPipeline pipeline = new TelemetryPipeline(aggregator, List.of(evaluator),
                new PipelineErrorRegistry());

        record Event(DimensionKey dim, long timestamp, boolean error) {
        }
        List<Event> events = new ArrayList<>();
        for (DimensionKey dim : DIMS) {
            for (int w = 0; w < ERRORS_PER_WINDOW.length; w++) {
                for (int i = 0; i < 10; i++) {
                    events.add(new Event(dim, w * 1_000L + i * 10,
                            i < ERRORS_PER_WINDOW[w]));
                }
            }
        }
        Collections.shuffle(events, new Random(seed));
        events.forEach(e -> pipeline.recordRequest(e.dim(), 10, e.error(), e.timestamp()));
        aggregator.finalizeDue(100_000);

        List<WindowSnapshot> completed = aggregator.completedSnapshots().stream()
                .sorted(java.util.Comparator.comparing(
                        (WindowSnapshot s) -> s.dimension().toString())
                        .thenComparing(WindowSnapshot::windowStartEpochMillis))
                .toList();
        completedSink.add(completed);
        return outcomeOf(evaluator, store);
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
