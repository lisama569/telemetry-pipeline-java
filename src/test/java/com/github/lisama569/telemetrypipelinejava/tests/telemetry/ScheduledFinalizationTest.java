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
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.WindowFinalizationScheduler;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idle-traffic finalization: windows whose time has passed are finalized,
 * closed and pushed through alert evaluation by the scheduler alone, with no
 * new requests. Empty ("no data") windows are explicitly distinguishable from
 * not-yet-finalized ones in local queries.
 */
class ScheduledFinalizationTest {

    private static final Logger log =
            LoggerFactory.getLogger(ScheduledFinalizationTest.class);

    private static final long WINDOW = 1_000;
    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "5xx");

    private static AlertEvaluator newEvaluator(AlertStore store, int triggerWindows,
            int recoverWindows) {
        AlertRule rule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                0.5, 0.3, triggerWindows, recoverWindows);
        AlertEvaluator evaluator = new AlertEvaluator(rule);
        store.register(evaluator);
        return evaluator;
    }

    @Test
    void idleWindowsFinalizeAndAlertFiresThenRecoversWithoutTraffic() {
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), 0, 0, new LateEventRegistry());
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store, 1, 2);
        PipelineErrorRegistry errors = new PipelineErrorRegistry();
        new TelemetryPipeline(aggregator, List.of(evaluator), errors);
        WindowFinalizationScheduler scheduler =
                new WindowFinalizationScheduler(aggregator, errors);

        log.info("input: 4 events (3 errors, rate=0.75) in window 0, then silence;"
                + " trigger=0.5 triggerWindows=1 recover=0.3 recoverWindows=2");
        aggregator.record(DIM, 10, true, 100);
        aggregator.record(DIM, 10, true, 200);
        aggregator.record(DIM, 10, true, 300);
        aggregator.record(DIM, 10, false, 400);

        log.info("intermediate: before any tick active={} pending={} completed={}"
                + " empty={} state={}", aggregator.activeSnapshots(500),
                aggregator.pendingSnapshots(500), aggregator.completedSnapshots(),
                aggregator.emptyWindowSnapshots(), evaluator.state());
        assertThat(aggregator.activeSnapshots(500))
                .as("window 0 not elapsed yet: still active").hasSize(1);
        assertThat(aggregator.pendingSnapshots(500))
                .as("window 0 not elapsed yet: nothing pending").isEmpty();
        assertThat(aggregator.completedSnapshots())
                .as("window 0 not yet finalized: nothing closed").isEmpty();
        assertThat(aggregator.emptyWindowSnapshots())
                .as("window 0 not yet finalized: no finalized empty window").isEmpty();
        assertThat(evaluator.state())
                .as("no evaluation happens before finalization").isEqualTo(AlertState.OK);

        scheduler.tick(1_000); // window 0 ends; no new traffic needed
        log.info("intermediate: after tick(1000) state={} completed={}",
                evaluator.state(), aggregator.completedSnapshots());
        assertThat(evaluator.state())
                .as("breaching window finalized on schedule fires the alert")
                .isEqualTo(AlertState.FIRING);

        scheduler.tick(2_000); // empty window 1
        log.info("intermediate: after tick(2000) state={}", evaluator.state());
        assertThat(evaluator.state())
                .as("first empty window starts recovery").isEqualTo(AlertState.RECOVERING);

        scheduler.tick(3_000); // empty window 2
        log.info("verdict: after tick(3000) state={} transitions={}",
                evaluator.state(), store.history());
        assertThat(evaluator.state())
                .as("sustained empty windows recover the alert without traffic")
                .isEqualTo(AlertState.OK);
        assertThat(store.history())
                .as("full fire->recover lifecycle recorded in order")
                .extracting(AlertStore.Transition::to)
                .containsExactly(AlertState.FIRING, AlertState.RECOVERING, AlertState.OK);

        List<WindowSnapshot> completed = aggregator.completedSnapshots();
        List<WindowSnapshot> emptyWindows = aggregator.emptyWindowSnapshots();
        log.info("verdict: completed(real)={} empty={} active={} pending={}",
                completed, emptyWindows, aggregator.activeSnapshots(3_000),
                aggregator.pendingSnapshots(3_000));
        assertThat(completed)
                .as("only the real window 0 is in the real-window retention").hasSize(1);
        assertThat(completed.get(0).requestCount())
                .as("the window with traffic survives and stays queryable").isEqualTo(4);
        assertThat(emptyWindows)
                .as("idle periods are finalized into the independent empty retention")
                .hasSize(2);
        assertThat(emptyWindows)
                .as("finalized empty windows explicitly carry requestCount=0")
                .allSatisfy(s -> assertThat(s.requestCount()).isZero());
        assertThat(aggregator.emptyWindowRegistry().totalEmptyWindowCount())
                .as("empty-window counter is exact").isEqualTo(2);
        assertThat(aggregator.activeSnapshots(3_000))
                .as("nothing in progress at the elapsed timestamp").isEmpty();
        assertThat(aggregator.pendingSnapshots(3_000))
                .as("window 3 elapsed at t=3000 but is finalized in the same tick")
                .isEmpty();
        assertThat(errors.errors()).as("no pipeline errors").isEmpty();
    }

    @Test
    void finalizationDelayIsConfigurableAndEmptyIsDistinctFromPending() {
        long delay = 500;
        RollingWindowAggregator aggregator = new RollingWindowAggregator(WINDOW, 4,
                new CardinalityLimiter(10), delay, 0, new LateEventRegistry());
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store, 1, 1);
        PipelineErrorRegistry errors = new PipelineErrorRegistry();
        new TelemetryPipeline(aggregator, List.of(evaluator), errors);
        WindowFinalizationScheduler scheduler =
                new WindowFinalizationScheduler(aggregator, errors);

        log.info("input: 1 breaching event in window 0, finalizationDelay={}ms", delay);
        aggregator.record(DIM, 10, true, 100);

        scheduler.tick(1_499); // window end (1000) + delay (500) not reached
        log.info("intermediate: tick(1499) active={} pending={} completed={}"
                + " empty={} state={}", aggregator.activeSnapshots(1_499),
                aggregator.pendingSnapshots(1_499), aggregator.completedSnapshots(),
                aggregator.emptyWindowSnapshots(), evaluator.state());
        assertThat(aggregator.activeSnapshots(1_499))
                .as("window 0 has elapsed: no longer active").isEmpty();
        assertThat(aggregator.pendingSnapshots(1_499))
                .as("elapsed but end+delay not reached: due, waiting its turn")
                .hasSize(1);
        assertThat(aggregator.pendingSnapshots(1_499).get(0).requestCount())
                .as("the pending entry is the real window holding its event").isEqualTo(1);
        assertThat(aggregator.completedSnapshots())
                .as("before end+delay nothing is closed").isEmpty();
        assertThat(aggregator.emptyWindowSnapshots())
                .as("before end+delay nothing is finalized as empty").isEmpty();
        assertThat(evaluator.state()).as("no premature evaluation")
                .isEqualTo(AlertState.OK);

        scheduler.tick(1_500); // end + delay reached
        log.info("verdict: tick(1500) completed(real)={} empty={} state={}",
                aggregator.completedSnapshots(), aggregator.emptyWindowSnapshots(),
                evaluator.state());
        assertThat(aggregator.completedSnapshots())
                .as("real window finalized exactly at end+delay").hasSize(1);
        assertThat(aggregator.emptyWindowSnapshots())
                .as("no idle gap exists yet, so no finalized empty window").isEmpty();
        assertThat(evaluator.state())
                .as("breaching window fires right after the configured delay")
                .isEqualTo(AlertState.FIRING);
    }
}
