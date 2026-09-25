package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertRule;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertState;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.TelemetryPipeline;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault isolation: failures inside the telemetry pipeline (listeners, alert
 * evaluators) are contained, categorized, locally queryable, and never affect
 * business responses.
 */
class PipelineFaultIsolationTest {

    private static final Logger log =
            LoggerFactory.getLogger(PipelineFaultIsolationTest.class);

    private static final DimensionKey DIM = new DimensionKey("GET /x", "2xx");

    @Test
    void throwingWindowListenerDoesNotBreakAggregationOrOtherListeners() {
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(1_000, 4, new CardinalityLimiter(10));
        List<WindowSnapshot> goodListenerSeen = new CopyOnWriteArrayList<>();
        aggregator.addWindowListener(s -> {
            throw new IllegalStateException("boom from faulty listener");
        });
        aggregator.addWindowListener(goodListenerSeen::add);

        log.info("input: 2 events in window 0, 1 event in window 1 (forces rotation)");
        assertThatCode(() -> {
            aggregator.record(DIM, 10, false, 100);
            aggregator.record(DIM, 20, true, 200);
            aggregator.record(DIM, 30, false, 1_100); // rotates, emits window 0
        }).as("record never throws despite faulty listener").doesNotThrowAnyException();

        log.info("verdict: listenerErrors={} goodListenerSeen={} completed={}",
                aggregator.listenerErrorCount(), goodListenerSeen,
                aggregator.completedSnapshots());
        assertThat(aggregator.listenerErrorCount())
                .as("listener failure is observable").isEqualTo(1);
        assertThat(goodListenerSeen).as("healthy listeners still receive the window")
                .hasSize(1);
        assertThat(aggregator.completedSnapshots()).hasSize(1);
    }

    @Test
    void throwingAlertEvaluatorIsContainedAndCategorized() {
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(1_000, 4, new CardinalityLimiter(10));
        PipelineErrorRegistry errorRegistry = new PipelineErrorRegistry();
        AlertEvaluator faulty = new AlertEvaluator(
                new AlertRule("faulty", AlertRule.Metric.ERROR_RATE, 0.5, 0.3, 1, 1)) {
            @Override
            public synchronized AlertState evaluate(WindowSnapshot snapshot) {
                throw new IllegalStateException("simulated alert evaluation failure");
            }
        };
        TelemetryPipeline pipeline =
                new TelemetryPipeline(aggregator, List.of(faulty), errorRegistry);

        log.info("input: events across a window rotation with a faulty evaluator");
        assertThatCode(() -> {
            pipeline.recordRequest(DIM, 10, false, 100);
            pipeline.recordRequest(DIM, 20, false, 1_100); // rotation triggers evaluation
        }).as("pipeline never propagates internal failures").doesNotThrowAnyException();

        long alertErrors =
                errorRegistry.countByStage(PipelineErrorRegistry.Stage.ALERT_EVALUATION);
        log.info("verdict: alertEvaluationErrors={} errors={}", alertErrors,
                errorRegistry.errors());
        assertThat(alertErrors).as("failure categorized by stage").isEqualTo(1);
        assertThat(errorRegistry.errors()).allSatisfy(e -> {
            assertThat(e.stage()).isEqualTo(PipelineErrorRegistry.Stage.ALERT_EVALUATION);
            assertThat(e.exceptionType()).contains("IllegalStateException");
            assertThat(e.reason()).contains("faulty");
        });
    }
}
