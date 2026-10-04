package com.github.lisama569.telemetrypipelinejava.telemetry.pipeline;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.List;

/**
 * Orchestrates metric recording and alert evaluation for completed requests.
 * Alert evaluation runs on the aggregator's closed-window stream, which is
 * released in canonical (windowStart, dimension) order, so alert states and
 * transitions are independent of event arrival order and scheduling. Every
 * stage is fault-isolated: internal failures are registered in
 * {@link PipelineErrorRegistry} with a distinguishable stage and never
 * propagate to the business request path.
 */
public class TelemetryPipeline {

    private final RollingWindowAggregator aggregator;
    private final List<AlertEvaluator> evaluators;
    private final PipelineErrorRegistry errorRegistry;

    public TelemetryPipeline(RollingWindowAggregator aggregator,
            List<AlertEvaluator> evaluators, PipelineErrorRegistry errorRegistry) {
        this.aggregator = aggregator;
        this.evaluators = List.copyOf(evaluators);
        this.errorRegistry = errorRegistry;
        this.aggregator.addCloseListener(this::onWindowClosed);
    }

    /** Records one finished request. Never throws. */
    public void recordRequest(DimensionKey dimension, long latencyMillis, boolean error) {
        recordRequest(dimension, latencyMillis, error, System.currentTimeMillis());
    }

    /** Test-visible overload with an explicit timestamp. Never throws. */
    public void recordRequest(DimensionKey dimension, long latencyMillis, boolean error,
            long nowEpochMillis) {
        try {
            aggregator.record(dimension, latencyMillis, error, nowEpochMillis);
        } catch (RuntimeException e) {
            errorRegistry.record(PipelineErrorRegistry.Stage.METRIC_AGGREGATION,
                    "aggregator rejected event for dimension " + dimension, e);
        }
    }

    private void onWindowClosed(WindowSnapshot snapshot) {
        for (AlertEvaluator evaluator : evaluators) {
            try {
                evaluator.evaluate(snapshot);
            } catch (RuntimeException e) {
                errorRegistry.record(PipelineErrorRegistry.Stage.ALERT_EVALUATION,
                        "rule " + evaluator.rule().name() + " failed on window "
                                + snapshot.windowStartEpochMillis(), e);
            }
        }
    }
}
