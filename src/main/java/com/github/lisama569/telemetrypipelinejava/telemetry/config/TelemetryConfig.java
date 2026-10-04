package com.github.lisama569.telemetrypipelinejava.telemetry.config;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertRule;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertStore;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.LateEventRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.TelemetryPipeline;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.WindowFinalizationScheduler;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.ContextPropagatingExecutor;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContextFilter;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceIssueRegistry;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wires the telemetry pipeline beans from {@link TelemetryProperties}.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(TelemetryProperties.class)
public class TelemetryConfig {

    @Bean
    public TraceIssueRegistry traceIssueRegistry() {
        return new TraceIssueRegistry();
    }

    @Bean
    public PipelineErrorRegistry pipelineErrorRegistry() {
        return new PipelineErrorRegistry();
    }

    @Bean
    public CardinalityLimiter cardinalityLimiter(TelemetryProperties properties) {
        return new CardinalityLimiter(properties.getMaxDimensions());
    }

    @Bean
    public LateEventRegistry lateEventRegistry() {
        return new LateEventRegistry();
    }

    @Bean
    public RollingWindowAggregator rollingWindowAggregator(TelemetryProperties properties,
            CardinalityLimiter limiter, LateEventRegistry lateEventRegistry) {
        return new RollingWindowAggregator(properties.getWindowSizeMillis(),
                properties.getWindowCount(), limiter,
                properties.getFinalizationDelayMillis(), properties.getLateGraceMillis(),
                lateEventRegistry);
    }

    @Bean
    public AlertStore alertStore() {
        return new AlertStore();
    }

    @Bean
    public AlertEvaluator errorRateAlertEvaluator(TelemetryProperties properties,
            AlertStore alertStore) {
        AlertRule rule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                properties.getErrorRateTrigger(), properties.getErrorRateRecover(),
                properties.getTriggerWindows(), properties.getRecoverWindows());
        AlertEvaluator evaluator = new AlertEvaluator(rule);
        alertStore.register(evaluator);
        return evaluator;
    }

    @Bean
    public TelemetryPipeline telemetryPipeline(RollingWindowAggregator aggregator,
            List<AlertEvaluator> evaluators, PipelineErrorRegistry errorRegistry) {
        return new TelemetryPipeline(aggregator, evaluators, errorRegistry);
    }

    @Bean
    public WindowFinalizationScheduler windowFinalizationScheduler(
            RollingWindowAggregator aggregator, PipelineErrorRegistry errorRegistry) {
        return new WindowFinalizationScheduler(aggregator, errorRegistry);
    }

    @Bean
    public TraceContextFilter traceContextFilter(TraceIssueRegistry issueRegistry,
            PipelineErrorRegistry errorRegistry, TelemetryPipeline pipeline) {
        return new TraceContextFilter(issueRegistry, errorRegistry, pipeline);
    }

    /**
     * Executor for asynchronous business work that keeps the caller's trace
     * context correlated across threads.
     */
    @Bean
    public Executor telemetryExecutor() {
        return new ContextPropagatingExecutor(Executors.newCachedThreadPool());
    }
}
