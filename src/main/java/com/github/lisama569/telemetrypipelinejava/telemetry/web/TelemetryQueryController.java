package com.github.lisama569.telemetrypipelinejava.telemetry.web;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertStore;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceIssueRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Local query endpoints for metrics, alerts, trace-header issues and
 * pipeline-internal errors.
 */
@RestController
@RequestMapping("/telemetry")
public class TelemetryQueryController {

    private final RollingWindowAggregator aggregator;
    private final CardinalityLimiter limiter;
    private final AlertStore alertStore;
    private final PipelineErrorRegistry errorRegistry;
    private final TraceIssueRegistry issueRegistry;

    public TelemetryQueryController(RollingWindowAggregator aggregator,
            CardinalityLimiter limiter, AlertStore alertStore,
            PipelineErrorRegistry errorRegistry, TraceIssueRegistry issueRegistry) {
        this.aggregator = aggregator;
        this.limiter = limiter;
        this.alertStore = alertStore;
        this.errorRegistry = errorRegistry;
        this.issueRegistry = issueRegistry;
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("activeWindows", aggregator.activeSnapshots(System.currentTimeMillis()));
        out.put("completedWindows", aggregator.completedSnapshots());
        out.put("lateEvents", aggregator.lateEventCount());
        out.put("listenerErrors", aggregator.listenerErrorCount());
        out.put("activeDimensions", limiter.activeDimensionCount());
        out.put("maxDimensions", limiter.maxDimensions());
        out.put("overflowedEvents", limiter.overflowedEventCount());
        return out;
    }

    @GetMapping("/alerts")
    public Map<String, Object> alerts() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("states", alertStore.currentStates());
        out.put("history", alertStore.history());
        return out;
    }

    @GetMapping("/errors")
    public Map<String, Object> errors() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pipelineErrors", errorRegistry.errors());
        Map<String, Long> byStage = new LinkedHashMap<>();
        for (PipelineErrorRegistry.Stage stage : PipelineErrorRegistry.Stage.values()) {
            byStage.put(stage.name(), errorRegistry.countByStage(stage));
        }
        out.put("countsByStage", byStage);
        out.put("traceHeaderIssues", issueRegistry.issues());
        out.put("traceHeaderIssueTotal", issueRegistry.totalCount());
        return out;
    }
}
