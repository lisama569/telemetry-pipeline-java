package com.github.lisama569.telemetrypipelinejava.telemetry.web;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertStore;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceIssueRegistry;
import java.util.LinkedHashMap;
import java.util.List;
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
        long now = System.currentTimeMillis();
        Map<String, Object> out = new LinkedHashMap<>();
        // Three non-overlapping states for local inspection:
        // 1) still in progress, 2) time is up but not finalized yet,
        // 3) finalized and definitely no data. Grace-open windows (late
        // events may still merge) are exposed separately.
        List<WindowSnapshot> emptyWindows = aggregator.emptySnapshots();
        out.put("activeWindows", aggregator.activeSnapshots(now));
        out.put("pendingWindows", aggregator.pendingSnapshots(now));
        out.put("openWindows", aggregator.openSnapshots());
        out.put("completedWindows", aggregator.completedSnapshots());
        out.put("emptyWindows", emptyWindows);
        out.put("emptyWindowTotal", aggregator.emptyWindowTotal());
        out.put("emptyWindowRetained", emptyWindows.size());
        out.put("emptyWindowEvictions", aggregator.emptyWindowEvictionCount());
        out.put("emptyGapSkipped", aggregator.emptyGapSkippedCount());
        out.put("realWindowEvictions", aggregator.realWindowEvictionCount());
        out.put("lateEvents", aggregator.lateEventCount());
        out.put("lateEventsMerged", aggregator.lateEventRegistry().mergedCount());
        out.put("lateEventsRejected", aggregator.lateEventRegistry().rejectedCount());
        out.put("lateEventClassifications", aggregator.lateEventRegistry().records());
        out.put("lateEventClassificationDrops",
                aggregator.lateEventRegistry().droppedRecordCount());
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
