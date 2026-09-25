package com.github.lisama569.telemetrypipelinejava.telemetry.pipeline;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records failures inside the telemetry pipeline itself, categorized by stage,
 * so they are locally queryable and never silently swallowed. Retention is
 * bounded; per-stage counts are exact and unbounded-safe (counters only).
 */
public class PipelineErrorRegistry {

    private static final Logger log = LoggerFactory.getLogger(PipelineErrorRegistry.class);
    private static final int MAX_RETAINED = 1_000;

    public enum Stage {
        TRACE_EXTRACTION,
        METRIC_AGGREGATION,
        ALERT_EVALUATION,
        ASYNC_PROPAGATION
    }

    public record PipelineError(Stage stage, String reason, String exceptionType,
            long timestampEpochMillis) {
    }

    private final ConcurrentLinkedDeque<PipelineError> errors = new ConcurrentLinkedDeque<>();
    private final Map<Stage, LongAdder> counts = new EnumMap<>(Stage.class);

    public PipelineErrorRegistry() {
        for (Stage stage : Stage.values()) {
            counts.put(stage, new LongAdder());
        }
    }

    public void record(Stage stage, String reason, Throwable error) {
        counts.get(stage).increment();
        String exceptionType = error == null ? "none" : error.getClass().getName();
        errors.addLast(new PipelineError(stage, reason, exceptionType,
                System.currentTimeMillis()));
        while (errors.size() > MAX_RETAINED) {
            errors.pollFirst();
        }
        log.warn("telemetry pipeline error: stage={} reason={} exception={}",
                stage, reason, exceptionType, error);
    }

    public List<PipelineError> errors() {
        return List.copyOf(errors);
    }

    public long countByStage(Stage stage) {
        return counts.get(stage).sum();
    }
}
