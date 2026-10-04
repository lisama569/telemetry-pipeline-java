package com.github.lisama569.telemetrypipelinejava.telemetry.pipeline;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Periodically drives {@link RollingWindowAggregator#finalizeDue(long)} so
 * windows whose time has passed are finalized, closed and pushed through
 * alert evaluation even when no new requests arrive.
 */
public class WindowFinalizationScheduler {

    private final RollingWindowAggregator aggregator;
    private final PipelineErrorRegistry errorRegistry;

    public WindowFinalizationScheduler(RollingWindowAggregator aggregator,
            PipelineErrorRegistry errorRegistry) {
        this.aggregator = aggregator;
        this.errorRegistry = errorRegistry;
    }

    /** One finalization tick against the wall clock. Never throws. */
    @Scheduled(fixedDelayString = "${telemetry.finalization-interval-millis:1000}",
            initialDelayString = "${telemetry.finalization-interval-millis:1000}")
    public void tick() {
        tick(System.currentTimeMillis());
    }

    /** One finalization tick for an explicit timestamp. Never throws. */
    public void tick(long nowEpochMillis) {
        try {
            aggregator.finalizeDue(nowEpochMillis);
        } catch (RuntimeException e) {
            errorRegistry.record(PipelineErrorRegistry.Stage.WINDOW_FINALIZATION,
                    "scheduled finalization tick failed", e);
        }
    }
}
