package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically finalizes windows whose close delay has elapsed, so window
 * completion and downstream alert evaluation make progress even when no new
 * requests arrive. Runs on a single daemon thread; each pass is fault
 * isolated — a failure is logged and retried on the next tick, never
 * propagated.
 */
public class WindowFinalizer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WindowFinalizer.class);

    private final RollingWindowAggregator aggregator;
    private final long intervalMillis;
    private final ScheduledExecutorService scheduler;

    public WindowFinalizer(RollingWindowAggregator aggregator, long intervalMillis) {
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("intervalMillis must be > 0");
        }
        this.aggregator = aggregator;
        this.intervalMillis = intervalMillis;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "telemetry-window-finalizer");
            t.setDaemon(true);
            return t;
        });
    }

    /** Starts the periodic finalization loop. */
    public WindowFinalizer start() {
        scheduler.scheduleWithFixedDelay(this::runSafely, intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
        log.info("window finalizer started: intervalMillis={} closeDelayMillis={} "
                        + "lateToleranceMillis={}", intervalMillis,
                aggregator.windowCloseDelayMillis(), aggregator.lateToleranceMillis());
        return this;
    }

    /** Runs one finalization pass against the supplied clock. */
    public void runOnce(long nowEpochMillis) {
        aggregator.finalizeDue(nowEpochMillis);
    }

    private void runSafely() {
        try {
            runOnce(System.currentTimeMillis());
        } catch (RuntimeException e) {
            log.warn("scheduled window finalization pass failed; will retry next tick", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
