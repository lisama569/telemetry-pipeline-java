package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

/**
 * Immutable snapshot of one completed (or in-progress) aggregation window
 * for a single dimension.
 */
public record WindowSnapshot(
        DimensionKey dimension,
        long windowStartEpochMillis,
        long windowSizeMillis,
        long requestCount,
        long errorCount,
        long totalLatencyMillis,
        long maxLatencyMillis) {

    public double avgLatencyMillis() {
        return requestCount == 0 ? 0.0 : (double) totalLatencyMillis / requestCount;
    }

    public double errorRate() {
        return requestCount == 0 ? 0.0 : (double) errorCount / requestCount;
    }
}
