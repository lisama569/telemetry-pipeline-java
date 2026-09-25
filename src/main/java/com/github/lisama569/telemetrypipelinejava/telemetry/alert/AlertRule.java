package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

/**
 * Threshold + hysteresis rule evaluated against completed window snapshots.
 */
public record AlertRule(
        String name,
        Metric metric,
        double triggerThreshold,
        double recoverThreshold,
        int triggerConsecutiveWindows,
        int recoverConsecutiveWindows) {

    public enum Metric {
        ERROR_RATE,
        AVG_LATENCY_MS,
        THROUGHPUT
    }

    public double valueOf(com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot s) {
        return switch (metric) {
            case ERROR_RATE -> s.errorRate();
            case AVG_LATENCY_MS -> s.avgLatencyMillis();
            case THROUGHPUT -> s.requestCount();
        };
    }
}
