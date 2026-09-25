package com.github.lisama569.telemetrypipelinejava.metrics;

import java.util.Map;

/**
 * 一条请求指标事件：延迟、是否错误及维度标签。
 */
public record MetricEvent(long latencyMillis, boolean error, Map<String, String> dimensions) {

    public MetricEvent {
        dimensions = dimensions == null ? Map.of() : Map.copyOf(dimensions);
        if (latencyMillis < 0) {
            throw new IllegalArgumentException("latencyMillis must be >= 0");
        }
    }
}
