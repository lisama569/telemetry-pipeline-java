package com.github.lisama569.telemetrypipelinejava.telemetry.metrics;

/**
 * Aggregation dimension for request metrics (e.g. route + status class).
 */
public record DimensionKey(String route, String statusClass) {

    /** The well-known overflow bucket used when cardinality exceeds the limit. */
    public static final DimensionKey OVERFLOW = new DimensionKey("__overflow__", "__all__");
}
