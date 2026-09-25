package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

/**
 * A deterministic, observable record of how inbound trace headers were handled.
 */
public record TraceHeaderIssue(Kind kind, String detail, long timestampEpochMillis) {

    public enum Kind {
        MISSING,
        MALFORMED_TRACEPARENT,
        CONFLICTING_HEADERS
    }
}
