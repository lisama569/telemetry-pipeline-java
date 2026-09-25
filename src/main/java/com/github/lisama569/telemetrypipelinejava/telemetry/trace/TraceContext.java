package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

import java.security.SecureRandom;

/**
 * Immutable tracing context for one inbound request and its nested/async calls.
 * Child contexts share the traceId so correlation is preserved across threads.
 */
public record TraceContext(
        String traceId,
        String spanId,
        String parentSpanId,
        boolean sampled,
        Origin origin) {

    /** Where this context came from. */
    public enum Origin {
        /** Parsed from a valid inbound traceparent header. */
        INBOUND_HEADER,
        /** Newly generated because header was missing. */
        GENERATED_MISSING,
        /** Newly generated because header was invalid or conflicting. */
        GENERATED_INVALID
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    public static TraceContext newRoot(Origin origin) {
        return new TraceContext(randomHex(16), randomHex(8), null, true, origin);
    }

    /** Creates a child span within the same trace. */
    public TraceContext child() {
        return new TraceContext(traceId, randomHex(8), spanId, sampled, origin);
    }

    static String randomHex(int byteCount) {
        byte[] bytes = new byte[byteCount];
        RANDOM.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(byteCount * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
