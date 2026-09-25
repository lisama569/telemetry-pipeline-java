package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

import java.util.Optional;

/**
 * Parses W3C traceparent headers deterministically.
 * Format: {@code version-traceId(32 hex)-parentSpanId(16 hex)-flags(2 hex)}.
 * Any deviation yields {@link Optional#empty()}; this method never throws.
 */
public final class TraceHeaderParser {

    private TraceHeaderParser() {
    }

    public record Parsed(String traceId, String parentSpanId, boolean sampled) {
    }

    /** Returns empty when the header is missing or malformed; never throws. */
    public static Optional<Parsed> parseTraceparent(String header) {
        if (header == null) {
            return Optional.empty();
        }
        String trimmed = header.trim();
        String[] parts = trimmed.split("-");
        if (parts.length != 4) {
            return Optional.empty();
        }
        String version = parts[0];
        String traceId = parts[1];
        String spanId = parts[2];
        String flags = parts[3];
        if (!isLowerHex(version, 2) || "ff".equals(version)) {
            return Optional.empty();
        }
        if (!isLowerHex(traceId, 32) || isAllZeros(traceId)) {
            return Optional.empty();
        }
        if (!isLowerHex(spanId, 16) || isAllZeros(spanId)) {
            return Optional.empty();
        }
        if (!isLowerHex(flags, 2)) {
            return Optional.empty();
        }
        boolean sampled = (Integer.parseInt(flags, 16) & 0x01) == 0x01;
        return Optional.of(new Parsed(traceId, spanId, sampled));
    }

    private static boolean isLowerHex(String s, int expectedLen) {
        if (s.length() != expectedLen) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllZeros(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) != '0') {
                return false;
            }
        }
        return true;
    }
}
