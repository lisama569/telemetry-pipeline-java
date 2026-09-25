package com.github.lisama569.telemetrypipelinejava.tracing;

import java.security.SecureRandom;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 解析 W3C traceparent 请求头（格式: version-traceid-spanid-flags）。
 * 解析结果必须明确且稳定：合法 / 缺失 / 非法 / 冲突 四种情况可区分；
 * 任何情况下都会产出一个可用的 TraceContext，绝不静默丢弃。
 */
public final class TraceHeaderParser {

    public enum Status { VALID, MISSING, INVALID, CONFLICT }

    public record ParseResult(Status status, TraceContext context, String reason) {
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    private TraceHeaderParser() {
    }

    /**
     * @param traceparentHeaders 请求中所有 traceparent 头的值（可能为 null/空/多个）
     */
    public static ParseResult parse(List<String> traceparentHeaders) {
        if (traceparentHeaders == null || traceparentHeaders.isEmpty()
                || traceparentHeaders.stream().allMatch(h -> h == null || h.isBlank())) {
            return new ParseResult(Status.MISSING, generate(TraceContext.Source.GENERATED_MISSING),
                    "traceparent header missing; generated new context");
        }

        // 去重后判断是否存在相互冲突的取值
        Set<String> distinct = new LinkedHashSet<>();
        for (String h : traceparentHeaders) {
            if (h != null && !h.isBlank()) {
                distinct.add(h.trim());
            }
        }

        if (distinct.size() > 1) {
            // 冲突：按稳定规则（字典序最小的合法值优先，否则新生成）解决，保证可重复
            String chosen = distinct.stream()
                    .filter(TraceHeaderParser::isValidFormat)
                    .min(String::compareTo)
                    .orElse(null);
            if (chosen != null) {
                TraceContext ctx = toContext(chosen, TraceContext.Source.RESOLVED_CONFLICT);
                return new ParseResult(Status.CONFLICT, ctx,
                        "conflicting traceparent headers " + distinct
                                + "; deterministically resolved to " + chosen);
            }
            return new ParseResult(Status.CONFLICT, generate(TraceContext.Source.GENERATED_INVALID),
                    "conflicting traceparent headers " + distinct
                            + "; none valid, generated new context");
        }

        String single = distinct.iterator().next();
        if (!isValidFormat(single)) {
            return new ParseResult(Status.INVALID, generate(TraceContext.Source.GENERATED_INVALID),
                    "malformed traceparent header '" + single + "'; generated new context");
        }
        return new ParseResult(Status.VALID, toContext(single, TraceContext.Source.HEADER), "ok");
    }

    /** 严格校验 W3C traceparent 格式。 */
    static boolean isValidFormat(String value) {
        String[] parts = value.split("-", -1);
        if (parts.length != 4) {
            return false;
        }
        String version = parts[0];
        String traceId = parts[1];
        String spanId = parts[2];
        String flags = parts[3];
        return isHex(version, 2) && !version.equals("ff")
                && isHex(traceId, 32) && !traceId.equals("0".repeat(32))
                && isHex(spanId, 16) && !spanId.equals("0".repeat(16))
                && isHex(flags, 2);
    }

    private static boolean isHex(String s, int len) {
        if (s.length() != len) {
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

    private static TraceContext toContext(String header, TraceContext.Source source) {
        String[] parts = header.split("-");
        boolean sampled = (Integer.parseInt(parts[3], 16) & 1) == 1;
        return new TraceContext(parts[1], parts[2], null, sampled, source);
    }

    private static TraceContext generate(TraceContext.Source source) {
        return new TraceContext(randomHex(32), randomHex(16), null, true, source);
    }

    static String randomHex(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(Character.forDigit(RANDOM.nextInt(16), 16));
        }
        return sb.toString();
    }
}
