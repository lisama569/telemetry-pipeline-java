package com.github.lisama569.telemetrypipelinejava.tracing;

/**
 * 单个请求的追踪上下文。
 * source 记录上下文来源，便于区分"缺失/非法/冲突"等解析结果。
 */
public record TraceContext(String traceId, String spanId, String parentSpanId,
                           boolean sampled, Source source) {

    public enum Source {
        /** 请求头中解析出的合法 traceparent */
        HEADER,
        /** 请求头缺失，本地新生成 */
        GENERATED_MISSING,
        /** 请求头格式非法，本地新生成并记录原因 */
        GENERATED_INVALID,
        /** 多个追踪头相互冲突，按稳定规则选定并记录 */
        RESOLVED_CONFLICT
    }

    public TraceContext {
        if (traceId == null || spanId == null || source == null) {
            throw new IllegalArgumentException("traceId/spanId/source must not be null");
        }
    }

    public TraceContext child(String newSpanId) {
        return new TraceContext(traceId, newSpanId, spanId, sampled, source);
    }
}
