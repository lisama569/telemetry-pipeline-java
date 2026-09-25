package com.github.lisama569.telemetrypipelinejava.tests;

import com.github.lisama569.telemetrypipelinejava.tracing.TraceContext;
import com.github.lisama569.telemetrypipelinejava.tracing.TraceHeaderParser;
import com.github.lisama569.telemetrypipelinejava.tracing.TraceHeaderParser.ParseResult;
import com.github.lisama569.telemetrypipelinejava.tracing.TraceHeaderParser.Status;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 非法/缺失/冲突追踪头的解析行为：结果必须明确且稳定。
 */
class TraceHeaderParserTest {

    private static final Logger log = LoggerFactory.getLogger(TraceHeaderParserTest.class);

    private static final String VALID = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private ParseResult logResult(String caseName, List<String> input, ParseResult result) {
        log.info("[{}] input={} -> status={} reason={} traceId={}",
                caseName, input, result.status(), result.reason(), result.context().traceId());
        return result;
    }

    @Test
    void validHeaderIsAdopted() {
        ParseResult r = logResult("valid", List.of(VALID), TraceHeaderParser.parse(List.of(VALID)));
        assertEquals(Status.VALID, r.status(), "合法头应判定为 VALID");
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", r.context().traceId(), "traceId 应来自请求头");
        assertEquals("00f067aa0ba902b7", r.context().spanId(), "spanId 应来自请求头");
        assertTrue(r.context().sampled(), "flags=01 应为 sampled");
        assertEquals(TraceContext.Source.HEADER, r.context().source(), "来源应为 HEADER");
    }

    @Test
    void missingHeaderGeneratesNewContext() {
        ParseResult r = logResult("missing", List.of(), TraceHeaderParser.parse(List.of()));
        assertEquals(Status.MISSING, r.status(), "缺失头应判定为 MISSING");
        assertEquals(32, r.context().traceId().length(), "应生成 32 位 traceId");
        assertEquals(TraceContext.Source.GENERATED_MISSING, r.context().source(), "来源应为 GENERATED_MISSING");
    }

    @Test
    void malformedHeaderGeneratesAndReports() {
        List<String> bad = List.of("not-a-traceparent");
        ParseResult r = logResult("malformed", bad, TraceHeaderParser.parse(bad));
        assertEquals(Status.INVALID, r.status(), "非法格式应判定为 INVALID");
        assertEquals(TraceContext.Source.GENERATED_INVALID, r.context().source(), "来源应为 GENERATED_INVALID");
        assertTrue(r.reason().contains("not-a-traceparent"), "原因应包含原始非法值");
    }

    @Test
    void allZeroTraceIdIsRejected() {
        String zero = "00-" + "0".repeat(32) + "-00f067aa0ba902b7-01";
        ParseResult r = logResult("zero-trace-id", List.of(zero), TraceHeaderParser.parse(List.of(zero)));
        assertEquals(Status.INVALID, r.status(), "全零 traceId 应判定为 INVALID");
    }

    @Test
    void conflictingHeadersResolveDeterministically() {
        String other = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01";
        List<String> headers = List.of(VALID, other);
        ParseResult r1 = logResult("conflict-1", headers, TraceHeaderParser.parse(headers));
        ParseResult r2 = logResult("conflict-2", headers, TraceHeaderParser.parse(headers));
        assertEquals(Status.CONFLICT, r1.status(), "冲突头应判定为 CONFLICT");
        assertEquals(r1.context().traceId(), r2.context().traceId(), "冲突解决必须稳定可重复");
        // 字典序最小者优先：4bf9... < aaaa...
        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", r1.context().traceId(), "应稳定选定字典序最小的合法值");
        assertEquals(TraceContext.Source.RESOLVED_CONFLICT, r1.context().source(), "来源应为 RESOLVED_CONFLICT");
        assertNotNull(r1.reason(), "冲突必须给出原因");
    }

    @Test
    void identicalDuplicateHeadersAreValid() {
        ParseResult r = logResult("duplicate-same", List.of(VALID, VALID),
                TraceHeaderParser.parse(List.of(VALID, VALID)));
        assertEquals(Status.VALID, r.status(), "相同重复头不视为冲突");
    }
}
