package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContextFilter;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceHeaderIssue;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceIssueRegistry;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * Trace header edge cases: missing, malformed and conflicting headers must be
 * handled deterministically, echoed back, and recorded as observable issues.
 */
@SpringBootTest
@AutoConfigureMockMvc
class TraceHeaderHandlingTest {

    private static final Logger log =
            LoggerFactory.getLogger(TraceHeaderHandlingTest.class);

    private static final String VALID_TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TraceIssueRegistry issueRegistry;

    private long issueCountBefore;

    @BeforeEach
    void snapshotIssueCount() {
        issueCountBefore = issueRegistry.totalCount();
    }

    @Test
    void validTraceparentIsContinued() throws Exception {
        log.info("input: traceparent={}", VALID_TRACEPARENT);
        MvcResult result = mockMvc.perform(get("/api/work")
                        .header("traceparent", VALID_TRACEPARENT))
                .andExpect(status().isOk())
                .andExpect(header().string(TraceContextFilter.RESPONSE_TRACE_HEADER,
                        "4bf92f3577b34da6a3ce929d0e0e4736"))
                .andReturn();
        String body = result.getResponse().getContentAsString();
        log.info("verdict: responseTraceId={} body={}",
                result.getResponse().getHeader(TraceContextFilter.RESPONSE_TRACE_HEADER), body);
        assertThat(body).as("async hop shares the inbound trace id")
                .contains("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(newIssues()).as("valid header produces no issue").isEmpty();
    }

    @Test
    void missingHeaderGeneratesFreshContextAndRecordsIssue() throws Exception {
        log.info("input: no trace headers");
        MvcResult result = mockMvc.perform(get("/api/work"))
                .andExpect(status().isOk())
                .andReturn();
        String traceId = result.getResponse()
                .getHeader(TraceContextFilter.RESPONSE_TRACE_HEADER);
        log.info("verdict: generated traceId={} issues={}", traceId, newIssues());
        assertThat(traceId).as("generated trace id is 32 lowercase hex")
                .matches("[0-9a-f]{32}");
        assertThat(newIssues()).as("missing header recorded")
                .anyMatch(i -> i.kind() == TraceHeaderIssue.Kind.MISSING);
    }

    @Test
    void malformedHeaderGeneratesFreshContextAndRecordsIssue() throws Exception {
        String malformed = "00-ZZZZ-not-hex-00f067aa0ba902b7-01";
        log.info("input: traceparent={}", malformed);
        MvcResult result = mockMvc.perform(get("/api/work")
                        .header("traceparent", malformed))
                .andExpect(status().isOk())
                .andReturn();
        String traceId = result.getResponse()
                .getHeader(TraceContextFilter.RESPONSE_TRACE_HEADER);
        log.info("verdict: generated traceId={} issues={}", traceId, newIssues());
        assertThat(traceId).matches("[0-9a-f]{32}");
        assertThat(traceId).as("malformed trace id is not reused")
                .isNotEqualTo("ZZZZ-not-hex");
        assertThat(newIssues()).as("malformed header recorded")
                .anyMatch(i -> i.kind() == TraceHeaderIssue.Kind.MALFORMED_TRACEPARENT);
    }

    @Test
    void conflictingHeadersResolveDeterministicallyToTraceparent() throws Exception {
        String conflictingLegacy = "aaaaaaaabbbbbbbbccccccccdddddddd";
        log.info("input: traceparent={} X-Trace-Id={}", VALID_TRACEPARENT, conflictingLegacy);
        MvcResult result = mockMvc.perform(get("/api/work")
                        .header("traceparent", VALID_TRACEPARENT)
                        .header("X-Trace-Id", conflictingLegacy))
                .andExpect(status().isOk())
                .andExpect(header().string(TraceContextFilter.RESPONSE_TRACE_HEADER,
                        "4bf92f3577b34da6a3ce929d0e0e4736"))
                .andReturn();
        List<TraceHeaderIssue> issues = newIssues();
        log.info("verdict: traceparent wins; issues={}", issues);
        assertThat(issues).as("conflict recorded, not silently dropped")
                .anyMatch(i -> i.kind() == TraceHeaderIssue.Kind.CONFLICTING_HEADERS);
    }

    @Test
    void repeatedMalformedRequestsNeverCrossWireContexts() throws Exception {
        log.info("input: two sequential malformed requests");
        MvcResult first = mockMvc.perform(get("/api/work")
                        .header("traceparent", "garbage-1")).andReturn();
        MvcResult second = mockMvc.perform(get("/api/work")
                        .header("traceparent", "garbage-2")).andReturn();
        String firstId = first.getResponse()
                .getHeader(TraceContextFilter.RESPONSE_TRACE_HEADER);
        String secondId = second.getResponse()
                .getHeader(TraceContextFilter.RESPONSE_TRACE_HEADER);
        log.info("verdict: first={} second={}", firstId, secondId);
        assertThat(firstId).isNotEqualTo(secondId);
        assertThat(first.getResponse().getContentAsString()).contains(firstId);
        assertThat(second.getResponse().getContentAsString()).contains(secondId);
    }

    private List<TraceHeaderIssue> newIssues() {
        List<TraceHeaderIssue> all = issueRegistry.issues();
        return all.stream().skip(Math.max(0, all.size() - (issueRegistry.totalCount()
                - issueCountBefore))).toList();
    }
}
