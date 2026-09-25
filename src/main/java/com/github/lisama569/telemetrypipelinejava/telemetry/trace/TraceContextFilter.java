package com.github.lisama569.telemetrypipelinejava.telemetry.trace;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.PipelineErrorRegistry;
import com.github.lisama569.telemetrypipelinejava.telemetry.pipeline.TelemetryPipeline;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Establishes a {@link TraceContext} per inbound request.
 *
 * <p>Header handling is deterministic:
 * <ul>
 *   <li>valid {@code traceparent} wins and is continued;</li>
 *   <li>a conflicting legacy {@code X-Trace-Id} is ignored and recorded;</li>
 *   <li>missing or malformed headers produce a freshly generated context.</li>
 * </ul>
 * Every non-trivial decision is recorded in {@link TraceIssueRegistry} and the
 * effective trace id is echoed via the {@code X-Trace-Id} response header.
 * Telemetry failures are caught and registered, never breaking the request.
 */
public class TraceContextFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TraceContextFilter.class);

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String LEGACY_TRACE_HEADER = "X-Trace-Id";
    public static final String RESPONSE_TRACE_HEADER = "X-Trace-Id";

    private final TraceIssueRegistry issueRegistry;
    private final PipelineErrorRegistry errorRegistry;
    private final TelemetryPipeline pipeline;

    public TraceContextFilter(TraceIssueRegistry issueRegistry,
            PipelineErrorRegistry errorRegistry, TelemetryPipeline pipeline) {
        this.issueRegistry = issueRegistry;
        this.errorRegistry = errorRegistry;
        this.pipeline = pipeline;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        TraceContext context = resolveContext(request);
        response.setHeader(RESPONSE_TRACE_HEADER, context.traceId());
        long startNanos = System.nanoTime();
        try (TraceContextHolder.Scope ignored = TraceContextHolder.open(context)) {
            filterChain.doFilter(request, response);
        } finally {
            TraceContextHolder.clear();
            recordMetricsSafely(request, response, startNanos);
        }
    }

    private TraceContext resolveContext(HttpServletRequest request) {
        try {
            String traceparent = request.getHeader(TRACEPARENT_HEADER);
            String legacy = request.getHeader(LEGACY_TRACE_HEADER);
            Optional<TraceHeaderParser.Parsed> parsed =
                    TraceHeaderParser.parseTraceparent(traceparent);
            if (parsed.isPresent()) {
                TraceHeaderParser.Parsed p = parsed.get();
                if (legacy != null && !legacy.isBlank() && !legacy.trim().equals(p.traceId())) {
                    recordIssue(TraceHeaderIssue.Kind.CONFLICTING_HEADERS,
                            "traceparent traceId=" + p.traceId()
                                    + " conflicts with X-Trace-Id=" + legacy.trim()
                                    + "; traceparent wins");
                }
                return new TraceContext(p.traceId(), TraceContext.randomHex(8),
                        p.parentSpanId(), p.sampled(), TraceContext.Origin.INBOUND_HEADER);
            }
            if (traceparent == null || traceparent.isBlank()) {
                recordIssue(TraceHeaderIssue.Kind.MISSING, "no traceparent header");
                return TraceContext.newRoot(TraceContext.Origin.GENERATED_MISSING);
            }
            recordIssue(TraceHeaderIssue.Kind.MALFORMED_TRACEPARENT,
                    "unparseable traceparent: " + abbreviate(traceparent));
            return TraceContext.newRoot(TraceContext.Origin.GENERATED_INVALID);
        } catch (RuntimeException e) {
            errorRegistry.record(PipelineErrorRegistry.Stage.TRACE_EXTRACTION,
                    "unexpected failure while resolving trace context", e);
            return TraceContext.newRoot(TraceContext.Origin.GENERATED_INVALID);
        }
    }

    private void recordIssue(TraceHeaderIssue.Kind kind, String detail) {
        issueRegistry.record(new TraceHeaderIssue(kind, detail, System.currentTimeMillis()));
        log.debug("trace header issue: kind={} detail={}", kind, detail);
    }

    private void recordMetricsSafely(HttpServletRequest request, HttpServletResponse response,
            long startNanos) {
        try {
            long latencyMillis = (System.nanoTime() - startNanos) / 1_000_000;
            int status = response.getStatus();
            DimensionKey dimension = new DimensionKey(
                    request.getMethod() + " " + request.getRequestURI(), statusClass(status));
            pipeline.recordRequest(dimension, latencyMillis, status >= 500);
        } catch (RuntimeException e) {
            errorRegistry.record(PipelineErrorRegistry.Stage.METRIC_AGGREGATION,
                    "failed to record request metrics", e);
        }
    }

    private static String statusClass(int status) {
        return (status / 100) + "xx";
    }

    private static String abbreviate(String s) {
        return s.length() <= 120 ? s : s.substring(0, 120) + "...";
    }
}
