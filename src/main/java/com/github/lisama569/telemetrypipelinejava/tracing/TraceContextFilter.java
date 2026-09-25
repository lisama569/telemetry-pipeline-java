package com.github.lisama569.telemetrypipelinejava.tracing;

import com.github.lisama569.telemetrypipelinejava.metrics.MetricEvent;
import com.github.lisama569.telemetrypipelinejava.pipeline.TelemetryPipeline;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 入站请求过滤器：
 * 1. 解析 traceparent 头，为每个请求建立追踪上下文（缺失/非法/冲突均有明确处理）；
 * 2. 将 traceId 写入响应头，便于关联；
 * 3. 请求结束后采集延迟/错误指标事件；
 * 4. 观测链路自身异常被捕获记录，绝不影响业务响应。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceContextFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TraceContextFilter.class);

    private final TelemetryPipeline pipeline;

    public TraceContextFilter(TelemetryPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        List<String> headers = Collections.list(request.getHeaders("traceparent"));
        TraceHeaderParser.ParseResult parsed = TraceHeaderParser.parse(headers);
        TraceContext context = parsed.context();
        if (parsed.status() != TraceHeaderParser.Status.VALID) {
            log.info("trace header {}: {}", parsed.status(), parsed.reason());
        }

        TraceContextHolder.set(context);
        response.setHeader("traceparent",
                "00-" + context.traceId() + "-" + context.spanId() + (context.sampled() ? "-01" : "-00"));

        long startNanos = System.nanoTime();
        try {
            filterChain.doFilter(request, response);
        } finally {
            long latencyMs = (System.nanoTime() - startNanos) / 1_000_000;
            try {
                boolean error = response.getStatus() >= 500;
                Map<String, String> dimensions = new LinkedHashMap<>();
                dimensions.put("method", request.getMethod());
                dimensions.put("path", request.getRequestURI());
                dimensions.put("status", String.valueOf(response.getStatus() / 100) + "xx");
                pipeline.recordRequest(new MetricEvent(latencyMs, error, dimensions));
            } catch (RuntimeException e) {
                // 双保险：观测链路异常绝不上抛影响业务
                pipeline.errorLog().record("filter-collect", e.getClass().getSimpleName(),
                        String.valueOf(e.getMessage()));
                log.warn("telemetry collection failed: {}", e.toString());
            } finally {
                TraceContextHolder.clear();
            }
        }
    }
}
