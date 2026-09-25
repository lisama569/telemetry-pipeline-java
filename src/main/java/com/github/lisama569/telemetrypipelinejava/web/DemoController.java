package com.github.lisama569.telemetrypipelinejava.web;

import com.github.lisama569.telemetrypipelinejava.tracing.TraceContext;
import com.github.lisama569.telemetrypipelinejava.tracing.TraceContextHolder;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * 演示业务端点：支持模拟延迟、错误与异步嵌套调用，
 * 异步任务中读取追踪上下文以验证传播。
 */
@RestController
public class DemoController {

    private final Executor executor = Executors.newCachedThreadPool();

    @GetMapping("/api/demo")
    public Map<String, Object> demo(@RequestParam(defaultValue = "0") long latencyMs,
                                    @RequestParam(defaultValue = "false") boolean fail,
                                    @RequestParam(defaultValue = "false") boolean async) {
        TraceContext before = TraceContextHolder.current();
        String asyncTraceId = null;
        if (async) {
            // 异步嵌套调用：通过 wrap 传播上下文
            asyncTraceId = CompletableFuture.supplyAsync(TraceContextHolder.wrapSupplier(() -> {
                TraceContext ctx = TraceContextHolder.current();
                return ctx == null ? null : ctx.traceId();
            }), executor).join();
        }
        if (latencyMs > 0) {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (fail) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "simulated failure");
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("traceId", before == null ? null : before.traceId());
        body.put("spanId", before == null ? null : before.spanId());
        body.put("source", before == null ? null : before.source().name());
        body.put("asyncTraceId", asyncTraceId);
        body.put("asyncContextMatched",
                asyncTraceId != null && before != null && asyncTraceId.equals(before.traceId()));
        return body;
    }
}
