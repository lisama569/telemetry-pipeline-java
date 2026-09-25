package com.github.lisama569.telemetrypipelinejava.telemetry.web;

import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContext;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContextHolder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Demo business endpoint exercising sync + async work under a trace context.
 * The async hop runs on the context-propagating executor, so the trace id is
 * preserved and a child span id is visible in the response.
 */
@RestController
public class DemoController {

    private final Executor telemetryExecutor;

    public DemoController(@Qualifier("telemetryExecutor") Executor telemetryExecutor) {
        this.telemetryExecutor = telemetryExecutor;
    }

    @GetMapping("/api/work")
    public ResponseEntity<Map<String, Object>> work(
            @RequestParam(defaultValue = "0") long latencyMillis,
            @RequestParam(defaultValue = "false") boolean fail)
            throws ExecutionException, InterruptedException {
        TraceContext sync = TraceContextHolder.current();
        CompletableFuture<Map<String, Object>> asyncHop = CompletableFuture.supplyAsync(() -> {
            sleep(latencyMillis);
            TraceContext async = TraceContextHolder.current();
            Map<String, Object> hop = new LinkedHashMap<>();
            hop.put("traceId", async == null ? null : async.traceId());
            hop.put("spanId", async == null ? null : async.spanId());
            hop.put("parentSpanId", async == null ? null : async.parentSpanId());
            return hop;
        }, telemetryExecutor);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("syncTraceId", sync == null ? null : sync.traceId());
        body.put("syncSpanId", sync == null ? null : sync.spanId());
        body.put("asyncHop", asyncHop.get());
        if (fail) {
            return ResponseEntity.status(500).body(body);
        }
        return ResponseEntity.ok(body);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
