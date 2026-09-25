package com.github.lisama569.telemetrypipelinejava.tests;

import com.github.lisama569.telemetrypipelinejava.tracing.TraceContext;
import com.github.lisama569.telemetrypipelinejava.tracing.TraceContextHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异步/嵌套调用中的追踪上下文传播：必须关联正确、线程间不串线、执行后无残留。
 */
class TraceContextPropagationTest {

    private static final Logger log = LoggerFactory.getLogger(TraceContextPropagationTest.class);

    @AfterEach
    void cleanup() {
        TraceContextHolder.clear();
    }

    private static TraceContext ctx(String traceId) {
        return new TraceContext(traceId, "00f067aa0ba902b7", null, true,
                TraceContext.Source.HEADER);
    }

    @Test
    void contextPropagatesToAsyncRunnableAndCallable() throws Exception {
        TraceContext parent = ctx("a".repeat(32));
        TraceContextHolder.set(parent);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> r = pool.submit(TraceContextHolder.wrap(() ->
                    log.info("runnable sees traceId={}", TraceContextHolder.current().traceId())));
            r.get(5, TimeUnit.SECONDS);

            Callable<String> callable = TraceContextHolder.wrap(() -> {
                TraceContext c = TraceContextHolder.current();
                log.info("callable sees traceId={}", c == null ? null : c.traceId());
                return c == null ? null : c.traceId();
            });
            String seen = pool.submit(callable).get(5, TimeUnit.SECONDS);
            log.info("input traceId={} async observed={}", parent.traceId(), seen);
            assertEquals(parent.traceId(), seen, "异步任务必须看到与提交线程一致的 traceId");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void workerThreadHasNoResidualContextAfterTask() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            TraceContextHolder.set(ctx("b".repeat(32)));
            pool.submit(TraceContextHolder.wrap(() ->
                    log.info("task1 traceId={}", TraceContextHolder.current().traceId())))
                    .get(5, TimeUnit.SECONDS);

            // 同一线程执行未包装任务：不应看到上一个任务的上下文
            TraceContextHolder.clear();
            Future<String> residual = pool.submit(() -> {
                TraceContext c = TraceContextHolder.current();
                log.info("unwrapped task sees context={}", c);
                return c == null ? "clean" : "leaked:" + c.traceId();
            });
            assertEquals("clean", residual.get(5, TimeUnit.SECONDS),
                    "包装任务执行后不得在工作线程残留上下文（防串线）");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentTasksDoNotCrossWire() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        ExecutorService inner = Executors.newCachedThreadPool();
        try {
            List<Future<Boolean>> futures = IntStream.range(0, 64)
                    .mapToObj(i -> {
                        String traceId = String.format("%032x", i);
                        return pool.submit(() -> {
                            TraceContextHolder.set(ctx(traceId));
                            Boolean ok = inner.submit(TraceContextHolder.wrap(() -> {
                                TraceContext c = TraceContextHolder.current();
                                boolean matched = c != null && c.traceId().equals(traceId);
                                if (!matched) {
                                    log.error("cross-wire: expected={} actual={}", traceId, c);
                                }
                                return matched;
                            })).get(5, TimeUnit.SECONDS);
                            TraceContextHolder.clear();
                            return ok;
                        });
                    })
                    .toList();
            for (Future<Boolean> f : futures) {
                assertTrue(f.get(10, TimeUnit.SECONDS), "并发任务间不得出现上下文串线");
            }
            log.info("64 concurrent tasks: no cross-wiring detected");
        } finally {
            pool.shutdownNow();
            inner.shutdownNow();
        }
    }

    @Test
    void childContextKeepsTraceIdAndChainsParent() {
        TraceContext parent = ctx("c".repeat(32));
        TraceContext child = parent.child("1111111111111111");
        log.info("parent span={} child span={} child.parentSpan={}",
                parent.spanId(), child.spanId(), child.parentSpanId());
        assertEquals(parent.traceId(), child.traceId(), "子上下文必须保持同一 traceId");
        assertEquals(parent.spanId(), child.parentSpanId(), "子上下文 parentSpanId 必须指向父 span");
    }

    @Test
    void clearRemovesContext() {
        TraceContextHolder.set(ctx("d".repeat(32)));
        TraceContextHolder.clear();
        assertNull(TraceContextHolder.current(), "clear 后当前线程不得持有上下文");
    }
}
