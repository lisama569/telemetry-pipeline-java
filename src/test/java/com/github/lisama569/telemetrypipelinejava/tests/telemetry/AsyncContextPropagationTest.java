package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.trace.ContextPropagatingExecutor;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContext;
import com.github.lisama569.telemetrypipelinejava.telemetry.trace.TraceContextHolder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Async propagation: the trace id must survive thread hops as a child span,
 * and worker threads must not leak context afterwards.
 */
class AsyncContextPropagationTest {

    private static final Logger log =
            LoggerFactory.getLogger(AsyncContextPropagationTest.class);

    @AfterEach
    void cleanup() {
        TraceContextHolder.clear();
    }

    @Test
    void contextPropagatesAcrossAsyncHopsAsChildSpan() {
        Executor executor = new ContextPropagatingExecutor(Executors.newCachedThreadPool());
        TraceContext root = TraceContext.newRoot(TraceContext.Origin.GENERATED_MISSING);
        log.info("input: root traceId={} spanId={}", root.traceId(), root.spanId());

        try (TraceContextHolder.Scope ignored = TraceContextHolder.open(root)) {
            // The nested hop is submitted from *inside* the first async task,
            // so the submission-time context is deterministic.
            TraceContext asyncSeen = CompletableFuture.supplyAsync(() -> {
                TraceContext first = TraceContextHolder.current();
                TraceContext second = CompletableFuture
                        .supplyAsync(TraceContextHolder::current, executor).join();
                log.info("intermediate: hop1 traceId={} spanId={} parent={}",
                        first.traceId(), first.spanId(), first.parentSpanId());
                log.info("intermediate: hop2 traceId={} spanId={} parent={}",
                        second.traceId(), second.spanId(), second.parentSpanId());
                assertThat(second.traceId())
                        .as("nested hop keeps trace id")
                        .isEqualTo(first.traceId());
                assertThat(second.parentSpanId())
                        .as("nested hop parents to hop1 span")
                        .isEqualTo(first.spanId());
                return first;
            }, executor).join();

            log.info("verdict: async traceId={} expected={} parentSpanId={} expectedParent={}",
                    asyncSeen.traceId(), root.traceId(), asyncSeen.parentSpanId(), root.spanId());
            assertThat(asyncSeen).as("context present inside async task").isNotNull();
            assertThat(asyncSeen.traceId()).as("trace id preserved across threads")
                    .isEqualTo(root.traceId());
            assertThat(asyncSeen.parentSpanId()).as("async span is a child of the request span")
                    .isEqualTo(root.spanId());
            assertThat(asyncSeen.spanId()).as("async span has its own span id")
                    .isNotEqualTo(root.spanId());
            assertThat(TraceContextHolder.current()).as("caller context intact after async hops")
                    .isSameAs(root);
        }
        assertThat(TraceContextHolder.current()).as("scope close clears the thread").isNull();
    }

    @Test
    void workerThreadDoesNotLeakContextBetweenTasks() throws Exception {
        ExecutorService single = Executors.newSingleThreadExecutor();
        Executor executor = new ContextPropagatingExecutor(single);
        TraceContext root = TraceContext.newRoot(TraceContext.Origin.GENERATED_MISSING);

        try (TraceContextHolder.Scope ignored = TraceContextHolder.open(root)) {
            TraceContext seen = CompletableFuture
                    .supplyAsync(TraceContextHolder::current, executor).join();
            assertThat(seen).isNotNull();
        }
        // Same worker thread, task submitted without any context: must see none.
        TraceContext leaked = CompletableFuture
                .supplyAsync(TraceContextHolder::current, executor).join();
        log.info("verdict: context on reused worker without submission context={}", leaked);
        assertThat(leaked).as("no context leakage on reused worker thread").isNull();
        single.shutdown();
    }

    @Test
    void taskSubmittedWithoutContextRunsWithoutFabricatedContext() {
        Executor executor = new ContextPropagatingExecutor(Executors.newCachedThreadPool());
        TraceContext seen = CompletableFuture
                .supplyAsync(TraceContextHolder::current, executor).join();
        log.info("verdict: context without active submission context={}", seen);
        assertThat(seen).as("no fabricated context").isNull();
    }
}
