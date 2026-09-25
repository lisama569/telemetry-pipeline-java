package com.github.lisama569.telemetrypipelinejava.tests;

import com.github.lisama569.telemetrypipelinejava.metrics.MetricEvent;
import com.github.lisama569.telemetrypipelinejava.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.metrics.WindowSnapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发聚合与窗口切换：事件不丢失、窗口序单调、结果可重复。
 */
class RollingWindowAggregatorTest {

    private static final Logger log = LoggerFactory.getLogger(RollingWindowAggregatorTest.class);

    private static final long WINDOW = 1_000;

    @Test
    void concurrentWritesAndRotationLoseNothing() throws Exception {
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, 1000);
        int threads = 8;
        int eventsPerThread = 5_000;
        AtomicLong clock = new AtomicLong(0);
        List<WindowSnapshot> closed = java.util.Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < eventsPerThread; i++) {
                    long now = clock.incrementAndGet(); // 时间单调推进，跨多个窗口
                    agg.record(new MetricEvent(10, i % 7 == 0, Map.of("path", "/api/demo")), now);
                    if (i % 50 == 0) {
                        agg.rotate(now, closed::add); // 写入与轮换并发交错
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "写入线程应全部完成");
        // 最终轮换：关闭所有剩余窗口
        agg.rotate(clock.get() + WINDOW * 2, closed::add);

        // 回调收集的窗口与聚合器内部已关闭窗口必须一致
        assertEquals(closed, agg.closedWindows(), "回调窗口序列应与聚合器内部记录一致");
        long totalCount = agg.closedWindows().stream()
                .flatMap(w -> w.statsByDimension().values().stream())
                .mapToLong(WindowSnapshot.DimensionStats::count).sum();
        long expected = (long) threads * eventsPerThread;
        log.info("input: threads={} eventsPerThread={} expected={} actual={} windows={}",
                threads, eventsPerThread, expected, totalCount, closed.size());
        assertEquals(expected, totalCount, "并发写入+并发轮换后事件总数不得丢失");

        for (int i = 1; i < closed.size(); i++) {
            long prev = closed.get(i - 1).windowStartEpochMillis();
            long curr = closed.get(i).windowStartEpochMillis();
            assertTrue(curr > prev, "已关闭窗口 windowStart 必须严格单调递增: " + prev + " -> " + curr);
        }
    }

    @Test
    void aggregationIsRepeatableForSameInput() {
        List<MetricEvent> events = List.of(
                new MetricEvent(10, false, Map.of("path", "/a")),
                new MetricEvent(30, true, Map.of("path", "/a")),
                new MetricEvent(20, false, Map.of("path", "/b")));
        List<WindowSnapshot> run1 = runOnce(events);
        List<WindowSnapshot> run2 = runOnce(events);
        log.info("run1={} run2={}", run1, run2);
        assertEquals(run1, run2, "相同输入的聚合结果必须可重复");
    }

    private List<WindowSnapshot> runOnce(List<MetricEvent> events) {
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, 100);
        long now = WINDOW; // 所有事件落在 [1000,2000) 窗口
        for (MetricEvent e : events) {
            agg.record(e, now);
        }
        List<WindowSnapshot> out = new ArrayList<>();
        agg.rotate(now + WINDOW, out::add);
        return out;
    }

    @Test
    void dimensionStatsAreCorrect() {
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, 100);
        long now = 0;
        agg.record(new MetricEvent(10, false, Map.of("path", "/a")), now);
        agg.record(new MetricEvent(30, true, Map.of("path", "/a")), now);
        List<WindowSnapshot> closed = new ArrayList<>();
        agg.rotate(WINDOW, closed::add);
        assertEquals(1, closed.size(), "应关闭一个窗口");
        WindowSnapshot.DimensionStats stats = closed.get(0).statsByDimension().get("path=/a");
        log.info("stats={}", stats);
        assertEquals(2, stats.count(), "count 不符");
        assertEquals(1, stats.errorCount(), "errorCount 不符");
        assertEquals(40, stats.totalLatencyMillis(), "totalLatency 不符");
        assertEquals(30, stats.maxLatencyMillis(), "maxLatency 不符");
        assertEquals(0.5, stats.errorRate(), 1e-9, "errorRate 不符");
    }
}
