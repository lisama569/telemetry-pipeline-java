package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrent aggregation: totals must be monotonic, repeatable and lossless.
 */
class RollingWindowAggregatorConcurrencyTest {

    private static final Logger log =
            LoggerFactory.getLogger(RollingWindowAggregatorConcurrencyTest.class);

    private static final long WINDOW = 1_000;

    @Test
    void concurrentWritesWithinOneWindowAreLosslessAndMonotonic() throws InterruptedException {
        int threads = 8;
        int eventsPerThread = 500;
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(WINDOW, 4, new CardinalityLimiter(10));
        DimensionKey dimension = new DimensionKey("GET /api/work", "2xx");
        long now = 5_000; // aligned: window index 5

        log.info("input: threads={} eventsPerThread={} windowStart={}", threads, eventsPerThread,
                now);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            pool.submit(() -> {
                await(start);
                for (int i = 0; i < eventsPerThread; i++) {
                    aggregator.record(dimension, 10 + (i % 5), (threadId + i) % 7 == 0, now);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS))
                .as("all writer threads finish").isTrue();

        List<WindowSnapshot> active = aggregator.activeSnapshots(now);
        log.info("intermediate: activeSnapshots={}", active);
        assertThat(active).as("exactly one active window for the dimension").hasSize(1);
        WindowSnapshot snapshot = active.get(0);

        long expectedCount = (long) threads * eventsPerThread;
        long expectedErrors = 0;
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < eventsPerThread; i++) {
                if ((t + i) % 7 == 0) {
                    expectedErrors++;
                }
            }
        }
        log.info("verdict: count={} expected={} errors={} expectedErrors={}",
                snapshot.requestCount(), expectedCount, snapshot.errorCount(), expectedErrors);
        assertThat(snapshot.requestCount()).as("no event lost under concurrency")
                .isEqualTo(expectedCount);
        assertThat(snapshot.errorCount()).as("error count exact under concurrency")
                .isEqualTo(expectedErrors);
        assertThat(snapshot.maxLatencyMillis()).as("max latency").isEqualTo(14);
        assertThat(aggregator.lateEventCount()).as("no late events").isZero();
    }

    @Test
    void windowRotationEmitsCompletedWindowExactlyOnceAndKeepsTotals() {
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(WINDOW, 4, new CardinalityLimiter(10));
        DimensionKey dimension = new DimensionKey("GET /a", "2xx");
        List<WindowSnapshot> emitted = new CopyOnWriteArrayList<>();
        aggregator.addWindowListener(emitted::add);

        log.info("input: 3 events in window[0,1000), then 2 events in window[1000,2000)");
        aggregator.record(dimension, 10, false, 100);
        aggregator.record(dimension, 20, true, 200);
        aggregator.record(dimension, 30, false, 300);
        aggregator.record(dimension, 40, false, 1_100);
        aggregator.record(dimension, 50, true, 1_200);

        log.info("intermediate: emitted={} active={}", emitted,
                aggregator.activeSnapshots(1_200));
        assertThat(emitted).as("old window emitted exactly once").hasSize(1);
        WindowSnapshot first = emitted.get(0);
        assertThat(first.windowStartEpochMillis()).as("completed window start").isEqualTo(0);
        assertThat(first.requestCount()).as("completed window count").isEqualTo(3);
        assertThat(first.errorCount()).as("completed window errors").isEqualTo(1);
        assertThat(first.totalLatencyMillis()).as("completed window latency").isEqualTo(60);

        List<WindowSnapshot> active = aggregator.activeSnapshots(1_200);
        assertThat(active).hasSize(1);
        assertThat(active.get(0).requestCount()).as("new window count").isEqualTo(2);

        long total = first.requestCount() + active.get(0).requestCount();
        log.info("verdict: completed={} active={} total={} expected=5",
                first.requestCount(), active.get(0).requestCount(), total);
        assertThat(total).as("rotation loses nothing").isEqualTo(5);
    }

    @Test
    void lateEventsAreCountedNotDropped() {
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(WINDOW, 2, new CardinalityLimiter(10));
        DimensionKey dimension = new DimensionKey("GET /b", "2xx");

        aggregator.record(dimension, 10, false, 5_000); // window 5 occupies the slot
        aggregator.record(dimension, 10, false, 1_000); // window 1: slot held by newer window
        log.info("intermediate: lateEvents={} active={}", aggregator.lateEventCount(),
                aggregator.activeSnapshots(5_000));
        assertThat(aggregator.lateEventCount())
                .as("late event is observable, not silently dropped").isEqualTo(1);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
