package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.CardinalityLimiter;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cardinality overflow: excess dimensions fold into a bounded overflow bucket
 * with an observable signal; no event is dropped and growth stays bounded.
 */
class DimensionOverflowTest {

    private static final Logger log = LoggerFactory.getLogger(DimensionOverflowTest.class);

    @Test
    void excessDimensionsFoldIntoOverflowBucketWithoutLoss() {
        int maxDimensions = 4;
        int distinctDimensions = 10;
        int eventsPerDimension = 3;
        CardinalityLimiter limiter = new CardinalityLimiter(maxDimensions);
        RollingWindowAggregator aggregator =
                new RollingWindowAggregator(1_000, 4, limiter);
        long now = 5_000;

        log.info("input: maxDimensions={} distinctDimensions={} eventsPerDimension={}",
                maxDimensions, distinctDimensions, eventsPerDimension);
        for (int d = 0; d < distinctDimensions; d++) {
            DimensionKey key = new DimensionKey("GET /route-" + d, "2xx");
            for (int i = 0; i < eventsPerDimension; i++) {
                aggregator.record(key, 10, false, now);
            }
        }

        List<WindowSnapshot> active = aggregator.activeSnapshots(now);
        long totalCounted = active.stream().mapToLong(WindowSnapshot::requestCount).sum();
        long expected = (long) distinctDimensions * eventsPerDimension;
        log.info("intermediate: activeDimensions={} snapshots={}", limiter.activeDimensionCount(),
                active);
        log.info("verdict: totalCounted={} expected={} overflowedEvents={} expectedOverflow={}",
                totalCounted, expected, limiter.overflowedEventCount(),
                (long) (distinctDimensions - maxDimensions) * eventsPerDimension);

        assertThat(limiter.activeDimensionCount())
                .as("dimension growth is bounded").isEqualTo(maxDimensions);
        assertThat(totalCounted).as("no event dropped: overflow events still counted")
                .isEqualTo(expected);
        assertThat(limiter.overflowedEventCount()).as("overflow signal is exact")
                .isEqualTo((long) (distinctDimensions - maxDimensions) * eventsPerDimension);
        WindowSnapshot overflow = active.stream()
                .filter(s -> s.dimension().equals(DimensionKey.OVERFLOW))
                .findFirst().orElseThrow();
        assertThat(overflow.requestCount()).as("overflow bucket holds the folded events")
                .isEqualTo((long) (distinctDimensions - maxDimensions) * eventsPerDimension);
    }

    @Test
    void concurrentAdmissionRespectsTheBound() throws InterruptedException {
        int maxDimensions = 8;
        int threads = 16;
        int keysPerThread = 10;
        CardinalityLimiter limiter = new CardinalityLimiter(maxDimensions);
        Set<DimensionKey> admitted = new HashSet<>();
        Set<DimensionKey> overflowed = new HashSet<>();

        log.info("input: maxDimensions={} threads={} keysPerThread={}",
                maxDimensions, threads, keysPerThread);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            int threadId = t;
            pool.submit(() -> {
                await(start);
                for (int k = 0; k < keysPerThread; k++) {
                    DimensionKey requested =
                            new DimensionKey("GET /r-" + threadId + "-" + k, "2xx");
                    DimensionKey effective = limiter.resolve(requested);
                    synchronized (admitted) {
                        (DimensionKey.OVERFLOW.equals(effective) ? overflowed : admitted)
                                .add(requested);
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        log.info("verdict: admitted={} overflowedKeys={} activeDimensions={}",
                admitted.size(), overflowed.size(), limiter.activeDimensionCount());
        assertThat(admitted.size()).as("at most maxDimensions admitted under concurrency")
                .isLessThanOrEqualTo(maxDimensions);
        assertThat(limiter.activeDimensionCount()).isLessThanOrEqualTo(maxDimensions);
        assertThat(admitted).as("admitted and overflowed sets are disjoint")
                .doesNotContainAnyElementsOf(overflowed);
        assertThat(admitted.size() + overflowed.size())
                .as("every key accounted for").isEqualTo(threads * keysPerThread);
    }

    @Test
    void repeatResolutionIsStable() {
        CardinalityLimiter limiter = new CardinalityLimiter(2);
        DimensionKey a = new DimensionKey("GET /a", "2xx");
        DimensionKey b = new DimensionKey("GET /b", "2xx");
        DimensionKey c = new DimensionKey("GET /c", "2xx");

        DimensionKey firstA = limiter.resolve(a);
        limiter.resolve(b);
        DimensionKey firstC = limiter.resolve(c);
        DimensionKey secondA = limiter.resolve(a);
        DimensionKey secondC = limiter.resolve(c);
        log.info("verdict: a->{} then {} ; c->{} then {}", firstA, secondA, firstC, secondC);
        assertThat(secondA).as("known dimension keeps its own bucket").isEqualTo(firstA);
        assertThat(secondC).as("overflowed dimension stays overflowed (stable)")
                .isEqualTo(DimensionKey.OVERFLOW);
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
