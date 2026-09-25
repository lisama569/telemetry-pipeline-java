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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 维度基数超限：超出部分归入溢出桶、给出可观测信号、原始事件不丢弃、基数不无界增长。
 */
class CardinalityOverflowTest {

    private static final Logger log = LoggerFactory.getLogger(CardinalityOverflowTest.class);

    private static final long WINDOW = 1_000;

    @Test
    void overflowDimensionsFoldIntoOverflowBucket() {
        int maxDimensions = 3;
        int distinctDimensions = 10;
        int eventsPerDimension = 4;
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, maxDimensions);
        for (int d = 0; d < distinctDimensions; d++) {
            for (int i = 0; i < eventsPerDimension; i++) {
                agg.record(new MetricEvent(5, false, Map.of("user", "u" + d)), 0);
            }
        }
        List<WindowSnapshot> closed = new ArrayList<>();
        agg.rotate(WINDOW, closed::add);
        assertEquals(1, closed.size(), "应关闭一个窗口");
        WindowSnapshot snap = closed.get(0);
        log.info("input: distinctDimensions={} eventsPerDimension={} maxDimensions={}",
                distinctDimensions, eventsPerDimension, maxDimensions);
        log.info("window dimensions={} overflowEvents={} globalOverflow={}",
                snap.statsByDimension().keySet(), snap.overflowEvents(), agg.overflowCount());

        assertTrue(snap.statsByDimension().size() <= maxDimensions + 1,
                "维度基数不得无界增长（上限 + 溢出桶）");
        WindowSnapshot.DimensionStats overflow =
                snap.statsByDimension().get(RollingWindowAggregator.OVERFLOW_DIMENSION);
        assertNotNull(overflow, "超限维度必须归入溢出桶 " + RollingWindowAggregator.OVERFLOW_DIMENSION);

        long expectedOverflow = (long) (distinctDimensions - maxDimensions) * eventsPerDimension;
        assertEquals(expectedOverflow, overflow.count(), "溢出桶必须保留全部超限原始事件");
        assertEquals(expectedOverflow, snap.overflowEvents(), "窗口级溢出信号计数不符");
        assertEquals(expectedOverflow, agg.overflowCount(), "全局溢出信号计数不符");

        long total = snap.statsByDimension().values().stream()
                .mapToLong(WindowSnapshot.DimensionStats::count).sum();
        assertEquals((long) distinctDimensions * eventsPerDimension, total, "原始事件总数不得丢失");
    }

    @Test
    void existingDimensionsStillAcceptedAfterLimit() {
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, 2);
        agg.record(new MetricEvent(1, false, Map.of("k", "a")), 0);
        agg.record(new MetricEvent(1, false, Map.of("k", "b")), 0);
        agg.record(new MetricEvent(1, false, Map.of("k", "c")), 0); // 溢出
        agg.record(new MetricEvent(1, false, Map.of("k", "a")), 0); // 已有维度仍正常计入
        List<WindowSnapshot> closed = new ArrayList<>();
        agg.rotate(WINDOW, closed::add);
        WindowSnapshot snap = closed.get(0);
        log.info("dimensions={} overflow={}", snap.statsByDimension().keySet(), agg.overflowCount());
        assertEquals(2, snap.statsByDimension().get("k=a").count(), "已有维度在超限后仍应正常累计");
        assertEquals(1, agg.overflowCount(), "只有新维度 c 应计入溢出");
    }

    @Test
    void lateEventsFoldIntoEarliestOpenWindow() {
        RollingWindowAggregator agg = new RollingWindowAggregator(WINDOW, 10);
        agg.record(new MetricEvent(1, false, Map.of("k", "x")), 0);
        List<WindowSnapshot> closed = new ArrayList<>();
        agg.rotate(WINDOW, closed::add); // 关闭窗口 [0,1000)
        // 迟到事件：时间戳仍落在已关闭窗口
        agg.record(new MetricEvent(1, false, Map.of("k", "x")), 100);
        agg.record(new MetricEvent(1, false, Map.of("k", "x")), 500);
        agg.rotate(2 * WINDOW, closed::add);
        log.info("closedWindows={} lateCount={}", closed, agg.lateCount());
        assertEquals(2, closed.size(), "迟到事件应折叠进下一个窗口而非丢弃");
        WindowSnapshot second = closed.get(1);
        assertEquals(2, second.statsByDimension().get("k=x").count(), "迟到事件必须被计数");
        assertEquals(2, second.lateEvents(), "窗口级迟到信号计数不符");
        assertEquals(2, agg.lateCount(), "全局迟到信号计数不符");
        assertTrue(closed.get(1).windowStartEpochMillis() > closed.get(0).windowStartEpochMillis(),
                "窗口序必须保持单调");
    }
}
