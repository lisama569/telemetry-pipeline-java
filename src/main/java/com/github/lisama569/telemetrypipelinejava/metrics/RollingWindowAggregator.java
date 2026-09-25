package com.github.lisama569.telemetrypipelinejava.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 滚动窗口聚合器：按维度对事件做滚动窗口聚合。
 *
 * 一致性保证：
 * - record 与 rotate 在同一监视器下互斥，窗口切换不丢事件；
 * - 已关闭窗口列表只追加且 windowStart 严格单调递增；
 * - 维度基数超过 maxDimensions 时，新维度的事件归入 "__overflow__" 桶
 *   （原始事件不丢弃），并累计溢出计数作为可观测信号；
 * - 窗口已关闭后才到达的迟到事件，确定性地折叠进最早未关闭窗口，
 *   并累计 lateEvents 信号，保证不丢失且结果可重复。
 */
public class RollingWindowAggregator {

    /** 溢出桶维度名 */
    public static final String OVERFLOW_DIMENSION = "__overflow__";

    private final long windowSizeMillis;
    private final int maxDimensions;
    /** 保护活跃窗口与已关闭列表 */
    private final Object lock = new Object();
    /** 串行化 rotate：保证并发调用者的回调也按窗口序全局有序 */
    private final Object rotationLock = new Object();

    /** windowStart -> 窗口累加器（仅未关闭窗口） */
    private final Map<Long, WindowAccumulator> activeWindows = new HashMap<>();
    /** 已关闭窗口快照，只追加，windowStart 单调递增 */
    private final List<WindowSnapshot> closed = new ArrayList<>();
    private final AtomicLong overflowCount = new AtomicLong();
    private final AtomicLong lateCount = new AtomicLong();
    /** 最近一次关闭的窗口起点；-1 表示尚未关闭过窗口 */
    private long lastClosedWindowStart = -1;

    public RollingWindowAggregator(long windowSizeMillis, int maxDimensions) {
        if (windowSizeMillis <= 0 || maxDimensions <= 0) {
            throw new IllegalArgumentException("windowSizeMillis and maxDimensions must be > 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.maxDimensions = maxDimensions;
    }

    /** 写入一条事件；维度超限时归入溢出桶并计数。 */
    public void record(MetricEvent event, long nowMillis) {
        long windowStart = (nowMillis / windowSizeMillis) * windowSizeMillis;
        String dimensionKey = dimensionKey(event.dimensions());
        synchronized (lock) {
            if (windowStart <= lastClosedWindowStart) {
                // 迟到事件：原窗口已关闭，确定性地折叠进最早未关闭窗口，不丢弃
                lateCount.incrementAndGet();
                windowStart = lastClosedWindowStart + windowSizeMillis;
                WindowAccumulator window = activeWindows.computeIfAbsent(windowStart,
                        k -> new WindowAccumulator());
                window.lateEvents++;
                window.add(dimensionKey, event);
                return;
            }
            WindowAccumulator window = activeWindows.computeIfAbsent(windowStart,
                    k -> new WindowAccumulator());
            window.add(dimensionKey, event);
        }
    }

    /**
     * 关闭所有在 now 之前已完整结束的窗口，按 windowStart 升序回调快照。
     * 返回本次关闭的窗口数。
     */
    public int rotate(long nowMillis, Consumer<WindowSnapshot> onWindowClosed) {
        // rotationLock 串行化整个"关闭+回调"过程：
        // 回调全局按 windowStart 升序投递，且 record 只受 lock 影响、不被慢回调阻塞
        synchronized (rotationLock) {
            List<WindowSnapshot> toClose = new ArrayList<>();
            synchronized (lock) {
                List<Long> closable = activeWindows.keySet().stream()
                        .filter(start -> start + windowSizeMillis <= nowMillis)
                        .sorted()
                        .toList();
                for (Long start : closable) {
                    WindowAccumulator acc = activeWindows.remove(start);
                    WindowSnapshot snapshot = acc.snapshot(start, windowSizeMillis);
                    // 单调性校验：已关闭窗口的 windowStart 必须严格递增
                    if (!closed.isEmpty()
                            && closed.get(closed.size() - 1).windowStartEpochMillis() >= start) {
                        throw new IllegalStateException("window order violation at " + start);
                    }
                closed.add(snapshot);
                toClose.add(snapshot);
                lastClosedWindowStart = start;
                }
            }
            // 回调在 record 锁之外、rotation 串行域之内执行
            toClose.forEach(onWindowClosed);
            return toClose.size();
        }
    }

    /** 当前已关闭窗口快照（只读，按时间升序）。 */
    public List<WindowSnapshot> closedWindows() {
        synchronized (lock) {
            return List.copyOf(closed);
        }
    }

    /** 溢出事件总数（可观测信号）。 */
    public long overflowCount() {
        return overflowCount.get();
    }

    /** 迟到事件总数（原窗口已关闭而被折叠的事件，可观测信号）。 */
    public long lateCount() {
        return lateCount.get();
    }

    /** 维度标签 -> 稳定字符串键（按 key 排序，保证可重复）。 */
    static String dimensionKey(Map<String, String> dimensions) {
        if (dimensions.isEmpty()) {
            return "<none>";
        }
        StringBuilder sb = new StringBuilder();
        new TreeMap<>(dimensions).forEach((k, v) -> {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(k).append('=').append(v);
        });
        return sb.toString();
    }

    /** 单个窗口的并发累加器（仅在 lock 内访问）。 */
    private final class WindowAccumulator {
        private final Map<String, MutableStats> byDimension = new LinkedHashMap<>();
        private long overflowEvents;
        private long lateEvents;

        void add(String dimensionKey, MetricEvent event) {
            String key = dimensionKey;
            if (!byDimension.containsKey(key)
                    && !OVERFLOW_DIMENSION.equals(key)
                    && byDimension.size() >= maxDimensions) {
                // 基数超限：归入溢出桶，原始事件不丢弃
                key = OVERFLOW_DIMENSION;
                overflowEvents++;
                overflowCount.incrementAndGet();
            }
            byDimension.computeIfAbsent(key, k -> new MutableStats()).add(event);
        }

        WindowSnapshot snapshot(long windowStart, long size) {
            Map<String, WindowSnapshot.DimensionStats> stats = new LinkedHashMap<>();
            byDimension.forEach((k, v) -> stats.put(k, v.toImmutable()));
            return new WindowSnapshot(windowStart, size, stats, overflowEvents, lateEvents);
        }
    }

    private static final class MutableStats {
        private long count;
        private long errorCount;
        private long totalLatency;
        private long maxLatency;

        void add(MetricEvent e) {
            count++;
            if (e.error()) {
                errorCount++;
            }
            totalLatency += e.latencyMillis();
            maxLatency = Math.max(maxLatency, e.latencyMillis());
        }

        WindowSnapshot.DimensionStats toImmutable() {
            return new WindowSnapshot.DimensionStats(count, errorCount, totalLatency, maxLatency);
        }
    }
}
