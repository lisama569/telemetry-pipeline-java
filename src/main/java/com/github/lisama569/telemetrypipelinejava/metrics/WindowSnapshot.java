package com.github.lisama569.telemetrypipelinejava.metrics;

import java.util.Map;

/**
 * 一个已完成窗口的不可变聚合快照。
 *
 * @param overflowEvents 本窗口内因维度超限被归入溢出桶的事件数（可观测信号）
 * @param lateEvents     本窗口内收纳的迟到事件数（原窗口已关闭，折叠进本窗口）
 */
public record WindowSnapshot(long windowStartEpochMillis, long windowSizeMillis,
                             Map<String, DimensionStats> statsByDimension,
                             long overflowEvents, long lateEvents) {

    public WindowSnapshot {
        statsByDimension = Map.copyOf(statsByDimension);
    }

    public record DimensionStats(long count, long errorCount, long totalLatencyMillis,
                                 long maxLatencyMillis) {
        public double avgLatencyMillis() {
            return count == 0 ? 0.0 : (double) totalLatencyMillis / count;
        }

        public double errorRate() {
            return count == 0 ? 0.0 : (double) errorCount / count;
        }
    }
}
