package com.github.lisama569.telemetrypipelinejava.tests;

import com.github.lisama569.telemetrypipelinejava.alerting.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.alerting.AlertRule;
import com.github.lisama569.telemetrypipelinejava.metrics.MetricEvent;
import com.github.lisama569.telemetrypipelinejava.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.pipeline.PipelineErrorLog;
import com.github.lisama569.telemetrypipelinejava.pipeline.TelemetryPipeline;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 采集/告警链路自身异常：不得影响业务调用，异常原因可区分、可本地查询。
 */
class PipelineResilienceTest {

    private static final Logger log = LoggerFactory.getLogger(PipelineResilienceTest.class);

    /** 构造 record 必抛异常的聚合器 */
    private static RollingWindowAggregator failingAggregator() {
        return new RollingWindowAggregator(1_000, 10) {
            @Override
            public void record(MetricEvent event, long nowMillis) {
                throw new IllegalStateException("simulated store failure");
            }
        };
    }

    @Test
    void aggregateFailureDoesNotPropagateAndIsLogged() {
        PipelineErrorLog errorLog = new PipelineErrorLog(16);
        TelemetryPipeline pipeline = new TelemetryPipeline(
                failingAggregator(),
                new AlertEvaluator(List.of()),
                errorLog);
        MetricEvent event = new MetricEvent(5, false, Map.of("path", "/a"));

        assertDoesNotThrow(() -> pipeline.recordRequest(event),
                "聚合阶段异常不得向业务调用方抛出");
        assertEquals(1, errorLog.totalRecorded(), "异常必须被记录");
        PipelineErrorLog.Entry entry = errorLog.entries().get(0);
        log.info("logged entry: stage={} reason={} detail={}",
                entry.stage(), entry.reason(), entry.detail());
        assertEquals("aggregate", entry.stage(), "异常阶段应可区分");
        assertEquals("IllegalStateException", entry.reason(), "异常原因应可区分");
        assertTrue(entry.detail().contains("simulated store failure"), "详情应包含原始信息");
    }

    @Test
    void alertFailureDoesNotPropagateAndIsLogged() {
        PipelineErrorLog errorLog = new PipelineErrorLog(16);
        // 评估必抛异常的告警器
        AlertEvaluator failingEvaluator = new AlertEvaluator(List.of(
                new AlertRule("r", AlertRule.Metric.ERROR_RATE, 0.5, 0.3, 1, 1))) {
            @Override
            public synchronized List<com.github.lisama569.telemetrypipelinejava.alerting.AlertEvent>
                    evaluate(com.github.lisama569.telemetrypipelinejava.metrics.WindowSnapshot snapshot) {
                throw new UnsupportedOperationException("simulated alert failure");
            }
        };
        java.util.concurrent.atomic.AtomicLong fakeClock =
                new java.util.concurrent.atomic.AtomicLong(0);
        TelemetryPipeline pipeline = new TelemetryPipeline(
                new RollingWindowAggregator(1_000, 10), failingEvaluator, errorLog,
                fakeClock::get);

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 3; i++) {
                pipeline.recordRequest(new MetricEvent(5, false, Map.of()));
                fakeClock.addAndGet(1_000); // 推进时钟使窗口关闭、触发评估
            }
        }, "告警阶段异常不得向业务调用方抛出");
        log.info("errorLog entries={}", errorLog.entries());
        assertTrue(errorLog.entries().stream().anyMatch(e -> e.stage().equals("alert-evaluate")),
                "告警评估异常必须被记录且阶段可区分");
    }

    @Test
    void errorLogRingBufferEvictsOldest() {
        PipelineErrorLog errorLog = new PipelineErrorLog(4);
        for (int i = 0; i < 10; i++) {
            errorLog.record("stage", "reason", "detail-" + i);
        }
        List<PipelineErrorLog.Entry> entries = errorLog.entries();
        log.info("totalRecorded={} retained={}", errorLog.totalRecorded(), entries);
        assertEquals(10, errorLog.totalRecorded(), "累计计数应包含被覆盖的记录");
        assertEquals(4, entries.size(), "环形缓冲只保留最近 N 条");
        assertEquals("detail-6", entries.get(0).detail(), "最旧保留记录应为 detail-6");
        assertEquals("detail-9", entries.get(3).detail(), "最新记录应为 detail-9");
    }
}
