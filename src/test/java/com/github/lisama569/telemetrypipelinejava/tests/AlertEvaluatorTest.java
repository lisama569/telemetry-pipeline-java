package com.github.lisama569.telemetrypipelinejava.tests;

import com.github.lisama569.telemetrypipelinejava.alerting.AlertEvent;
import com.github.lisama569.telemetrypipelinejava.alerting.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.alerting.AlertRule;
import com.github.lisama569.telemetrypipelinejava.alerting.AlertState;
import com.github.lisama569.telemetrypipelinejava.metrics.WindowSnapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警滞回状态机：触发/持续/恢复明确，滞回区间内波动不抖动。
 */
class AlertEvaluatorTest {

    private static final Logger log = LoggerFactory.getLogger(AlertEvaluatorTest.class);

    private static final long WINDOW = 1_000;

    /** 触发阈值 0.5，恢复阈值 0.3，连续 2 窗口触发 / 2 窗口恢复 */
    private static final AlertRule RULE = new AlertRule("error-rate",
            AlertRule.Metric.ERROR_RATE, 0.5, 0.3, 2, 2);

    private static WindowSnapshot window(long start, long count, long errors) {
        WindowSnapshot.DimensionStats stats =
                new WindowSnapshot.DimensionStats(count, errors, 0, 0);
        return new WindowSnapshot(start, WINDOW, Map.of("path=/a", stats), 0, 0);
    }

    private AlertState feed(AlertEvaluator ev, List<WindowSnapshot> windows) {
        AlertState state = AlertState.OK;
        for (WindowSnapshot w : windows) {
            List<AlertEvent> events = ev.evaluate(w);
            state = ev.currentStates().get("error-rate");
            log.info("window start={} value={} transitions={} -> state={}",
                    w.windowStartEpochMillis(),
                    AlertEvaluator.observe(AlertRule.Metric.ERROR_RATE, w), events, state);
        }
        return state;
    }

    @Test
    void triggersAfterConsecutiveBreachesAndRecoversAfterConsecutiveDrops() {
        AlertEvaluator ev = new AlertEvaluator(List.of(RULE));
        // 窗口序列：0.6(越限) 0.7(越限->触发) 0.8(持续) 0.1(回落) 0.1(回落->恢复)
        AlertState state = feed(ev, List.of(
                window(0, 10, 6),
                window(WINDOW, 10, 7),
                window(2 * WINDOW, 10, 8),
                window(3 * WINDOW, 10, 1),
                window(4 * WINDOW, 10, 1)));
        log.info("final state={}", state);
        assertEquals(AlertState.OK, state, "连续两个回落窗口后应恢复为 OK");
    }

    @Test
    void singleWindowSpikeDoesNotFire() {
        AlertEvaluator ev = new AlertEvaluator(List.of(RULE));
        // 单次尖峰 1.0 后立刻回落：不得触发
        AlertState state = feed(ev, List.of(
                window(0, 10, 10),
                window(WINDOW, 10, 0),
                window(2 * WINDOW, 10, 0)));
        assertEquals(AlertState.OK, state, "短暂尖峰不得触发告警（防抖动）");
    }

    @Test
    void hysteresisBandFluctuationDoesNotFlap() {
        AlertEvaluator ev = new AlertEvaluator(List.of(RULE));
        // 先触发
        AlertState s1 = feed(ev, List.of(window(0, 10, 6), window(WINDOW, 10, 6)));
        assertEquals(AlertState.FIRING, s1, "前置条件：应已触发");
        // 滞回区间 [0.3, 0.5) 内反复波动：0.4, 0.35, 0.45 —— 不得恢复也不得抖动
        AlertState s2 = feed(ev, List.of(
                window(2 * WINDOW, 10, 4),
                window(3 * WINDOW, 10, 3),   // 0.3 不算回落（需 < 0.3）
                window(4 * WINDOW, 10, 4)));
        log.info("state after in-band fluctuation={}", s2);
        assertEquals(AlertState.FIRING, s2, "滞回区间内波动不得改变触发状态");
    }

    @Test
    void duplicateWindowIsIgnoredForRepeatability() {
        AlertEvaluator ev = new AlertEvaluator(List.of(RULE));
        feed(ev, List.of(window(0, 10, 6)));
        List<AlertEvent> dup = ev.evaluate(window(0, 10, 6));
        log.info("duplicate window transitions={}", dup);
        assertTrue(dup.isEmpty(), "重复窗口必须被忽略，保证评估可重复");
        assertEquals(AlertState.PENDING, ev.currentStates().get("error-rate"),
                "重复评估不得推进状态机");
    }

    @Test
    void invalidRuleIsRejected() {
        try {
            new AlertRule("bad", AlertRule.Metric.ERROR_RATE, 0.3, 0.5, 2, 2);
        } catch (IllegalArgumentException e) {
            log.info("invalid rule rejected: {}", e.getMessage());
            return;
        }
        throw new AssertionError("恢复阈值 >= 触发阈值的规则必须被拒绝");
    }
}
