package com.github.lisama569.telemetrypipelinejava.alerting;

import com.github.lisama569.telemetrypipelinejava.metrics.WindowSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 告警评估器：消费已完成窗口快照，按规则推进状态机。
 *
 * 状态机：OK --(连续 triggerWindows 次越限)--> FIRING
 *        FIRING --(连续 recoverWindows 次回落)--> OK
 * 中间经 PENDING / RECOVERING 过渡态，滞回区间内的波动不改变状态、不抖动。
 * 同一窗口不会被重复评估（按 windowStart 去重，保证可重复）。
 */
public class AlertEvaluator {

    private final List<AlertRule> rules;
    private final Map<String, RuleRuntime> runtimeByRule = new LinkedHashMap<>();
    private long lastEvaluatedWindowStart = -1;

    public AlertEvaluator(List<AlertRule> rules) {
        this.rules = List.copyOf(rules);
        for (AlertRule r : this.rules) {
            runtimeByRule.put(r.name(), new RuleRuntime());
        }
    }

    /** 评估一个已完成窗口，返回本窗口产生的状态迁移事件（可能为空）。 */
    public synchronized List<AlertEvent> evaluate(WindowSnapshot snapshot) {
        if (snapshot.windowStartEpochMillis() <= lastEvaluatedWindowStart) {
            // 重复或乱序窗口：忽略，保证评估结果可重复
            return List.of();
        }
        lastEvaluatedWindowStart = snapshot.windowStartEpochMillis();

        List<AlertEvent> events = new ArrayList<>();
        for (AlertRule rule : rules) {
            double value = observe(rule.metric(), snapshot);
            RuleRuntime rt = runtimeByRule.get(rule.name());
            AlertState before = rt.state;
            AlertState after = advance(rule, rt, value);
            if (after != before) {
                events.add(new AlertEvent(snapshot.windowStartEpochMillis(),
                        rule.name(), before, after, value));
            }
        }
        return events;
    }

    private AlertState advance(AlertRule rule, RuleRuntime rt, double value) {
        boolean breached = value >= rule.triggerThreshold();
        boolean recovered = value < rule.recoverThreshold();
        // 滞回区间 [recover, trigger)：不计数也不清零，保持当前累积
        switch (rt.state) {
            case OK, PENDING -> {
                if (breached) {
                    rt.consecutiveBreaches++;
                    rt.consecutiveRecoveries = 0;
                    rt.state = rt.consecutiveBreaches >= rule.triggerWindows()
                            ? AlertState.FIRING : AlertState.PENDING;
                } else if (recovered) {
                    rt.consecutiveBreaches = 0;
                    rt.state = AlertState.OK;
                }
            }
            case FIRING, RECOVERING -> {
                if (recovered) {
                    rt.consecutiveRecoveries++;
                    rt.consecutiveBreaches = 0;
                    rt.state = rt.consecutiveRecoveries >= rule.recoverWindows()
                            ? AlertState.OK : AlertState.RECOVERING;
                } else if (breached) {
                    rt.consecutiveRecoveries = 0;
                    rt.state = AlertState.FIRING;
                }
            }
        }
        return rt.state;
    }

    /** 从窗口快照提取指标观测值（跨维度合计）。 */
    public static double observe(AlertRule.Metric metric, WindowSnapshot snapshot) {
        long count = 0, errors = 0, totalLatency = 0;
        for (WindowSnapshot.DimensionStats s : snapshot.statsByDimension().values()) {
            count += s.count();
            errors += s.errorCount();
            totalLatency += s.totalLatencyMillis();
        }
        return switch (metric) {
            case ERROR_RATE -> count == 0 ? 0.0 : (double) errors / count;
            case AVG_LATENCY_MS -> count == 0 ? 0.0 : (double) totalLatency / count;
            case THROUGHPUT -> snapshot.windowSizeMillis() == 0 ? 0.0
                    : count * 1000.0 / snapshot.windowSizeMillis();
        };
    }

    /** 当前各规则状态（只读快照）。 */
    public synchronized Map<String, AlertState> currentStates() {
        Map<String, AlertState> states = new LinkedHashMap<>();
        runtimeByRule.forEach((name, rt) -> states.put(name, rt.state));
        return Map.copyOf(states);
    }

    private static final class RuleRuntime {
        private AlertState state = AlertState.OK;
        private int consecutiveBreaches;
        private int consecutiveRecoveries;
    }
}
