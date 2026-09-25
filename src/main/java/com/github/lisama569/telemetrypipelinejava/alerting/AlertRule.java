package com.github.lisama569.telemetrypipelinejava.alerting;

/**
 * 告警规则：对某维度指标按阈值 + 滞回评估。
 *
 * @param name                规则名
 * @param metric              指标类型
 * @param triggerThreshold    触发阈值（>= 视为越限）
 * @param recoverThreshold    恢复阈值（< 视为回落）
 * @param triggerWindows      连续越限多少窗口才触发
 * @param recoverWindows      连续回落多少窗口才恢复
 */
public record AlertRule(String name, Metric metric, double triggerThreshold,
                        double recoverThreshold, int triggerWindows, int recoverWindows) {

    public enum Metric { ERROR_RATE, AVG_LATENCY_MS, THROUGHPUT }

    public AlertRule {
        if (recoverThreshold >= triggerThreshold) {
            throw new IllegalArgumentException("recoverThreshold must be < triggerThreshold");
        }
        if (triggerWindows < 1 || recoverWindows < 1) {
            throw new IllegalArgumentException("window counts must be >= 1");
        }
    }
}
