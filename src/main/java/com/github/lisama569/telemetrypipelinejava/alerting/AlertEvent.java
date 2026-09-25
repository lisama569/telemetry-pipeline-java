package com.github.lisama569.telemetrypipelinejava.alerting;

/**
 * 一次告警状态迁移记录。
 */
public record AlertEvent(long timestampMillis, String ruleName,
                         AlertState from, AlertState to, double observedValue) {
}
