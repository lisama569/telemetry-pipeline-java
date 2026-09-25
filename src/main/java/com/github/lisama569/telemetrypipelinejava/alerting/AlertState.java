package com.github.lisama569.telemetrypipelinejava.alerting;

/**
 * 告警状态机：OK -> PENDING -> FIRING -> RECOVERING -> OK。
 * 通过滞回（触发阈值 > 恢复阈值 + 连续窗口计数）避免抖动。
 */
public enum AlertState {
    /** 正常 */
    OK,
    /** 已越限但未满足连续窗口数，暂不触发 */
    PENDING,
    /** 告警触发中 */
    FIRING,
    /** 已回落但未满足恢复窗口数 */
    RECOVERING
}
