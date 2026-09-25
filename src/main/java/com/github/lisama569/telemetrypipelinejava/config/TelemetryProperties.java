package com.github.lisama569.telemetrypipelinejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 可配置项：窗口大小、维度基数上限、告警规则参数等。
 */
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    /** 滚动窗口大小（毫秒） */
    private long windowSizeMillis = 10_000;

    /** 维度基数上限，超过后归入溢出桶 */
    private int maxDimensions = 100;

    /** 错误日志环形缓冲容量 */
    private int errorLogCapacity = 256;

    /** 错误率告警触发阈值（0~1） */
    private double errorRateTrigger = 0.5;

    /** 错误率告警恢复阈值（0~1），需小于触发阈值 */
    private double errorRateRecover = 0.3;

    /** 连续越限窗口数才触发 */
    private int triggerWindows = 2;

    /** 连续回落窗口数才恢复 */
    private int recoverWindows = 2;

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public void setWindowSizeMillis(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    public int getMaxDimensions() {
        return maxDimensions;
    }

    public void setMaxDimensions(int maxDimensions) {
        this.maxDimensions = maxDimensions;
    }

    public int getErrorLogCapacity() {
        return errorLogCapacity;
    }

    public void setErrorLogCapacity(int errorLogCapacity) {
        this.errorLogCapacity = errorLogCapacity;
    }

    public double getErrorRateTrigger() {
        return errorRateTrigger;
    }

    public void setErrorRateTrigger(double errorRateTrigger) {
        this.errorRateTrigger = errorRateTrigger;
    }

    public double getErrorRateRecover() {
        return errorRateRecover;
    }

    public void setErrorRateRecover(double errorRateRecover) {
        this.errorRateRecover = errorRateRecover;
    }

    public int getTriggerWindows() {
        return triggerWindows;
    }

    public void setTriggerWindows(int triggerWindows) {
        this.triggerWindows = triggerWindows;
    }

    public int getRecoverWindows() {
        return recoverWindows;
    }

    public void setRecoverWindows(int recoverWindows) {
        this.recoverWindows = recoverWindows;
    }
}
