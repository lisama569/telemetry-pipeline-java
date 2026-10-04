package com.github.lisama569.telemetrypipelinejava.telemetry.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized configuration for the telemetry pipeline.
 */
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    /** Rolling window size in milliseconds. */
    private long windowSizeMillis = 10_000;

    /** Number of retained rolling windows. */
    private int windowCount = 6;

    /** Maximum distinct dimension keys before overflow folding. */
    private int maxDimensions = 100;

    /** Error-rate alert trigger threshold. */
    private double errorRateTrigger = 0.5;

    /** Error-rate alert recovery threshold (hysteresis lower bound). */
    private double errorRateRecover = 0.3;

    /** Consecutive breaching windows required to fire. */
    private int triggerWindows = 2;

    /** Consecutive recovered windows required to resolve. */
    private int recoverWindows = 2;

    /** Delay after window end before a window is finalized (idle or not). */
    private long finalizationDelayMillis = 0;

    /** Grace after finalization during which late events still merge. */
    private long lateGraceMillis = 0;

    /** Interval between scheduled finalization ticks. */
    private long finalizationIntervalMillis = 1_000;

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public void setWindowSizeMillis(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    public int getWindowCount() {
        return windowCount;
    }

    public void setWindowCount(int windowCount) {
        this.windowCount = windowCount;
    }

    public int getMaxDimensions() {
        return maxDimensions;
    }

    public void setMaxDimensions(int maxDimensions) {
        this.maxDimensions = maxDimensions;
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

    public long getFinalizationDelayMillis() {
        return finalizationDelayMillis;
    }

    public void setFinalizationDelayMillis(long finalizationDelayMillis) {
        this.finalizationDelayMillis = finalizationDelayMillis;
    }

    public long getLateGraceMillis() {
        return lateGraceMillis;
    }

    public void setLateGraceMillis(long lateGraceMillis) {
        this.lateGraceMillis = lateGraceMillis;
    }

    public long getFinalizationIntervalMillis() {
        return finalizationIntervalMillis;
    }

    public void setFinalizationIntervalMillis(long finalizationIntervalMillis) {
        this.finalizationIntervalMillis = finalizationIntervalMillis;
    }
}
