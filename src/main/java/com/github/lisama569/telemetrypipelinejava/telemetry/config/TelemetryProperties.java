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
}
