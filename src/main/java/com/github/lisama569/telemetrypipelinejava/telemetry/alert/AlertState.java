package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

/**
 * Alert lifecycle states with hysteresis.
 */
public enum AlertState {
    /** Metric within normal bounds. */
    OK,
    /** Breach observed but not yet sustained long enough to fire. */
    PENDING,
    /** Alert actively firing. */
    FIRING,
    /** Firing alert observed back within recovery bounds, awaiting confirmation. */
    RECOVERING
}
