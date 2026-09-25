package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;

/**
 * Evaluates an {@link AlertRule} against completed windows with a hysteresis
 * state machine:
 *
 * <pre>
 *   OK --(breach xN)--> PENDING --(breach xN)--> FIRING
 *   FIRING --(recovered xM)--> RECOVERING --(recovered xM)--> OK
 * </pre>
 *
 * Values between {@code recoverThreshold} and {@code triggerThreshold} fall in
 * the hysteresis band: they neither fire a new alert nor recover a firing one,
 * so short-lived fluctuations cannot cause flapping. Evaluation is serialized
 * per evaluator, making transitions deterministic and repeatable.
 */
public class AlertEvaluator {

    /** Callback invoked on every state transition. */
    @FunctionalInterface
    public interface TransitionListener {
        void onTransition(AlertRule rule, AlertState from, AlertState to, String reason);
    }

    private final AlertRule rule;
    private volatile TransitionListener transitionListener = (r, f, t, reason) -> {
    };

    private AlertState state = AlertState.OK;
    private int breachCount;
    private int recoverCount;

    public AlertEvaluator(AlertRule rule) {
        if (rule.recoverThreshold() >= rule.triggerThreshold()) {
            throw new IllegalArgumentException(
                    "recoverThreshold must be < triggerThreshold for hysteresis");
        }
        this.rule = rule;
    }

    public void setTransitionListener(TransitionListener listener) {
        this.transitionListener = listener;
    }

    /** Feeds one completed window; returns the state after evaluation. */
    public synchronized AlertState evaluate(WindowSnapshot snapshot) {
        double value = rule.valueOf(snapshot);
        AlertState from = state;
        switch (state) {
            case OK, PENDING -> {
                if (value >= rule.triggerThreshold()) {
                    breachCount++;
                    state = breachCount >= rule.triggerConsecutiveWindows()
                            ? AlertState.FIRING : AlertState.PENDING;
                } else {
                    breachCount = 0;
                    state = AlertState.OK;
                }
            }
            case FIRING, RECOVERING -> {
                if (value <= rule.recoverThreshold()) {
                    recoverCount++;
                    state = recoverCount >= rule.recoverConsecutiveWindows()
                            ? AlertState.OK : AlertState.RECOVERING;
                } else {
                    recoverCount = 0;
                    state = AlertState.FIRING;
                }
            }
        }
        if (state == AlertState.OK) {
            breachCount = 0;
            recoverCount = 0;
        }
        if (state == AlertState.FIRING) {
            recoverCount = 0;
        }
        if (state != from) {
            String reason = "metric=" + rule.metric() + " value=" + value
                    + " trigger=" + rule.triggerThreshold()
                    + " recover=" + rule.recoverThreshold()
                    + " windowStart=" + snapshot.windowStartEpochMillis();
            transitionListener.onTransition(rule, from, state, reason);
        }
        return state;
    }

    public synchronized AlertState state() {
        return state;
    }

    public AlertRule rule() {
        return rule;
    }
}
