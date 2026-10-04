package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Evaluates an {@link AlertRule} against completed windows with a hysteresis
 * state machine <em>per dimension</em>:
 *
 * <pre>
 *   OK --(breach xN)--> PENDING --(breach xN)--> FIRING
 *   FIRING --(recovered xM)--> RECOVERING --(recovered xM)--> OK
 * </pre>
 *
 * Values between {@code recoverThreshold} and {@code triggerThreshold} fall in
 * the hysteresis band: they neither fire a new alert nor recover a firing one,
 * so short-lived fluctuations cannot cause flapping.
 *
 * <p>Each dimension has its own independent state machine, fed by the
 * aggregator in canonical window-index order. The final state and the
 * transition sequence of a dimension therefore depend only on that
 * dimension's window contents — never on the arrival order, grouping, or
 * concurrency scheduling of other dimensions' windows. Evaluation is
 * serialized per evaluator, making transitions deterministic and repeatable.
 */
public class AlertEvaluator {

    /** Callback invoked on every state transition. */
    @FunctionalInterface
    public interface TransitionListener {
        void onTransition(AlertRule rule, DimensionKey dimension, AlertState from,
                AlertState to, String reason);
    }

    private final AlertRule rule;
    private volatile TransitionListener transitionListener = (r, d, f, t, reason) -> {
    };

    private final Map<DimensionKey, Machine> machines = new ConcurrentHashMap<>();

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

    /** Feeds one completed window; returns the dimension's state after evaluation. */
    public AlertState evaluate(WindowSnapshot snapshot) {
        Machine machine;
        // Serialize evaluation so each dimension's window sequence is applied
        // in the exact order supplied by the aggregator.
        synchronized (this) {
            machine = machines.computeIfAbsent(snapshot.dimension(), d -> new Machine());
            step(machine, snapshot);
        }
        return machine.state;
    }

    private void step(Machine m, WindowSnapshot snapshot) {
        double value = rule.valueOf(snapshot);
        AlertState from = m.state;
        switch (m.state) {
            case OK, PENDING -> {
                if (value >= rule.triggerThreshold()) {
                    m.breachCount++;
                    m.state = m.breachCount >= rule.triggerConsecutiveWindows()
                            ? AlertState.FIRING : AlertState.PENDING;
                } else {
                    m.breachCount = 0;
                    m.state = AlertState.OK;
                }
            }
            case FIRING, RECOVERING -> {
                if (value <= rule.recoverThreshold()) {
                    m.recoverCount++;
                    m.state = m.recoverCount >= rule.recoverConsecutiveWindows()
                            ? AlertState.OK : AlertState.RECOVERING;
                } else {
                    m.recoverCount = 0;
                    m.state = AlertState.FIRING;
                }
            }
        }
        if (m.state == AlertState.OK) {
            m.breachCount = 0;
            m.recoverCount = 0;
        }
        if (m.state == AlertState.FIRING) {
            m.recoverCount = 0;
        }
        if (m.state != from) {
            String reason = "metric=" + rule.metric() + " value=" + value
                    + " trigger=" + rule.triggerThreshold()
                    + " recover=" + rule.recoverThreshold()
                    + " windowStart=" + snapshot.windowStartEpochMillis();
            transitionListener.onTransition(rule, snapshot.dimension(), from, m.state, reason);
        }
    }

    /** Current state per dimension (snapshot copy). */
    public synchronized Map<DimensionKey, AlertState> states() {
        Map<DimensionKey, AlertState> out = new LinkedHashMap<>();
        machines.forEach((d, m) -> out.put(d, m.state));
        return Map.copyOf(out);
    }

    public AlertRule rule() {
        return rule;
    }

    /** Mutable per-dimension hysteresis state. */
    private static final class Machine {
        AlertState state = AlertState.OK;
        int breachCount;
        int recoverCount;
    }
}
