package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


/**
 * Locally queryable store of alert states and their transition history.
 * History retention is bounded; states are always current.
 */
public class AlertStore {

    private static final int MAX_HISTORY = 1_000;

    public record Transition(String ruleName, AlertState from, AlertState to,
            long timestampEpochMillis, String reason) {
    }

    private final Map<String, AlertEvaluator> evaluators = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Transition> boundedHistory = new ConcurrentLinkedDeque<>();

    /** Registers an evaluator and wires its transitions into this store. */
    public void register(AlertEvaluator evaluator) {
        evaluators.put(evaluator.rule().name(), evaluator);
        evaluator.setTransitionListener((rule, from, to, reason) ->
                recordTransition(rule.name(), from, to, reason));
    }

    public void recordTransition(String ruleName, AlertState from, AlertState to, String reason) {
        Transition t = new Transition(ruleName, from, to, System.currentTimeMillis(), reason);
        boundedHistory.addLast(t);
        while (boundedHistory.size() > MAX_HISTORY) {
            boundedHistory.pollFirst();
        }
    }

    public Map<String, AlertState> currentStates() {
        Map<String, AlertState> out = new ConcurrentHashMap<>();
        evaluators.forEach((name, evaluator) -> out.put(name, evaluator.state()));
        return Map.copyOf(out);
    }

    public List<Transition> history() {
        return List.copyOf(boundedHistory);
    }
}
