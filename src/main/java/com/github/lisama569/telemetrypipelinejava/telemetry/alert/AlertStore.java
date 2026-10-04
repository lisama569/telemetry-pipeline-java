package com.github.lisama569.telemetrypipelinejava.telemetry.alert;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


/**
 * Locally queryable store of alert states and their transition history.
 * States are tracked per rule and dimension; history retention is bounded.
 * The transition sequence for a given (rule, dimension) pair is deterministic:
 * it depends only on that dimension's finalized window sequence.
 */
public class AlertStore {

    private static final int MAX_HISTORY = 1_000;

    public record Transition(String ruleName, DimensionKey dimension, AlertState from,
            AlertState to, long timestampEpochMillis, String reason) {
    }

    private final Map<String, AlertEvaluator> evaluators = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Transition> boundedHistory = new ConcurrentLinkedDeque<>();

    /** Registers an evaluator and wires its transitions into this store. */
    public void register(AlertEvaluator evaluator) {
        evaluators.put(evaluator.rule().name(), evaluator);
        evaluator.setTransitionListener((rule, dimension, from, to, reason) ->
                recordTransition(rule.name(), dimension, from, to, reason));
    }

    public void recordTransition(String ruleName, DimensionKey dimension, AlertState from,
            AlertState to, String reason) {
        Transition t = new Transition(ruleName, dimension, from, to,
                System.currentTimeMillis(), reason);
        boundedHistory.addLast(t);
        while (boundedHistory.size() > MAX_HISTORY) {
            boundedHistory.pollFirst();
        }
    }

    /** Current states: rule name -&gt; (dimension -&gt; state). */
    public Map<String, Map<String, AlertState>> currentStates() {
        Map<String, Map<String, AlertState>> out = new TreeMap<>();
        evaluators.forEach((name, evaluator) -> {
            Map<String, AlertState> byDimension = new TreeMap<>();
            evaluator.states().forEach((d, s) -> byDimension.put(d.toString(), s));
            out.put(name, Map.copyOf(byDimension));
        });
        return Map.copyOf(out);
    }

    public List<Transition> history() {
        return List.copyOf(boundedHistory);
    }
}
