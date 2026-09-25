package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertRule;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertState;
import com.github.lisama569.telemetrypipelinejava.telemetry.alert.AlertStore;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.DimensionKey;
import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.WindowSnapshot;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alert hysteresis: sustained breaches fire, brief fluctuations do not flap,
 * and recovery requires sustained normal values.
 */
class AlertHysteresisTest {

    private static final Logger log = LoggerFactory.getLogger(AlertHysteresisTest.class);

    private static final DimensionKey DIM = new DimensionKey("GET /api/work", "2xx");

    private static AlertEvaluator newEvaluator(AlertStore store) {
        AlertRule rule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                0.5, 0.3, 2, 2);
        AlertEvaluator evaluator = new AlertEvaluator(rule);
        store.register(evaluator);
        return evaluator;
    }

    private static WindowSnapshot window(long startMillis, long count, long errors) {
        return new WindowSnapshot(DIM, startMillis, 1_000, count, errors,
                count * 10, 10);
    }

    @Test
    void sustainedBreachFiresAfterConfiguredWindows() {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        List<AlertState> states = new ArrayList<>();

        log.info("input: error rates [0.6, 0.7] with trigger=0.5 triggerWindows=2");
        states.add(evaluator.evaluate(window(0, 10, 6)));   // 0.6 breach 1/2
        states.add(evaluator.evaluate(window(1_000, 10, 7))); // 0.7 breach 2/2
        log.info("intermediate: states={} transitions={}", states, store.history());

        assertThat(states.get(0)).as("first breach only pends").isEqualTo(AlertState.PENDING);
        assertThat(states.get(1)).as("sustained breach fires").isEqualTo(AlertState.FIRING);
        assertThat(store.history()).as("transitions are recorded with reasons")
                .anyMatch(t -> t.to() == AlertState.FIRING && t.reason().contains("ERROR_RATE"));
    }

    @Test
    void briefFluctuationDoesNotFlap() {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        List<AlertState> states = new ArrayList<>();

        // Alternating breach/normal: never two breaches in a row.
        double[] errorsOutOf10 = {6, 1, 7, 2, 8, 1};
        log.info("input: alternating errors/10={} (never 2 consecutive breaches)",
                errorsOutOf10);
        for (int i = 0; i < errorsOutOf10.length; i++) {
            states.add(evaluator.evaluate(
                    window(i * 1_000L, 10, (long) errorsOutOf10[i])));
        }
        log.info("intermediate: states={}", states);
        log.info("verdict: firingTransitions={}",
                store.history().stream().filter(t -> t.to() == AlertState.FIRING).count());

        assertThat(states).as("never fires on isolated spikes")
                .doesNotContain(AlertState.FIRING);
        assertThat(store.history()).as("no FIRING transition recorded")
                .noneMatch(t -> t.to() == AlertState.FIRING);
    }

    @Test
    void hysteresisBandHoldsFiringAndRecoveryNeedsSustainedNormal() {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        List<AlertState> states = new ArrayList<>();

        // Fire: two breaches. Then values in the hysteresis band (0.3 < v < 0.5)
        // must NOT recover. Then two normal values recover.
        long[] errorsOutOf10 = {6, 6, 4, 4, 2, 1};
        log.info("input: errors/10={} trigger=0.5 recover=0.3 recoverWindows=2",
                errorsOutOf10);
        for (int i = 0; i < errorsOutOf10.length; i++) {
            states.add(evaluator.evaluate(window(i * 1_000L, 10, errorsOutOf10[i])));
        }
        log.info("intermediate: states={}", states);
        log.info("verdict: transitions={}", store.history());

        assertThat(states).containsExactly(
                AlertState.PENDING,   // 0.6 breach 1/2
                AlertState.FIRING,    // 0.6 breach 2/2
                AlertState.FIRING,    // 0.4 in hysteresis band: stays firing
                AlertState.FIRING,    // 0.4 in hysteresis band: stays firing
                AlertState.RECOVERING, // 0.2 below recover 1/2
                AlertState.OK);        // 0.1 below recover 2/2
        assertThat(store.history()).as("full lifecycle recorded")
                .extracting(AlertStore.Transition::to)
                .containsExactly(AlertState.PENDING, AlertState.FIRING,
                        AlertState.RECOVERING, AlertState.OK);
    }

    @Test
    void recoveryInterruptedByBreachReturnsToFiring() {
        AlertStore store = new AlertStore();
        AlertEvaluator evaluator = newEvaluator(store);
        List<AlertState> states = new ArrayList<>();

        long[] errorsOutOf10 = {6, 6, 2, 8};
        log.info("input: errors/10={} (recovery interrupted by a new breach)",
                errorsOutOf10);
        for (int i = 0; i < errorsOutOf10.length; i++) {
            states.add(evaluator.evaluate(window(i * 1_000L, 10, errorsOutOf10[i])));
        }
        log.info("verdict: states={}", states);
        assertThat(states).containsExactly(
                AlertState.PENDING, AlertState.FIRING,
                AlertState.RECOVERING, AlertState.FIRING);
    }

    @Test
    void invalidRuleWithoutHysteresisBandIsRejected() {
        AlertRule bad = new AlertRule("bad", AlertRule.Metric.ERROR_RATE, 0.3, 0.5, 2, 2);
        log.info("input: trigger=0.3 recover=0.5 (recover >= trigger)");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new AlertEvaluator(bad))
                .as("rule without a hysteresis band is rejected")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
