package com.github.lisama569.telemetrypipelinejava.pipeline;

import com.github.lisama569.telemetrypipelinejava.alerting.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.metrics.MetricEvent;
import com.github.lisama569.telemetrypipelinejava.metrics.RollingWindowAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.function.LongSupplier;

/**
 * 遥测流水线门面：聚合 + 窗口轮换 + 告警评估。
 * 自身任何异常都被捕获并记录到 PipelineErrorLog（含阶段与原因），
 * 绝不影响业务请求返回。
 */
@Component
public class TelemetryPipeline {

    private static final Logger log = LoggerFactory.getLogger(TelemetryPipeline.class);

    private final RollingWindowAggregator aggregator;
    private final AlertEvaluator alertEvaluator;
    private final PipelineErrorLog errorLog;
    private final LongSupplier clock;

    @Autowired
    public TelemetryPipeline(RollingWindowAggregator aggregator,
                             AlertEvaluator alertEvaluator,
                             PipelineErrorLog errorLog) {
        this(aggregator, alertEvaluator, errorLog, System::currentTimeMillis);
    }

    /** 可注入时钟，便于测试窗口轮换。 */
    public TelemetryPipeline(RollingWindowAggregator aggregator,
                             AlertEvaluator alertEvaluator,
                             PipelineErrorLog errorLog,
                             LongSupplier clock) {
        this.aggregator = aggregator;
        this.alertEvaluator = alertEvaluator;
        this.errorLog = errorLog;
        this.clock = clock;
    }

    /** 记录一条请求指标事件（永不抛异常）。 */
    public void recordRequest(MetricEvent event) {
        long now = clock.getAsLong();
        try {
            aggregator.record(event, now);
        } catch (RuntimeException e) {
            errorLog.record("aggregate", e.getClass().getSimpleName(), String.valueOf(e.getMessage()));
            log.warn("telemetry aggregate failed: {}", e.toString());
        }
        try {
            aggregator.rotate(now, snapshot -> {
                try {
                    alertEvaluator.evaluate(snapshot).forEach(ev ->
                            log.info("alert transition: rule={} {} -> {} value={}",
                                    ev.ruleName(), ev.from(), ev.to(), ev.observedValue()));
                } catch (RuntimeException e) {
                    errorLog.record("alert-evaluate", e.getClass().getSimpleName(),
                            String.valueOf(e.getMessage()));
                    log.warn("telemetry alert evaluation failed: {}", e.toString());
                }
            });
        } catch (RuntimeException e) {
            errorLog.record("rotate", e.getClass().getSimpleName(), String.valueOf(e.getMessage()));
            log.warn("telemetry window rotation failed: {}", e.toString());
        }
    }

    public RollingWindowAggregator aggregator() {
        return aggregator;
    }

    public AlertEvaluator alertEvaluator() {
        return alertEvaluator;
    }

    public PipelineErrorLog errorLog() {
        return errorLog;
    }
}
