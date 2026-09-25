package com.github.lisama569.telemetrypipelinejava.config;

import com.github.lisama569.telemetrypipelinejava.alerting.AlertEvaluator;
import com.github.lisama569.telemetrypipelinejava.alerting.AlertRule;
import com.github.lisama569.telemetrypipelinejava.metrics.RollingWindowAggregator;
import com.github.lisama569.telemetrypipelinejava.pipeline.PipelineErrorLog;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 装配遥测流水线各组件。
 */
@Configuration
@EnableConfigurationProperties(TelemetryProperties.class)
public class TelemetryConfig {

    @Bean
    public RollingWindowAggregator rollingWindowAggregator(TelemetryProperties props) {
        return new RollingWindowAggregator(props.getWindowSizeMillis(), props.getMaxDimensions());
    }

    @Bean
    public AlertEvaluator alertEvaluator(TelemetryProperties props) {
        AlertRule errorRateRule = new AlertRule("error-rate", AlertRule.Metric.ERROR_RATE,
                props.getErrorRateTrigger(), props.getErrorRateRecover(),
                props.getTriggerWindows(), props.getRecoverWindows());
        return new AlertEvaluator(List.of(errorRateRule));
    }

    @Bean
    public PipelineErrorLog pipelineErrorLog(TelemetryProperties props) {
        return new PipelineErrorLog(props.getErrorLogCapacity());
    }
}
