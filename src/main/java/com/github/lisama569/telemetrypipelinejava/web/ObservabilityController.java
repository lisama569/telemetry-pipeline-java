package com.github.lisama569.telemetrypipelinejava.web;

import com.github.lisama569.telemetrypipelinejava.pipeline.TelemetryPipeline;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 本地可观测性查询端点：窗口快照、告警状态、流水线异常。
 */
@RestController
public class ObservabilityController {

    private final TelemetryPipeline pipeline;

    public ObservabilityController(TelemetryPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @GetMapping("/observability/windows")
    public Map<String, Object> windows() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("closedWindows", pipeline.aggregator().closedWindows());
        body.put("overflowCount", pipeline.aggregator().overflowCount());
        return body;
    }

    @GetMapping("/observability/alerts")
    public Map<String, Object> alerts() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("states", pipeline.alertEvaluator().currentStates());
        return body;
    }

    @GetMapping("/observability/errors")
    public Map<String, Object> errors() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("totalRecorded", pipeline.errorLog().totalRecorded());
        body.put("entries", pipeline.errorLog().entries());
        return body;
    }
}
