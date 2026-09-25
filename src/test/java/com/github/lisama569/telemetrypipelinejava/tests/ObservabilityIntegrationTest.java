package com.github.lisama569.telemetrypipelinejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：入站请求建立追踪上下文、响应头回写、异步传播、非法头处理、
 * 观测端点可查询、业务错误不影响观测链路。
 */
@SpringBootTest(properties = {
        "telemetry.window-size-millis=500",
        "telemetry.max-dimensions=5"
})
@AutoConfigureMockMvc
class ObservabilityIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityIntegrationTest.class);

    private static final String VALID = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Autowired
    private MockMvc mvc;

    @Test
    void validTraceparentIsAdoptedAndEchoed() throws Exception {
        MvcResult result = mvc.perform(get("/api/demo").header("traceparent", VALID))
                .andExpect(status().isOk())
                .andReturn();
        String echoed = result.getResponse().getHeader("traceparent");
        String body = result.getResponse().getContentAsString();
        log.info("input traceparent={} echoed={} body={}", VALID, echoed, body);
        assertNotNull(echoed, "响应必须回写 traceparent 头");
        assertTrue(echoed.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
                "响应 traceId 应与入站一致");
        assertTrue(body.contains("\"source\":\"HEADER\""), "上下文来源应为 HEADER");
    }

    @Test
    void malformedTraceparentFallsBackToGenerated() throws Exception {
        MvcResult result = mvc.perform(get("/api/demo").header("traceparent", "garbage"))
                .andExpect(status().isOk())
                .andReturn();
        String echoed = result.getResponse().getHeader("traceparent");
        String body = result.getResponse().getContentAsString();
        log.info("input=garbage echoed={} body={}", echoed, body);
        assertNotNull(echoed, "非法头也必须回写有效 traceparent（不静默丢弃）");
        assertTrue(echoed.matches("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]"),
                "回写头必须是合法 traceparent 格式");
        assertTrue(body.contains("\"source\":\"GENERATED_INVALID\""), "来源应标记为 GENERATED_INVALID");
    }

    @Test
    void asyncNestedCallKeepsTraceContext() throws Exception {
        MvcResult result = mvc.perform(get("/api/demo")
                        .param("async", "true")
                        .header("traceparent", VALID))
                .andExpect(status().isOk())
                .andReturn();
        String body = result.getResponse().getContentAsString();
        log.info("async body={}", body);
        assertTrue(body.contains("\"asyncContextMatched\":true"),
                "异步嵌套调用必须保持同一 traceId");
    }

    @Test
    void businessErrorStillReturnsAndIsObservable() throws Exception {
        mvc.perform(get("/api/demo").param("fail", "true"))
                .andExpect(status().isInternalServerError());
        // 观测端点可用，且流水线自身无异常
        MvcResult errors = mvc.perform(get("/observability/errors"))
                .andExpect(status().isOk())
                .andReturn();
        log.info("pipeline errors after business failure: {}",
                errors.getResponse().getContentAsString());
        assertTrue(errors.getResponse().getContentAsString().contains("\"totalRecorded\":0"),
                "业务请求失败不得导致观测流水线自身异常");
    }

    @Test
    void observabilityEndpointsAreQueryable() throws Exception {
        mvc.perform(get("/api/demo")).andExpect(status().isOk());
        Thread.sleep(600); // 等待窗口（500ms）关闭
        mvc.perform(get("/api/demo")).andExpect(status().isOk()); // 触发轮换

        MvcResult windows = mvc.perform(get("/observability/windows"))
                .andExpect(status().isOk())
                .andReturn();
        MvcResult alerts = mvc.perform(get("/observability/alerts"))
                .andExpect(status().isOk())
                .andReturn();
        log.info("windows={}", windows.getResponse().getContentAsString());
        log.info("alerts={}", alerts.getResponse().getContentAsString());
        assertTrue(windows.getResponse().getContentAsString().contains("closedWindows"),
                "窗口快照端点应返回 closedWindows");
        assertTrue(alerts.getResponse().getContentAsString().contains("error-rate"),
                "告警端点应返回规则状态");
        assertEquals(200, windows.getResponse().getStatus(), "观测端点必须可用");
    }
}
