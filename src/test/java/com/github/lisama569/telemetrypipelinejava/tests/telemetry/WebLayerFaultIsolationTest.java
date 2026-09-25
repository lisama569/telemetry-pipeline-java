package com.github.lisama569.telemetrypipelinejava.tests.telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.lisama569.telemetrypipelinejava.telemetry.metrics.RollingWindowAggregator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * End-to-end fault isolation: a failing telemetry listener must not affect
 * business HTTP responses.
 */
@SpringBootTest
@AutoConfigureMockMvc
class WebLayerFaultIsolationTest {

    private static final Logger log =
            LoggerFactory.getLogger(WebLayerFaultIsolationTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private RollingWindowAggregator aggregator;

    @Test
    void businessRequestSucceedsWhileTelemetryListenerFails() throws Exception {
        aggregator.addWindowListener(s -> {
            throw new IllegalStateException("fault injected by test");
        });
        log.info("input: GET /api/work with a faulty telemetry listener registered");
        MvcResult result = mockMvc.perform(get("/api/work"))
                .andExpect(status().isOk())
                .andReturn();
        log.info("verdict: status={} body={}", result.getResponse().getStatus(),
                result.getResponse().getContentAsString());
        assertThat(result.getResponse().getStatus())
                .as("business response unaffected by telemetry fault").isEqualTo(200);
    }
}
