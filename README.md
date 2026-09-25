# telemetry-pipeline-java

本地可运行的请求可观测性流水线：为每个入站请求建立追踪上下文，按可配置维度做滚动窗口指标聚合，并基于已完成窗口进行阈值 + 滞回告警。整条观测链路自身的异常被隔离记录，绝不影响业务请求返回。

## 构建与运行

```bash
mvn compile          # 编译
mvn test             # 运行全部测试
mvn spring-boot:run  # 本地启动（默认 8080 端口）
```

## 架构

```
HTTP 请求
  └─ TraceContextFilter          tracing  解析 traceparent，建立 TraceContext，回写响应头
       └─ 业务 Controller         web      /api/demo（可模拟延迟/错误/异步嵌套调用）
       └─ TelemetryPipeline       pipeline 门面：聚合 + 轮换 + 告警，异常隔离
            ├─ RollingWindowAggregator  metrics   滚动窗口聚合、维度溢出桶、迟到事件折叠
            ├─ AlertEvaluator           alerting  阈值 + 滞回状态机
            └─ PipelineErrorLog         pipeline  流水线自身异常的环形日志
```

## 追踪语义

- 遵循 W3C `traceparent` 格式：`version-traceid-spanid-flags`（如 `00-<32hex>-<16hex>-01`）。
- 解析结果四态明确区分（`TraceHeaderParser.Status`）：
  - `VALID`：采用请求头中的 traceId/spanId，上下文来源标记为 `HEADER`；
  - `MISSING`：头缺失，本地生成新上下文（`GENERATED_MISSING`）；
  - `INVALID`：格式非法（含全零 traceId、非法 version `ff` 等），本地生成并记录原因（`GENERATED_INVALID`）；
  - `CONFLICT`：多个互不相同的 traceparent 头，确定性地选择字典序最小的合法值（`RESOLVED_CONFLICT`）；全部非法时生成新上下文。同样的输入永远得到同样的结果。
- 任何情况下都会产出一个可用上下文并回写响应头 `traceparent`，绝不静默丢弃。
- 上下文通过 `TraceContextHolder`（ThreadLocal）保存；异步/嵌套调用用 `wrap(Runnable)` / `wrap(Callable)` / `wrapSupplier(Supplier)` 显式传播，任务结束后恢复工作线程原状态，杜绝线程池串线。
- 子上下文 `TraceContext.child(newSpanId)` 保持同一 traceId 并链接 parentSpanId。

## 指标与聚合语义

- 每个请求产生一条 `MetricEvent`：延迟（ms）、是否错误（HTTP 状态 ≥ 500）、维度标签（默认 `method`、`path`、`status` 类）。
- `RollingWindowAggregator` 按窗口大小（`telemetry.window-size-millis`）滚动聚合，产出 `WindowSnapshot`：每个维度的 count / errorCount / totalLatency / maxLatency，以及 errorRate、avgLatency 派生值。
- 一致性保证：
  - `record` 与窗口关闭互斥，窗口切换不丢事件；
  - 已关闭窗口只追加且 `windowStart` 严格单调递增（内部校验，违反即抛错）；
  - 轮换由独立的 `rotationLock` 串行化：并发调用者收到的窗口回调同样全局按窗口序升序，且慢回调（如告警评估）不会阻塞事件写入；
  - 相同输入序列的聚合结果完全可重复。
- **维度溢出**：单窗口维度数超过 `telemetry.max-dimensions` 时，新维度事件归入 `__overflow__` 桶；原始事件不丢弃，窗口级 `overflowEvents` 与全局 `overflowCount()` 提供可观测信号；基数有界（上限 + 1 个溢出桶）。
- **迟到事件**：时间戳落在已关闭窗口的事件，确定性地折叠进最早未关闭窗口，窗口级 `lateEvents` 与全局 `lateCount()` 计数，保证不丢失且窗口序单调。

## 告警状态机

```
OK --越限(≥trigger)--> PENDING --连续 triggerWindows 次越限--> FIRING
FIRING --回落(<recover)--> RECOVERING --连续 recoverWindows 次回落--> OK
```

- 滞回区间 `[recoverThreshold, triggerThreshold)` 内的波动不改变累积计数与状态，短暂尖峰（单窗口越限）不会触发告警，避免抖动。
- 每次状态迁移产生 `AlertEvent`（时间、规则、前后状态、观测值）并打印日志。
- 已完成窗口按 `windowStart` 去重，重复/乱序窗口被忽略，评估结果可重复。
- 内置规则 `error-rate`：对窗口内跨维度合计错误率评估（阈值见配置）。

## 异常隔离

- `TelemetryPipeline.recordRequest` 对聚合、轮换、告警三个阶段分别 try/catch，任何异常只记录不外抛，业务请求返回不受影响。
- 异常记录到 `PipelineErrorLog`（环形缓冲，容量 `telemetry.error-log-capacity`），每条含 `stage`（aggregate / rotate / alert-evaluate / filter-collect）、`reason`（异常类型）、`detail`，可通过端点本地查询。

## 可配置项（application.properties，`telemetry.*` 前缀）

| 配置 | 默认 | 说明 |
|---|---|---|
| `telemetry.window-size-millis` | 10000 | 滚动窗口大小（毫秒） |
| `telemetry.max-dimensions` | 100 | 单窗口维度基数上限 |
| `telemetry.error-log-capacity` | 256 | 流水线异常环形日志容量 |
| `telemetry.error-rate-trigger` | 0.5 | 错误率告警触发阈值 |
| `telemetry.error-rate-recover` | 0.3 | 错误率告警恢复阈值（须 < 触发阈值） |
| `telemetry.trigger-windows` | 2 | 连续越限窗口数才触发 |
| `telemetry.recover-windows` | 2 | 连续回落窗口数才恢复 |

## 本地验证

演示端点：

```bash
# 正常请求（async=true 验证异步上下文传播）
curl -D- -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" \
  "http://localhost:8080/api/demo?async=true"

# 非法追踪头 -> 本地生成上下文，source=GENERATED_INVALID
curl -H "traceparent: garbage" http://localhost:8080/api/demo

# 模拟错误与延迟
curl "http://localhost:8080/api/demo?fail=true"
curl "http://localhost:8080/api/demo?latencyMs=200"
```

观测端点：

```bash
curl http://localhost:8080/observability/windows   # 已关闭窗口快照 + 溢出计数
curl http://localhost:8080/observability/alerts    # 各告警规则当前状态
curl http://localhost:8080/observability/errors    # 流水线自身异常日志
```

## 测试

`mvn test` 覆盖以下场景，断言信息可区分失败原因，测试日志打印输入、关键中间状态与最终判定：

| 测试类 | 场景 |
|---|---|
| `TraceHeaderParserTest` | 合法/缺失/非法/冲突追踪头的明确稳定处理 |
| `TraceContextPropagationTest` | 异步传播、工作线程无残留、64 路并发不串线、子上下文链接 |
| `RollingWindowAggregatorTest` | 8 线程 × 5000 事件并发写入与轮换交错不丢失、窗口序单调、结果可重复 |
| `CardinalityOverflowTest` | 维度溢出归入 `__overflow__` 桶、基数有界、迟到事件折叠 |
| `AlertEvaluatorTest` | 触发/持续/恢复状态迁移、单窗口尖峰不触发、滞回区间不抖动、重复窗口忽略 |
| `PipelineResilienceTest` | 聚合/告警异常不外抛、异常阶段与原因可区分、环形日志淘汰 |
| `ObservabilityIntegrationTest` | 端到端：响应头回写、非法头回退、异步传播、业务失败不影响观测链路 |
