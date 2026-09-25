# telemetry-pipeline-java

本地可运行的请求可观测性流水线：为每个入站请求建立追踪上下文，按可配置维度做滚动窗口指标聚合，并基于已完成窗口进行带滞回的阈值告警。整条流水线对自身故障隔离，业务请求永远不受遥测异常影响。

## 快速开始

```bash
mvn test                 # 运行全部测试（23 个）
mvn spring-boot:run      # 本地启动（默认 8080 端口）
```

冒烟验证：

```bash
# 正常请求（延续传入的 traceparent）
curl -D- -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" \
  "http://localhost:8080/api/work?latencyMillis=20"

# 非法追踪头：生成新 traceId 并记录可观测问题
curl -D- -H "traceparent: garbage" "http://localhost:8080/api/work?fail=true"

# 本地查询遥测状态
curl -s http://localhost:8080/telemetry/metrics
curl -s http://localhost:8080/telemetry/alerts
curl -s http://localhost:8080/telemetry/errors
```

## 追踪语义

- 入口过滤器 `TraceContextFilter` 解析 W3C `traceparent` 头（`version-traceId-spanId-flags`，严格小写十六进制、非全零、`ff` 版本拒绝）。
- 头部处理是**确定性**的，每种情况都有明确结果与可观测信号：

  | 情况 | 结果 | 记录的问题 |
  |---|---|---|
  | `traceparent` 合法 | 延续该 traceId，生成新 spanId，parentSpanId 指向入站 span | 无 |
  | 缺失 | 生成全新根上下文 | `MISSING` |
  | 格式非法 | 生成全新根上下文（绝不复用非法 id） | `MALFORMED_TRACEPARENT` |
  | `traceparent` 与 `X-Trace-Id` 冲突 | `traceparent` 胜出，忽略旧头 | `CONFLICTING_HEADERS` |

- 生效的 traceId 总是通过响应头 `X-Trace-Id` 回显；所有头部问题写入 `TraceIssueRegistry`（有界保留 1000 条，丢弃计数可观测），可经 `GET /telemetry/errors` 查询。
- 上下文通过 `TraceContextHolder`（ThreadLocal + try-with-resources `Scope`）传播，作用域关闭时恢复先前上下文，杜绝请求间串线。
- 异步传播：`ContextPropagatingExecutor` 在**提交时**捕获当前上下文，任务执行时绑定其**子 span**（同 traceId、新 spanId、parentSpanId 指向提交方 span），执行后清理工作线程，复用线程不会残留上下文。无上下文提交的任务不会凭空捏造上下文。

## 指标与滚动窗口聚合

- 维度：`DimensionKey(route, statusClass)`，route 为 `METHOD + 请求路径`，statusClass 为 `2xx/5xx` 等。
- 每个维度持有 `window-count` 个按 `window-size-millis` 对齐的时间桶环形缓冲；桶内计数只增不减（单调）。
- 窗口切换：新窗口事件到达时，所有更旧的非空桶先快照并**恰好一次**发射给监听器，再复位复用——并发写入与窗口切换下不丢失、可重复。
- 迟到事件（所属窗口已被挤出环形缓冲）计入 `lateEvents`，不静默丢弃。
- 监听器异常计入 `listenerErrors`，不影响聚合与其他监听器。
- 查询：`GET /telemetry/metrics` 返回活动窗口、已完成窗口（有界保留 1000 条）、迟到/监听器错误计数与基数状态。

### 基数与溢出策略

- 不同维度数上限为 `telemetry.max-dimensions`；准入判定串行化，并发下严格有界。
- 超限维度的**事件不丢弃**，确定性地折叠进 `__overflow__/__all__` 桶，并累计 `overflowedEvents` 信号。
- 已准入维度始终保留自己的桶（解析结果稳定，不在自有桶与溢出桶之间抖动）。

## 告警状态机

对**已完成窗口**按规则求值（当前内置 `error-rate` 规则，基于错误率）：

```
OK --(连续 N 窗 ≥ trigger)--> PENDING --(再连续达标)--> FIRING
FIRING --(连续 M 窗 ≤ recover)--> RECOVERING --(再连续达标)--> OK
```

- `recover < trigger` 之间为滞回带：带内取值既不触发新告警也不恢复进行中的告警，短暂波动不会抖动（构造时拒绝 `recover >= trigger` 的规则）。
- PENDING/RECOVERING 被反向取值打断时立即回到 OK/FIRING 并清零计数。
- 每次状态迁移携带原因（指标值、阈值、窗口起点）写入 `AlertStore`，经 `GET /telemetry/alerts` 查询当前状态与迁移历史（有界 1000 条）。

## 故障隔离

- 遥测各阶段（`TRACE_EXTRACTION` / `METRIC_AGGREGATION` / `ALERT_EVALUATION` / `ASYNC_PROPAGATION`）的异常全部捕获并按阶段分类记录到 `PipelineErrorRegistry`，可经 `GET /telemetry/errors` 区分查询。
- 任何遥测故障都不会改变业务响应（有端到端测试覆盖）。

## 可配置项（`application.properties`，前缀 `telemetry.`）

| 配置 | 默认 | 含义 |
|---|---|---|
| `window-size-millis` | 10000 | 滚动窗口大小 |
| `window-count` | 6 | 每维度保留的窗口数 |
| `max-dimensions` | 100 | 维度基数上限 |
| `error-rate-trigger` | 0.5 | 错误率告警触发阈值 |
| `error-rate-recover` | 0.3 | 错误率恢复阈值（滞回下界） |
| `trigger-windows` | 2 | 触发所需连续越限窗口数 |
| `recover-windows` | 2 | 恢复所需连续正常窗口数 |

## 测试

`src/test/java/.../telemetry/` 下每个测试都打印输入、关键中间状态与最终判定（日志前缀 `input:` / `intermediate:` / `verdict:`），断言均带可区分的失败原因：

- `RollingWindowAggregatorConcurrencyTest` — 并发写入无丢失、窗口轮换恰好一次发射、迟到事件计数
- `AsyncContextPropagationTest` — 跨线程/嵌套异步的上下文传播、无线程泄漏、无捏造上下文
- `TraceHeaderHandlingTest` — 缺失/非法/冲突追踪头的确定性处理与防串线
- `DimensionOverflowTest` — 基数上限、溢出折叠不丢事件、并发准入有界、解析稳定
- `AlertHysteresisTest` — 触发/持续/恢复全生命周期、抗抖动、滞回带、非法规则拒绝
- `PipelineFaultIsolationTest` / `WebLayerFaultIsolationTest` — 监听器与告警评估故障的隔离、分类与业务无感

## 代码结构

```
telemetry/
  trace/     TraceContext, TraceContextHolder, TraceHeaderParser,
             TraceContextFilter, ContextPropagatingExecutor, TraceIssueRegistry
  metrics/   DimensionKey, WindowSnapshot, CardinalityLimiter, RollingWindowAggregator
  alert/     AlertState, AlertRule, AlertEvaluator, AlertStore
  pipeline/  TelemetryPipeline, PipelineErrorRegistry
  config/    TelemetryProperties, TelemetryConfig
  web/       DemoController (/api/work), TelemetryQueryController (/telemetry/*)
```
