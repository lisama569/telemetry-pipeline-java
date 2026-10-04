# telemetry-pipeline-java

本地可运行的请求可观测性流水线：为每个入站请求建立追踪上下文，按可配置维度做滚动窗口指标聚合，并基于已完成窗口进行带滞回的阈值告警。整条流水线对自身故障隔离，业务请求永远不受遥测异常影响。

## 快速开始

```bash
mvn test                 # 运行全部测试（32 个）
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
- 窗口生命周期：`OPEN`（接收本窗口事件）→ `CLOSED`（被轮换或已过窗口末尾，容错期内的迟到事件仍并入）→ `FINALIZED`（在 `窗口末尾 + window-close-delay-millis` 时**恰好一次**发射给监听器）。每个窗口按窗口序号递增收尾，已算过的事件不重算、不丢失。
- 监听器异常计入 `listenerErrors`，不影响聚合与其他监听器。
- 查询：`GET /telemetry/metrics` 返回活动窗口、待收尾窗口、已完成窗口、空窗口、迟到事件分类记录、迟到/监听器错误计数与基数状态。

### 定时收尾（空流量也能推进）

- `WindowFinalizer` 按 `finalizer-interval-millis` 周期触发 `finalizeDue(now)`：所有 `窗口末尾 + window-close-delay-millis <= now` 的窗口被收尾发射，**不需要新请求进来**。收尾延迟可配置（`window-close-delay-millis`）。
- 完全没有事件的窗口会发射**显式空窗口**（`requestCount=0`）进入告警评估——告警该触发就触发、该恢复就恢复，不会卡在半路。
- 本地查询可区分两种"没数据"：`pendingCloseWindows`（已关窗但还没到收尾时间，"还没轮到收尾"）与 `emptyWindows` / `emptyWindowsFinalized`（已收尾但确实零事件，"这段时间确实没数据"）。空窗口单独有界保留（100 条）并精确计数，不会把真实数据窗口挤出 `completedWindows`（有界 1000 条）。
- 单次收尾补空窗有上限（1000 个/轮）：长时间静默或时钟跳变后只补最近的一段空窗，带数据的窗口不受影响、全部照常发射。

### 迟到与乱序事件

- 迟到判定基于**事件时间水位线**（观测到的最大事件时间戳），与墙钟调度无关，结果可复现。
- 水位线超出窗口末尾不超过 `late-tolerance-millis` 的迟到事件：并入尚未收尾的对应窗口（即使该窗口原本没有任何数据），并在 `LateEventRegistry` 留下 `MERGED_WITHIN_TOLERANCE` 归类记录（可查询）。
- 超出容错范围或窗口已收尾的迟到事件：计入 `lateEvents` 并留下 `DROPPED_BEYOND_TOLERANCE` 归类记录——有明确信号，绝不静默消失。
- 归类记录有界保留（默认 1000 条），但合并/丢弃**计数精确**，不会无界堆积。
- 约束：`late-tolerance-millis <= window-close-delay-millis`（构造时强制），保证容错期内的事件一定在收尾前落地。

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

### 告警结果与到达顺序无关

- 告警状态机**按维度独立**：每个 `(规则, 维度)` 各有一条状态机，只消费该维度按窗口序号递增的收尾窗口序列。
- 因此对同一批窗口数据，无论哪个维度先报、上报顺序怎么变、评估在多少线程上并发调度，每个维度的最终告警状态与状态迁移序列都**唯一、可复现**（有 `AlertOrderIndependenceTest` 覆盖：顺序/轮转/洗牌/并发四种喂法结果完全一致）。
- `GET /telemetry/alerts` 的 `states` 结构为 `规则 -> 维度 -> 状态`；迁移历史的每条记录都带维度。

## 故障隔离

- 遥测各阶段（`TRACE_EXTRACTION` / `METRIC_AGGREGATION` / `ALERT_EVALUATION` / `ASYNC_PROPAGATION`）的异常全部捕获并按阶段分类记录到 `PipelineErrorRegistry`，可经 `GET /telemetry/errors` 区分查询。
- 任何遥测故障都不会改变业务响应（有端到端测试覆盖）。

## 可配置项（`application.properties`，前缀 `telemetry.`）

| 配置 | 默认 | 含义 |
|---|---|---|
| `window-size-millis` | 10000 | 滚动窗口大小 |
| `window-count` | 6 | 每维度保留的窗口数 |
| `max-dimensions` | 100 | 维度基数上限 |
| `window-close-delay-millis` | 10000 | 窗口末尾后延迟多久收尾（定时收尾的空窗等待） |
| `late-tolerance-millis` | 2000 | 迟到事件仍可并入的容错范围（≤ 收尾延迟） |
| `finalizer-interval-millis` | 1000 | 定时收尾的触发间隔 |
| `error-rate-trigger` | 0.5 | 错误率告警触发阈值 |
| `error-rate-recover` | 0.3 | 错误率恢复阈值（滞回下界） |
| `trigger-windows` | 2 | 触发所需连续越限窗口数 |
| `recover-windows` | 2 | 恢复所需连续正常窗口数 |

## 测试

`src/test/java/.../telemetry/` 下每个测试都打印输入、关键中间状态与最终判定（日志前缀 `input:` / `intermediate:` / `verdict:`），断言均带可区分的失败原因：

- `RollingWindowAggregatorConcurrencyTest` — 并发写入无丢失、窗口轮换恰好一次发射、迟到事件计数
- `ScheduledWindowFinalizationTest` — 空流量下定时收尾驱动告警触发/恢复、空窗与待收尾可区分、并发收尾不重算不丢
- `LateEventToleranceTest` — 容错期内迟到并入与归类记录、超限丢弃与有界统计、乱序重放结果一致
- `AlertOrderIndependenceTest` — 同一批数据在顺序/轮转/洗牌/并发喂入下告警状态与迁移序列唯一
- `AsyncContextPropagationTest` — 跨线程/嵌套异步的上下文传播、无线程泄漏、无捏造上下文
- `TraceHeaderHandlingTest` — 缺失/非法/冲突追踪头的确定性处理与防串线
- `DimensionOverflowTest` — 基数上限、溢出折叠不丢事件、并发准入有界、解析稳定
- `AlertHysteresisTest` — 触发/持续/恢复全生命周期、抗抖动、滞回带、非法规则拒绝
- `PipelineFaultIsolationTest` / `WebLayerFaultIsolationTest` — 监听器与告警评估故障的隔离、分类与业务无感

## 兼容范围与本地验证

- 行为兼容：`RollingWindowAggregator` 原三参构造保持原语义（收尾延迟与容错均为 0，即轮换即发射）；只有显式传入新参数或由 Spring 配置装配时才启用定时收尾与迟到容错。原有 23 个测试全部保留并通过。
- 接口兼容：`GET /telemetry/metrics` 只**新增**字段（`pendingCloseWindows` / `emptyWindows` / `emptyWindowsFinalized` / `lateEventsMergedWithinTolerance` / `lateEventsDroppedBeyondTolerance` / `lateEventRecords` 等），原有字段不变；`GET /telemetry/alerts` 的 `states` 变为 `规则 -> 维度 -> 状态` 两级结构，迁移记录新增 `dimension` 字段。
- 本地验证：
  - `mvn test` — 32 个测试全量跑通（含空流量定时收尾、迟到容错、顺序无关告警三个新测试类，每个测试打印 `input:` / `intermediate:` / `verdict:` 日志）。
  - `mvn spring-boot:run` 后请求 `curl "http://localhost:8080/api/work?latencyMillis=20"`，再查 `curl -s http://localhost:8080/telemetry/metrics`：停止请求约 `window-close-delay-millis` 后可见空窗口按 `finalizer-interval-millis` 节奏进入 `emptyWindows`，`emptyWindowsFinalized` 单调递增；告警在静默期按空窗推进恢复。

## 代码结构

```
telemetry/
  trace/     TraceContext, TraceContextHolder, TraceHeaderParser,
             TraceContextFilter, ContextPropagatingExecutor, TraceIssueRegistry
  metrics/   DimensionKey, WindowSnapshot, CardinalityLimiter, RollingWindowAggregator,
             WindowFinalizer, LateEventRegistry
  alert/     AlertState, AlertRule, AlertEvaluator, AlertStore
  pipeline/  TelemetryPipeline, PipelineErrorRegistry
  config/    TelemetryProperties, TelemetryConfig
  web/       DemoController (/api/work), TelemetryQueryController (/telemetry/*)
```
