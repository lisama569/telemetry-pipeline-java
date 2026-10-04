# telemetry-pipeline-java

本地可运行的请求可观测性流水线：为每个入站请求建立追踪上下文，按可配置维度做滚动窗口指标聚合，并基于已完成窗口进行带滞回的阈值告警。整条流水线对自身故障隔离，业务请求永远不受遥测异常影响。

## 快速开始

```bash
mvn test                 # 运行全部测试（30 个）
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
- 窗口生命周期为三态：`ACTIVE → OPEN（已收尾、容错期内可合并）→ CLOSED（不可变、进入告警）`。
- 窗口切换：新窗口事件到达时，所有更旧的非空桶先快照并**恰好一次**发射给监听器，再复位复用——并发写入与窗口切换下不丢失、可重复。
- 监听器异常计入 `listenerErrors`，不影响聚合与其他监听器。
- 查询：`GET /telemetry/metrics` 返回活动窗口（`activeWindows`，还没轮到收尾）、容错期内的已收尾窗口（`openWindows`）、已关闭窗口（`completedWindows`，有界保留 1000 条）、迟到事件分类记录与基数状态。

### 定时收尾（空流量也能完成窗口）

- `WindowFinalizationScheduler` 按 `telemetry.finalization-interval-millis`（默认 1000ms）周期性驱动 `finalizeDue`：凡是`窗口结束时间 + telemetry.finalization-delay-millis ≤ 当前时间`的窗口都会被收尾，**不需要新请求进来**。
- 流量中断期间的空窗口会被合成为 `requestCount=0` 的空窗口并照常进入告警评估——该触发就触发、该恢复就恢复，不会卡在半路。空窗口合成有界（每个维度每次最多 `window-count` 个，更老的空档直接跳过）。
- 本地查询可区分两种“没数据”：`activeWindows`/`openWindows` 里是**还没轮到收尾**的窗口；`completedWindows` 里 `requestCount=0` 的是**这段时间确实没数据**的已收尾空窗口。

### 迟到与乱序事件

- 窗口收尾后进入容错期（`telemetry.late-grace-millis`）：期间到达的该窗口事件会**合并**进已收尾窗口，并在 `LateEventRegistry` 留下 `MERGED_INTO_OPEN_WINDOW` 分类记录（可查询）。合并与窗口切换在维度级串行化，并发下每个事件恰好计算一次。
- 超出容错范围（窗口已关闭、或老到环形缓冲 + 容错跨度都容纳不下）的事件**不静默丢弃**：计入 `lateEvents` 并留下 `REJECTED_BEYOND_GRACE` 分类记录。分类记录有界保留 1000 条，被淘汰的条数计入 `lateEventClassificationDrops`，计数器本身精确无界。
- 容量边界：一个维度最多同时容纳 `window-count` 个活动窗口 + 容错跨度内的已收尾窗口；乱序超出该范围的事件按上一条拒绝并留痕。
- 乱序到达但仍在容量范围内的事件会补开对应窗口（收尾前沿自动回拉），不会误丢。

### 告警结果与到达顺序无关

- 告警评估只消费 **CLOSED** 窗口；关闭时间由窗口时间戳决定（`窗口结束 + 收尾延迟 + 容错期`），与事件到达先后无关。
- 关闭后的窗口经水位线机制按**规范序（windowStart, dimension）**释放：只有当所有已知维度都关闭了更早的窗口，才按排序释放——同一批窗口数据，无论哪个分组先到、上报顺序怎么变、并发怎么调度，告警状态与迁移序列唯一、可复现（有专门测试验证顺序/打乱/并发三种到达方式结果一致）。
- 代价是告警延迟为 `窗口大小 + 收尾延迟 + 容错期`，三者均可配置。

### 基数与溢出策略

- 不同维度数上限为 `telemetry.max-dimensions`；准入判定串行化，并发下严格有界。
- 超限维度的**事件不丢弃**，确定性地折叠进 `__overflow__/__all__` 桶，并累计 `overflowedEvents` 信号。
- 已准入维度始终保留自己的桶（解析结果稳定，不在自有桶与溢出桶之间抖动）。

## 告警状态机

对**已关闭（CLOSED）窗口**按规范序求值（当前内置 `error-rate` 规则，基于错误率）：

```
OK --(连续 N 窗 ≥ trigger)--> PENDING --(再连续达标)--> FIRING
FIRING --(连续 M 窗 ≤ recover)--> RECOVERING --(再连续达标)--> OK
```

- `recover < trigger` 之间为滞回带：带内取值既不触发新告警也不恢复进行中的告警，短暂波动不会抖动（构造时拒绝 `recover >= trigger` 的规则）。
- PENDING/RECOVERING 被反向取值打断时立即回到 OK/FIRING 并清零计数。
- 每次状态迁移携带原因（指标值、阈值、窗口起点）写入 `AlertStore`，经 `GET /telemetry/alerts` 查询当前状态与迁移历史（有界 1000 条）。

## 故障隔离

- 遥测各阶段（`TRACE_EXTRACTION` / `METRIC_AGGREGATION` / `ALERT_EVALUATION` / `ASYNC_PROPAGATION` / `WINDOW_FINALIZATION`）的异常全部捕获并按阶段分类记录到 `PipelineErrorRegistry`，可经 `GET /telemetry/errors` 区分查询。
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
| `finalization-delay-millis` | 0 | 窗口结束后延迟多久收尾（定时收尾，无需新流量） |
| `late-grace-millis` | 0 | 收尾后容忍迟到事件合并的容错期 |
| `finalization-interval-millis` | 1000 | 定时收尾的执行间隔 |

## 兼容范围与本地验证

- 本次新增完全向后兼容：`finalization-delay-millis` / `late-grace-millis` 默认为 0，此时行为与旧版一致（轮换即收尾即评估）；原有 23 个测试全部保留并通过。
- 本地验证：
  - `mvn test` 跑全部 30 个测试（含新增的定时收尾、迟到合并/拒绝、告警确定性用例）。
  - `mvn spring-boot:run` 后调整 `telemetry.finalization-interval-millis` 等配置，打几个请求再静置，观察 `GET /telemetry/metrics` 中窗口从 `activeWindows` → `openWindows` → `completedWindows` 的流转，以及 `GET /telemetry/alerts` 在空流量下的自动恢复。
  - 用带旧时间戳的请求（或单测中的显式时间戳）可观察 `lateEventClassifications` 中的 MERGED/REJECTED 记录。

## 测试

`src/test/java/.../telemetry/` 下每个测试都打印输入、关键中间状态与最终判定（日志前缀 `input:` / `intermediate:` / `verdict:`），断言均带可区分的失败原因：

- `RollingWindowAggregatorConcurrencyTest` — 并发写入无丢失、窗口轮换恰好一次发射、迟到事件计数
- `AsyncContextPropagationTest` — 跨线程/嵌套异步的上下文传播、无线程泄漏、无捏造上下文
- `TraceHeaderHandlingTest` — 缺失/非法/冲突追踪头的确定性处理与防串线
- `DimensionOverflowTest` — 基数上限、溢出折叠不丢事件、并发准入有界、解析稳定
- `AlertHysteresisTest` — 触发/持续/恢复全生命周期、抗抖动、滞回带、非法规则拒绝
- `ScheduledFinalizationTest` — 空流量下定时收尾驱动告警触发与恢复、收尾延迟可配、空窗口与未收尾窗口可区分
- `LateEventHandlingTest` — 容错期内迟到事件合并（恰好一次、可查询）、超限拒绝与有界分类记录、并发合并不重不丢
- `AlertDeterminismTest` — 同一批数据在顺序/打乱/并发三种到达方式下告警状态与迁移序列完全一致
- `PipelineFaultIsolationTest` / `WebLayerFaultIsolationTest` — 监听器与告警评估故障的隔离、分类与业务无感

## 代码结构

```
telemetry/
  trace/     TraceContext, TraceContextHolder, TraceHeaderParser,
             TraceContextFilter, ContextPropagatingExecutor, TraceIssueRegistry
  metrics/   DimensionKey, WindowSnapshot, CardinalityLimiter, RollingWindowAggregator,
             LateEventRegistry
  alert/     AlertState, AlertRule, AlertEvaluator, AlertStore
  pipeline/  TelemetryPipeline, PipelineErrorRegistry, WindowFinalizationScheduler
  config/    TelemetryProperties, TelemetryConfig
  web/       DemoController (/api/work), TelemetryQueryController (/telemetry/*)
```
