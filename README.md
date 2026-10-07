# telemetry-pipeline-java

本地可运行的请求可观测性流水线：为每个入站请求建立追踪上下文，按可配置维度做滚动窗口指标聚合，并基于已完成窗口进行带滞回的阈值告警。整条流水线对自身故障隔离，业务请求永远不受遥测异常影响。

## 快速开始

```bash
mvn test                 # 运行全部测试（34 个）
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
- 查询：`GET /telemetry/metrics` 把窗口按生命周期分成互不重叠的分组（见下），另有迟到事件分类记录与基数状态。

### 空流量下的定时收尾、三种窗口状态与空窗口容量边界

- `WindowFinalizationScheduler` 按 `telemetry.finalization-interval-millis`（默认 1000ms）周期性驱动 `finalizeDue`：凡是 `窗口结束时间 + telemetry.finalization-delay-millis ≤ 当前时间` 的窗口都会被收尾，**不需要新请求进来**。
- 本地查询一眼可分三种状态，互不混在一个列表里：

  | 查询分组 | 含义 |
  |---|---|
  | `activeWindows` | **进行中**：窗口时间还没走完（当前时间所在窗口） |
  | `pendingWindows` | **已到点、还没轮到收尾**：窗口时间已走完，但还没到 `结束+收尾延迟`（含同期空档的空占位，`requestCount=0`） |
  | `emptyWindows` | **那段时间确实没数据且已收尾**：独立保留的已关闭空窗口（`requestCount=0`） |

  另有 `openWindows`（已收尾、容错期内还可合并迟到事件）与 `completedWindows`（只放**有数据**的已关闭真实窗口）。“确实没数据”（`emptyWindows`）与“还没轮到收尾”（`pendingWindows`）因此可以明确分开看出来。
- **空窗口不能把真实窗口顶出去**：关闭释放时按 `requestCount` 分流，真实窗口进 `completedWindows`（有界 1000 条），空窗口进独立的 `EmptyWindowRegistry`（`emptyWindows`，有界 1000 条，与真实窗口互不挤占）。所以请求停下来、静置多久之后再查，之前真实记录到的窗口（计数、错误水平等）都还查得到。
- **空窗口独立计数、容量边界可见、不无声丢弃**：
  - `emptyWindowsTotal`：自启动以来收尾出的空窗口总数，**精确不丢**（静置再久也准确）；
  - `emptyWindowsEvicted`：独立保留区装满后被淘汰的空窗口数（老的先淘汰，边界可见）；
  - `emptyWindowsSkippedGaps`：墙上时间发生大跨度跳跃（如机器长时间挂起）时，老于“每次最多合成 `window-count` 个”范围而被整段跳过的空窗口数——这些窗口不逐个合成，但计数精确，`retained + evicted + skipped = total` 永远对得上；
  - `emptyWindowsCapacity` / `completedWindowsCapacity`：两个保留区各自的容量；`completedWindowsEvicted`：真实窗口的淘汰计数（正常情况下为 0）。
- 空窗口照常进入告警评估——该触发就触发、该恢复就恢复，不会卡在半路。

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

- 本次改动完全向后兼容：`finalization-delay-millis` / `late-grace-millis` 默认为 0，此时行为与旧版一致（轮换即收尾即评估）；`completedSnapshots()` 等既有方法签名保留（语义收窄为“只含真实窗口”），原有用例全部保留并通过。
- 本地验证：
  - `mvn test` 跑全部 34 个测试（含新增的静置保留/空窗口计数/三态区分/保留确定性用例）。
  - `mvn spring-boot:run` 后打几个请求再**长时间静置**（默认 1s 一个收尾 tick），反复 `curl -s http://localhost:8080/telemetry/metrics | python3 -m json.tool`，观察：
    - `completedWindows` 里的真实窗口随时间一直查得到，不会被空窗口顶掉；
    - `emptyWindows` 独立列出已收尾空窗口，`emptyWindowsTotal` 精确增长、`emptyWindowsEvicted` 在超过 1000 后可见增长，`completedWindowsEvicted` 始终为 0；
    - 静置过程中窗口在 `activeWindows`（进行中）→ `pendingWindows`（已到点未收尾，仅在配了 `finalization-delay-millis` 时能看到）→ `emptyWindows`/`completedWindows`（已收尾）之间流转，三组互不重叠；
    - `GET /telemetry/alerts` 在空流量下仍能自动触发/恢复。
  - 想快速看到容量边界，可临时把空窗口保留容量调小（构造 `new EmptyWindowRegistry(10)`，见 `IdleRetentionTest`）或模拟墙上时间大跳；用单测中的显式时间戳也能直接驱动 `finalizeDue` 观察 `emptyWindowsSkippedGaps`。
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
- `IdleRetentionTest` — 长时间静置下真实窗口不被空窗口挤掉、空窗口独立保留与精确计数（含淘汰边界与大跳空档计数）、进行中/待收尾/已收尾空窗口三态可区分
- `RetentionDeterminismTest` — 同一批含空档与稀疏维度的数据在顺序/打乱/并发三种到达方式下，真实窗口保留、空窗口列表与计数、告警结果完全一致
- `PipelineFaultIsolationTest` / `WebLayerFaultIsolationTest` — 监听器与告警评估故障的隔离、分类与业务无感

## 代码结构

```
telemetry/
  trace/     TraceContext, TraceContextHolder, TraceHeaderParser,
             TraceContextFilter, ContextPropagatingExecutor, TraceIssueRegistry
  metrics/   DimensionKey, WindowSnapshot, CardinalityLimiter, RollingWindowAggregator,
             EmptyWindowRegistry, LateEventRegistry
  alert/     AlertState, AlertRule, AlertEvaluator, AlertStore
  pipeline/  TelemetryPipeline, PipelineErrorRegistry, WindowFinalizationScheduler
  config/    TelemetryProperties, TelemetryConfig
  web/       DemoController (/api/work), TelemetryQueryController (/telemetry/*)
```
