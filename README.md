# dsh-subagent-queue

DSH（DeepSeek Harness）的**子代理并发排队闸门**插件。

DSH 的子代理上限是硬闸：超过 `maxActiveSubagents` 时新建 activation 直接抛
`subagent limit reached (active child limit: N)`（`ACTIVATION_LIMIT_REACHED`），
服务端明确**不排队**。于是出现两类实际损失：

- 一次并发开太多 → **整批失败**（没有部分成功，也不重试）；
- 上层只能靠"少开一点"的经验值降低并发，机器与机器之间上限还不同（同一台机器
  换 profile / 换宿主版本也可能变）。

本插件把"排队"搬到**工具 dispatch 阶段**（`tools/execute` 拦截缝）：委派调用在闸门处
**等待**而不是失败；名额按 `subagent/start` / `subagent/end` 生命周期事件归还；
`workflow` 这类一次扇出多个子代理的工具按**独占**处理。

> **为什么不是 `tools/pre-execute`**（0.1.3 修正）：宿主 agent loop 的并行调度器是单车道——
> 它先 `await startCall`（内含 `await prepare` = pre-execute），再 `await commitReady`
> （才发 `tools/post-execute`/`tools/result`）。在 pre-execute 排队会阻塞 `prepare`，让
> **同批**已放行调用的结果永远提交不了（而结果正是名额释放信号）→ 整批自锁到超时。
> `tools/execute` 在 dispatch 阶段（`startCall` 只创建 dispatch promise、不 await），
> 在此排队不会阻塞 prepare/commit 循环。

## 它如何工作

```
模型调用 subagent / subagent_fork / workflow
        │
        ▼
tools/execute ──► 排队闸门（capacity 名额，FIFO）
        │                    │
        │ 有名额              │ 无名额 → 等待（可被取消 / 超时）
        ▼                    ▼
    放行工具调用         tools/result（调用结束）或 subagent/end（子代理结束）
        │                    │
        ▼                    ▼
subagent/start … subagent/end  归还名额 → 放行下一个等待者
```

计数口径（三者相加视为已占用）：

| 计数 | 来源 | 作用 |
|---|---|---|
| `activeChildren` | `subagent/start` / `subagent/end` | 真实在线子代理数（真正的容量消耗者） |
| `pending` | 已放行、尚未收到 `tools/result` 的委派调用 | 防止并发的放行抢同一个名额 |
| `graceHolds` | 工具结束后保留 `graceMs` | 给迟到的 `subagent/start` 事件留时间，避免瞬时超额 |

`workflow` 例外：它会一次扇出多个子代理，无法从外部按槽计数，因此按
`workflowConcurrency`（默认 1）**独占**——运行期间不再放行任何其它委派。

### 嵌套委派与死锁防护

子代理自己也能委派（Host `maxDepth > 1` 时）。这类**嵌套委派不排队**：
`nestedPolicy: deny`（默认）下，池满时立即拒绝并给出理由——因为祖先可能正占着名额
等这个子代理结束，若子代理再排队等名额，两者互等＝死锁。有空位时嵌套委派直接占用
（`tryAcquire`，绝不入队）。若你确知自己的编排不会形成互等（例如父级用
`delivery: 'caller'` 且不等待），可设 `nestedPolicy: queue` 恢复排队语义。

### 容量从哪来

优先级：`config.capacity` > `subagents` 服务的实时 `maxActiveSubagents`
（`ctx.get('subagents').config.maxActiveSubagents`，用户可在「插件 → 子智能体」
页面修改）> `config.fallbackCapacity`（默认 8，DSH 的默认值）。
每次放行前重读，**改上限不需要重启插件**。

> 注意：容量是"每棵 activation 树"的服务端语义，本插件按**宿主全局**保守计数
> （同一时刻所有会话合计不超过名额）。单会话场景完全等价；多会话并行时偏保守。

### 失效姿态：fail-open

排队机制自身出任何问题（读配置异常、监听器异常、调度器已释放）都**只记日志并放行**，
绝不阻塞正常工具调用。`next()` 严格只调用一次（判定整段在 try 内、放行在 try 外）。

## 安装

### 方式 A：bundle 安装（推荐）

```bash
# 先构建
pnpm install && pnpm run build

# 装进活动 profile（桌面端形态 = desktop，CLI 形态无 DSH_PROFILE 时为 web）
dsh plugin --profile desktop add <插件目录绝对路径>
```

> 把 `<插件目录绝对路径>` 换成本机该工程的实际路径（本机示例可从包内 `package.json` 所在目录取得）。

`plugin add` 会把包加入 profile 的 `dsh.profile.bundles`，本包的
`cordis.patch.yml` 随之生效；bundle 层变更需要**重启宿主**才生效。

### 方式 B：profile patch 直接 insert（指到构建产物）

```yaml
- insert:
    - id: tool-subagent-queue
      name: '<插件目录>/dist/index.js'   # 或包名（bundle 安装时）
      config:
        fallbackCapacity: 8
```

patch 层的 `config` 热生效；插件**代码**改动不会热重载，需要重启宿主。

## 配置

| 键 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 总开关；false 时委派全部直通 |
| `capacity` | `0` | 0 = 自动读 `subagents.maxActiveSubagents` |
| `fallbackCapacity` | `8` | 自动读取失败时的保守名额 |
| `watchTools` | `[subagent, subagent_fork]` | 按单槽排队处理的工具名 |
| `workflowTools` | `[workflow]` | 按独占处理的工具名 |
| `workflowConcurrency` | `1` | 允许同时运行的 workflow 数 |
| `nestedPolicy` | `deny` | 嵌套委派（depth>0）无空位时：`deny` 立即拒绝不排队（防死锁）／`queue` 照常排队（有死锁风险） |
| `queueTimeoutMs` | `600000` | 排队超时（0 = 不超时），默认 10 分钟 |
| `onTimeout` | `deny` | 超时策略：`deny` 拒绝 / `allow` 放行（可能超额） |
| `graceMs` | `3000` | 工具结束后的容量保留时间 |
| `pendingTtlMs` | `300000` | pending 兜底释放（结束信号丢失时防永久占槽），默认 5 分钟 |
| `stuckChildWarnMs` | `1800000` | 活跃子代理超时告警阈值（疑似僵尸占槽） |
| `childStaleTtlMs` | `21600000` | 活跃子代理强制回收 TTL（默认 6 小时，丢失 `end` 事件时释放名额） |
| `monitorIntervalMs` | `30000` | 巡检间隔（0 = 关闭） |
| `registerStatusTool` | `true` | 注册 `subagent_queue_status` 工具 |

## 状态工具

模型可调用 `subagent_queue_status`，返回：

```
capacity=8(subagents.config) active=2 pending=0 workflow=0 queued=1
```

以及完整快照（队列成员、等待时长、活跃子代理运行时长、累计计数、最近 60 条事件）。

## 已知边界

1. **排队期间工具调用显示为运行中**：这是设计（等价于"等子代理完成"）。默认 10 分钟
   超时兜底，可按需调整；超时以工具错误返回可读原因。
2. **计数是近似而不是服务端计数**：`subagent/start` 事件与工具调用结束之间存在毫秒级
   窗口，用 `graceMs` 保守补偿；`pending` 与 `activeChildren` 对同一子代理存在**双计窗口**，
   因此有效并发可能略低于 `capacity`（不会高于）；最终兜底仍是服务端硬闸。
3. **workflow 只能粗粒度独占**：脚本内部的并发由脚本自身决定，插件无法按子代理槽拆解。
4. **嵌套委派默认直接拒绝而非等待**：`nestedPolicy: deny` 下，多级委派在池满时立即失败并说明原因——这是有意的死锁防护，不是缺陷；确知无互等时可设 `queue`。
5. **外部 provider 的子代理**同样会发 `subagent/start|end`，纳入计数；若某后端不发
   生命周期事件，只能按 `pending`（工具调用期间）计数——检测到漂移会告警；丢失 `end` 的
   子代理会按 `childStaleTtlMs`（默认 6 小时）强制回收名额。
6. **工具名按精确匹配**：宿主把 subagent 工具改成别名时，需同步修改 `watchTools` /
   `workflowTools`，否则闸门会静默不生效（启动日志会打印当前生效名单）。
7. **不修改工具参数**（拦截缝明确禁止改写 `exec.arguments`），也不做审批交互——排队是
   时间维度的等待，不是权限判定。

## 与同类插件的关系（2026-11-02 同类检索）

DSH 官方明确「满池拒绝、不排队」（`ACTIVATION_LIMIT_REACHED`），所以排队只能由第三方补。检索 8281 条公开目录 + web 补漏后，同题同层只有两家，且都**未发布 npm**：

| 插件 | 形态 | 与本插件的差异 |
|---|---|---|
| `dsh-subagent-concurrency-limit`（GitHub） | 拦截缝 `tools/execute`；**固定进程级池，默认 2**；嵌套满则抛错 | 它不读 Host 配置、无 workflow 独占、无 TTL/僵尸兜底；本插件容量随 Host `maxActiveSubagents`、自动接管原生调用 |
| `dsh-tool-subagent-batch`（GitHub） | **新增工具** `subagent_batch`（模型主动传任务队列） | 它不接管原生 `subagent` 调用，但提供批量汇总报告与异步状态——属另一形态，不是替代关系 |

相邻层 **不是同类**：`dsh-concurrency-guard`（npm 已发布）管 `llm/stream` 模型请求并发（防 429/锁号），与子代理槽位无关，二者可共存。完整对比见备份仓 `vet/audits/similar-plugins-caesarloo-dsh-subagent-queue-20261102.md`。

## 开发

```bash
pnpm install
pnpm run build     # tsc -> dist/
pnpm test          # 43 例：调度核心 17 + 插件层 24 + 宿主 ToolRuntime 时序集成 1 + cordis 加载冒烟 1
```

- `src/scheduler.ts`：纯逻辑调度核心（无 DSH 依赖，可独立单测）。
- `src/index.ts`：cordis 插件外壳（事件接线、配置解析、状态工具、fail-open）。
- 事件契约取源自 DSH 源码：`tools/execute` 为 dispatch 阶段 waterfall `(exec, next)`（排队放在
  这里，避免阻塞宿主 prepare/commit 单车道）；`tools/result` 为 emit `(exec, result)`；
  `subagent/start|end` 为 scoped 生命周期事件。
- `test/host-order.test.mjs`：真实 `@deepseek-ai/dsh-tools` `ToolRuntime` 按 `runGroup` 顺序
  （prepare → dispatch → commit）驱动，防止闸门被放回 `tools/pre-execute`。

## License

MIT
