# CHANGELOG

## [0.1.4] — 2026-11-02

发布与文档完善（无行为变更）。

- `package.json` 增加 `repository` / `homepage` / `bugs` —— npm 包页面出现源码仓库与 issue 链接。
- README 安装节改为三种方式：**A 从 npm 安装（推荐）**、B 本地源码安装（开发调试用）、
  C profile patch 直接 insert（指到包名或构建产物）。
- 版本号 0.1.3 → 0.1.4；测试集不变（43 例全绿），发布闸门六项 PASS。

## [0.1.3] — 2026-11-02

独立源码审查（fail-closed）发现的 **P1 死锁**修复与一批 P2 加固。**修前不要发布 0.1.2。**

### P1（阻断发布）
- **闸门从 `tools/pre-execute` 移到 `tools/execute`**：宿主 agent loop 的并行调度器（`runGroup`）是单车道——
  先 `await startCall`（内含 `await prepare`，即 pre-execute waterfall），再 `await commitReady`（才发
  `tools/post-execute` / `tools/result`）。在 pre-execute 里排队会阻塞 `prepare` → 同批已放行调用的结果永远
  提交不了 → pending 不归还 → 整批自锁到 `queueTimeoutMs`（正是 `capacity+1` 以上并行批的目标场景）。
  `tools/execute` 位于 dispatch 阶段（`startCall` 只创建 dispatch promise、不 await 它），在此排队不阻塞
  prepare/commit 循环。
- 新增真实 `@deepseek-ai/dsh-tools` `ToolRuntime` 的 runGroup 时序集成测试（`test/host-order.test.mjs`）：
  断言「容量满时第 2 个 prepare 不被阻塞、commit 后第 2 个放行」，作为该 P1 的回归门。

### P2（同批修）
- `workflowConcurrency > 1` 生效：`canGrant` 不再把 workflow 自身 pending 算进 `used`。
- `activeChildren` 增加强制回收 TTL（`childStaleTtlMs`，默认 6 小时）：丢失 `subagent/end` 不再永久占槽。
- 队列放行改为「扫描可放行者」（跳过等待中的 workflow），避免队头 workflow 饿死后续 delegate。
- 非法 runtime `subagentDepth` 按嵌套处理（至少 1），不再误当顶层去排队而绕过防死锁。
- `next()` 缺失/非法时显式抛错（不再返回 `undefined`）；`tools/post-execute` 兜底返回合法 `{ kind: 'accept' }`。
- `extractRunId` 只认 `runId`，删除 `id+local` 兜底（避免把 session id 当 runId 导致 activeChildren 少算）；
  异常 payload 只告警一次。
- 清理 effect 提前注册（重复注册状态工具抛错时定时器不再泄漏）；状态工具注册失败降级为 warn；
  状态工具声明 `isConcurrencySafe`（不再被宿主当 exclusive barrier）。
- 拒绝理由不再向模型暴露容量来源/策略名（来源只进日志）。
- `queueTimeoutMs` 默认 30 分钟 → **10 分钟**；`pendingTtlMs` 默认 30 分钟 → **5 分钟**。
- README 已知边界重排编号，补充「pending+active 双计窗口会压低有效并发」「watchTools 需随宿主工具名同步」
  「僵尸子代理按 TTL 强制回收」。
- 移除非 npm 引擎键 `engines.dsh`；补 `LICENSE`（MIT）；测试 35 → **42** 例。

## [0.1.2] — 2026-11-02

源码审核（人工通读 + 独立子代理 fail-closed 审查）后的加固，无行为回退。

- **去掉 `@deepseek-ai/dsh-subagent` 运行时依赖**：`resolveDelegationDepth` 改为本地等价实现
  （session header 权威 + runtime `subagentDepth` 只增不减，非法值按 0）。消除「跨包符号缺失 /
  宿主解析不到该包 → 整个插件装载失败」的两类风险；构建产物现在只 import `dsh-tools` 与 `schemastery`。
- `AcquireResult.ahead` 由恒 0 改为**真实入队位置**（入队时前方等待者数量），状态工具可直接反映排队位次。
- `dispose()` 清空 `pending` / `workflowActive` / `activeChildren` 计数（卫生：卸载后不留陈旧状态）。
- 测试：新增 ahead 断言；**35 例**全绿。

## [0.1.1] — 2026-11-02

同类插件对比后的改进（详见备份仓 `vet/audits/similar-plugins-caesarloo-dsh-subagent-queue-20261102.md`）。

- **嵌套委派防死锁（P1）**：新增 `nestedPolicy`（默认 `deny`）。`delegationDepthOf(agent) > 0`
  的委派**不排队**：有空位直接占用（`tryAcquire`），无空位立即 `deny` 并说明理由——避免
  「祖先占着名额等子代理、子代理排队在等名额」的互等死锁。可配 `queue` 恢复旧语义（有死锁风险）。
- 新增依赖 `@deepseek-ai/dsh-subagent`（读 `delegationDepthOf`；peer optional）。**该依赖已于 0.1.2 移除**（改为本地等价实现，见 0.1.2 条目）。
- 新增调度器 `tryAcquire`（非阻塞准入，绝不入队）。
- README 增「嵌套委派与死锁防护」「与同类插件的关系」两节；配置表补 `nestedPolicy`。
- 测试 28 → **35** 例（调度核心 14、插件层 20，含 4 条嵌套/深度用例）。

## [0.1.0] — 2026-11-02

首个版本。

- `tools/pre-execute` 委派准入排队：满则等待（FIFO），不再直接撞 `ACTIVATION_LIMIT_REACHED`。
- 计数三口径：`subagent/start|end` 真实子代理 + 已放行未结束调用 + 结束后的 grace 保留。
- 容量自动读 `subagents.maxActiveSubagents`（含配置覆盖与 fallback），每次放行前重读。
- `workflow` 独占模式（`workflowConcurrency`，默认 1）。
- 排队取消（`exec.signal`）→ `cancel` 决策；排队超时 → `deny` 或 `allow`。
- 兜底巡检：`pendingTtlMs` 强制释放、`stuckChildWarnMs` 僵尸占槽告警、最近事件环。
- `subagent_queue_status` 状态工具。
- fail-open：排队机制异常一律记日志后放行；`next()` 只调用一次。
- 单测 24 例（调度核心 12 + 插件层 12），含「拦截时 next 未被调用」断言。
