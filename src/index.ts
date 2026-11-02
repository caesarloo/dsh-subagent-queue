/**
 * dsh-subagent-queue — 子代理队列插件（host 层，`tools/execute` 拦截缝）。
 *
 * 为什么是插件而不是技能：DSH 的子代理上限是**硬闸**——`ACTIVATION_LIMIT_REACHED`
 * （`subagent limit reached (active child limit: N)`）直接抛错，服务端明确不排队；
 * 技能只能劝模型"别一次开太多"，拦不住调用，也接不住已经发起的并发。
 *
 * 为什么挂在 `tools/execute` 而不是 `tools/pre-execute`（0.1.3 修正，独立审查发现）：
 * 宿主 agent loop 的并行工具调度器（`dsh-agent-loop` 的 `runGroup`）是**单车道**的——
 * 它先 `await startCall(...)`（内含 `await prepare(exec)`，即 pre-execute waterfall），
 * 再 `await commitReady()`（`finalize`/`finish` 才会发 `tools/post-execute` 与
 * `tools/result`）。因此在 **pre-execute** 里排队会阻塞 `startCall`，进而让**同批**
 * 已放行调用的结果永远提交不了；而结果正是 pending 的释放信号 → 整批自锁到
 * `queueTimeoutMs`。`tools/execute` 位于 dispatch 阶段（`startCall` 只创建 dispatch
 * promise、不 await 它），在此排队不会阻塞 `prepare`/`commit` 循环。
 *
 * 事件契约（源码取证，dsh-tools / dsh-subagent / dsh-agent-loop）：
 *   - `tools/execute`: waterfall `(exec, next)`，dispatch 阶段；`next()` 返回工具结果。
 *   - `tools/result`: emit `(exec, result)`，最终结果确定后触发 → 归还槽位。
 *   - `subagent/start` / `subagent/end`: 每次 activation 的首尾边沿，payload 含 `runId`。
 *
 * 失效姿态（fail-open）：排队机制自身的异常绝不阻塞正常工具调用——判定异常只记日志
 * 并继续 `next()`；只有**明确的排队拒绝**（超时/嵌套配额不足）才抛出错误。
 */

import type { Context } from '@deepseek-ai/cordis'
import { defineTool } from '@deepseek-ai/dsh-tools'
import z from '@deepseek-ai/schemastery'
import { DelegationScheduler, type AcquireResult, type DelegateToolKind } from './scheduler.js'

export const name = 'tool-subagent-queue'

// `tools` 服务由 @deepseek-ai/dsh-base 提供（注册状态工具需要）；pre-execute
// 监听本身不需要 inject，但声明后可保证 apply 时 tools.register 一定可用。
export const inject = ['tools']

export const Config = z.object({
  enabled: z.boolean().default(true).description('总开关；false 时委派调用全部直通。'),
  capacity: z.natural().default(0).description('并发名额；0 = 自动读 subagents 服务的 maxActiveSubagents。'),
  fallbackCapacity: z.natural().default(8).description('自动读取失败时使用的保守名额（DSH 默认 8）。'),
  watchTools: z.array(z.string()).default(['subagent', 'subagent_fork']).description('按单槽处理的委派工具名。'),
  workflowTools: z.array(z.string()).default(['workflow']).description('按独占处理的扇出工具名。'),
  workflowConcurrency: z.natural().default(1).description('允许同时运行的 workflow 数。'),
  nestedPolicy: z
    .union([z.const('deny'), z.const('queue')])
    .default('deny')
    .description(
      '嵌套委派（depth>0）无空位时的策略：deny=立即拒绝、不排队（避免与正在等它的祖先死锁）；queue=照常排队（有死锁风险）。',
    ),
  queueTimeoutMs: z.natural().default(600000).description('排队超时毫秒；0 = 不超时。'),
  onTimeout: z.union([z.const('deny'), z.const('allow')]).default('deny').description('排队超时策略。'),
  graceMs: z.natural().default(3000).description('工具结束后的容量保留时间，等待迟到的 start 事件。'),
  pendingTtlMs: z.natural().default(300000).description('pending 兜底释放时间（防止结束信号丢失后永久占槽）。'),
  stuckChildWarnMs: z.natural().default(1800000).description('活跃子代理超时告警阈值。'),
  childStaleTtlMs: z
    .natural()
    .default(21600000)
    .description('活跃子代理强制回收 TTL（默认 6 小时）：丢失 end 事件时避免名额永久被占。'),
  monitorIntervalMs: z.natural().default(30000).description('巡检间隔；0 = 关闭。'),
  registerStatusTool: z.boolean().default(true).description('注册 subagent_queue_status 工具。'),
})

export interface ResolvedConfig {
  enabled: boolean
  capacity: number
  fallbackCapacity: number
  watchTools: string[]
  workflowTools: string[]
  workflowConcurrency: number
  nestedPolicy: 'deny' | 'queue'
  queueTimeoutMs: number
  onTimeout: 'deny' | 'allow'
  graceMs: number
  pendingTtlMs: number
  stuckChildWarnMs: number
  childStaleTtlMs: number
  monitorIntervalMs: number
  registerStatusTool: boolean
}

const DEFAULT_WATCH = ['subagent', 'subagent_fork']
const DEFAULT_WORKFLOW = ['workflow']

/** 宽松 exec 视图：只读我们需要的字段，避免把核心类型耦合进运行期。 */
interface ExecutionLike {
  name?: unknown
  callId?: unknown
  signal?: AbortSignal
  agent?: unknown
  arguments?: unknown
}

type AnyListener = (...args: unknown[]) => unknown

interface PluginContextLike {
  on?: (event: string, listener: AnyListener) => unknown
  get?: (name: string) => unknown
  logger?: { info?: (message: string) => void; warn?: (message: string) => void; debug?: (message: string) => void }
  tools?: { register?: (definition: unknown) => unknown }
  effect?: (callback: () => unknown) => unknown
}

/**
 * 明确的"排队拒绝"：与"判定异常（fail-open 放行）"区分开。
 * 该错误在 `tools/execute` 阶段抛出 → 本次工具调用失败，错误文本回给模型。
 */
export class QueueRejectedError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'SubagentQueueRejected'
  }
}

/** 把 patch/config 的原始值规范化成完整配置（loader 已按 Config 校验，这里再做一层容错）。 */
export function normalizeConfig(raw: unknown): ResolvedConfig {
  const input = (raw !== null && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  const num = (value: unknown, fallback: number, min = 0): number => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return fallback
    return Math.max(min, Math.floor(value))
  }
  const list = (value: unknown, fallback: string[]): string[] => {
    if (!Array.isArray(value)) return fallback
    const items = value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    return items.length > 0 ? items : fallback
  }
  return {
    enabled: input.enabled === undefined ? true : input.enabled !== false,
    capacity: num(input.capacity, 0),
    fallbackCapacity: num(input.fallbackCapacity, 8, 1),
    watchTools: list(input.watchTools, DEFAULT_WATCH),
    workflowTools: list(input.workflowTools, DEFAULT_WORKFLOW),
    workflowConcurrency: num(input.workflowConcurrency, 1, 1),
    nestedPolicy: input.nestedPolicy === 'queue' ? 'queue' : 'deny',
    queueTimeoutMs: num(input.queueTimeoutMs, 600_000),
    onTimeout: input.onTimeout === 'allow' ? 'allow' : 'deny',
    graceMs: num(input.graceMs, 3_000),
    pendingTtlMs: num(input.pendingTtlMs, 300_000, 1_000),
    stuckChildWarnMs: num(input.stuckChildWarnMs, 1_800_000, 1_000),
    childStaleTtlMs: num(input.childStaleTtlMs, 21_600_000, 1_000),
    monitorIntervalMs: num(input.monitorIntervalMs, 30_000),
    registerStatusTool: input.registerStatusTool === undefined ? true : input.registerStatusTool !== false,
  }
}

/** 从 subagents 服务读真实容量（用户可在「插件 → 子智能体」里改，运行时以它为准）。 */
export function readServiceCapacity(
  ctx: PluginContextLike,
  config: ResolvedConfig,
): { value: number; source: string } {
  if (config.capacity > 0) return { value: config.capacity, source: 'config' }
  try {
    const service = ctx.get?.('subagents') as
      | { config?: { maxActiveSubagents?: unknown } }
      | undefined
    const raw = service?.config?.maxActiveSubagents
    const live =
      typeof raw === 'object' && raw !== null && typeof (raw as { get?: unknown }).get === 'function'
        ? ((raw as { get: () => unknown }).get() as unknown)
        : raw
    if (typeof live === 'number' && Number.isFinite(live) && live >= 1) {
      return { value: Math.floor(live), source: 'subagents.config' }
    }
  } catch {
    // 读服务配置只是尽力而为：失败就用 fallback，不影响排队
  }
  return { value: config.fallbackCapacity, source: 'fallback' }
}

/**
 * 从生命周期事件参数里取出 runId（scoped dispatch 可能在参数前部带 carrier）。
 * 只认 `runId`：payload 缺少它时返回 undefined（调用方忽略并告警一次），
 * **不要**退化成 `info.id`——那是子代理 session id，会让同一父会话下的多个 run
 * 共用同一个 key，导致 activeChildren 少算、超额放行。
 */
export function extractRunId(args: readonly unknown[]): string | undefined {
  for (const arg of args) {
    if (arg === null || typeof arg !== 'object') continue
    const record = arg as Record<string, unknown>
    if (typeof record.runId === 'string' && record.runId.length > 0) return record.runId
  }
  return undefined
}

/** 从事件参数里取出 callId（工具流水线事件首个参数即 exec）。 */
export function extractCallId(args: readonly unknown[]): string | undefined {
  const first = args[0]
  if (first === null || typeof first !== 'object') return undefined
  const callId = (first as Record<string, unknown>).callId
  return typeof callId === 'string' && callId.length > 0 ? callId : undefined
}

/**
 * 读取调用方 agent 的委派深度（0 = 顶层）。
 *
 * 语义与 `@deepseek-ai/dsh-subagent` 的 `delegationDepthOf` 一致：持久化 session header
 * 权威、runtime `subagentDepth` 只增不减，两者取最大值。
 *
 * **为什么本地实现而不 import 官方函数**：① 顶层 import 一个可能缺失/变更的跨包符号，
 * 一旦解析或导出失败会让**整个插件装载失败**，与 fail-open 定位相悖；② 该语义只有几行。
 *
 * 非法值（存在但不是非负安全整数）**按嵌套处理**（返回至少 1）——这是保守方向：
 * 宁可让它在 nestedPolicy 下被拒绝/单独排队，也不能把嵌套误判成顶层去排队，
 * 那会绕过防死锁设计。字段缺失才按 0。
 */
export function resolveDelegationDepth(agent: unknown): number {
  if (agent === null || typeof agent !== 'object') return 0
  const record = agent as {
    options?: { subagentDepth?: unknown }
    session?: { header?: { delegationDepth?: unknown } }
  }
  const headerRaw = record.session?.header?.delegationDepth
  const runtimeRaw = record.options?.subagentDepth
  const header = typeof headerRaw === 'number' && Number.isSafeInteger(headerRaw) && headerRaw >= 0 ? headerRaw : 0
  const runtimeValid =
    typeof runtimeRaw === 'number' && Number.isSafeInteger(runtimeRaw) && runtimeRaw >= 0
  const runtimeIllegal = runtimeRaw !== undefined && runtimeRaw !== null && !runtimeValid
  return Math.max(header, runtimeValid ? (runtimeRaw as number) : 0, runtimeIllegal ? 1 : 0)
}

export function apply(ctx: Context, rawConfig: unknown = {}): void {
  const config = normalizeConfig(rawConfig)
  const pluginCtx = ctx as unknown as PluginContextLike
  const logger = pluginCtx.logger

  const scheduler = new DelegationScheduler({
    capacity: 0,
    workflowConcurrency: config.workflowConcurrency,
    queueTimeoutMs: config.queueTimeoutMs,
    onTimeout: config.onTimeout,
    graceMs: config.graceMs,
    pendingTtlMs: config.pendingTtlMs,
    stuckChildWarnMs: config.stuckChildWarnMs,
    childStaleTtlMs: config.childStaleTtlMs,
    monitorIntervalMs: config.monitorIntervalMs,
    log: (level, message) => {
      if (level === 'warn') logger?.warn?.(message)
      else logger?.info?.(message)
    },
  })

  // 清理要尽早注册：后面任何一步（如重复注册状态工具）抛错时，fiber 仍会释放定时器。
  if (typeof pluginCtx.effect === 'function') {
    try {
      pluginCtx.effect(() => () => scheduler.dispose())
    } catch {
      // 老版本 cordis 没有 effect：定时器已 unref，进程退出不受影响
    }
  }

  /** 每次放行前重读容量，用户改了 subagents 设置后无需重启插件。 */
  const refreshCapacity = (): { value: number; source: string } => {
    const resolved = readServiceCapacity(pluginCtx, config)
    scheduler.setCapacity(resolved.value, resolved.source)
    return resolved
  }
  refreshCapacity()

  const classify = (toolName: string): DelegateToolKind | undefined => {
    if (config.workflowTools.includes(toolName)) return 'workflow'
    if (config.watchTools.includes(toolName)) return 'delegate'
    return undefined
  }

  const on = pluginCtx.on
  if (typeof on !== 'function') {
    logger?.warn?.('[subagent-queue] ctx.on 不可用，插件未生效')
    return
  }

  // ---------------- 1. 委派调用入口排队（dispatch 阶段，不阻塞宿主 prepare/commit 循环） ----------------
  on('tools/execute', async (...handlerArgs: unknown[]): Promise<unknown> => {
    const exec = handlerArgs[0] as ExecutionLike | undefined
    const next = handlerArgs[1] as (() => Promise<unknown>) | undefined
    if (typeof next !== 'function') {
      // 契约变化：不能返回 undefined（host 会当决策读），显式抛错保持可诊断。
      throw new Error('subagent-queue: tools/execute listener 缺少 next()（宿主契约可能已变化）')
    }
    const toolName = typeof exec?.name === 'string' ? exec.name : ''
    const kind = config.enabled ? classify(toolName) : undefined
    if (kind === undefined) return await next()

    const depth = resolveDelegationDepth(exec?.agent)
    const nested = depth > 0
    const callId = typeof exec?.callId === 'string' ? exec.callId : `${toolName}:${Date.now()}`

    // 判定整段包在 try 内；`next()` 只在 try 之外调用一次——异常不会被误当成"放行/拒绝"。
    let queuedMs = -1
    let timedOut = false
    try {
      const resolved = refreshCapacity()
      if (nested && config.nestedPolicy === 'deny') {
        if (scheduler.tryAcquire({ callId, toolName, kind, signal: exec?.signal })) {
          logger?.debug?.(`[subagent-queue] ${toolName} 嵌套委派（depth=${depth}）直接放行`)
        } else {
          logger?.warn?.(
            `[subagent-queue] 嵌套委派（depth=${depth}）无名额，拒绝（capacity=${resolved.value}, source=${resolved.source}）`,
          )
          throw new QueueRejectedError(
            'subagent-queue: 嵌套委派的并发配额已满，已直接拒绝而不排队（排队会与正在等待该子代理的祖先互等）。' +
              '请等待并发下降，或由父级改为串行委派后重试。',
          )
        }
      } else {
        const gate: AcquireResult = await scheduler.acquire({ callId, toolName, kind, signal: exec?.signal })
        if (gate.kind === 'aborted') {
          throw new QueueRejectedError('subagent-queue: 排队期间调用被取消')
        }
        if (gate.kind === 'timeout') {
          logger?.warn?.(
            `[subagent-queue] ${toolName} 排队超时 waited=${gate.waitedMs}ms（capacity=${resolved.value}, source=${resolved.source}）`,
          )
          throw new QueueRejectedError(
            `subagent-queue: ${toolName} 排队 ${Math.round(gate.waitedMs / 1000)}s 仍未获得名额。` +
              '请等待已启动的子代理完成后重试，或减少一次性并发。',
          )
        }
        if (gate.kind === 'bypass') {
          logger?.debug?.(`[subagent-queue] bypass: ${gate.reason}`)
        } else {
          queuedMs = gate.queuedMs
          timedOut = gate.timedOut
        }
      }
    } catch (error) {
      // 明确的排队拒绝照原样抛出（调用失败、理由回给模型）；其余异常 = fail-open 放行。
      if (error instanceof QueueRejectedError) throw error
      logger?.warn?.(`[subagent-queue] 准入判定异常，已放行：${renderError(error)}`)
    }

    if (timedOut) {
      logger?.warn?.(`[subagent-queue] ${toolName} 排队超时但按 onTimeout=allow 放行（可能超出真实容量）`)
    } else if (queuedMs > 0) {
      logger?.info?.(`[subagent-queue] ${toolName} 排队 ${queuedMs}ms 后放行`)
    }
    return await next()
  })

  // ---------------- 2. 工具调用结束 → 归还名额 ----------------
  on('tools/result', (...handlerArgs: unknown[]): void => {
    const callId = extractCallId(handlerArgs)
    if (callId !== undefined) scheduler.endCall(callId)
  })

  on('tools/post-execute', async (...handlerArgs: unknown[]): Promise<unknown> => {
    const next = handlerArgs[2] as (() => Promise<unknown>) | undefined
    const callId = extractCallId(handlerArgs)
    if (callId !== undefined) scheduler.endCall(callId)
    return typeof next === 'function' ? await next() : { kind: 'accept' }
  })

  // ---------------- 3. 真实子代理生命周期 → 槽位计数 ----------------
  let warnedMissingRunId = false
  on('subagent/start', (...handlerArgs: unknown[]): void => {
    const runId = extractRunId(handlerArgs)
    if (runId !== undefined) scheduler.recordChildStart(runId)
    else if (!warnedMissingRunId) {
      warnedMissingRunId = true
      logger?.warn?.('[subagent-queue] subagent/start payload 缺少 runId，已忽略（同名事件形态变化？）')
    }
  })

  on('subagent/end', (...handlerArgs: unknown[]): void => {
    const runId = extractRunId(handlerArgs)
    if (runId !== undefined) scheduler.recordChildEnd(runId)
  })

  // ---------------- 4. 状态工具（失败不拖垮闸门） ----------------
  if (config.registerStatusTool && typeof pluginCtx.tools?.register === 'function') {
    try {
      pluginCtx.tools.register(
        defineTool({
          name: 'subagent_queue_status',
          description:
            'Inspect the subagent-queue gate: configured capacity (and where it came from), currently ' +
            'active subagents, in-flight delegation calls, queued waiters, workflow occupancy, and ' +
            'cumulative counters. Use it when delegations seem to stall or when you need to know how ' +
            'many concurrent subagents this host allows.',
          parameters: {},
          // 只读快照，天然并发安全：不要让它当 exclusive barrier 占住一轮。
          isConcurrencySafe: () => true,
          output: {
            schema: {
              type: 'object',
              properties: {
                summary: { type: 'string' },
                details: { type: 'string' },
              },
              additionalProperties: false,
            },
            render: (_args, value) => {
              const text =
                typeof value.details === 'string' && value.details.length > 0
                  ? value.details
                  : String(value.summary ?? '(empty)')
              return [{ type: 'text', text }]
            },
          },
          timeoutMs: 5000,
          async execute() {
            const snapshot = scheduler.snapshot()
            const summary =
              `capacity=${snapshot.capacity}(${snapshot.capacitySource}) ` +
              `active=${snapshot.activeChildren.length} pending=${snapshot.pending.length} ` +
              `workflow=${snapshot.workflowActive} queued=${snapshot.queued}`
            return { summary, details: JSON.stringify({ summary, ...snapshot }, null, 2) }
          },
        }),
      )
    } catch (error) {
      logger?.warn?.(`[subagent-queue] 状态工具注册失败（不影响排队闸门）：${renderError(error)}`)
    }
  }

  logger?.info?.(
    `[subagent-queue] 已启用：capacity=${scheduler.getCapacity()}(${scheduler.getCapacitySource()}) ` +
      `watch=${config.watchTools.join(',')} workflow=${config.workflowTools.join(',')} ` +
      `queueTimeout=${config.queueTimeoutMs}ms`,
  )
}

function renderError(error: unknown): string {
  try {
    return error instanceof Error ? `${error.name}: ${error.message}` : String(error)
  } catch {
    return '<unrenderable thrown value>'
  }
}
