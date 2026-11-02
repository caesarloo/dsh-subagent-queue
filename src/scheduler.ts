/**
 * dsh-subagent-queue — 调度核心（纯逻辑，不依赖 DSH / cordis）。
 *
 * 设计要点：DSH 的子代理并发上限（`subagent limit reached (active child limit: N)`）
 * 是**硬闸**：超出直接抛错、整批失败，服务端明确"不排队"。本调度器把排队搬到
 * 工具调用层（`tools/pre-execute`），让委派调用在闸门处等待而不是失败。
 *
 * 计数口径（三者相加视为已占用）：
 *   - activeChildren：`subagent/start` 与 `subagent/end` 之间的真实子代理；
 *   - pending：已放行但工具调用尚未结束的委派调用（避免并发放行抢同一槽）；
 *   - graceHolds：工具刚结束后的短暂保留，给迟到的 `subagent/start` 事件留时间，
 *     防止"工具先返回、start 后到"期间瞬时超额。
 *
 * workflow 单独处理：它会一次扇出多个子代理，无法从外部按槽计数，因此按
 * `workflowConcurrency`（默认 1）**独占**：运行期间不再放行任何其它委派。
 */

export type DelegateToolKind = 'delegate' | 'workflow'

export interface SchedulerOptions {
  /** 每台宿主允许的并发子代理数（0 = 由外部 setCapacity 注入，仍为 0 时视为不可用）。 */
  capacity: number
  /** 同时允许运行的 workflow 数（默认 1）。 */
  workflowConcurrency: number
  /** 排队超时毫秒（0 = 不超时）。 */
  queueTimeoutMs: number
  /** 排队超时后的策略：deny = 拒绝该调用；allow = 放行（可能超出真实容量）。 */
  onTimeout: 'deny' | 'allow'
  /** 工具调用结束后的容量保留时间（毫秒），用于等待迟到的 start 事件。 */
  graceMs: number
  /** pending 的兜底存活时间（毫秒）：超过仍未收到结束信号则强制释放并告警。 */
  pendingTtlMs: number
  /** 单个活跃子代理的告警阈值（毫秒）：超过仍未收到 end 事件则告警"疑似占槽"。 */
  stuckChildWarnMs: number
  /** 活跃子代理的强制回收 TTL（毫秒）：丢失 end 事件时避免名额被永久占用。 */
  childStaleTtlMs: number
  /** 监控巡检间隔（毫秒，0 = 关闭）。 */
  monitorIntervalMs: number
  /** 可注入时钟（测试用）。 */
  now?: () => number
  /** 可注入日志（测试用）。 */
  log?: (level: 'debug' | 'info' | 'warn', message: string) => void
}

export interface AcquireInput {
  callId: string
  toolName: string
  kind: DelegateToolKind
  signal?: AbortSignal
}

export type AcquireResult =
  | { kind: 'granted'; queuedMs: number; ahead: number; timedOut: boolean }
  | { kind: 'timeout'; waitedMs: number }
  | { kind: 'aborted' }
  | { kind: 'bypass'; reason: string }

export interface WaiterView {
  callId: string
  toolName: string
  kind: DelegateToolKind
  waitedMs: number
}

export interface SchedulerSnapshot {
  capacity: number
  capacitySource: string
  workflowConcurrency: number
  activeChildren: Array<{ runId: string; runningMs: number; warned: boolean }>
  pending: Array<{ callId: string; toolName: string; kind: DelegateToolKind; heldMs: number }>
  workflowActive: number
  graceHolds: number
  queued: number
  waiters: WaiterView[]
  stats: {
    granted: number
    timedOut: number
    timedOutGranted: number
    aborted: number
    releasedByTtl: number
    reclaimedStaleChildren: number
    childStarts: number
    childEnds: number
    peakActive: number
    peakQueued: number
  }
  recent: Array<{ at: number; level: 'info' | 'warn' | 'debug'; event: string; detail: string }>
}

interface Waiter {
  input: AcquireInput
  enqueuedAt: number
  /** 入队时前面的等待者数量（用于 granted.ahead 报告）。 */
  ahead: number
  resolve: (result: AcquireResult) => void
  settled: boolean
  timer?: ReturnType<typeof setTimeout>
  onAbort?: () => void
}

interface PendingEntry {
  callId: string
  toolName: string
  kind: DelegateToolKind
  grantedAt: number
}

interface ActiveChild {
  runId: string
  startedAt: number
  warned: boolean
}

const RECENT_MAX = 60

export class DelegationScheduler {
  private options: SchedulerOptions
  private readonly now: () => number
  private readonly log: (level: 'debug' | 'info' | 'warn', message: string) => void

  private capacity: number
  private capacitySource = 'config'
  private workflowConcurrency: number

  private readonly activeChildren = new Map<string, ActiveChild>()
  private readonly pending = new Map<string, PendingEntry>()
  private readonly workflowActive = new Set<string>()
  private readonly graceTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private readonly waiters: Waiter[] = []
  private readonly recent: SchedulerSnapshot['recent'] = []
  private monitorTimer?: ReturnType<typeof setInterval>
  private disposed = false

  private stats = {
    granted: 0,
    timedOut: 0,
    timedOutGranted: 0,
    aborted: 0,
    releasedByTtl: 0,
    reclaimedStaleChildren: 0,
    childStarts: 0,
    childEnds: 0,
    peakActive: 0,
    peakQueued: 0,
  }

  constructor(options: SchedulerOptions) {
    this.options = options
    this.now = options.now ?? (() => Date.now())
    this.log = options.log ?? (() => {})
    this.capacity = Math.max(0, Math.floor(options.capacity))
    this.workflowConcurrency = Math.max(1, Math.floor(options.workflowConcurrency))
    if (options.monitorIntervalMs > 0) {
      this.monitorTimer = setInterval(() => this.inspect(), options.monitorIntervalMs)
      // Node 下不阻止进程退出
      ;(this.monitorTimer as unknown as { unref?: () => void }).unref?.()
    }
  }

  /** 运行期更新容量（用户改了 subagents 设置或配置热更新）。 */
  setCapacity(capacity: number, source = 'config'): void {
    if (!Number.isFinite(capacity) || capacity < 1) return
    const next = Math.floor(capacity)
    if (next !== this.capacity || source !== this.capacitySource) {
      this.record('info', 'capacity', `capacity ${this.capacity}(${this.capacitySource}) -> ${next}(${source})`)
    }
    this.capacity = next
    this.capacitySource = source
    this.pump()
  }

  setWorkflowConcurrency(value: number): void {
    if (!Number.isFinite(value) || value < 1) return
    this.workflowConcurrency = Math.floor(value)
    this.pump()
  }

  getCapacity(): number {
    return this.capacity
  }

  getCapacitySource(): string {
    return this.capacitySource
  }

  getStats(): SchedulerSnapshot['stats'] {
    return { ...this.stats }
  }

  /**
   * 申请一个委派名额。返回 granted 时**已登记 pending**（调用方无需再 occupy），
   * 调用方在工具调用结束时用 {@link endCall} 归还。
   */
  async acquire(input: AcquireInput): Promise<AcquireResult> {
    if (this.disposed) return { kind: 'bypass', reason: 'scheduler disposed' }
    if (this.capacity < 1) return { kind: 'bypass', reason: 'no capacity configured' }
    if (input.signal?.aborted === true) return { kind: 'aborted' }

    if (this.waiters.length === 0 && this.canGrant(input.kind)) {
      this.stats.granted += 1
      this.occupy(input.callId, input.toolName, input.kind)
      this.record('info', 'grant-now', `${input.toolName} call=${input.callId}`)
      return { kind: 'granted', queuedMs: 0, ahead: 0, timedOut: false }
    }

    return await new Promise<AcquireResult>((resolve) => {
      const waiter: Waiter = {
        input,
        enqueuedAt: this.now(),
        ahead: this.waiters.length,
        resolve,
        settled: false,
      }
      this.waiters.push(waiter)
      this.stats.peakQueued = Math.max(this.stats.peakQueued, this.waiters.length)
      this.record('info', 'queued', `${input.toolName} call=${input.callId} pos=${this.waiters.length}`)
      // 入场即评估：pump 会跳过当前不可放行的队头（如等待中的 workflow），
      // 不会让一个队头把整条队列拖住。
      this.pump()

      if (this.options.queueTimeoutMs > 0) {
        waiter.timer = setTimeout(() => this.timeout(waiter), this.options.queueTimeoutMs)
        ;(waiter.timer as unknown as { unref?: () => void }).unref?.()
      }
      if (input.signal !== undefined) {
        waiter.onAbort = () => this.abort(waiter)
        input.signal.addEventListener('abort', waiter.onAbort, { once: true })
      }
    })
  }

  /** 放行后登记 pending（调用方在真正 dispatch 前调用；幂等）。 */
  occupy(callId: string, toolName: string, kind: DelegateToolKind): void {
    if (this.pending.has(callId)) return
    this.pending.set(callId, { callId, toolName, kind, grantedAt: this.now() })
    if (kind === 'workflow') this.workflowActive.add(callId)
  }

  /**
   * 非阻塞尝试（嵌套委派专用）：有空位且无排队者时立即占用并返回 true，
   * 否则返回 false —— **绝不排队**。嵌套子代理若排队等待，可能阻塞正在等它的
   * 祖先（祖先占着名额、等子代理结束、子代理却排队等名额），形成死锁。
   */
  tryAcquire(input: AcquireInput): boolean {
    if (this.disposed) return false
    if (this.capacity < 1) return false
    if (input.signal?.aborted === true) return false
    if (this.waiters.length > 0 || !this.canGrant(input.kind)) return false
    this.stats.granted += 1
    this.occupy(input.callId, input.toolName, input.kind)
    this.record('info', 'grant-nested', `${input.toolName} call=${input.callId}`)
    return true
  }

  /**
   * 工具调用结束（`tools/result` / `tools/post-execute`）：释放 pending，
   * 但保留 graceMs 再归还容量。
   */
  endCall(callId: string): void {
    if (!this.pending.has(callId)) return
    this.pending.delete(callId)
    this.workflowActive.delete(callId)
    if (this.graceTimers.has(callId)) return
    if (this.options.graceMs <= 0) {
      this.pump()
      return
    }
    const timer = setTimeout(() => {
      this.graceTimers.delete(callId)
      this.pump()
    }, this.options.graceMs)
    ;(timer as unknown as { unref?: () => void }).unref?.()
    this.graceTimers.set(callId, timer)
  }

  /** `subagent/start`：真实子代理开始。 */
  recordChildStart(runId: string): void {
    if (this.activeChildren.has(runId)) return
    this.stats.childStarts += 1
    this.activeChildren.set(runId, { runId, startedAt: this.now(), warned: false })
    this.stats.peakActive = Math.max(this.stats.peakActive, this.activeChildren.size)
    this.record('debug', 'child-start', runId)
  }

  /** `subagent/end`：真实子代理结束，归还容量。 */
  recordChildEnd(runId: string): void {
    if (!this.activeChildren.has(runId)) return
    this.stats.childEnds += 1
    this.activeChildren.delete(runId)
    this.record('debug', 'child-end', runId)
    this.pump()
  }

  snapshot(): SchedulerSnapshot {
    const t = this.now()
    return {
      capacity: this.capacity,
      capacitySource: this.capacitySource,
      workflowConcurrency: this.workflowConcurrency,
      activeChildren: [...this.activeChildren.values()].map((child) => ({
        runId: child.runId,
        runningMs: t - child.startedAt,
        warned: child.warned,
      })),
      pending: [...this.pending.values()].map((entry) => ({
        callId: entry.callId,
        toolName: entry.toolName,
        kind: entry.kind,
        heldMs: t - entry.grantedAt,
      })),
      workflowActive: this.workflowActive.size,
      graceHolds: this.graceTimers.size,
      queued: this.waiters.length,
      waiters: this.waiters.map((waiter) => ({
        callId: waiter.input.callId,
        toolName: waiter.input.toolName,
        kind: waiter.input.kind,
        waitedMs: t - waiter.enqueuedAt,
      })),
      stats: { ...this.stats },
      recent: [...this.recent],
    }
  }

  dispose(): void {
    this.disposed = true
    if (this.monitorTimer !== undefined) clearInterval(this.monitorTimer)
    this.monitorTimer = undefined
    for (const timer of this.graceTimers.values()) clearTimeout(timer)
    this.graceTimers.clear()
    // 卫生：清空残留计数，避免插件 fiber 卸载后若仍被外部持有引用时看到陈旧状态。
    this.pending.clear()
    this.workflowActive.clear()
    this.activeChildren.clear()
    for (const waiter of this.waiters.splice(0)) {
      this.clearWaiter(waiter)
      waiter.settled = true
      waiter.resolve({ kind: 'bypass', reason: 'scheduler disposed' })
    }
  }

  // ---------------- internals ----------------

  private canGrant(kind: DelegateToolKind): boolean {
    // workflow 自身的 pending 不算"普通委派占用"，否则 workflowConcurrency>1 永远无法生效。
    const delegatePending = this.pending.size - this.workflowActive.size
    const used = delegatePending + this.activeChildren.size + this.graceTimers.size
    if (kind === 'workflow') {
      return this.workflowActive.size < this.workflowConcurrency && used === 0
    }
    return this.workflowActive.size === 0 && used < this.capacity
  }

  /**
   * 放行队列：**扫描全队列**而不是只看队头——队头若是等待中的 workflow（它要等所有
   * 委派结束），后面的 delegate 不该被一起饿死；同类之间仍保持 FIFO 顺序。
   */
  private pump(): void {
    if (this.disposed) return
    for (let index = 0; index < this.waiters.length; ) {
      const head = this.waiters[index] as Waiter
      if (!this.canGrant(head.input.kind)) {
        index += 1
        continue
      }
      this.waiters.splice(index, 1)
      this.clearWaiter(head)
      if (head.settled) continue
      head.settled = true
      this.stats.granted += 1
      this.occupy(head.input.callId, head.input.toolName, head.input.kind)
      const queuedMs = this.now() - head.enqueuedAt
      this.record('info', 'grant', `${head.input.toolName} call=${head.input.callId} waited=${queuedMs}ms`)
      head.resolve({ kind: 'granted', queuedMs, ahead: head.ahead, timedOut: false })
    }
  }

  private timeout(waiter: Waiter): void {
    if (waiter.settled) return
    waiter.settled = true
    const index = this.waiters.indexOf(waiter)
    if (index >= 0) this.waiters.splice(index, 1)
    this.clearWaiter(waiter)
    const waitedMs = this.now() - waiter.enqueuedAt
    if (this.options.onTimeout === 'allow') {
      this.stats.timedOutGranted += 1
      this.occupy(waiter.input.callId, waiter.input.toolName, waiter.input.kind)
      this.record('warn', 'timeout-allow', `${waiter.input.toolName} call=${waiter.input.callId} waited=${waitedMs}ms`)
      waiter.resolve({ kind: 'granted', queuedMs: waitedMs, ahead: waiter.ahead, timedOut: true })
    } else {
      this.stats.timedOut += 1
      this.record('warn', 'timeout-deny', `${waiter.input.toolName} call=${waiter.input.callId} waited=${waitedMs}ms`)
      waiter.resolve({ kind: 'timeout', waitedMs })
    }
    this.pump()
  }

  private abort(waiter: Waiter): void {
    if (waiter.settled) return
    waiter.settled = true
    const index = this.waiters.indexOf(waiter)
    if (index >= 0) this.waiters.splice(index, 1)
    this.clearWaiter(waiter)
    this.stats.aborted += 1
    this.record('info', 'abort', `${waiter.input.toolName} call=${waiter.input.callId}`)
    waiter.resolve({ kind: 'aborted' })
    this.pump()
  }

  private clearWaiter(waiter: Waiter): void {
    if (waiter.timer !== undefined) clearTimeout(waiter.timer)
    waiter.timer = undefined
    if (waiter.onAbort !== undefined) {
      waiter.input.signal?.removeEventListener('abort', waiter.onAbort)
      waiter.onAbort = undefined
    }
  }


  /** 巡检：僵尸占槽告警 + 强制回收、pending 兜底释放。 */
  private inspect(): void {
    if (this.disposed) return
    const t = this.now()
    for (const [runId, child] of [...this.activeChildren.entries()]) {
      const runningMs = t - child.startedAt
      if (!child.warned && runningMs > this.options.stuckChildWarnMs) {
        child.warned = true
        this.record('warn', 'stuck-child', `${child.runId} running=${runningMs}ms (no subagent/end seen)`)
      }
      // 丢失 end 事件时不能永久占槽：超过 TTL 强制回收（可配置，默认 6 小时）。
      if (runningMs > this.options.childStaleTtlMs) {
        this.activeChildren.delete(runId)
        this.stats.reclaimedStaleChildren += 1
        this.record('warn', 'child-ttl', `${runId} running=${runningMs}ms (forced reclaim, no subagent/end)`)
        this.pump()
      }
    }
    for (const [callId, entry] of [...this.pending.entries()]) {
      const heldMs = t - entry.grantedAt
      if (heldMs > this.options.pendingTtlMs) {
        this.stats.releasedByTtl += 1
        this.record('warn', 'pending-ttl', `${entry.toolName} call=${callId} held=${heldMs}ms (forced release)`)
        this.endCall(callId)
      }
    }
  }

  private record(level: 'info' | 'warn' | 'debug', event: string, detail: string): void {
    if (level !== 'debug') this.log(level, `[subagent-queue] ${event}: ${detail}`)
    this.recent.push({ at: this.now(), level, event, detail })
    if (this.recent.length > RECENT_MAX) this.recent.splice(0, this.recent.length - RECENT_MAX)
  }
}
