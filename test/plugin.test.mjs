// 插件层单测：假 ctx 捕获 `tools/execute` 监听器，直接喂 exec。
// 关键断言：拦截时决策为 deny/cancel **且 next 未被调用**（工具确实不会被执行）。
// 运行：node test/plugin.test.mjs

import test from 'node:test'
import assert from 'node:assert/strict'
import {
  apply,
  normalizeConfig,
  readServiceCapacity,
  extractRunId,
  extractCallId,
  resolveDelegationDepth,
  name as pluginName,
  Config as pluginConfig,
} from '../dist/index.js'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function fakeCtx(services = {}) {
  const listeners = new Map()
  const tools = []
  const logs = []
  const ctx = {
    on: (event, handler) => {
      listeners.set(event, handler)
      return () => listeners.delete(event)
    },
    get: (serviceName) => services[serviceName],
    logger: {
      info: (message) => logs.push(['info', message]),
      warn: (message) => logs.push(['warn', message]),
      debug: (message) => logs.push(['debug', message]),
    },
    tools: {
      register: (definition) => {
        tools.push(definition)
        return definition
      },
    },
  }
  return { ctx, listeners, tools, logs }
}

const BASE = {
  capacity: 1,
  fallbackCapacity: 1,
  graceMs: 5,
  monitorIntervalMs: 0,
  queueTimeoutMs: 0,
  workflowConcurrency: 1,
}

function execLike(callId, signal, toolName = 'subagent', agent = undefined) {
  return {
    name: toolName,
    callId,
    signal: signal ?? new AbortController().signal,
    arguments: {},
    ...(agent === undefined ? {} : { agent }),
  }
}

function allowNext(counter) {
  return async () => {
    counter.calls += 1
    return { isError: false, content: [], value: null }
  }
}

/** 断言"拒绝"（抛错）且 next 未被调用。 */
async function expectRejected(promiseFactory, counter, pattern) {
  await assert.rejects(promiseFactory, (error) => {
    assert.match(String(error?.message ?? error), pattern)
    return true
  })
  assert.equal(counter.calls, 0, '被拒绝的调用绝不能被放行')
}

test('插件名与 Config 导出齐备', () => {
  assert.equal(pluginName, 'tool-subagent-queue')
  assert.ok(pluginConfig && (typeof pluginConfig === 'object' || typeof pluginConfig === 'function'))
})

test('只监听 tools/execute，不监听 tools/pre-execute（P1 死锁回归门）', () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE)
  assert.equal(listeners.has('tools/execute'), true)
  assert.equal(listeners.has('tools/pre-execute'), false, '闸门不得放回 pre-execute：会阻塞宿主 prepare/commit 单车道')
})

test('非委派工具直通，next 调用一次', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE)
  const handler = listeners.get('tools/execute')
  const counter = { calls: 0 }
  const result = await handler(execLike('c0', undefined, 'pwsh'), allowNext(counter))
  assert.equal(counter.calls, 1)
  assert.equal(result.isError, false)
})

test('enabled=false 时委派直通', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, enabled: false })
  const handler = listeners.get('tools/execute')
  const counter = { calls: 0 }
  await handler(execLike('c0'), allowNext(counter))
  assert.equal(counter.calls, 1)
})

test('容量内放行委派，pending 占用一个名额', async () => {
  const { ctx, listeners, tools } = fakeCtx()
  apply(ctx, { ...BASE, capacity: 2 })
  const handler = listeners.get('tools/execute')
  const counter = { calls: 0 }
  await handler(execLike('c1'), allowNext(counter))
  assert.equal(counter.calls, 1)

  const status = await tools.find((tool) => tool.name === 'subagent_queue_status').execute({}, {
    signal: new AbortController().signal,
  })
  assert.match(status.summary, /capacity=2/)
  assert.match(status.summary, /pending=1/)
})

test('满容量时第二个委派排队，工具结果事件释放后放行', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE)
  const handler = listeners.get('tools/execute')
  const release = listeners.get('tools/result')

  const firstCounter = { calls: 0 }
  await handler(execLike('c1'), allowNext(firstCounter))
  assert.equal(firstCounter.calls, 1)

  const secondCounter = { calls: 0 }
  const pending = handler(execLike('c2'), allowNext(secondCounter))
  await sleep(20)
  assert.equal(secondCounter.calls, 0, '满容量时第二个调用不得立即执行')

  release({ callId: 'c1' }, { isError: false })
  await sleep(25)
  assert.equal(secondCounter.calls, 1, '归还名额后第二个调用应放行')
  await pending
})

test('排队中取消 → 抛出取消错误且 next 未被调用', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE)
  const handler = listeners.get('tools/execute')
  await handler(execLike('c1'), allowNext({ calls: 0 }))

  const controller = new AbortController()
  const counter = { calls: 0 }
  const pending = handler(execLike('c2', controller.signal), allowNext(counter))
  await sleep(15)
  controller.abort()
  await expectRejected(() => pending, counter, /取消/)
})

test('排队超时 → 抛出超时错误、next 未被调用', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, queueTimeoutMs: 25, onTimeout: 'deny' })
  const handler = listeners.get('tools/execute')
  await handler(execLike('c1'), allowNext({ calls: 0 }))

  const counter = { calls: 0 }
  const pending = handler(execLike('c2'), allowNext(counter))
  await expectRejected(() => pending, counter, /排队/)
})

test('排队超时 + onTimeout=allow → 放行（配置选择）', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, queueTimeoutMs: 25, onTimeout: 'allow' })
  const handler = listeners.get('tools/execute')
  await handler(execLike('c1'), allowNext({ calls: 0 }))
  const counter = { calls: 0 }
  await handler(execLike('c2'), allowNext(counter))
  assert.equal(counter.calls, 1)
})

test('workflow 独占：运行中委派排队', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, capacity: 4 })
  const handler = listeners.get('tools/execute')
  const counter = { calls: 0 }

  await handler(execLike('w1', undefined, 'workflow'), allowNext(counter))
  assert.equal(counter.calls, 1)

  const delegateCounter = { calls: 0 }
  const pending = handler(execLike('d1'), allowNext(delegateCounter))
  await sleep(20)
  assert.equal(delegateCounter.calls, 0, 'workflow 运行期间委派应排队')

  listeners.get('tools/result')({ callId: 'w1' }, {})
  await sleep(25)
  assert.equal(delegateCounter.calls, 1)
  await pending
})

test('workflowConcurrency=2 时两个 workflow 可同时放行', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, capacity: 4, workflowConcurrency: 2 })
  const handler = listeners.get('tools/execute')
  const c1 = { calls: 0 }
  const c2 = { calls: 0 }
  await handler(execLike('w1', undefined, 'workflow'), allowNext(c1))
  await handler(execLike('w2', undefined, 'workflow'), allowNext(c2))
  assert.equal(c1.calls, 1)
  assert.equal(c2.calls, 1, 'workflowConcurrency=2 时第二个 workflow 不应被自身 pending 挡住')
})

test('subagent/start|end 事件驱动状态工具里的活跃计数', async () => {
  const { ctx, listeners, tools } = fakeCtx()
  apply(ctx, BASE)
  const statusTool = tools.find((tool) => tool.name === 'subagent_queue_status')

  listeners.get('subagent/start')({ runId: 'r1', provider: 'spawn', id: 's1', local: true })
  let status = await statusTool.execute({}, { signal: new AbortController().signal })
  assert.match(status.summary, /active=1/)

  listeners.get('subagent/end')({ runId: 'r1', provider: 'spawn', id: 's1', local: true, stopReason: 'completed' })
  status = await statusTool.execute({}, { signal: new AbortController().signal })
  assert.match(status.summary, /active=0/)
})

test('payload 缺 runId 时忽略并只告警一次（不回退到 session id）', async () => {
  const { ctx, listeners, logs, tools } = fakeCtx()
  apply(ctx, BASE)
  const statusTool = tools.find((tool) => tool.name === 'subagent_queue_status')
  listeners.get('subagent/start')({ provider: 'spawn', id: 's1', local: true })
  listeners.get('subagent/start')({ provider: 'spawn', id: 's2', local: true })
  const status = await statusTool.execute({}, { signal: new AbortController().signal })
  assert.match(status.summary, /active=0/, 'id+local 不得被当作 runId')
  assert.equal(logs.filter(([level]) => level === 'warn').length, 1, '只告警一次')
})

test('scoped dispatch 带 carrier 参数时仍能提取 runId', () => {
  assert.equal(extractRunId([{ __scope: 'x' }, { runId: 'r9', provider: 'spawn', id: 's9', local: true }]), 'r9')
  assert.equal(extractRunId([{ runId: 'direct' }]), 'direct')
  assert.equal(extractRunId([{ provider: 'x', id: 's1', local: true }]), undefined)
})

test('extractCallId 只认首个参数里的 callId', () => {
  assert.equal(extractCallId([{ callId: 'c7' }, { isError: false }]), 'c7')
  assert.equal(extractCallId([null]), undefined)
  assert.equal(extractCallId([{ name: 'x' }]), undefined)
})

test('readServiceCapacity：配置优先，其次服务实时值，最后 fallback', () => {
  const config = normalizeConfig({})
  assert.deepEqual(readServiceCapacity({}, config), { value: 8, source: 'fallback' })

  const live = { get: () => 3 }
  const withService = readServiceCapacity(
    { get: (serviceName) => (serviceName === 'subagents' ? { config: { maxActiveSubagents: live } } : undefined) },
    config,
  )
  assert.deepEqual(withService, { value: 3, source: 'subagents.config' })

  const explicit = readServiceCapacity({}, normalizeConfig({ capacity: 5 }))
  assert.deepEqual(explicit, { value: 5, source: 'config' })

  const throwing = readServiceCapacity(
    { get: () => { throw new Error('boom') } },
    config,
  )
  assert.deepEqual(throwing, { value: 8, source: 'fallback' })
})

test('normalizeConfig 容错：非法值回落默认（含新默认值）', () => {
  const config = normalizeConfig({
    capacity: -3,
    fallbackCapacity: 0,
    watchTools: [],
    workflowTools: ['flow'],
    onTimeout: 'whatever',
    queueTimeoutMs: Number.NaN,
  })
  assert.equal(config.capacity, 0)
  assert.equal(config.fallbackCapacity, 1)
  assert.deepEqual(config.watchTools, ['subagent', 'subagent_fork'])
  assert.deepEqual(config.workflowTools, ['flow'])
  assert.equal(config.onTimeout, 'deny')
  assert.equal(config.queueTimeoutMs, 600_000)
  assert.equal(config.pendingTtlMs, 300_000)
  assert.equal(config.childStaleTtlMs, 21_600_000)
})

test('post-execute 监听器放行下游决策并归还名额', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE)
  const handler = listeners.get('tools/execute')
  await handler(execLike('c5'), allowNext({ calls: 0 }))

  const post = listeners.get('tools/post-execute')
  const counter = { calls: 0 }
  const decision = await post({ callId: 'c5' }, { isError: false }, async () => {
    counter.calls += 1
    return { kind: 'accept' }
  })
  assert.equal(counter.calls, 1)
  assert.equal(decision.kind, 'accept')
})

test('缺 ctx.on 时不抛错（防御性）', () => {
  const { ctx } = fakeCtx()
  const broken = { ...ctx, on: undefined }
  assert.doesNotThrow(() => apply(broken, BASE))
})

// ---------------- 嵌套委派防死锁 ----------------

const nestedAgent = { options: {}, session: { header: { delegationDepth: 1 } } }

test('嵌套委派（depth>0）满池时立即拒绝，且 next 未被调用', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE) // capacity 1
  const handler = listeners.get('tools/execute')
  await handler(execLike('c1'), allowNext({ calls: 0 })) // 顶层调用占满唯一名额

  const counter = { calls: 0 }
  await expectRejected(
    () => handler(execLike('c2', undefined, 'subagent', nestedAgent), allowNext(counter)),
    counter,
    /嵌套委派/,
  )
})

test('嵌套委派有空位时直接放行（不排队）', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, capacity: 2 })
  const handler = listeners.get('tools/execute')
  const counter = { calls: 0 }
  await handler(execLike('n1', undefined, 'subagent', nestedAgent), allowNext(counter))
  assert.equal(counter.calls, 1)
})

test('nestedPolicy=queue 时嵌套委派仍排队（显式选择可回退）', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, { ...BASE, nestedPolicy: 'queue' })
  const handler = listeners.get('tools/execute')
  await handler(execLike('c1'), allowNext({ calls: 0 }))

  const counter = { calls: 0 }
  const pending = handler(execLike('c2', undefined, 'subagent', nestedAgent), allowNext(counter))
  await sleep(20)
  assert.equal(counter.calls, 0, 'nestedPolicy=queue 时嵌套应排队等待')
  listeners.get('tools/result')({ callId: 'c1' }, {})
  await sleep(25)
  assert.equal(counter.calls, 1)
  await pending
})

// ---------------- 委派深度（本地实现） ----------------

test('resolveDelegationDepth：顶层=0、header 权威、runtime 只增不减、非法值按嵌套', () => {
  assert.equal(resolveDelegationDepth(undefined), 0)
  assert.equal(resolveDelegationDepth({ options: {}, session: { header: {} } }), 0)
  assert.equal(resolveDelegationDepth({ options: {}, session: { header: { delegationDepth: 2 } } }), 2)
  assert.equal(resolveDelegationDepth({ options: { subagentDepth: 1 }, session: { header: {} } }), 1)
  assert.equal(
    resolveDelegationDepth({ options: { subagentDepth: 1 }, session: { header: { delegationDepth: 3 } } }),
    3,
    'header 与 runtime 取最大值',
  )
  // 存在但非法：按嵌套处理（至少 1），避免把嵌套误判成顶层去排队
  assert.equal(resolveDelegationDepth({ options: { subagentDepth: -1 }, session: { header: {} } }), 1)
  assert.equal(resolveDelegationDepth({ options: { subagentDepth: 1.5 }, session: { header: {} } }), 1)
})

// ---------------- 宿主调度时序回归（P1 死锁门） ----------------

test('模拟宿主 runGroup：容量满时后续调用的 prepare 不被阻塞（不会自锁）', async () => {
  const { ctx, listeners } = fakeCtx()
  apply(ctx, BASE) // capacity 1
  const release = listeners.get('tools/result')

  // runGroup 的单车道结构：prepare（含 pre-execute）串行，dispatch promise 不 await。
  const prepare = async (exec) => {
    const pre = listeners.get('tools/pre-execute')
    if (typeof pre === 'function') {
      // 若闸门放回 pre-execute，这里会被排队阻塞 → 后续 prepare 不返回 → 自锁。
      await pre(exec, async () => ({ kind: 'allow' }))
    }
    return { kind: 'dispatch' }
  }
  const counter0 = { calls: 0 }
  const counter1 = { calls: 0 }

  // startCall(0)：prepare 立即返回，dispatch promise 启动（execute 阶段占满名额）
  const started0 = Date.now()
  const prepared0 = await prepare(execLike('c1'))
  assert.equal(prepared0.kind, 'dispatch')
  const dispatch0 = listeners.get('tools/execute')(execLike('c1'), allowNext(counter0))
  await sleep(5)
  assert.equal(counter0.calls, 1)

  // startCall(1)：prepare 必须在毫秒级返回（不因 c1 的 execute 排队而阻塞）
  const prepared1 = await prepare(execLike('c2'))
  assert.equal(prepared1.kind, 'dispatch')
  assert.ok(Date.now() - started0 < 100, '第二个调用的 prepare 不应被首个调用的排队阻塞')

  // 第二个 dispatch 进入 execute 后排队
  const dispatch1 = listeners.get('tools/execute')(execLike('c2'), allowNext(counter1))
  await sleep(20)
  assert.equal(counter1.calls, 0, '容量满时第二个调用排队')

  // 第一个的结果提交（宿主 commit 阶段）→ 名额归还 → 第二个放行
  release({ callId: 'c1' }, { isError: false })
  await sleep(25)
  assert.equal(counter1.calls, 1)
  await Promise.all([dispatch0, dispatch1])
})
