// 调度核心单测（纯逻辑，不启动 DSH）。
// 运行：node test/scheduler.test.mjs（或 npm test）

import test from 'node:test'
import assert from 'node:assert/strict'
import { DelegationScheduler } from '../dist/scheduler.js'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/** 构造一个可测的调度器（短 grace / 可配置监控）。 */
function make(overrides = {}) {
  return new DelegationScheduler({
    capacity: 1,
    workflowConcurrency: 1,
    queueTimeoutMs: 0,
    onTimeout: 'deny',
    graceMs: 10,
    pendingTtlMs: 60_000,
    stuckChildWarnMs: 60_000,
    childStaleTtlMs: 60_000,
    monitorIntervalMs: 0,
    log: () => {},
    ...overrides,
  })
}

function delegate(callId, signal) {
  return { callId, toolName: 'subagent', kind: 'delegate', signal }
}

test('容量内立即放行，并登记 pending', async () => {
  const scheduler = make({ capacity: 2 })
  const result = await scheduler.acquire(delegate('c1'))
  assert.equal(result.kind, 'granted')
  assert.equal(result.queuedMs, 0)
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.pending.length, 1)
  assert.equal(snapshot.capacity, 2)
  scheduler.dispose()
})

test('满则排队，endCall + grace 后按 FIFO 放行', async () => {
  const scheduler = make({ capacity: 1 })
  const first = await scheduler.acquire(delegate('a'))
  assert.equal(first.kind, 'granted')

  let secondResult = null
  const secondPromise = scheduler
    .acquire(delegate('b'))
    .then((result) => {
      secondResult = result
      return result
    })

  await sleep(25)
  assert.equal(secondResult, null, 'B 在满容量时应保持排队')
  assert.equal(scheduler.snapshot().queued, 1)

  scheduler.endCall('a')
  await sleep(25) // grace 10ms 之后才归还容量
  const second = await secondPromise
  assert.equal(second.kind, 'granted')
  assert.ok(second.queuedMs >= 10, `应记录排队时长，实际 ${second.queuedMs}`)
  scheduler.dispose()
})

test('FIFO：先排队者先放行', async () => {
  const scheduler = make({ capacity: 1 })
  await scheduler.acquire(delegate('a'))

  const order = []
  const aheadSeen = []
  const bPromise = scheduler
    .acquire(delegate('b'))
    .then((r) => {
      order.push(`b:${r.kind}`)
      aheadSeen.push(r.ahead)
    })
  const cPromise = scheduler
    .acquire(delegate('c'))
    .then((r) => {
      order.push(`c:${r.kind}`)
      aheadSeen.push(r.ahead)
    })
  await sleep(15)
  assert.deepEqual(order, [])

  scheduler.endCall('a')
  await sleep(25)
  assert.deepEqual(order, ['b:granted'], '只应放行队头 B，C 仍排队')

  scheduler.endCall('b')
  await sleep(25)
  assert.deepEqual(order, ['b:granted', 'c:granted'])
  assert.deepEqual(aheadSeen, [0, 1], 'ahead 应为入队时前方等待者数')
  await Promise.all([bPromise, cPromise])
  scheduler.dispose()
})

test('排队中取消 → aborted，且不占用名额', async () => {
  const scheduler = make({ capacity: 1 })
  await scheduler.acquire(delegate('a'))

  const controller = new AbortController()
  const pending = scheduler.acquire(delegate('b', controller.signal))
  await sleep(15)
  assert.equal(scheduler.snapshot().queued, 1)

  controller.abort()
  const result = await pending
  assert.equal(result.kind, 'aborted')
  assert.equal(scheduler.snapshot().queued, 0)
  scheduler.dispose()
})

test('排队超时：deny 策略返回 timeout', async () => {
  const scheduler = make({ capacity: 1, queueTimeoutMs: 25, onTimeout: 'deny' })
  await scheduler.acquire(delegate('a'))

  const result = await scheduler.acquire(delegate('b'))
  assert.equal(result.kind, 'timeout')
  assert.ok(result.waitedMs >= 15)
  assert.equal(scheduler.snapshot().stats.timedOut, 1)
  scheduler.dispose()
})

test('排队超时：allow 策略放行并标记 timedOut', async () => {
  const scheduler = make({ capacity: 1, queueTimeoutMs: 25, onTimeout: 'allow' })
  await scheduler.acquire(delegate('a'))

  const result = await scheduler.acquire(delegate('b'))
  assert.equal(result.kind, 'granted')
  assert.equal(result.timedOut, true)
  assert.equal(scheduler.snapshot().stats.timedOutGranted, 1)
  scheduler.dispose()
})

test('workflow 独占：运行期间不放行普通委派', async () => {
  const scheduler = make({ capacity: 4 })
  const workflow = await scheduler.acquire({ callId: 'w', toolName: 'workflow', kind: 'workflow' })
  assert.equal(workflow.kind, 'granted')

  let delegateResult = null
  const delegatePromise = scheduler.acquire(delegate('d')).then((r) => {
    delegateResult = r
    return r
  })
  await sleep(20)
  assert.equal(delegateResult, null, 'workflow 运行期间委派应排队')

  scheduler.endCall('w')
  await sleep(25)
  const granted = await delegatePromise
  assert.equal(granted.kind, 'granted')
  scheduler.dispose()
})

test('subagent 生命周期事件驱动活跃计数', () => {
  const scheduler = make({ capacity: 2 })
  scheduler.recordChildStart('r1')
  scheduler.recordChildStart('r2')
  assert.equal(scheduler.snapshot().activeChildren.length, 2)

  scheduler.recordChildEnd('r1')
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.activeChildren.length, 1)
  assert.equal(snapshot.stats.childStarts, 2)
  assert.equal(snapshot.stats.childEnds, 1)
  assert.equal(snapshot.stats.peakActive, 2)
  scheduler.dispose()
})

test('升容量后队列立即放行', async () => {
  const scheduler = make({ capacity: 1 })
  await scheduler.acquire(delegate('a'))
  let released = null
  const pending = scheduler.acquire(delegate('b')).then((r) => {
    released = r
    return r
  })
  await sleep(15)
  assert.equal(released, null)

  scheduler.setCapacity(2, 'test')
  const result = await pending
  assert.equal(result.kind, 'granted')
  scheduler.dispose()
})

test('pending 超过 TTL 被兜底释放并计入统计', async () => {
  const scheduler = make({ capacity: 1, pendingTtlMs: 20, monitorIntervalMs: 10 })
  await scheduler.acquire(delegate('stuck'))
  await sleep(60)
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.pending.length, 0)
  assert.ok(snapshot.stats.releasedByTtl >= 1)
  assert.ok(snapshot.recent.some((entry) => entry.event === 'pending-ttl'))
  scheduler.dispose()
})

test('僵尸子代理（无 end 事件）会被标记告警', async () => {
  const scheduler = make({ stuckChildWarnMs: 20, monitorIntervalMs: 10 })
  scheduler.recordChildStart('zombie')
  await sleep(60)
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.activeChildren.length, 1)
  assert.equal(snapshot.activeChildren[0].warned, true)
  assert.ok(snapshot.recent.some((entry) => entry.event === 'stuck-child'))
  scheduler.dispose()
})

test('僵尸子代理超过 TTL 被强制回收（名额不再永久被占）', async () => {
  const scheduler = make({ stuckChildWarnMs: 20, childStaleTtlMs: 30, monitorIntervalMs: 10 })
  scheduler.recordChildStart('zombie')
  await sleep(70)
  const snapshot = scheduler.snapshot()
  assert.equal(snapshot.activeChildren.length, 0)
  assert.ok(snapshot.stats.reclaimedStaleChildren >= 1)
  assert.ok(snapshot.recent.some((entry) => entry.event === 'child-ttl'))
  scheduler.dispose()
})

test('workflowConcurrency=2 时两个 workflow 可并存', async () => {
  const scheduler = make({ capacity: 4, workflowConcurrency: 2 })
  const first = await scheduler.acquire({ callId: 'w1', toolName: 'workflow', kind: 'workflow' })
  const second = await scheduler.acquire({ callId: 'w2', toolName: 'workflow', kind: 'workflow' })
  assert.equal(first.kind, 'granted')
  assert.equal(second.kind, 'granted', 'workflow 自身 pending 不应占掉另一个 workflow 的名额')
  scheduler.dispose()
})

test('队头 workflow 等待时不饿死后续 delegate（pump 跳过不可放行者）', async () => {
  const scheduler = make({ capacity: 2 })
  await scheduler.acquire(delegate('a')) // pending=1 → used=1

  let workflowResult = null
  const workflowPromise = scheduler
    .acquire({ callId: 'w1', toolName: 'workflow', kind: 'workflow' })
    .then((result) => {
      workflowResult = result
      return result
    })
  await sleep(15)
  assert.equal(workflowResult, null, 'workflow 需等 used===0，先排队')

  let delegateResult = null
  const delegatePromise = scheduler.acquire(delegate('b')).then((result) => {
    delegateResult = result
    return result
  })
  await sleep(15)
  assert.equal(delegateResult?.kind, 'granted', 'workflow 在队头时，delegate 不应被饿死')

  scheduler.dispose()
  const workflowOutcome = await workflowPromise
  assert.equal(workflowOutcome.kind, 'bypass', 'dispose 后排队者收到 bypass')
  await delegatePromise
})

test('dispose 后排队者收到 bypass，不再阻塞', async () => {
  const scheduler = make({ capacity: 1 })
  await scheduler.acquire(delegate('a'))
  const pending = scheduler.acquire(delegate('b'))
  await sleep(10)
  scheduler.dispose()
  const result = await pending
  assert.equal(result.kind, 'bypass')
})

test('tryAcquire：空位立即占用，满位返回 false（嵌套委派不排队）', async () => {
  const scheduler = make({ capacity: 1 })
  assert.equal(scheduler.tryAcquire(delegate('n1')), true)
  assert.equal(scheduler.tryAcquire(delegate('n2')), false, '满位时嵌套尝试必须立即失败')
  assert.equal(scheduler.snapshot().queued, 0, 'tryAcquire 绝不入队')

  scheduler.endCall('n1')
  await sleep(25)
  assert.equal(scheduler.tryAcquire(delegate('n3')), true)
  scheduler.dispose()
})

test('tryAcquire：已取消的信号直接拒绝', () => {
  const scheduler = make({ capacity: 2 })
  const controller = new AbortController()
  controller.abort()
  assert.equal(scheduler.tryAcquire(delegate('x', controller.signal)), false)
  scheduler.dispose()
})
