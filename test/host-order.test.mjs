// 宿主调度时序集成测试：用**真实的** @deepseek-ai/dsh-tools ToolRuntime，
// 按 dsh-agent-loop `runGroup` 的顺序驱动一批调用（prepare 串行 → dispatch 不 await →
// 最后 commit 发 tools/result），验证本插件的排队不会阻塞 prepare/commit 循环。
//
// 这条用例是 0.1.3 的 P1 回归门：闸门若放回 tools/pre-execute，第 2 个调用的 prepare
// 会被第 1 个的排队挡住 → 超时失败。
//
// 运行：node test/host-order.test.mjs

import test from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import { ToolRuntime, defineTool, TOOL_RUNTIME_SCHEDULER } from '@deepseek-ai/dsh-tools'
import { apply } from '../dist/index.js'

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function makeExec(callId) {
  return {
    callId,
    name: 'subagent',
    arguments: {},
    signal: new AbortController().signal,
  }
}

test('真实 ToolRuntime + runGroup 顺序：容量满时后续 prepare 不被阻塞，commit 后放行', async () => {
  const app = new Context()
  // ToolRuntime 构造会调用 ctx.systemPrompt.tools(...)（defaultMode=native 时只这一处）；
  // 提供最小实现即可。
  app.provide('systemPrompt', {
    tools: () => () => undefined,
    section: () => () => undefined,
  })
  const toolsRuntime = new ToolRuntime(app)

  let executed = 0
  toolsRuntime.register(
    defineTool({
      name: 'subagent',
      description: 'fake delegation tool for host-order test',
      parameters: {},
      output: {
        schema: {
          type: 'object',
          properties: { ok: { type: 'boolean', required: true } },
          additionalProperties: false,
        },
        render: () => [{ type: 'text', text: 'ok' }],
      },
      async execute() {
        executed += 1
        return { ok: true }
      },
    }),
  )

  apply(app, { capacity: 1, fallbackCapacity: 1, graceMs: 0, monitorIntervalMs: 0, queueTimeoutMs: 0 })

  const scheduler = toolsRuntime[TOOL_RUNTIME_SCHEDULER]
  assert.ok(scheduler, '应能取到宿主工具调度器')

  // startCall(0)：prepare 立即返回；dispatch 启动但不 await（runGroup 的做法）
  const prepared0 = await scheduler.prepare(makeExec('c1'))
  assert.equal(prepared0.kind, 'dispatch')
  const outcome0 = scheduler.dispatch(prepared0.exec)

  // startCall(1)：第 2 个 prepare 必须立即返回（这正是 P1 死锁点）
  const startedAt = Date.now()
  const prepared1 = await scheduler.prepare(makeExec('c2'))
  assert.equal(prepared1.kind, 'dispatch')
  assert.ok(Date.now() - startedAt < 500, `第 2 个 prepare 不应被排队阻塞（实际 ${Date.now() - startedAt}ms）`)

  // 第 2 个 dispatch 进入 execute 阶段排队：executed 仍为 1
  const outcome1 = scheduler.dispatch(prepared1.exec)
  await sleep(30)
  assert.equal(executed, 1, '容量满时第 2 个调用应排队，尚未执行工具')

  // commit（runGroup 的 commitReady）：finalize 会发 tools/result → 名额归还 → 第 2 个放行
  const settled0 = await outcome0
  const result0 =
    settled0.kind === 'post-result'
      ? await scheduler.finalize(prepared0.exec, settled0.result)
      : scheduler.finish(prepared0.exec, settled0.result)
  assert.equal(result0.isError, false)

  const settled1 = await outcome1
  assert.equal(executed, 2, 'commit 之后第 2 个调用应被放行并执行')
  const result1 =
    settled1.kind === 'post-result'
      ? await scheduler.finalize(prepared1.exec, settled1.result)
      : scheduler.finish(prepared1.exec, settled1.result)
  assert.equal(result1.isError, false)
})
