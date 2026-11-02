// 真实 cordis 运行时加载冒烟：不用假 ctx，用 @deepseek-ai/cordis 的 Context
// 提供 `tools` 服务后加载插件，确认 apply 真正执行、状态工具真正注册。
// 运行：node test/load-smoke.mjs

import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'

const registered = []
const app = new Context()
app.provide('tools', {
  register: (definition) => {
    registered.push(definition)
    return definition
  },
})

const plugin = await import('../dist/index.js')
const fiber = app.plugin(plugin, { capacity: 2, fallbackCapacity: 2, monitorIntervalMs: 0 })
await fiber

const statusTool = registered.find((tool) => tool && tool.name === 'subagent_queue_status')
assert.ok(statusTool, 'cordis 加载后应注册 subagent_queue_status 工具')

const result = await statusTool.execute({}, { signal: new AbortController().signal })
assert.match(result.summary, /capacity=2/)
assert.match(result.summary, /source=config|\(config\)/)

// 卸载 fiber：应触发插件 dispose（调度器清理），不抛错
await fiber.dispose()
console.log('PASS  cordis 运行时加载 + 工具注册 + 卸载清理')
