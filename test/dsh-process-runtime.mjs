import { req } from './dsh-native-runtime.mjs'
import { mkdir } from 'node:fs/promises'
import { basename, join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createAdapter } from '../dsh/host.mjs'

// This executable owns one in-memory native Host and its synthetic task only.
// It never opens a desktop profile, a user session or a real model service.
const [directory, label, fault] = process.argv.slice(2)
const dataDir = resolve(directory ?? '')
if (!process.send || !basename(dataDir).startsWith('sentinel-native-process-') || !['process-a', 'process-b'].includes(label)) {
  throw new Error('process fixture requires its own IPC connection and isolated temporary directory')
}
if (fault === 'stall-pre-ready') {
  // An intentionally broken bootstrap: it never registers a source or an Actor.
  // Only the parent fork owner may force-clean this fixture after a failed deadline.
  process.on('disconnect', () => {})
  setInterval(() => {}, 1000)
  process.send({ type: 'bootstrapping', pid: process.pid })
  await new Promise(() => {})
}
const profileDir = join(dataDir, 'profiles', label)
await mkdir(profileDir, { recursive: true })
const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const ctx = new Context()
ctx.provide('profileContext', { name: label, dir: profileDir, home: dataDir, cwd: profileDir })
let providerAborted = false, toolCalls = 0, service, handle, closing

class FixedProvider extends LlmAdapter {
  async resolveModel(provider, id) { return { provider, id, name: id } }
  async *stream(options) {
    let release
    const waiting = new Promise(resolve => { release = resolve })
    const abort = () => { providerAborted = true; release() }
    options.signal.addEventListener('abort', abort, { once: true })
    try {
      yield { type: 'text-delta', index: 0, text: `${label} synthetic waiting` }
      if (options.signal.aborted) abort()
      await waiting
    } finally { options.signal.removeEventListener('abort', abort) }
  }
}

async function waitFor(predicate) {
  const deadline = Date.now() + 15_000
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('native process fixture startup deadline')
    await delay(10)
  }
}

async function state() {
  const snapshot = await service.snapshot()
  const source = snapshot.sources.find(value => value.key === snapshot.adapter.sourceKey)
  const task = snapshot.tasks.find(value => value.sourceKey === source?.key && value.id === handle.agent.id)
  return { pid: process.pid, sourceKey: source?.key, hostRunId: source?.descriptor.hostRunId,
    registrationId: source?.descriptor.registrationId, profile: source?.descriptor.profile, label,
    taskId: handle.agent.id, lifecycleId: task?.lifecycleId, actorStatus: handle.agent.status,
    providerAborted, toolCalls, adapter: service.diagnostics() }
}

async function close() {
  if (closing) return closing
  closing = (async () => {
    const startedAt = Date.now(), result = await service?.close()
    const actorStatusAfterAdapter = handle?.agent.status
    await handle?.dispose()
    await ctx.fiber.dispose()
    return { result, actorStatusAfterAdapter, elapsedMs: Date.now() - startedAt, adapter: service?.diagnostics() }
  })()
  return closing
}
process.on('disconnect', () => { void close().catch(() => { process.exitCode = 1 }) })
process.once('SIGTERM', () => { void close().then(() => { if (process.connected) process.disconnect() }, () => { process.exitCode = 1 }) })

try {
  await ctx.plugin(LlmRuntime)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) {
    await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  }
  ctx.llm.registerAdapter(['sentinel-process-fixture'], new FixedProvider())
  ctx.tools.register({ name: 'process_fixture_tool', description: 'Fixed synthetic output; no side effects', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
    execute() { return `${label}:tool-${++toolCalls}` } })
  service = createAdapter(ctx, { dataDir })
  await waitFor(() => service.connected)
  handle = await ctx.agents.create({ sessionId: 'same-native-agent', agentOptions: { provider: 'sentinel-process-fixture', model: 'fixed' } })
  handle.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: `${label} synthetic process lifecycle request` }] }))
  await waitFor(() => handle.agent.status === 'running')
  let initial
  const deadline = Date.now() + 10_000
  while (!(initial = await state()).lifecycleId) {
    if (Date.now() >= deadline) throw new Error('native fixture task did not reach the shared core')
    await delay(20)
  }
  process.send({ type: 'ready', value: initial })
  process.on('message', async message => {
    if (!Number.isSafeInteger(message?.id)) return
    try {
      let value
      if (message.method === 'state') value = await state()
      else if (message.method === 'tool') {
        const controller = new AbortController()
        const result = await ctx.tools.execute({ callId: String(message.callId), name: 'process_fixture_tool', arguments: {}, agent: handle.agent, signal: controller.signal })
        value = { value: result.value, isError: result.isError, signalAborted: controller.signal.aborted, actorStatus: handle.agent.status, providerAborted }
      } else if (message.method === 'close') value = await close()
      else throw new Error('unknown process fixture method')
      if (process.connected) process.send({ id: message.id, value }, message.method === 'close' ? () => process.disconnect() : undefined)
    } catch (error) { if (process.connected) process.send({ id: message.id, error: String(error.message).slice(0, 200) }) }
  })
} catch (error) {
  if (process.connected) process.send({ type: 'fatal', error: String(error.message).slice(0, 200) })
  await close().catch(() => {})
  process.exitCode = 1
  if (process.connected) process.disconnect()
}
