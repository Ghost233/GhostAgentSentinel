import test from 'node:test'
import assert from 'node:assert/strict'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { normalizeBatch } from '../src/protocol.mjs'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { startServer } from '../src/server.mjs'
import { mkdtemp, rm, readFile, writeFile, access } from 'node:fs/promises'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
import { tmpdir } from 'node:os'

const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage, createToolResultMessage } = req('@deepseek-ai/dsh-llm')
const nativeModules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(name => req(`@deepseek-ai/${name}`).default)
const until = async predicate => {
  const deadline = Date.now() + 3000
  while (!await predicate()) {
    assert.ok(Date.now() < deadline, 'native canonical fixture did not reach expected state')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

async function actualToolFixture(t, ptc, count = 1, { allocated, prepare, releaseServer = server => server?.close(), reportCleanup } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-canonical-native-'))
  const committed = [], executions = []
  let server, ctx, handle, preparationError
  t.after(async () => {
    const failures = []
    for (const release of [() => handle?.dispose(), () => ctx?.fiber.dispose(), () => releaseServer(server), () => rm(dataDir, { recursive: true, force: true })]) {
      try { await release() } catch (error) { failures.push(error) }
    }
    await reportCleanup?.({ failures, preparationError })
    if (failures.length) throw new AggregateError(failures, 'native canonical cleanup failed', { cause: preparationError })
  })
  try {
    server = await startServer({ dataDir }); ctx = new Context()
    await allocated?.({ dataDir, url: server.url })
    await ctx.plugin(LlmRuntime)
    await prepare?.(ctx)
    for (const plugin of nativeModules) await ctx.plugin(plugin, { agents: [] })
    if (ptc) {
      for (const name of ['dsh-fs-local', 'dsh-subprocess-local', 'dsh-sandbox-local']) await ctx.plugin(req(`@deepseek-ai/${name}`).default)
      await ctx.plugin(req('@deepseek-ai/dsh-working-directory').default, { defaultDirectory: '/private/tmp' })
      await ctx.plugin(req('@deepseek-ai/dsh-sandbox-policy').default, { mode: 'danger-full-access', workspaceRoot: '/private/tmp' })
      await ctx.plugin(req('@deepseek-ai/dsh-ptc-runtime-node').default, { timeoutMs: 20000, maxTimeoutMs: 20000 })
    }
    const id = ptc ? 'canonical-native-ptc' : 'canonical-native-ordinary'
    const callIds = Array.from({ length: count }, (_, index) => `${id}-call${count === 1 ? '' : `-${index + 1}`}`), callId = callIds[0]
    class SyntheticAdapter extends LlmAdapter {
      async resolveModel(provider, model) { return { provider, id: model, name: model } }
      async *stream(options) {
        if (callIds.every(id => options.messages.some(message => message.role === 'tool' && message.toolCallId === id))) {
          yield { type: 'text-delta', index: 0, text: 'fixed native task completed' }
          yield { type: 'finish', reason: { kind: 'stop' } }; return
        }
        const name = ptc ? 'run_code' : 'sentinel_canonical_tool'
        const args = ptc ? { description: 'fixed native canonical fixture', code: 'return await tools.sentinel_canonical_tool({})', timeoutMs: 20000 } : {}
        for (const [index, id] of callIds.entries()) {
          yield { type: 'block-start', index, blockType: 'tool-call' }
          yield { type: 'tool-call-delta', index, id, name, argumentsDelta: JSON.stringify(args) }
          yield { type: 'block-end', index, block: { type: 'tool-call', id, name, arguments: JSON.stringify(args) } }
        }
        yield { type: 'finish', reason: { kind: 'tool-calls' } }
      }
    }
    ctx.llm.registerAdapter(['canonical-native-fixture'], new SyntheticAdapter())
    ctx.on('session/event', (session, event) => { if (handle?.agent.session === session && event.type === 'tool/result') committed.push(event) })
    ctx.on('tools/result', (exec, result) => { if (exec.agent === handle?.agent) executions.push({ callId: exec.callId, isError: result.isError }) })
    await ctx.plugin({ inject: ['agents'], apply(child) { createAdapter(child, { dataDir, registrationId: 'canonical-native-fixture' }, { flushMs: 10, heartbeatMs: 100 }) } })
    await until(async () => (await ctx.sentinel.snapshot()).adapter.connected)
    handle = await ctx.agents.create({ sessionId: id, meta: { cwd: '/private/tmp' }, agentOptions: { provider: 'canonical-native-fixture', model: id }, setup(agentCtx) {
      agentCtx.tools.presentAs(ptc ? 'ptc' : 'native')
      agentCtx.tools.register({ name: 'sentinel_canonical_tool', description: 'fixed canonical fixture; no I/O', parameters: {},
        output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] }, execute() { return 'fixed native tool result' } })
      agentCtx.tools.restrict({ allow: [] })
    } })
    handle.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'fixed native canonical fixture input' }] }))
    await handle.agent.whenIdle()
    let task
    await until(async () => { task = (await ctx.sentinel.snapshot()).tasks.find(task => task.id === id); return task?.status === 'idle' })
    return { task, callId, callIds, committed, executions }
  } catch (error) { preparationError = error; throw error }
}

const foreground = exitCode => ({ kind: 'foreground', cwd: '/private/tmp', exitCode, signal: null, timedOut: false, aborted: false,
  timeoutMs: 2000, stdout: { text: 'fixed result', truncated: false }, stderr: { text: '', truncated: false } })

test('native frozen process facts cross the real public protocol and refine a durable fallback without renewing its first capture', () => {
  let clock = Date.now()
  const collector = createObservation({ identity: hostIdentity({}), now: () => clock })
  const agent = { id: 'a', status: 'running', session: { id: 's', header: {} } }
  collector.observe('agent/created', { agent })
  const exec = { name: 'bash', callId: 'call', token: Symbol(), arguments: { command: 'exit 7', workdir: '/private/tmp' }, agent }
  collector.observe('tools/execute', { exec })
  collector.observe('session/event', { session: agent.session, event: { seq: 1, time: clock, type: 'tool/result', data: { message: createToolResultMessage({ callId: 'call', isError: false, content: [] }), meta: { cwd: '/private/tmp' } } } })
  const first = collector.batch()
  const fallback = first.events.find(event => event.kind === 'tool-result')
  collector.acknowledge(first.events.map(event => event.id), first.tasks)
  clock += 200
  collector.observe('tools/result', { exec, result: Object.freeze({ isError: false, value: foreground(7), content: [{ type: 'text', text: 'fixed result' }] }) })
  const normalized = normalizeBatch(collector.batch())
  const canonical = normalized.events.find(event => event.kind === 'tool-result')
  assert.equal(canonical.data.exitCode, 7)
  assert.equal(canonical.data.cwd, '/private/tmp')
  assert.equal(canonical.data.resultSource, 'native-shell-canonical')
  assert.equal(canonical.data.refinesEventId, fallback.id)
  assert.equal(canonical.capturedAt, fallback.capturedAt)
  assert.notEqual(canonical.id, fallback.id)
  collector.observe('tools/result', { exec, result: Object.freeze({ isError: false, value: foreground(7), content: [] }) })
  assert.equal(collector.batch().events.filter(event => event.kind === 'tool-result').length, 1)
  collector.close()
})

for (const ptc of [false, true]) test(`native ${ptc ? 'PTC outer and inner calls' : 'ordinary call'} preserve real tool references without adding message IDs as result calls`, async t => {
  const f = await actualToolFixture(t, ptc)
  assert.equal(f.committed.length, 1)
  const message = f.committed[0].data.message
  assert.equal(message.toolCallId, f.callId)
  assert.notEqual(message.id, message.toolCallId, 'the public SDK message identity differs from its tool call reference')
  assert.ok(f.executions.every(result => result.isError === false))
  assert.equal(f.executions.length, ptc ? 2 : 1)
  const results = f.task.events.filter(event => event.kind === 'tool-result')
  const realCallIds = f.executions.map(result => result.callId).sort()
  assert.deepEqual(results.map(event => event.data.callId).sort(), realCallIds)
  assert.ok(!results.some(event => event.data.callId === message.id))
})

test('two actual native calls with identical result text remain two distinct calls without duplicate message identities', async t => {
  const f = await actualToolFixture(t, false, 2)
  const results = f.task.events.filter(event => event.kind === 'tool-result')
  assert.equal(f.committed.length, 2); assert.equal(f.executions.length, 2)
  assert.equal(new Set(results.map(event => event.text)).size, 1)
  assert.deepEqual(results.map(event => event.data.callId).sort(), ['canonical-native-ordinary-call-1', 'canonical-native-ordinary-call-2'])
})

test('durable result with only a message identity reports a gap instead of inventing a tool call reference', () => {
  const collector = createObservation({ identity: hostIdentity({}) })
  const agent = { id: 'missing-reference', status: 'running', session: { id: 'missing-reference-session', header: {} } }
  collector.observe('agent/created', { agent })
  collector.observe('session/event', { session: agent.session, event: { seq: 0, time: Date.now(), type: 'tool/result',
    data: { message: { id: 'message-only-id', role: 'tool', content: [{ type: 'text', text: 'fixed unlinked result' }], isError: false } } } })
  const batch = normalizeBatch(collector.batch())
  assert.equal(batch.events.filter(event => event.kind === 'tool-result').length, 0)
  assert.ok(batch.tasks[0].gaps.some(gap => gap.includes('真实工具调用引用')))
  collector.close()
})

if (process.env.SENTINEL_CANONICAL_FAILURE_CHILD) {
  test('canonical preparation child failure', async t => {
    await actualToolFixture(t, false, 1, {
      allocated: info => writeFile(process.env.SENTINEL_CANONICAL_FAILURE_STATE, JSON.stringify({ ...info, allocatedAt: Date.now() })),
      prepare: async ctx => {
        const path = process.env.SENTINEL_CANONICAL_FAILURE_STATE, info = JSON.parse(await readFile(path, 'utf8'))
        await writeFile(path, JSON.stringify({ ...info, preparationAt: Date.now() }))
        await ctx.plugin({ name: 'canonical-preparation-fault', apply() { throw new Error('original canonical preparation failure') } })
      },
      releaseServer: async server => {
        await server?.close()
        if (process.env.SENTINEL_CANONICAL_FAILURE_CHILD === 'cleanup-error') throw new Error('expected canonical cleanup failure')
      },
      reportCleanup: async ({ failures, preparationError }) => {
        const path = process.env.SENTINEL_CANONICAL_FAILURE_STATE, info = JSON.parse(await readFile(path, 'utf8'))
        await writeFile(path, JSON.stringify({ ...info, cleanupCompletedAt: Date.now(), cleanupErrors: failures.map(error => error.message), originalPreparationError: preparationError?.message }))
      },
    })
  })
} else for (const mode of ['setup-error', 'cleanup-error']) test(`native setup failure releases port and temporary data without replacing its original error: ${mode}`, async t => {
  const receiptDir = await mkdtemp(join(tmpdir(), 'sentinel-canonical-failure-receipt-')), receiptPath = join(receiptDir, 'resources.json')
  t.after(() => rm(receiptDir, { recursive: true, force: true }))
  const result = await new Promise(resolve => {
    const environment = { ...process.env, SENTINEL_CANONICAL_FAILURE_CHILD: mode, SENTINEL_CANONICAL_FAILURE_STATE: receiptPath }
    delete environment.NODE_TEST_CONTEXT
    const child = spawn(process.execPath, ['--test', '--test-name-pattern=canonical preparation child failure', fileURLToPath(import.meta.url)], {
      env: environment, stdio: ['ignore', 'pipe', 'pipe'],
    })
    let output = '', forced = false, startedAt = Date.now()
    child.stdout.on('data', chunk => { output += chunk }); child.stderr.on('data', chunk => { output += chunk })
    const timeout = setTimeout(() => { forced = true; child.kill('SIGTERM') }, 3000)
    child.once('close', (code, signal) => { clearTimeout(timeout); resolve({ code, signal, output, forced, startedAt, closedAt: Date.now() }) })
  })
  const info = JSON.parse(await readFile(receiptPath, 'utf8'))
  // Failure-path cleanup is tested before this emergency cleanup; forcing exit never counts as passing.
  t.after(() => rm(info.dataDir, { recursive: true, force: true }))
  const timing = { startedAt: result.startedAt, allocatedAt: info.allocatedAt, preparationAt: info.preparationAt,
    cleanupCompletedAt: info.cleanupCompletedAt, closedAt: result.closedAt }
  assert.equal(result.forced, false, `setup failure must exit naturally after its owned teardown, not require SIGTERM; phases=${JSON.stringify(timing)}`)
  assert.equal(result.signal, null); assert.equal(result.code, 1)
  assert.ok(result.output.includes('original canonical preparation failure'))
  assert.equal(info.originalPreparationError, 'original canonical preparation failure')
  if (mode === 'cleanup-error') assert.ok(info.cleanupErrors.includes('expected canonical cleanup failure'))
  else assert.deepEqual(info.cleanupErrors, [])
  await assert.rejects(access(info.dataDir))
  await assert.rejects(fetch(`${info.url}/v1/health`, { signal: AbortSignal.timeout(1000) }))
})
