import { req } from './dsh-native-runtime.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createAdapter } from '../dsh/host.mjs'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { GS_SUMMARY_FEATURE } from '../src/protocol.mjs'
import descriptors from '../dsh/remote.cjs'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const { Context } = req('@deepseek-ai/cordis')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(name => req(`@deepseek-ai/${name}`).default)
const until = async predicate => {
  const deadline = Date.now() + 5000
  while (!predicate()) {
    assert.ok(Date.now() < deadline, 'resource fixture did not reach its expected state')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

async function host(t) {
  const ctx = new Context()
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  t.after(() => ctx.fiber.dispose())
  return ctx
}

function transport() {
  const records = { sources: [], batches: [], unregisters: [], timeline: [], closed: 0 }
  const connect = async () => {
    const index = records.sources.length + 1
    let closed = false
    return {
      async registerSource(descriptor) { records.sources.push(descriptor); records.timeline.push(`register-${index}`); return 'stable-source' },
      async heartbeat() { if (closed) throw new Error('closed') },
      async ingest(sourceKey, batch) { if (closed) throw new Error('closed'); records.batches.push(structuredClone(batch)); return { accepted: batch.events.length } },
      async unregisterSource(sourceKey) { records.unregisters.push(sourceKey); records.timeline.push(`unregister-${index}`) },
      async snapshot() { return { protocol: 1, monitoring: 'active', sources: [], tasks: [] } },
      async cancelPending() {},
      async close() { if (!closed) records.closed++; closed = true },
    }
  }
  return { records, connect }
}

test('100 real native task/tool lifecycles and adapter replacements return owned resource counts to zero', async t => {
  const ctx = await host(t), io = transport(), samples = []
  const parentWaitAuditEvents = ['subagent/wait-start', 'subagent/wait-targets', 'subagent/wait-end', 'subagent/wait-gap', 'subagent/wait-caller-start', 'subagent/wait-caller-end']
  let actualCalls = 0
  ctx.tools.register({ name: 'resource_fixed_tool', description: 'Fixed output without side effects', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] }, execute() { actualCalls++; return 'fixed-result' } })
  for (let i = 0; i < 100; i++) {
    const service = createAdapter(ctx, { registrationId: 'one-host' }, { connect: io.connect, flushMs: 1, heartbeatMs: 1000 })
    await until(() => service.connected && !service.diagnostics().adapter.tickInFlight)
    const counts = service.diagnostics().adapter
    assert.equal(counts.timers, 1); assert.equal(counts.handlers, 12 + parentWaitAuditEvents.length, 'original 12 observer hooks plus six named native wait audit hooks'); assert.equal(counts.subscriptions, 1)
    const handle = await ctx.agents.create({ sessionId: `resource-task-${i}`, agentOptions: {} })
    const signal = new AbortController().signal
    const result = await ctx.tools.execute({ callId: `resource-call-${i}`, name: 'resource_fixed_tool', arguments: {}, agent: handle.agent, signal })
    assert.equal(result.value, 'fixed-result'); assert.equal(signal.aborted, false)
    await handle.dispose()
    await until(() => service.diagnostics().collector.taskRows === 0 && service.diagnostics().adapter.sentTasks === 0)
    await service.close(); await service.close()
    const closed = service.diagnostics()
    assert.deepEqual(closed.adapter, { closed: true, connected: false, timers: 0, activeRequests: 0,
      flushTimers: 0, shutdownTimers: 0,
      handlers: 0, subscriptions: 0, sentTasks: 0, sentGaps: 0, tickInFlight: false,
      shutdownStatus: 'closed', shutdownTimedOut: false, shutdownFailures: 0, lateSettledRequests: 0 })
    assert.equal(closed.collector.toolDedupeEntries, 0); assert.equal(closed.collector.pendingTools, 0)
    for (const field of ['hierarchyRows', 'hierarchyEvidence', 'hierarchyRelations', 'hierarchyStates', 'hierarchyInputs', 'hierarchyCatalogs', 'hierarchyRuns', 'hierarchyWaits', 'hierarchyWaitTargets', 'hierarchyWaitGaps']) assert.equal(closed.collector[field], 0)
    if (i % 10 === 0 || i === 99) {
      global.gc?.()
      const system = process.platform === 'darwin' ? JSON.parse(execFileSync('python3', [fileURLToPath(new URL('../scripts/memory-footprint.py', import.meta.url)), String(process.pid)], { encoding: 'utf8', timeout: 2000 })) : { metric: 'unavailable' }
      if (process.platform === 'darwin') assert.ok(system.physicalFootprint > 0)
      samples.push({ completedLifecycles: i + 1, at: Date.now(), memory: process.memoryUsage(), system, diagnostics: closed })
    }
  }
  assert.equal(actualCalls, 100)
  const results = io.records.batches.flatMap(batch => batch.events).filter(event => event.kind === 'tool-result' && event.data.toolName === 'resource_fixed_tool')
  assert.equal(results.length, 100); assert.equal(new Set(results.map(event => event.id)).size, 100)
  assert.equal(io.records.sources.length, 100); assert.equal(io.records.unregisters.length, 100); assert.equal(io.records.closed, 100)
  assert.equal(new Set(io.records.sources.map(source => source.hostRunId)).size, 1)
  if (process.env.SENTINEL_ADAPTER_RESOURCE_REPORT) await writeFile(process.env.SENTINEL_ADAPTER_RESOURCE_REPORT, JSON.stringify({ runtime: process.version, pid: process.pid, gcBeforeSample: !!global.gc, scope: '100 actual native task/tool lifecycles and adapter replacement; no model/installed Host/OS UI', actualCalls, observerHooks: { original: 12, parentWaitAuditEvents, total: 18 }, samples, memoryAssessment: 'continuous measurements; no formal whole-Host memory budget inferred' }, null, 2))
})

test('repeated apply in the same native Host stops the old hooks before replacement and orders stable-key unregister first', async t => {
  const ctx = await host(t), io = transport()
  const first = createAdapter(ctx, { registrationId: 'stable' }, { connect: io.connect, flushMs: 1 })
  await until(() => first.connected)
  const second = createAdapter(ctx, { registrationId: 'stable' }, { connect: io.connect, flushMs: 1 })
  assert.equal(first.diagnostics().adapter.flushTimers, 0); assert.equal(first.diagnostics().adapter.handlers, 0)
  await until(() => second.connected)
  assert.deepEqual(io.records.timeline.slice(0, 3), ['register-1', 'unregister-1', 'register-2'])
  await first.close()
  assert.equal(io.records.unregisters.length, 1); assert.equal(second.connected, true)
  await second.close(); assert.equal(io.records.unregisters.length, 2)
})

test('closing during core bootstrap aborts only its own startup and leaves the native Host untouched', async t => {
  const ctx = await host(t)
  const actor = await ctx.agents.create({ sessionId: 'bootstrap-original', agentOptions: {} })
  t.after(() => actor.dispose())
  let seenSignal
  const connect = ({ signal }) => new Promise((_, reject) => {
    seenSignal = signal
    signal.addEventListener('abort', () => reject(signal.reason), { once: true })
  })
  const service = createAdapter(ctx, {}, { connect, flushMs: 1 })
  await until(() => seenSignal)
  await service.close()
  assert.equal(seenSignal.aborted, true)
  assert.equal(actor.agent.status, 'idle')
  assert.equal(service.diagnostics().adapter.activeRequests, 0)
  assert.equal(service.diagnostics().adapter.handlers, 0)
})

test('an actual idle baseline and titles changing during or after its send reach the transport without retained idle indexes', async t => {
  const ctx = await host(t), io = transport()
  const actor = await ctx.agents.create({ sessionId: 'idle-live-title', agentOptions: {} })
  t.after(() => actor.dispose())
  ctx.sessionProjections.register({ key: 'title', stateVersion: 1, stateSchema: { parse: value => value }, init: () => 'baseline-title',
    apply: (value, event) => event.type === 'session/title' ? event.data.title : value,
    wire: { viewSchema: { parse: value => value }, view: value => value } })
  ctx.sessionProjections.snapshot(actor.agent.session, ['title'])
  actor.agent.session.snapshotEvents = () => { throw new Error('observer must not read history') }
  let release, firstSend = true
  const connect = async options => {
    const client = await io.connect(options), original = client.ingest
    client.ingest = async (...args) => {
      await original(...args)
      if (firstSend) { firstSend = false; await new Promise(resolve => { release = resolve }) }
    }
    return client
  }
  const service = createAdapter(ctx, {}, { connect, flushMs: 1 })
  await until(() => release)
  assert.equal(io.records.batches[0].tasks[0].title, 'baseline-title')
  const update = title => actor.agent.session.append('session/title', { title, messageSeqs: [], source: { kind: 'user' } })
  const firstTitle = update('changed-in-flight'); release()
  assert.equal(firstTitle.seq, 0)
  await until(() => io.records.batches.some(batch => batch.tasks.some(task => task.title === 'changed-in-flight')))
  await until(() => service.diagnostics().collector.taskRows === 0 && service.diagnostics().adapter.sentTasks === 0)
  update('changed-after-ack')
  await until(() => io.records.batches.some(batch => batch.tasks.some(task => task.title === 'changed-after-ack')))
  await until(() => service.diagnostics().collector.taskRows === 0 && service.diagnostics().adapter.sentTasks === 0)
  const snapshots = io.records.batches.flatMap(batch => batch.tasks)
  assert.equal(new Set(snapshots.map(task => task.lifecycleId)).size, 1)
  assert.equal(actor.agent.status, 'idle')
  await service.close()
})

test('a delayed final flush from the replaced adapter cannot unregister the newly connected stable source', async t => {
  const ctx = await host(t), pending = [], timeline = []
  let index = 0
  const connect = async () => {
    const own = ++index
    return {
      async registerSource() { timeline.push(`register-${own}`); return 'stable-source' },
      async heartbeat() {},
      async ingest() { if (own === 1) await new Promise(resolve => pending.push(resolve)) },
      async unregisterSource() { timeline.push(`unregister-${own}`) },
      async cancelPending() {}, async close() {},
    }
  }
  const first = createAdapter(ctx, {}, { connect, flushMs: 1, closeTimeoutMs: 25 })
  await until(() => pending.length === 1)
  const actor = await ctx.agents.create({ sessionId: 'delayed-original', agentOptions: {} })
  t.after(() => actor.dispose())
  const second = createAdapter(ctx, {}, { connect, flushMs: 1, closeTimeoutMs: 25 })
  await until(() => second.connected)
  assert.ok(pending.length >= 2)
  const cleanup = await first.close()
  assert.equal(cleanup.status, 'incomplete'); assert.equal(cleanup.timedOut, true)
  assert.ok(first.diagnostics().adapter.activeRequests >= 2)
  pending.splice(0).forEach(resolve => resolve())
  await new Promise(resolve => setTimeout(resolve, 10))
  assert.equal(first.diagnostics().adapter.activeRequests, 0)
  assert.equal(first.diagnostics().adapter.shutdownStatus, 'incomplete')
  assert.equal(first.diagnostics().adapter.shutdownTimedOut, true)
  assert.ok(first.diagnostics().adapter.lateSettledRequests >= 2)
  assert.ok(!timeline.includes('unregister-1'))
  assert.equal(second.connected, true); assert.equal(actor.agent.status, 'idle')
  await second.close()
})

test('closing during an in-flight source registration aborts that request and closes its client before assignment', async t => {
  const ctx = await host(t)
  let registeredSignal, registrationStarted = false, clientClosed = false
  const connect = async () => ({
    registerSource(_descriptor, signal) {
      registeredSignal = signal
      registrationStarted = true
      return new Promise((_, reject) => signal?.addEventListener('abort', () => reject(signal.reason), { once: true }))
    },
    async cancelPending() {}, async close() { clientClosed = true },
  })
  const service = createAdapter(ctx, {}, { connect, flushMs: 1, closeTimeoutMs: 25 })
  await until(() => registrationStarted)
  const result = await service.close()
  assert.equal(registeredSignal.aborted, true); assert.equal(clientClosed, true)
  assert.equal(result.status, 'closed'); assert.equal(result.timedOut, false)
  assert.equal(service.diagnostics().adapter.activeRequests, 0)
})

test('real HTTP shutdown uses a fresh unregister budget and keeps another source and the native actor alive', async t => {
  const ctx = await host(t), dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-source-close-'))
  const server = await startServer({ dataDir }), other = await connectCore({ dataDir })
  const otherKey = await other.registerSource({ registrationId: 'other', hostRunId: 'other-host', name: 'Other source' })
  const actor = await ctx.agents.create({ sessionId: 'source-close-original', agentOptions: {} })
  const service = createAdapter(ctx, { registrationId: 'stable-native-source', dataDir }, { connect: connectCore, flushMs: 1 })
  t.after(async () => {
    await service.close(); await actor.dispose(); await other.unregisterSource(otherKey, 'test cleanup')
    await other.close(); await server.close(); await rm(dataDir, { recursive: true, force: true })
  })
  await until(() => service.connected)
  const sourceKey = (await service.snapshot()).adapter.sourceKey
  const result = await service.close()
  assert.equal(result.status, 'closed'); assert.equal(result.timedOut, false)
  assert.equal(result.pendingRequests, 0); assert.equal(result.tickInFlight, false)
  const state = await other.snapshot()
  assert.equal(state.sources.find(source => source.key === sourceKey).status, 'disconnected')
  assert.equal(state.sources.find(source => source.key === otherKey).status, 'connected')
  assert.equal(actor.agent.status, 'idle')
  assert.equal(service.diagnostics().adapter.shutdownStatus, 'closed')
})

test('GS summary binds its own source and a legacy core returns an explicit gap without full snapshot reads', async t => {
  const ctx = await host(t), io = transport(), calls = []
  let healthCalls = 0, snapshots = 0
  const connect = async () => {
    const client = await io.connect()
    client.health = async () => { healthCalls++; return { features: [GS_SUMMARY_FEATURE] } }
    client.gsSummary = async (...args) => { calls.push(args); return { protocol: 1, generatedAt: Date.now(), sourceKey: args[0], sessionId: args[1], sourceStatus: 'connected', scopeKnown: true, counts: { green: 0, yellow: 0, red: 0, gray: 1 }, coverage: { complete: false, gaps: ['fixture scope gap'] }, gsStatus: { supported: true } } }
    client.snapshot = async () => { snapshots++; throw new Error('summary must not use the full snapshot') }
    return client
  }
  const service = createAdapter(ctx, { registrationId: 'summary-source' }, { connect, flushMs: 1 })
  t.after(() => service.close())
  await until(() => service.connected && !service.diagnostics().adapter.tickInFlight)
  const controller = new AbortController(), summary = await service.gsSummary('opaque/session', controller.signal)
  assert.equal(calls[0][0], 'stable-source')
  assert.equal(calls[0][1], 'opaque/session')
  assert.equal(calls[0][2].aborted, false)
  controller.abort()
  assert.equal(calls[0][2].aborted, true)
  assert.equal(summary.sourceKey, 'stable-source')
  assert.equal(summary.sessionId, 'opaque/session')
  assert.equal(snapshots, 0)
  assert.equal(healthCalls, 1)
  await service.close()
  const legacy = createAdapter(ctx, {}, { connect: async () => { const client = await io.connect(); client.health = async () => { healthCalls++; return { features: [] } }; client.snapshot = async () => { snapshots++; throw new Error('legacy summary must not use the full snapshot') }; return client }, flushMs: 1 })
  t.after(() => legacy.close())
  await until(() => legacy.connected && !legacy.diagnostics().adapter.tickInFlight)
  const unsupported = await legacy.gsSummary('opaque/session')
  assert.equal(unsupported.scopeKnown, false)
  assert.equal(unsupported.generatedAt, null, 'unsupported counts have no authoritative core read time')
  assert.equal(unsupported.gsStatus.supported, false)
  assert.equal(unsupported.coverage.complete, false)
  assert.ok(unsupported.coverage.gaps.some(gap => gap.includes('计数摘要')))
  assert.deepEqual(unsupported.counts, { green: 0, yellow: 0, red: 0, gray: 0 })
  assert.equal(snapshots, 0)
  assert.equal(healthCalls, 2)
  const descriptor = descriptors().find(entry => entry.method === 'gsSummary')
  assert.deepEqual(descriptor.parameters.map(parameter => parameter.name), ['sessionId'])
  const parse = descriptor.parameters[0].codec.create().parse
  assert.equal(parse(' opaque/session '), ' opaque/session ')
  for (const invalid of ['', '\n', { sourceKey: 'other', sessionId: 'opaque/session' }]) assert.throws(() => parse(invalid))
})
