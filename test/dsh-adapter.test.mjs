import { req } from './dsh-native-runtime.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { createAdapter } from '../dsh/host.mjs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(name => req(`@deepseek-ai/${name}`).default)
const until = async predicate => {
  const deadline = Date.now() + 3000
  while (!predicate()) {
    assert.ok(Date.now() < deadline, 'native adapter did not reach expected state')
    await new Promise(resolve => setTimeout(resolve, 5))
  }
}

async function fixture(t, transport = {}) {
  const ctx = new Context(), streams = new Map(), handles = []
  const records = { sources: [], batches: [], unregistered: [] }
  let settings = { registrationId: 'fixture', config: {}, revision: 'initial' }, monitoring = { registrationId: 'fixture', paused: false, revision: 'initial-monitoring' }
  let fail = false
  const connector = async () => ({
    async registerSource(source) { if (fail) throw new Error('offline'); records.sources.push(source); return `source-${records.sources.length}` },
    async heartbeat() { if (fail) throw new Error('offline') },
    async ingest(sourceKey, batch) { if (fail) throw new Error('offline'); records.batches.push({ sourceKey, ...structuredClone(batch) }); return { accepted: batch.events.length } },
    async unregisterSource(key) { records.unregistered.push(key) },
    async snapshot() { return { protocol: 1, generatedAt: Date.now(), monitoring: 'active', overview: {}, sources: [], tasks: [] } },
    async claimNotifications() { return [] }, async ackNotification() {}, async close() {},
    async installationAnalysisConfig() { return settings }, async configureInstallationAnalysis(registrationId, config, revision) { assert.equal(revision, settings.revision); settings = { registrationId, config, revision: 'saved' }; return settings },
    async installationMonitoring() { return monitoring }, async setInstallationMonitoring(registrationId, paused) { monitoring = { registrationId, paused, revision: 'saved-monitoring' }; return monitoring },
  })
  class Adapter extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const stream = { chunks: [], done: false }
      streams.set(options.model, stream)
      const wake = () => { stream.done = true; stream.wake?.() }
      options.signal.addEventListener('abort', wake, { once: true })
      try {
        while (!stream.done) {
          if (!stream.chunks.length) await new Promise(resolve => { stream.wake = resolve })
          for (const chunk of stream.chunks.splice(0)) yield chunk
        }
      } finally { options.signal.removeEventListener('abort', wake) }
    }
  }
  await ctx.plugin(LlmRuntime)
  for (const mod of modules) await ctx.plugin(mod, { agents: [] })
  ctx.llm.registerAdapter(['sentinel-test'], new Adapter())
  let service, pluginHandle
  const install = async () => {
    pluginHandle = await ctx.plugin({ inject: ['agents'], apply(child) {
      service = createAdapter(child, { registrationId: transport.registrationId === undefined ? 'fixture' : transport.registrationId, dataDir: transport.dataDir || '/private/tmp/sentinel-adapter-test' }, { connect: transport.connect || connector, flushMs: 10, heartbeatMs: 25 })
      child.effect(() => () => service.close())
    } })
    if (transport.waitForConnection !== false) await until(() => service.connected)
    return service
  }
  t.after(async () => {
    fail = false
    for (const stream of streams.values()) { stream.done = true; stream.wake?.() }
    for (const handle of handles) await handle.dispose()
    await ctx.fiber.dispose()
  })
  return { ctx, records, streams, install,
    get service() { return service },
    offline(value) { fail = value },
    async remove() { await pluginHandle.dispose() },
    async create(id) {
      const handle = await ctx.agents.create({ sessionId: id, agentOptions: { provider: 'sentinel-test', model: id } })
      handles.push(handle)
      handle.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: `request-${id}` }] }))
      await until(() => streams.has(id))
      return handle
    },
    send(id, text) { const stream = streams.get(id); stream.chunks.push({ type: 'text-delta', index: 0, text }); stream.wake?.() },
  }
}

test('native events reach the observation transport while tool results and running agents remain unchanged', async t => {
  const f = await fixture(t), existing = await f.create('existing')
  await f.install()
  const newer = await f.create('newer')
  f.send('existing', 'alpha'); f.send('newer', 'beta')
  f.ctx.tools.register({ name: 'safe_tool', description: 'fixed no-side-effect tool', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] },
    execute() { return 'native-result' } })
  const controller = new AbortController()
  const result = await f.ctx.tools.execute({ callId: 'real-call', name: 'safe_tool', arguments: {}, agent: newer.agent, signal: controller.signal })
  assert.equal(result.value, 'native-result'); assert.equal(controller.signal.aborted, false)
  await until(() => f.records.batches.flatMap(batch => batch.events).some(event => event.text === 'native-result' && event.kind === 'tool-result'))
  const tasks = f.records.batches.flatMap(batch => batch.tasks), events = f.records.batches.flatMap(batch => batch.events)
  assert.ok(tasks.some(task => task.id === 'existing' && task.status === 'running'))
  assert.ok(events.some(event => event.kind === 'input' && event.text === 'request-newer'))
  assert.ok(events.filter(event => event.taskId === 'newer').every(event => event.text !== 'alpha'))
  assert.equal(existing.agent.status, 'running'); assert.equal(newer.agent.status, 'running')
  await f.remove()
  assert.equal(newer.agent.status, 'running'); assert.equal(f.records.unregistered.length, 1)
})

test('an existing core without hierarchy capability stays connected as flat monitoring and reports its explicit gap', async t => {
  const f = await fixture(t)
  await f.install(); await f.create('legacy-core-agent')
  await until(() => f.records.batches.some(batch => batch.tasks.some(task => task.id === 'legacy-core-agent')))
  const snapshot = await f.service.snapshot()
  assert.equal(snapshot.adapter.connected, true)
  assert.equal(snapshot.adapter.hierarchyContext.supported, false)
  assert.equal(snapshot.adapter.hierarchyContext.code, 'hierarchy_context_unsupported')
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes('当前仅平铺观察')))
  await f.remove()
  assert.ok(f.records.batches.every(batch => !Object.hasOwn(batch, 'hierarchyContext')))
  assert.equal(f.records.sources.length, 1, 'capability mismatch does not create another connection or core')
})

test('native gateway exposes only observer controls and JSON parameters reach analysis and notification methods', async t => {
  const f = await fixture(t)
  await f.ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await f.ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await f.ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  const { Peer } = req('@deepseek-ai/dsh-client-connection')
  const peer = new Peer(f.ctx, { principal: { kind: 'owner' }, interactions: 'none' })
  await f.install()
  const config = { llm: { baseUrl: 'http://127.0.0.1:54841', model: 'fake-model' } }
  const current = await f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'analysisConfigState', args: {} })
  const response = await f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'configureAnalysisIfCurrent', args: { config, revision: current.revision } })
  assert.deepEqual(response.config, config)
  await assert.rejects(f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'configureAnalysis', args: { config } }))
  const snapshot = await f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'snapshot', args: {} })
  assert.equal(snapshot.protocol, 1); assert.equal(snapshot.adapter.connected, true)
  await assert.rejects(f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'followup', args: {} }))
})

test('stable DSH installation settings remain available while collection cannot register and never import legacy model values', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-disconnected-settings-')), server = await startServer({ dataDir }), control = await connectCore({ dataDir })
  const currentInstance = (await control.health()).instanceId
  await control.configureAnalysis({ llm: { model: 'legacy-must-not-import' } })
  let activeClients = 0
  const f = await fixture(t, { dataDir, waitForConnection: false, connect: async options => {
    const connected = await connectCore(options), close = connected.close.bind(connected)
    activeClients++
    let closed = false
    connected.close = async () => { if (!closed) { closed = true; activeClients-- }; await close() }
    connected.registerSource = async () => { throw new Error('controlled collection registration unavailable') }
    return connected
  } })
  t.after(async () => { await f.service?.close(); await control.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  await f.install()
  const first = await f.service.analysisConfigState()
  assert.equal(first.registrationId, 'fixture'); assert.deepEqual(first.config, {})
  const saved = await f.service.configureAnalysisIfCurrent({ llm: { model: 'installation-only' } }, first.revision)
  assert.deepEqual(await f.service.analysisConfigState(), saved)
  assert.equal(f.service.connected, false)
  assert.deepEqual(await control.installationAnalysisConfig('fixture'), saved)
  assert.equal((await control.analysisConfig()).llm.model, 'legacy-must-not-import')
  assert.equal((await control.health()).instanceId, currentInstance, 'settings requests reuse the same shared Core')
  await f.service.close()
  assert.equal(activeClients, 0, 'all temporary settings and failed collection clients are closed')
})

test('a DSH Host without stable installation identity cannot read or save a temporary model scope', async t => {
  const f = await fixture(t, { registrationId: null })
  await f.install()
  await assert.rejects(f.service.analysisConfigState(), /稳定 registrationId.*身份未确认/)
  await assert.rejects(f.service.configureAnalysisIfCurrent({}, 'revision'), /稳定 registrationId.*身份未确认/)
  await assert.rejects(f.service.pause(), /稳定 registrationId.*身份未确认/)
})

test('native GS summary RPC reaches the authenticated shared core with adapter-owned source identity and no full snapshot fallback', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-summary-')), server = await startServer({ dataDir })
  let snapshotReads = 0, healthReads = 0
  const f = await fixture(t, { dataDir, connect: async options => {
    const client = await connectCore(options), snapshot = client.snapshot.bind(client), health = client.health.bind(client)
    client.snapshot = (...args) => { snapshotReads++; return snapshot(...args) }
    client.health = (...args) => { healthReads++; return health(...args) }
    return client
  } })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  await f.ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await f.ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await f.ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  const { Peer } = req('@deepseek-ai/dsh-client-connection')
  const peer = new Peer(f.ctx, { principal: { kind: 'owner' }, interactions: 'none' })
  await f.install()
  const actor = await f.create('native-summary-target')
  let summary
  for (let index = 0; index < 100; index++) {
    summary = await f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'gsSummary', args: { sessionId: actor.agent.session.id } })
    if (summary.scopeKnown && Object.values(summary.counts).reduce((sum, count) => sum + count, 0) === 1) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.equal(summary.scopeKnown, true)
  assert.equal(summary.gsStatus.supported, true)
  assert.equal(summary.sessionId, actor.agent.session.id)
  assert.ok(summary.sourceKey)
  assert.equal(Object.values(summary.counts).reduce((sum, count) => sum + count, 0), 1, 'the authoritative core counts exactly the one actual running actor')
  assert.equal(snapshotReads, 0)
  assert.equal(healthReads, 1)
  assert.ok(!JSON.stringify(summary).includes('request-native-summary-target'))
  const unknown = await f.ctx.typertGateway.invoke({ peer, namespace: 'sentinel', method: 'gsSummary', args: { sessionId: 'another-source-only' } })
  assert.equal(unknown.sourceKey, summary.sourceKey)
  assert.equal(unknown.scopeKnown, false)
  assert.equal(unknown.coverage.complete, false)
  assert.equal(actor.agent.status, 'running')
})

test('transport reconnect preserves captured time and Host identity; reloading the adapter does not restart existing task identity', async t => {
  const f = await fixture(t)
  await f.install(); await f.create('reload-existing')
  await until(() => f.records.batches.some(batch => batch.tasks.some(task => task.id === 'reload-existing')))
  const firstSource = f.records.sources[0], firstTask = f.records.batches.flatMap(batch => batch.tasks).find(task => task.id === 'reload-existing')
  f.offline(true); f.send('reload-existing', 'offline-evidence')
  const before = Date.now()
  await until(() => !f.service.connected)
  assert.equal((await f.service.snapshot()).adapter.bufferedEvents > 0, true)
  f.offline(false)
  await until(() => f.records.sources.length > 1 && f.records.batches.flatMap(batch => batch.events).some(event => event.text === 'offline-evidence'))
  const event = f.records.batches.flatMap(batch => batch.events).find(item => item.text === 'offline-evidence')
  assert.ok(event.capturedAt <= before + 100)
  assert.equal(f.records.sources[1].hostRunId, firstSource.hostRunId)
  await f.remove(); await f.install()
  await until(() => f.records.batches.some(batch => batch.sourceKey === 'source-3'))
  const current = f.records.batches.filter(batch => batch.sourceKey === 'source-3').flatMap(batch => batch.tasks).find(task => task.id === 'reload-existing')
  assert.equal(current.lifecycleId, firstTask.lifecycleId)
  assert.equal(f.records.sources[2].hostRunId, firstSource.hostRunId)
  assert.ok(current.gaps.some(reason => reason.includes('断档')))
})

test('independent native Hosts with identical local Agent IDs remain separate sources and lifecycles', async t => {
  const a = await fixture(t), b = await fixture(t)
  await a.install(); await b.install(); await a.create('same-id'); await b.create('same-id')
  a.send('same-id', 'host-a'); b.send('same-id', 'host-b')
  await until(() => a.records.batches.flatMap(batch => batch.events).some(event => event.text === 'host-a') && b.records.batches.flatMap(batch => batch.events).some(event => event.text === 'host-b'))
  assert.notEqual(a.records.sources[0].hostRunId, b.records.sources[0].hostRunId)
  const ar = a.records.batches.flatMap(batch => batch.tasks).find(task => task.id === 'same-id')
  const br = b.records.batches.flatMap(batch => batch.tasks).find(task => task.id === 'same-id')
  assert.notEqual(ar.lifecycleId, br.lifecycleId)
  assert.ok(a.records.batches.flatMap(batch => batch.events).every(event => event.text !== 'host-b'))
})

test('actual native Host events cross authenticated core HTTP and appear in the persisted monitoring snapshot', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-dsh-http-'))
  const server = await startServer({ dataDir })
  const f = await fixture(t, { dataDir, connect: connectCore })
  t.after(async () => { await f.remove(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  await f.install(); await f.create('http-task'); f.send('http-task', '真实采集 HTTP 摘录')
  f.ctx.tools.register({ name: 'http_safe_tool', description: 'fixed output', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] }, execute() { return 'http-tool-result' } })
  const agent = f.ctx.agents.get('http-task')
  const result = await f.ctx.tools.execute({ callId: 'http-call', name: 'http_safe_tool', arguments: {}, agent, signal: new AbortController().signal })
  assert.equal(result.value, 'http-tool-result')
  let state
  const deadline = Date.now() + 3000
  do { state = await f.service.snapshot(); if (state.tasks.some(task => task.events.some(event => event.text === 'http-tool-result'))) break; await new Promise(resolve => setTimeout(resolve, 10)) } while (Date.now() < deadline)
  const task = state.tasks.find(task => task.id === 'http-task')
  assert.ok(task, state.adapter.error || 'HTTP task missing')
  assert.ok(task.events.some(event => event.text === '真实采集 HTTP 摘录'))
  assert.ok(task.events.some(event => event.kind === 'input' && event.text === 'request-http-task'))
  assert.ok(task.events.some(event => event.kind === 'tool-result' && event.data.callId === 'http-call' && event.text === 'http-tool-result'))
  assert.equal(typeof task.events[0].evidence, 'string')
  assert.equal(state.overview.runningTasks, 1); assert.equal(state.overview.coverage.complete, false)
  assert.equal(state.adapter.connected, true)
  assert.equal(task.status, 'running'); assert.equal(agent.status, 'running')
})

test('credential-bearing native tool observations are omitted before excerpting while actual tool execution remains intact', async t => {
  const f = await fixture(t)
  await f.install(); const handle = await f.create('credential-task')
  f.ctx.tools.register({ name: 'credential_safe_tool', description: 'fixed no-side-effect output', parameters: {},
    output: { schema: { type: 'string' }, render: (_args, value) => [{ type: 'text', text: value }] }, execute() { return 'secret-derived-output' } })
  const command = `${'prefix '.repeat(40)}curl -H 'Authorization: Bearer dsh-test-secret' http://127.0.0.1`
  const result = await f.ctx.tools.execute({ callId: 'credential-call', name: 'credential_safe_tool', arguments: { command },
    agent: handle.agent, signal: new AbortController().signal })
  assert.equal(result.value, 'secret-derived-output')
  await until(() => f.records.batches.flatMap(batch => batch.events).some(event => event.kind === 'tool-result' && event.data.callId === 'credential-call'))
  const events = f.records.batches.flatMap(batch => batch.events).filter(event => event.data.callId === 'credential-call')
  assert.ok(events.length >= 2)
  assert.ok(events.every(event => event.text === '' && event.data.redacted === true && event.data.command === undefined))
  assert.ok(!JSON.stringify(f.records).includes('dsh-test-secret'))
  assert.ok(!JSON.stringify(f.records).includes('secret-derived-output'))
  assert.ok(f.records.batches.flatMap(batch => batch.tasks).some(task => task.id === 'credential-task' && task.gaps.some(gap => gap.includes('凭据'))))
  assert.equal(handle.agent.status, 'running')
})

test('native cached title projection supplies task names without requesting session history', async t => {
  const f = await fixture(t), handle = await f.create('named-task')
  f.ctx.sessionProjections.register({ key: 'title', stateVersion: 1, stateSchema: { parse: value => value }, init: () => '公开缓存任务名称',
    apply: state => state, wire: { viewSchema: { parse: value => value }, view: state => state } })
  // The title provider, rather than the observer, materializes its existing cache.
  f.ctx.sessionProjections.snapshot(handle.agent.session, ['title'])
  handle.agent.session.snapshotEvents = () => { throw new Error('observer must not read session history') }
  await f.install()
  await until(() => f.records.batches.some(batch => batch.tasks.some(task => task.id === 'named-task')))
  const task = f.records.batches.flatMap(batch => batch.tasks).find(item => item.id === 'named-task')
  assert.equal(task.title, '公开缓存任务名称')
  assert.equal(handle.agent.status, 'running')
})

test('sensitive cached and live titles retain explicit omission gaps while normal titles and native task execution remain intact', async t => {
  const f = await fixture(t), initial = await f.create('title-initial'), live = await f.create('title-live')
  f.ctx.sessionProjections.register({ key: 'title', stateVersion: 1, stateSchema: { parse: value => value },
    init: () => 'Authorization: Bearer initial-title-secret',
    apply: (state, event) => event.type === 'session/title' ? event.data.title : state,
    wire: { viewSchema: { parse: value => value }, view: state => state } })
  live.agent.session.append('session/title', { title: '正常公开标题', messageSeqs: [], source: { kind: 'user' } })
  for (const handle of [initial, live]) f.ctx.sessionProjections.snapshot(handle.agent.session, ['title'])
  await f.install()
  await until(() => f.records.batches.some(batch => batch.tasks.length === 2))
  const first = f.records.batches.flatMap(batch => batch.tasks)
  assert.ok(first.find(task => task.id === 'title-initial').gaps.includes('任务名称中认证材料已省略'))
  assert.equal(first.find(task => task.id === 'title-live').title, '正常公开标题')
  assert.ok(!first.find(task => task.id === 'title-live').gaps.includes('任务名称中认证材料已省略'))
  live.agent.session.append('session/title', { title: 'Authorization: Bearer event-title-secret', messageSeqs: [], source: { kind: 'user' } })
  await until(() => f.records.batches.flatMap(batch => batch.tasks).some(task => task.id === 'title-live' && task.gaps.includes('任务名称中认证材料已省略')))
  live.agent.session.append('session/title', { title: '修订后的公开标题', messageSeqs: [], source: { kind: 'user' } })
  await until(() => f.records.batches.flatMap(batch => batch.tasks).some(task => task.id === 'title-live' && task.title === '修订后的公开标题'))
  assert.ok(!JSON.stringify(f.records).includes('initial-title-secret')); assert.ok(!JSON.stringify(f.records).includes('event-title-secret'))
  assert.equal(initial.agent.status, 'running'); assert.equal(live.agent.status, 'running')
})
