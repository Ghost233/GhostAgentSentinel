import test from 'node:test'
import assert from 'node:assert/strict'
import { performance } from 'node:perf_hooks'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { req } from './dsh-native-runtime.mjs'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { startServer } from '../src/server.mjs'

const { Context } = req('@deepseek-ai/cordis')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-session-query'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
async function until(predicate, label) {
  const deadline = performance.now() + 3000
  while (!await predicate()) { assert.ok(performance.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 2)) }
}
async function fixture(t) {
  const ctx = new Context(), batches = [], handles = []
  let adapter
  t.after(async () => {
    const failures = []
    for (const release of [async () => { for (const handle of handles) await handle.dispose() }, async () => { if (adapter) assert.equal((await adapter.close()).status, 'closed') }, () => ctx.fiber.dispose()]) { try { await release() } catch (error) { failures.push(error) } }
    if (failures.length) throw new AggregateError(failures, 'native context retention cleanup failed')
  })
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  const connect = async () => ({
    async registerSource() { return 'memory-source' }, async heartbeat() {}, async health() { return { features: ['hierarchy-context/1'] } },
    async ingest(_sourceKey, batch) { batches.push(structuredClone(batch)); return { hierarchyContext: { schemaVersion: 1, acceptedRevision: batch.hierarchyContext.revision } } },
    async unregisterSource() {}, async snapshot() { return { protocol: 1, sources: [], tasks: [] } }, async cancelPending() {}, async close() {},
  })
  adapter = createAdapter(ctx, { registrationId: 'native-retention' }, { connect, flushMs: 1, heartbeatMs: 1000 })
  await until(() => adapter.connected && !adapter.diagnostics().adapter.tickInFlight, 'memory adapter connected')
  return { ctx, batches, adapter, async create(id) { const handle = await ctx.agents.create({ sessionId: id, agentOptions: { provider: 'unused', model: 'unused' } }); handles.push(handle); return handle } }
}

test('the long-lived native observer bounds and expires watermarks when read-only discovery sees already loaded Agents', async t => {
  const originalNow = Date.now
  let clock = originalNow()
  Date.now = () => clock
  t.after(() => { Date.now = originalNow })
  const f = await fixture(t)
  for (let index = 0; index < 505; index++) {
    const reads = f.adapter.diagnostics().hierarchyReader.reads
    const handle = await f.create(`watermark-${index}`)
    await until(() => f.adapter.diagnostics().hierarchyReader.reads > reads && !f.adapter.diagnostics().hierarchyReader.pending, 'native loaded Session observed')
    await handle.dispose()
    await until(() => f.adapter.diagnostics().collector.taskRows === 0, 'ended native row acknowledged')
  }
  assert.ok(f.adapter.diagnostics().collector.hierarchyObservedSessions <= 500)
  clock += 86_400_001
  assert.equal(f.adapter.diagnostics().collector.hierarchyObservedSessions, 0)
})

test('a no-Agent cold native title is sanitized before the adapter memory transport sees either its head or a credential after character 240', async t => {
  const f = await fixture(t), parent = await f.create('private-title-root')
  const headers = new Map()
  let pageReads = 0, legacyReads = 0
  f.ctx.provide('sessionPersistence', { identity: 'private-title-store', async stat(id) { return headers.has(id) ? { header: headers.get(id), revision: 'one' } : undefined },
    observationPageSupported: true,
    async readObservationPage(id) { pageReads++; return { header: headers.get(id), inheritedEventCount: 0, events: [], cursor: `title/${id}`, hasMore: false, complete: true,
      revision: 'title-one', validation: 'event-rows', usage: { directoryEntries: 0, physicalBytes: 128, decodedBytes: 256, events: 0, elapsedMs: 1 }, gaps: [] } },
    releaseObservationPageCursor() {},
    async open(id) { legacyReads++; return { id, header: headers.get(id), inheritedEventCount: 0, async read() { return { eventState: 'detached', events: [] } }, async close() {} } },
  })
  for (const [index, title] of ['Authorization: Bearer sentinel-test-secret', `${'x'.repeat(300)} Authorization: Bearer sentinel-test-secret`].entries()) {
    const id = `private-title-${index}`, header = { ...parent.agent.session.header, id, parentSession: parent.agent.session.id, origin: 'subagent', title }
    headers.set(id, header)
    parent.agent.session.append('subagent/catalog', { version: 0, childId: id, childCreatedAt: Date.now(), mode: 'one-shot' })
    let row
    await until(() => { row = f.batches.flatMap(batch => batch.tasks).find(task => task.sessionId === id); return row }, 'cold title reaches adapter memory transport')
    assert.ok(row.gaps.includes('任务名称中认证材料已省略'))
    assert.equal(row.title, '认证材料已省略，当前证据不完整')
    assert.ok(!JSON.stringify(f.batches).includes('sentinel-test-secret'))
    assert.equal(f.ctx.agents.get(id), undefined)
  }
  assert.equal(pageReads, 2)
  assert.equal(legacyReads, 0)
})


test('directory-only native sessions obey the history budget without evicting a loaded Agent and retire after retention', async t => {
  const f = await fixture(t), live = await f.create('history-live'), collector = createObservation({ identity: hostIdentity({}), hierarchy: true, maxEvents: 4, now: () => clock })
  let clock = Date.now()
  t.after(() => collector.close())
  collector.baseline([live.agent])
  for (let index = 0; index < 5; index++) {
    const session = f.ctx.sessions.create(`history-only-${index}`)
    collector.observe('session/observed', { sessionId: session.id, header: session.header, inheritedEventCount: 0, events: [], cursor: -1, readComplete: true, gaps: [] })
  }
  let snapshot = collector.batch()
  assert.equal(collector.diagnostics().hierarchyRows, 4)
  assert.equal(collector.diagnostics().hierarchyObservedSessions, 4)
  assert.equal(snapshot.tasks.filter(task => task.id.startsWith('observed-session/')).length, 4)
  assert.ok(snapshot.tasks.some(task => task.id === live.agent.id))
  assert.equal(snapshot.hierarchyContext.coverage.complete, false)
  assert.ok(snapshot.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'history-only-0'))
  clock += 86_400_001
  assert.equal(collector.diagnostics().hierarchyRows, 0)
  assert.equal(collector.diagnostics().hierarchyObservedSessions, 0)
  await live.dispose()
})

test('the history budget retains a grounded native directory and rejects facts from a branch it cannot admit', async t => {
  const f = await fixture(t), collector = createObservation({ identity: hostIdentity({}), hierarchy: true, maxEvents: 1 })
  t.after(() => collector.close())
  const publish = id => {
    const session = f.ctx.sessions.create(id)
    session.append('subagent/catalog', { version: 2, childId: `${id}-child`, childCreatedAt: Date.now(), mode: 'one-shot' })
    return collector.observe('session/observed', { sessionId: id, header: session.header, inheritedEventCount: 0, events: session.snapshotEvents(), cursor: 0, readComplete: true, gaps: [] })
  }
  assert.equal(publish('grounded-directory').complete, true)
  const denied = publish('budget-pressure')
  assert.equal(denied.complete, false)
  assert.equal(denied.acceptedThrough, -1)
  const batch = collector.batch()
  assert.equal(collector.diagnostics().hierarchyRows, 1)
  assert.equal(collector.diagnostics().hierarchyRelations, 1)
  assert.ok(batch.tasks.some(task => task.sessionId === 'grounded-directory'))
  assert.ok(!batch.events.some(event => event.taskId === 'observed-session/budget-pressure'))
  assert.ok(batch.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'budget-pressure'))
})

for (const failure of ['malformed', 'expired']) test(`a successful native tail page cannot erase a ${failure} catalog failure but a fresh matching catalog can repair the branch`, async t => {
  const originalNow = Date.now
  let clock = originalNow()
  Date.now = () => clock
  t.after(() => { Date.now = originalNow })
  const f = await fixture(t), session = f.ctx.sessions.create(`paged-catalog-gap-${failure}`), collector = createObservation({ identity: hostIdentity({}), hierarchy: true })
  t.after(() => collector.close())
  session.append('subagent/catalog', { version: failure === 'malformed' ? 99 : 2, childId: 'correctable-child', childCreatedAt: Date.now(), mode: 'one-shot' })
  for (let index = 1; index < 501; index++) session.append('session/title', { title: `ignored ${index}` })
  if (failure === 'expired') clock += 86_400_001
  const reason = failure === 'malformed' ? '目录格式无法核对' : '目录超过 24 小时'
  const publish = (start, end, complete) => collector.observe('session/observed', { sessionId: session.id, header: session.header, inheritedEventCount: 0, events: session.snapshotEvents(start, end), cursor: end - 1, readComplete: complete, gaps: [] })
  const first = publish(0, 500, false)
  assert.equal(first.acceptedThrough, 499)
  assert.equal(first.complete, false)
  assert.ok(first.gaps.some(gap => gap.includes(reason)))
  const tail = publish(500, 501, true)
  assert.equal(tail.acceptedThrough, 500)
  assert.equal(tail.complete, false)
  assert.ok(tail.gaps.some(gap => gap.includes(reason)))
  assert.equal(collector.batch().hierarchyContext.coverage.complete, false)
  session.append('subagent/catalog', { version: 2, childId: 'correctable-child', childCreatedAt: Date.now(), mode: 'one-shot' })
  const repaired = publish(501, 502, true)
  assert.equal(repaired.complete, true)
  assert.deepEqual(repaired.gaps, [])
})

test('a normal 501-event native directory becomes complete only after its final page', async t => {
  const f = await fixture(t), session = f.ctx.sessions.create('paged-catalog-complete'), collector = createObservation({ identity: hostIdentity({}), hierarchy: true })
  t.after(() => collector.close())
  for (let index = 0; index < 501; index++) session.append('session/title', { title: `ignored ${index}` })
  const publish = (start, end, complete) => collector.observe('session/observed', { sessionId: session.id, header: session.header, inheritedEventCount: 0, events: session.snapshotEvents(start, end), cursor: end - 1, readComplete: complete, gaps: [] })
  assert.deepEqual(publish(0, 500, false), { acceptedThrough: 499, complete: false, gaps: [] })
  assert.equal(collector.batch().hierarchyContext.coverage.complete, false)
  assert.deepEqual(publish(500, 501, true), { acceptedThrough: 500, complete: true, gaps: [] })
  assert.equal(collector.batch().hierarchyContext.coverage.complete, true)
})

test('three genuine native external runs retain a non-null budget gap accepted by the public HTTP core', async t => {
  const f = await fixture(t), parent = await f.create('external-budget-parent'), identity = hostIdentity({})
  const collector = createObservation({ identity, hierarchy: true, maxEvents: 1, readCatalog: session => f.ctx.sessionProjections.snapshot(session, ['subagentCatalog']).values.subagentCatalog })
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-external-budget-')), server = await startServer({ dataDir })
  const starts = [], catalogs = [], activations = [], finishes = [], disposers = []
  t.after(async () => {
    const failures = []
    for (const release of [async () => { for (const finish of finishes) finish({ output: [], stopReason: 'aborted' }); for (const activation of activations) await activation.result }, () => { for (const dispose of disposers) dispose(); collector.close() }, () => server.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { failures.push(error) } }
    if (failures.length) throw new AggregateError(failures, 'external budget fixture cleanup failed')
  })
  collector.baseline([parent.agent])
  disposers.push(f.ctx.on('subagent/start', activation => { starts.push(activation) }))
  disposers.push(f.ctx.on('session/event', (session, event) => {
    if (session === parent.agent.session && event.type === 'subagent/catalog') { catalogs.push(event); collector.observe('session/event', { session, event }) }
  }))
  f.ctx.subagents.registerProvider({ name: 'external-budget-fixture', capabilities: { agentOptions: false, outputSchema: false, depthLimit: false, toolFilter: false, persona: false }, inheritsParentContext: false,
    async start({ signal }) { let finish; const result = new Promise(resolve => { finish = resolve; finishes.push(resolve) }); signal.addEventListener('abort', () => finish({ output: [], stopReason: 'aborted' }), { once: true }); return { id: 'external-budget-execution', result, dispose() { finish({ output: [], stopReason: 'aborted' }) } } },
  })
  for (let index = 0; index < 3; index++) {
    const activation = await f.ctx.subagents.startActivation({ provider: 'external-budget-fixture', label: 'External budget work', delivery: 'parent', signal: new AbortController().signal, request: { parent: parent.agent, prompt: [{ type: 'text', text: 'Inspect independently.' }] } })
    activations.push(activation)
    await until(() => starts.length === index + 1 && catalogs.length === index + 1, 'each genuine external run has its own parent-owned native catalog and start')
    const start = starts[index], catalog = catalogs[index]
    assert.equal(catalog.data.mode, 'external')
    assert.equal(catalog.data.childId, activation.childId)
    assert.equal(start.id, activation.childId)
    assert.equal(f.ctx.sessions.get(start.id), undefined)
    assert.equal(f.ctx.agents.get(start.id), undefined)
    collector.observe('subagent/start', { parent: parent.agent, activation: start })
    if (index < 2) { finishes[index]({ output: [], stopReason: 'completed' }); await activation.result }
  }
  assert.equal(new Set(starts.map(start => start.runId)).size, 3)
  const request = async (path, body) => {
    const response = await fetch(`${server.url}/v1/${path}`, { method: 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: JSON.stringify(body) })
    return { status: response.status, body: await response.json() }
  }
  const registered = await request('source/register', { registrationId: 'external-budget', hostRunId: identity.hostRunId, name: 'External budget fixture' })
  assert.equal(registered.status, 200)
  const batch = collector.batch(), original = await request('source/ingest', { sourceKey: registered.body, batch })
  if (process.env.SENTINEL_EXTERNAL_DIAGNOSIS) await writeFile(process.env.SENTINEL_EXTERNAL_DIAGNOSIS, JSON.stringify({ nativeRuns: starts.map(({ id, runId }) => ({ id, runId })), nativeCatalogs: catalogs.map(event => ({ seq: event.seq, childId: event.data.childId, mode: event.data.mode })), batch, original, collector: collector.diagnostics() }, null, 2))
  assert.equal(original.status, 200, JSON.stringify(original))
  assert.ok(batch.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === starts[0].id && gap.reason.includes('历史执行节点超过采集预算')))
  assert.ok(batch.hierarchyContext.coverage.branchGaps.every(gap => typeof gap.sessionId === 'string'))
  assert.equal(collector.diagnostics().hierarchyRows, 1)
  const plainIdentity = hostIdentity({}), plain = createObservation({ identity: plainIdentity, hierarchy: true, maxEvents: 1 })
  t.after(() => plain.close())
  for (let index = 0; index < 2; index++) {
    const session = f.ctx.sessions.create(`plain-gap-${index}`)
    plain.observe('session/observed', { sessionId: session.id, header: session.header, events: [], cursor: -1, readComplete: true, gaps: [] })
  }
  const plainBatch = plain.batch(), plainRegistered = await request('source/register', { registrationId: 'plain-budget', hostRunId: plainIdentity.hostRunId, name: 'Plain budget fixture' })
  assert.equal(plainRegistered.status, 200)
  assert.equal((await request('source/ingest', { sourceKey: plainRegistered.body, batch: plainBatch })).status, 200)
  assert.ok(plainBatch.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'plain-gap-0'))
})
