import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { req } from './dsh-native-runtime.mjs'
import { startServer } from '../src/server.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { HIERARCHY_FEATURE } from '../src/hierarchy.mjs'
const boundedPagingSupported = ['observeSessionPage', 'releaseObservationPageCursor'].every(name => typeof req('@deepseek-ai/dsh-session-query').default.prototype[name] === 'function')
const extendedPaging = { skip: boundedPagingSupported ? false : 'unsupported: stock SDK has no bounded cold page / cursor-release API; extended SDK scenario retained' }
const { Context } = req('@deepseek-ai/cordis')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop', 'dsh-fs-local', 'dsh-working-directory', 'dsh-subagent', 'dsh-session-query'].map(name => { const module = req(`@deepseek-ai/${name}`); return module.default ?? module })
const until = async (predicate, label) => { const deadline = Date.now() + 2500; while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 10)) } }

async function fixture(t, { boundedPages = false, unsupportedPages = false, timeoutMs = 1000, closeTimeoutMs = 5000, expectedCloseStatus = 'closed', sizeBytes = 128, eventCount = 0, connect } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-reader-')), ctx = new Context(), server = await startServer({ dataDir })
  let parent, adapter
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  parent = await ctx.agents.create({ sessionId: 'reader-root', agentOptions: { provider: 'unused', model: 'unused' } })
  const header = { ...parent.agent.session.header, id: 'cold-child', parentSession: parent.agent.session.id, origin: 'subagent' }
  const counters = { observes: 0, stats: 0, opens: 0, prepares: 0, reads: 0, pageReads: 0, pageBudgets: [], releasedCursors: [], handles: 0, closed: 0 }, operations = []
  ctx.provide('sessionPersistence', { identity: 'reader-store',
    observationPageSupported: boundedPages,
    async readObservationPage(id, request) {
      counters.pageReads++; counters.pageBudgets.push(request.budget)
      if (counters.pageReads === 2) await new Promise(resolve => operations.push(resolve))
      const first = counters.pageReads === 1, time = Date.now()
      return { header: { ...header, isSeeded: true }, inheritedEventCount: first ? null : 500,
        events: first ? [
          { seq: 0, time, type: 'user/message', data: { id: 'inherited-hidden', source: { kind: 'user' }, content: [{ type: 'text', text: 'INHERITED-ONLY-NOT-OWN' }] } },
          { seq: 1, time, type: 'subagent/catalog', data: { version: 2, childId: 'inherited-foreign-child', mode: 'external' } },
        ] : [
          { seq: 500, time, type: 'session/end-seed', data: { inherited: true } },
          { seq: 501, time, type: 'subagent/catalog', data: { version: 2, childId: 'bounded-owned-leaf', mode: 'external' } },
        ], cursor: first ? 'bounded-first' : 'bounded-last', hasMore: first, complete: !first, revision: 'bounded-one', validation: 'event-rows',
        usage: { directoryEntries: 2, physicalBytes: 128, decodedBytes: 256, events: 2, elapsedMs: 1 }, gaps: first ? ['继承切点尚未取得'] : [] }
    },
    releaseObservationPageCursor(cursor) { counters.releasedCursors.push(cursor) },
    async stat(id) { counters.stats++; return id === 'cold-child' ? { header, revision: 'one', ...(sizeBytes === undefined ? {} : { sizeBytes }), ...(eventCount === undefined ? {} : { eventCount }) } : undefined },
    async open(id, _access, options) {
      counters.opens++; counters.handles++
      return { id, header, inheritedEventCount: 0, async read(_offset, _length, readOptions) {
        counters.reads++
        assert.ok(options.signal); assert.ok(readOptions.signal)
        return { eventState: 'detached', events: [] }
      }, async close() { counters.handles--; counters.closed++ } }
    },
  })
  if (!boundedPages && !unsupportedPages) ctx.sessionQuery.observeSessionPage = undefined
  const observeSession = ctx.sessionQuery.observeSession.bind(ctx.sessionQuery), prepare = ctx.sessions.prepare.bind(ctx.sessions)
  ctx.sessionQuery.observeSession = (...args) => { counters.observes++; return observeSession(...args) }
  ctx.sessions.prepare = (...args) => { counters.prepares++; return prepare(...args) }
  parent.agent.session.append('subagent/catalog', { version: 0, childId: 'cold-child', childCreatedAt: Date.now(), mode: 'one-shot' })
  adapter = createAdapter(ctx, { dataDir, registrationId: 'reader' }, { connect, flushMs: 5, heartbeatMs: 20, closeTimeoutMs, hierarchyReadTimeoutMs: timeoutMs })
  t.after(async () => {
    const failures = []
    for (const release of [() => { for (const operation of operations) operation() }, () => parent.dispose(), async () => { const result = await adapter.close(); assert.equal(result.status, expectedCloseStatus, JSON.stringify(result)) }, () => ctx.fiber.dispose(), () => server.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { failures.push(error) } }
    if (failures.length) throw new AggregateError(failures, 'directory reader cleanup failed')
  })
  return { ctx, parent, adapter, counters, dataDir, releaseReads: () => { for (const operation of operations.splice(0)) operation() } }
}

test('a legacy SDK keeps an unloaded descendant known and unfinished without reading or restoring it', async t => {
  const f = await fixture(t)
  let snapshot
  await until(async () => { snapshot = await f.adapter.snapshot(); return snapshot.hierarchyContext?.scopes?.some(scope => scope.sessionId === 'cold-child') }, 'cold child directory scope')
  assert.equal(f.ctx.agents.get('cold-child'), undefined)
  assert.equal(f.ctx.sessions.get('cold-child'), undefined, 'native prepared read stays detached')
  assert.equal(snapshot.tasks.find(task => task.sessionId === 'cold-child').status, 'unknown')
  assert.deepEqual(snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'reader-root').counts, { green: 0, yellow: 0, red: 0, gray: 0 })
  assert.equal(f.counters.handles, 0)
  for (let index = 0; index < 20; index++) await f.adapter.snapshot()
  await new Promise(resolve => setTimeout(resolve, 80))
  assert.deepEqual([f.counters.observes, f.counters.stats, f.counters.opens, f.counters.reads, f.counters.prepares], [0, 0, 0, 0, 0])
  assert.equal(f.adapter.diagnostics().hierarchyReader.pending, 0)
  assert.equal(snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'reader-root').coverage.complete, false)
})

test('a refused legacy cold branch resumes native incremental observation when its session becomes loaded', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'legacy branch refused')
  assert.equal(f.counters.prepares, 0)
  const session = f.ctx.sessions.create('cold-child', { meta: { parentSession: 'reader-root', origin: 'subagent' } })
  const preparedByProducer = f.counters.prepares
  session.append('subagent/catalog', { version: 2, childId: 'later-loaded-external', childCreatedAt: Date.now(), mode: 'external' })
  f.parent.agent.session.append('subagent/catalog', { version: 0, childId: session.id, childCreatedAt: Date.now(), mode: 'one-shot' })
  await until(async () => (await f.adapter.snapshot()).tasks.some(task => task.id === 'later-loaded-external'), 'loaded branch catalog reaches public Core')
  assert.deepEqual([f.counters.observes, f.counters.stats, f.counters.opens, f.counters.reads], [0, 0, 0, 0])
  assert.equal(f.counters.prepares, preparedByProducer, 'observer adds no preparation beyond the native producer creating its own loaded session')
  assert.equal(f.ctx.agents.get(session.id), undefined)
})

test('a pending native end followed by its real catalog never reads or forwards an unused large message body', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && f.adapter.diagnostics().hierarchyReader.pending === 0, 'initial native directory settled')
  let bodyReads = 0
  const unusedBody = [{ type: 'text', text: `TEST-UNUSED-AUTH ${'x'.repeat(160000)}` }]
  f.ctx.emit('subagent/end', { id: 'external-later', runId: 'raw-boundary-run', provider: 'boundary', local: false, stopReason: 'completed',
    get lastAssistantMessage() { bodyReads++; return unusedBody } })
  f.parent.agent.session.append('subagent/catalog', { version: 2, childId: 'external-later', childCreatedAt: Date.now(), mode: 'external', label: 'External later' })
  let snapshot
  await until(async () => { snapshot = await f.adapter.snapshot(); return snapshot.hierarchyContext?.groups?.some(group => group.nodes.some(node => node.external && node.state === 'ended')) }, 'late catalog resolves native terminal identity')
  assert.equal(bodyReads, 0, 'unused terminal body must stay outside the observer read/retention path')
  assert.ok(!JSON.stringify(snapshot).includes('TEST-UNUSED-AUTH'))
  assert.equal(f.adapter.diagnostics().hierarchyReader.pendingActivations, 0)
  assert.equal(snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'reader-root').counts.gray, 0)
})

test('a loaded directory-only session with a catalog after 500 own events continues to its last catalog without pretending the first page was complete', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'initial source settled')
  const session = f.ctx.sessions.create('loaded-501', { meta: { parentSession: 'reader-root', origin: 'subagent' } })
  for (let index = 0; index < 500; index++) session.append('session/title', { title: `title ${index}` })
  session.append('subagent/catalog', { version: 2, childId: 'last-external', childCreatedAt: Date.now(), mode: 'external', label: 'Last exact catalog' })
  f.parent.agent.session.append('subagent/catalog', { version: 0, childId: session.id, childCreatedAt: Date.now(), mode: 'one-shot' })
  let snapshot
  await until(async () => { snapshot = await f.adapter.snapshot(); return snapshot.hierarchyContext?.groups?.some(group => group.nodes.some(node => node.external && snapshot.tasks.find(task => task.key === node.taskKey)?.id === 'last-external')) }, '501st own catalog reaches the public subtree')
  assert.equal(f.ctx.agents.get(session.id), undefined)
  assert.equal(f.ctx.agents.get('last-external'), undefined)
  const scope = snapshot.hierarchyContext.scopes.find(scope => scope.sessionId === session.id)
  assert.ok(scope.coverage.branchGaps.some(gap => gap.reason.includes('外部执行内部材料不可观察')))
  assert.equal(snapshot.tasks.find(task => task.sessionId === session.id).status, 'unknown')
  const reads = f.adapter.diagnostics().hierarchyReader.reads
  for (let index = 0; index < 10; index++) await f.adapter.snapshot()
  await new Promise(resolve => setTimeout(resolve, 50))
  assert.equal(f.adapter.diagnostics().hierarchyReader.reads, reads, 'unchanged final cursor must not replay either page')
})

test('loaded directory discovery uses incremental native events without materializing a log or replaying all projections', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'initial directory settled')
  const session = f.ctx.sessions.create('incremental-directory', { meta: { parentSession: 'reader-root', origin: 'subagent' } })
  for (let index = 0; index < 1200; index++) session.append('session/title', { title: `prior title ${index}` })
  session.append('subagent/catalog', { version: 2, childId: 'incremental-external', childCreatedAt: Date.now(), mode: 'external' })
  session.snapshotEvents = () => { throw new Error('directory observer must not materialize the log') }
  let projectionReplays = 0
  const snapshot = f.ctx.sessionProjections.snapshot.bind(f.ctx.sessionProjections)
  f.ctx.sessionProjections.snapshot = (target, ...args) => {
    if (target.id === session.id) { projectionReplays++; throw new Error('directory observer must not replay the projection registry') }
    return snapshot(target, ...args)
  }
  f.parent.agent.session.append('subagent/catalog', { version: 0, childId: session.id, childCreatedAt: Date.now(), mode: 'one-shot' })
  let state
  await until(async () => { state = await f.adapter.snapshot(); return state.tasks.some(task => task.id === 'incremental-external') }, 'last native catalog reaches public snapshot incrementally')
  assert.equal(projectionReplays, 0)
  assert.equal(f.ctx.agents.get(session.id), undefined)
  assert.equal(f.ctx.agents.get('incremental-external'), undefined)
  assert.ok(f.adapter.diagnostics().hierarchyReader.reads >= 4)
})

test('legacy cold admission refuses an oversized artifact before querying metadata and keeps the descendant unknown', async t => {
  const f = await fixture(t, { sizeBytes: 2_000_000 })
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'cold admission settled')
  const state = await f.adapter.snapshot()
  assert.equal(f.counters.reads, 0)
  assert.equal(f.counters.handles, 0)
  assert.equal(f.ctx.agents.get('cold-child'), undefined)
  assert.equal(f.counters.stats, 0)
  assert.equal(f.counters.observes, 0)
  assert.ok(state.adapter.gaps.some(gap => gap.includes('有预算的只读会话分页')))
  assert.equal(state.hierarchyContext.sources[0].coverage.complete, false)
  assert.equal(state.hierarchyContext.groups.find(group => group.rootSessionId === 'reader-root').runningCount, 0)
})

test('directory diagnostics expose the per-round event and time budgets while long live histories continue on later rounds', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'initial directory settled')
  const { LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
  class NativeActor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      yield { type: 'text-delta', index: 0, text: 'native actor continues during directory catchup' }
      if (!options.signal.aborted) await new Promise(resolve => options.signal.addEventListener('abort', resolve, { once: true }))
      yield { type: 'finish', reason: { kind: 'stop' } }
    }
  }
  f.ctx.llm.registerAdapter(['unused'], new NativeActor())
  f.parent.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Continue while the observer reads the separate directory.' }] }))
  await until(() => f.parent.agent.status === 'running', 'actual native actor running')
  const session = f.ctx.sessions.create('round-budget-directory', { meta: { parentSession: 'reader-root', origin: 'subagent' } })
  for (let index = 0; index < 7000; index++) session.append('session/title', { title: `prior ${index}` })
  session.append('subagent/catalog', { version: 2, childId: 'after-round-budget', childCreatedAt: Date.now(), mode: 'external' })
  f.parent.agent.session.append('subagent/catalog', { version: 0, childId: session.id, childCreatedAt: Date.now(), mode: 'one-shot' })
  const observedScopes = []
  await until(async () => {
    const snapshot = await f.adapter.snapshot(), scope = snapshot.hierarchyContext.scopes.find(scope => scope.sessionId === session.id)
    if (scope) observedScopes.push({ revision: snapshot.adapter.hierarchyContext.acceptedRevision, gaps: scope.coverage.branchGaps.map(gap => gap.reason) })
    return snapshot.tasks.some(task => task.id === 'after-round-budget')
  }, 'deferred pages eventually reach their final catalog')
  const diagnostics = f.adapter.diagnostics().hierarchyReader
  assert.ok(diagnostics.rounds >= 4)
  assert.ok(diagnostics.peakRoundEvents <= diagnostics.budgets.maxRoundEvents)
  assert.ok(diagnostics.peakRoundReads <= diagnostics.budgets.maxReads)
  assert.equal(diagnostics.budgets.nativeReplayBudgetEnforced, false)
  assert.ok(diagnostics.peakRoundMs >= 0)
  assert.equal(f.ctx.agents.get(session.id), undefined)
  const final = await f.adapter.snapshot(), scope = final.hierarchyContext.scopes.find(scope => scope.sessionId === session.id)
  assert.ok(!final.adapter.gaps.some(gap => gap.includes('本轮读取预算已用完')), 'completed continuation must not retain a permanent temporary budget warning')
  assert.ok(!scope.coverage.branchGaps.some(gap => gap.reason.includes('页面尚未读完')))
  assert.ok(scope.coverage.branchGaps.some(gap => gap.reason.includes('外部执行内部材料不可观察')), 'clearing the pending-page gap must preserve the distinct external visibility gap')
  assert.equal(f.parent.agent.status, 'running')
  assert.equal(final.tasks.find(task => task.id === 'reader-root').status, 'running')
  assert.ok(observedScopes.every(item => item.revision <= final.adapter.hierarchyContext.acceptedRevision))
})

test('refresh does not renew directory caches, parent proofs or unowned native activations beyond 24 hours', async t => {
  const actualNow = Date.now
  let time = actualNow()
  Date.now = () => time
  t.after(() => { time = actualNow(); Date.now = actualNow })
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'directory cache populated')
  f.ctx.emit('subagent/start', { id: 'unowned-pending', runId: 'pending-old', local: false })
  const before = f.adapter.diagnostics().hierarchyReader
  assert.ok(before.cached > 0)
  assert.ok(before.parents > 0)
  assert.equal(before.pendingActivations, 1)
  for (let index = 0; index < 20; index++) await f.adapter.snapshot()
  time += 24 * 60 * 60 * 1000 + 1
  const expired = f.adapter.diagnostics().hierarchyReader
  assert.equal(expired.cached, 0)
  assert.equal(expired.parents, 0)
  assert.equal(expired.pendingActivations, 0)
  assert.equal(f.parent.agent.status, 'idle')
})

test('closing a legacy observer has no cold IO or leases to leave pending and preserves the native Agent', async t => {
  // Keep this reader deadline independent of HTTP scheduling; real HTTP shutdown is covered in dsh-resource-adapter.test.mjs.
  const connect = async () => ({
    async registerSource() { return 'reader-source' },
    async health() { return { features: [HIERARCHY_FEATURE] } },
    async heartbeat() {},
    async ingest(_sourceKey, batch) { return { hierarchyContext: { schemaVersion: 1, acceptedRevision: batch.hierarchyContext.revision } } },
    async unregisterSource() {}, async cancelPending() {}, async close() {},
  })
  const f = await fixture(t, { closeTimeoutMs: 25, connect })
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'legacy refusal settled')
  const result = await f.adapter.close()
  assert.equal(result.status, 'closed')
  const diagnostics = f.adapter.diagnostics().hierarchyReader
  assert.equal(f.counters.handles, 0)
  assert.equal(diagnostics.pendingColdReads, 0)
  assert.equal(diagnostics.activeLeases, 0)
  assert.deepEqual([f.counters.observes, f.counters.stats, f.counters.opens, f.counters.reads, f.counters.prepares], [0, 0, 0, 0, 0])
  assert.equal(f.ctx.agents.get('cold-child'), undefined)
  assert.equal(f.ctx.sessions.get('cold-child'), undefined)
  assert.equal(f.ctx.agents.get('reader-root'), f.parent.agent)
  assert.equal(f.parent.agent.status, 'idle')
})

test('a public page method backed by an unsupported provider never falls back to legacy storage or restoration', extendedPaging, async t => {
  const f = await fixture(t, { unsupportedPages: true })
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'provider capability refusal settled')
  const snapshot = await f.adapter.snapshot()
  assert.ok(snapshot.adapter.gaps.some(gap => gap.includes('SESSION_OBSERVATION_PAGE_UNSUPPORTED')))
  assert.equal(snapshot.hierarchyContext.sources[0].coverage.complete, false)
  assert.deepEqual([f.counters.observes, f.counters.stats, f.counters.opens, f.counters.reads, f.counters.prepares, f.counters.pageReads], [0, 0, 0, 0, 0, 0])
  assert.equal(f.ctx.agents.get('cold-child'), undefined)
  assert.equal(f.ctx.sessions.get('cold-child'), undefined)
  assert.equal(f.parent.agent.status, 'idle')
})

test('public SDK cold pages keep an unknown seed outside own facts and share continued directory reads across viewers', extendedPaging, async t => {
  const f = await fixture(t, { boundedPages: true })
  await until(() => f.counters.pageReads === 2, 'second bounded native page pending')
  const first = await f.adapter.snapshot()
  assert.ok(!JSON.stringify(first).includes('INHERITED-ONLY-NOT-OWN'))
  assert.ok(!first.tasks.some(task => task.id === 'inherited-foreign-child'))
  assert.equal(f.counters.reads, 0, 'new SDK pages must not fall back to a complete native log read')
  f.releaseReads()
  await until(async () => (await f.adapter.snapshot()).tasks.some(task => task.id === 'bounded-owned-leaf'), 'known own cut permits the actual leaf')
  for (let index = 0; index < 5; index++) await Promise.all(Array.from({ length: 20 }, () => f.adapter.snapshot()))
  await new Promise(resolve => setTimeout(resolve, 50))
  assert.equal(f.counters.pageReads, 2)
  assert.equal(f.counters.reads, 0)
  assert.ok(f.counters.pageBudgets.every(budget => budget.maxDirectoryEntries <= 2000 && budget.maxPhysicalBytes === 262144 && budget.maxDecodedBytes === 262144 && budget.maxEvents <= 500 && budget.deadlineMs <= 1000))
  assert.equal(f.adapter.diagnostics().hierarchyReader.boundedPageReads, 2)
  assert.equal(f.ctx.agents.get('cold-child'), undefined)
  assert.equal(f.ctx.sessions.get('cold-child'), undefined)
  assert.equal(f.parent.agent.status, 'idle')
})

test('closing a source releases its bounded continuation and a late SDK page cannot register a replacement cursor or publish', extendedPaging, async t => {
  const f = await fixture(t, { boundedPages: true, closeTimeoutMs: 25, expectedCloseStatus: 'incomplete' })
  await until(() => f.counters.pageReads === 2, 'second bounded page in flight')
  const result = await f.adapter.close()
  assert.equal(result.status, 'incomplete')
  assert.equal(result.timedOut, true)
  assert.ok(f.counters.releasedCursors.includes('bounded-first'))
  assert.equal(f.adapter.diagnostics().hierarchyReader.pendingColdReads, 1)
  f.releaseReads()
  await until(() => f.adapter.diagnostics().hierarchyReader.pending === 0, 'bounded SDK operation actually settled')
  assert.ok(f.counters.releasedCursors.includes('bounded-last'), 'query releases a late page before rejecting cancellation')
  assert.equal(f.adapter.diagnostics().hierarchyReader.cached, 0)
  assert.equal(f.adapter.diagnostics().hierarchyReader.activeLeases, 0)
  assert.equal(f.ctx.agents.get('bounded-owned-leaf'), undefined)
  assert.equal(f.parent.agent.status, 'idle')
})

test('a preloaded directory deeper than the traversal budget preserves the unfinished branch without creating running descendants', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'initial source settled')
  await f.adapter.close()
  const directories = Array.from({ length: 66 }, (_, index) => f.ctx.sessions.create(`depth-directory-${index}`, { meta: { parentSession: index ? `depth-directory-${index - 1}` : 'reader-root', origin: 'subagent' } }))
  for (let index = 0; index < directories.length - 1; index++) directories[index].append('subagent/catalog', { version: 0, childId: directories[index + 1].id, childCreatedAt: Date.now(), mode: 'one-shot' })
  f.parent.agent.session.append('subagent/catalog', { version: 0, childId: directories[0].id, childCreatedAt: Date.now(), mode: 'one-shot' })
  const source = createAdapter(f.ctx, { registrationId: 'depth-reader', dataDir: f.dataDir }, { flushMs: 5 })
  t.after(async () => { assert.equal((await source.close()).status, 'closed') })
  let snapshot
  await until(async () => { snapshot = await source.snapshot(); return snapshot.adapter.gaps.some(gap => gap.includes('深度超过读取预算')) }, 'deep branch remains unfinished')
  assert.ok(snapshot.hierarchyContext.sources.some(item => !item.coverage.complete))
  assert.ok(snapshot.hierarchyContext.groups.every(group => group.runningCount === 0))
  assert.ok(directories.every(session => !f.ctx.agents.get(session.id)))
  assert.equal(source.diagnostics().hierarchyReader.budgets.maxDepth, 64)
  assert.equal(f.parent.agent.status, 'idle')
})

test('an already loaded native Agent baseline never starts a complete catalog projection replay', async t => {
  const f = await fixture(t)
  await until(() => f.adapter.connected && !f.adapter.diagnostics().hierarchyReader.pending, 'initial source settled')
  await f.adapter.close()
  const actor = f.parent.agent
  for (let index = 0; index < 2500; index++) actor.session.append('session/title', { title: `prior ${index}` })
  actor.session.append('subagent/catalog', { version: 2, childId: 'baseline-last-external', mode: 'external', childCreatedAt: Date.now() })
  const snapshot = f.ctx.sessionProjections.snapshot.bind(f.ctx.sessionProjections)
  let catalogReplays = 0
  f.ctx.sessionProjections.snapshot = (session, ...args) => {
    if (session.id === actor.session.id) { catalogReplays++; throw new Error('baseline must not start a full native projection replay') }
    return snapshot(session, ...args)
  }
  const source = createAdapter(f.ctx, { registrationId: 'baseline-incremental-source', dataDir: f.dataDir }, { flushMs: 5 })
  t.after(async () => { assert.equal((await source.close()).status, 'closed') })
  await until(async () => (await source.snapshot()).tasks.some(task => task.id === 'baseline-last-external'), 'incremental baseline reaches its final directory entry')
  assert.equal(catalogReplays, 0)
  assert.equal(actor.status, 'idle')
  assert.equal(f.ctx.agents.get('baseline-last-external'), undefined)
})
