import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createCore } from '../src/core/index.mjs'
import { mountGS } from './gs-session-client-runtime.mjs'
import { clientRequire } from './dsh-native-runtime.mjs'

const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
async function observedTree(t, ids = ['root', 'normal', 'normal-child', 'deep', 'red', 'yellow', 'gray', 'history'], ended = new Set(['history'])) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-compact-ui-'))
  const at = Date.now(); let clock = at, modelCalls = 0
  const core = await createCore({ dataDir, now: () => clock, leaseMs: 172800000, analyzer: { async analyze(input) {
    modelCalls++
    return { summary: { currentTask: input.task.currentRequest, currentState: '', recentProgress: '', attention: '' },
      checks: checks.map(id => ({ id, status: id === 'request_drift' ? input.task.id === 'red' ? 'attention' : input.task.id === 'gray' ? 'unknown' : 'clear' : 'unknown',
        reason: `${input.task.id} own basis`, evidenceIds: id === 'request_drift' ? ['input'] : [],
        applicability: { state: id === 'request_drift' ? 'triggered' : 'not-triggered', reason: 'Complete scope', evidenceIds: ['input'] } })), coverage: input.coverage }
  } } })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  const sourceKey = await core.registerSource({ registrationId: 'compact-ui', hostRunId: 'controlled', name: 'Own controlled Host' })
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const event = (taskId, id, kind, data = {}) => ({ id, taskId, taskLifecycleId: 'life', kind, capturedAt: at, occurredAt: at, text: `${taskId} ${id}`, data })
  const parents = { normal: 'root', 'normal-child': 'normal', deep: 'root', red: 'deep', yellow: 'root', gray: 'root', history: 'root', 'normal-leaf': 'normal-root' }
  const relations = ids.filter(id => parents[id] && ids.includes(parents[id])).map(id => ({ parentSessionId: parents[id], childSessionId: id, kind: 'delegation', evidenceRefs: [ref(parents[id], `catalog-${id}`)] }))
  const events = ids.flatMap(id => [event(id, 'state', 'status', { nativeType: 'agent/status', status: ended.has(id) ? 'disposed' : 'running' }),
    event(id, 'claimed', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: id }),
    event(id, 'input', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: parents[id] ? 'delegated' : 'unknown', messageId: id, inherited: false }),
    ...(id === 'yellow' || ended.has(id) ? [] : [event(id, 'output', 'output')])])
  events.push(...relations.map(relation => event(relation.parentSessionId, `catalog-${relation.childSessionId}`, 'status', { nativeType: 'subagent/catalog', childSessionId: relation.childSessionId })))
  await core.ingest(sourceKey, { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: `Name ${id}`, status: ended.has(id) ? 'disposed' : 'running' })), events,
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })),
      activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: ended.has(taskId) ? 'ended' : 'running', stateRefs: [ref(taskId, 'state')] })), relations,
      instructionBindings: ids.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, 'input'), phase: 'consumed', origin: parents[taskId] && ids.includes(parents[taskId]) ? 'delegated' : 'user-entry', originRefs: [parents[taskId] && ids.includes(parents[taskId]) ? ref(parents[taskId], `catalog-${taskId}`) : ref(taskId, 'claimed')], ancestorLimitRefs: parents[taskId] && ids.includes(parents[taskId]) ? [ref(taskId === 'normal-leaf' ? 'normal-root' : 'root', 'input')] : [] })), coverage: { complete: true, branchGaps: [] } } })
  let snapshot
  for (let attempt = 0; attempt < 100; attempt++) {
    snapshot = await core.snapshot()
    if (snapshot.tasks.filter(task => task.status === 'running').every(task => task.analysis.state === 'cooldown')) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.ok(snapshot.tasks.filter(task => task.status === 'running').every(task => task.analysis.state === 'cooldown'), 'Controlled production analysis settles')
  let claims = 0
  const service = { async snapshot() { return { ...await core.snapshot(), adapter: { sourceKey } } }, async gsSummary(sessionId) { return core.gsSummary({ sourceKey, sessionId }) }, analysisConfig: async () => ({}), async claimNotifications() { claims++; return [] } }
  return { core, service, sourceKey, snapshot, claims: () => claims, modelCalls: () => modelCalls, now: () => clock, advance: ms => clock += ms, key: id => snapshot.tasks.find(task => task.id === id).key }
}
const shown = node => ![...node.closest('nav').querySelectorAll('details:not([open])')].some(detail => detail.contains(node) && !detail.querySelector('summary')?.contains(node))
const counts = parent => [...parent.querySelectorAll('[data-gs-count]')].map(node => node.textContent)
test('visible closed GS uses the real core counts summary while only an open popup refreshes full trees', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red'])
  let fullReads = 0, summaryReads = 0
  const read = data.service.snapshot, summary = data.service.gsSummary
  data.service.snapshot = async () => { fullReads++; return read() }
  data.service.gsSummary = async sessionId => { summaryReads++; return summary(sessionId) }
  const f = await mountGS(t, data.service), document = f.dom.window.document
  await f.until(() => fullReads + summaryReads > 0, 'Actual Native Remote begins an observation read')
  assert.equal(fullReads, 0, 'A visible closed GS entry must not fetch full task bodies or tree projection')
  await f.until(() => document.querySelector('[data-gs-count=red]')?.textContent === '1', 'Real lightweight core summary reaches the entry')
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['2', '0', '1', '0'])
  assert.equal(f.plugin.diagnostics().summaryTimer, 1)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  const analyzed = data.modelCalls()
  await f.click(document.querySelector('[data-gs-entry]'))
  await f.until(() => document.querySelector(`[data-gs-node="${data.key('red')}"]`), 'Opening GS acquires the real tree once')
  assert.equal(fullReads, 1)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 1)
  await f.key('Escape')
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  assert.equal(f.plugin.diagnostics().pendingSnapshot, 0)
  assert.equal(f.plugin.diagnostics().summaryTimer, 1)
  const lastFull = fullReads
  await data.core.ingest(data.sourceKey, { tasks: [{ id: 'red', lifecycleId: 'life', sessionId: 'red', title: 'Name red', status: 'disposed' }], events: [{ id: 'red-ended', taskId: 'red', taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: data.snapshot.generatedAt, occurredAt: null, data: { nativeType: 'agent/status', status: 'disposed' } }] })
  await f.tick()
  await f.until(() => document.querySelector('[data-gs-count=red]')?.textContent === '0', 'Closed summary continues tracking core activity')
  assert.equal(fullReads, lastFull)
  assert.equal(data.modelCalls(), analyzed, 'View activity does not reschedule unchanged background analysis')
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['2', '0', '0', '0'])
  t.diagnostic(JSON.stringify({ fullReads, summaryReads, analyzed, client: f.plugin.diagnostics() }))
})
test('GS uses native entry visibility and document visibility to release read demand without stopping background notifications', async t => {
  const data = await observedTree(t, ['root', 'gray'])
  const f = await mountGS(t, data.service), document = f.dom.window.document
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0 && document.querySelector('[data-gs-count=green]')?.textContent === '1', 'Production summary is ready')
  const calls = f.rpcActivity.methods.gsSummary
  await f.render('root', true, false)
  assert.equal(f.plugin.diagnostics().summaryTimer, 0); assert.equal(f.plugin.diagnostics().summaryViewers, 0)
  assert.equal(f.plugin.diagnostics().listeners, 0)
  const claims = data.claims()
  await f.tick()
  assert.equal(f.rpcActivity.methods.gsSummary, calls)
  assert.ok(data.claims() > claims, 'The separate notification Remote remains active')
  await f.render('root', true, true)
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0, 'One resumed summary finishes')
  assert.equal(f.rpcActivity.methods.gsSummary, calls + 1)
  await f.click(document.querySelector('[data-gs-entry]'))
  await f.visibility('hidden')
  assert.equal(document.querySelector('[role=dialog]'), null)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0); assert.equal(f.plugin.diagnostics().summaryTimer, 0)
  assert.equal(f.rpcActivity.inFlight, 0)
  await f.visibility('visible')
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0, 'Summary resumes after document visibility')
  assert.equal(f.plugin.diagnostics().summaryTimer, 1)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  await f.abortView()
  assert.equal(f.plugin.diagnostics().summaryTimer, 0); assert.equal(f.plugin.diagnostics().listeners, 0)
  t.diagnostic(JSON.stringify({ RPC: f.rpcActivity, client: f.plugin.diagnostics() }))
})
test('GS summary IO ignoring abort stays bounded across scopes and connection generations and discards old results', async t => {
  const data = await observedTree(t, ['root', 'gray'])
  const summary = data.service.gsSummary, held = []; let hold = false, inFlight = 0, finished = 0, peak = 0
  t.after(() => { for (const item of held) item.finish() })
  data.service.gsSummary = async (sessionId, signal) => {
    if (!hold) return summary(sessionId)
    inFlight++; peak = Math.max(peak, inFlight)
    try { await new Promise(resolve => held.push({ sessionId, signal, finish: resolve })); return { ...await summary(sessionId), counts: { green: 999, yellow: 0, red: 0, gray: 0 } } }
    finally { inFlight--; finished++ }
  }
  const f = await mountGS(t, data.service), document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count=green]')?.textContent === '1', 'Initial real summary is current')
  hold = true
  for (let cycle = 0; cycle < 5; cycle++) { await f.tick(); await f.render(cycle % 2 ? 'root' : 'gray'); await f.settle() }
  await f.reinstall(); await f.reinstall()
  assert.ok(peak <= 2); assert.equal(inFlight, 2)
  assert.equal(f.plugin.diagnostics().unfinishedRequests, 2)
  assert.ok(document.querySelector('[data-gs-entry]').textContent.includes('摘要不可用') || document.querySelector('[data-gs-entry]').textContent.includes('上次读取'))
  const notices = data.claims()
  await f.tick(); assert.ok(data.claims() > notices)
  hold = false
  for (const item of held) item.finish()
  await f.until(() => inFlight === 0 && f.plugin.diagnostics().unfinishedRequests === 0, 'Actual old summary RPCs finish')
  await f.render('root'); await f.tick()
  await f.until(() => document.querySelector('[data-gs-count=green]')?.textContent === '1', 'The current scope refreshes after the old pool drains')
  assert.equal(finished, 2)
  assert.ok(!document.querySelector('[data-gs-entry]').textContent.includes('999'))
  t.diagnostic(JSON.stringify({ nativeRPC: { peak, inFlight, finished }, carrier: f.rpcActivity, client: f.plugin.diagnostics() }))
})
test('a core without lightweight summary reports its capability gap without falling back to closed full-tree polling', async t => {
  const empty = { protocol: 1, generatedAt: Date.now(), sourceKey: 'own', sessionId: 'root', sourceStatus: 'unknown', scopeKnown: false, counts: { green: 0, yellow: 0, red: 0, gray: 0 }, coverage: { complete: false, gaps: ['GS summary unsupported by this core'] }, gsStatus: { supported: false } }
  let full = 0
  const f = await mountGS(t, { gsSummary: async () => empty, snapshot: async () => { full++; return { protocol: 1, generatedAt: Date.now(), sources: [], tasks: [], adapter: { sourceKey: 'own' } } }, claimNotifications: async () => [], analysisConfig: async () => ({}) })
  await f.until(() => f.element.querySelector('[data-gs-entry]').textContent.includes('摘要不支持'), 'Capability gap appears on the closed entry')
  await f.tick()
  assert.equal(full, 0); assert.deepEqual(counts(f.element.querySelector('[data-gs-entry]')), ['0', '0', '0', '0'])
  await f.click(f.element.querySelector('[data-gs-entry]'))
  assert.ok(f.element.textContent.includes('共享核心尚不支持轻量 GS 摘要'))
  assert.equal(full, 1)
})
test('native intersection visibility owns one entry observer and stops only invisible GS read demand', async t => {
  const data = await observedTree(t, ['root'])
  const { JSDOM } = clientRequire('jsdom'), { act } = clientRequire('react')
  const dom = new JSDOM('<html><head></head><body><div id="view"></div><input id="draft" value="observer draft"></body></html>', { pretendToBeVisual: true })
  const original = { window: globalThis.window, document: globalThis.document, act: globalThis.IS_REACT_ACT_ENVIRONMENT }, observers = new Set()
  class EntryObserver {
    constructor(callback) { this.callback = callback }
    observe(element) { this.element = element; observers.add(this) }
    disconnect() { observers.delete(this) }
  }
  dom.window.IntersectionObserver = EntryObserver
  const f = await mountGS(t, data.service, 'root', { dom, element: dom.window.document.getElementById('view') })
  t.after(() => { globalThis.window = original.window; globalThis.document = original.document; globalThis.IS_REACT_ACT_ENVIRONMENT = original.act; dom.window.close() })
  assert.equal(observers.size, 1); assert.equal(f.plugin.diagnostics().entryObservers, 2, 'One entry intersection and one bounded footer visibility observer')
  assert.equal(f.rpcActivity.methods.gsSummary || 0, 0, 'An unconfirmed intersection does not subscribe an invisible entry')
  const observer = [...observers][0]
  await act(async () => observer.callback([{ isIntersecting: true }]))
  await f.until(() => f.rpcActivity.methods.gsSummary === 1 && f.plugin.diagnostics().pendingSummary === 0, 'Visible entry gets one immediate core summary')
  await f.click(f.element.querySelector('[data-gs-entry]'))
  await act(async () => observer.callback([{ isIntersecting: false }]))
  await f.settle()
  assert.equal(f.element.querySelector('[role=dialog]'), null)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0); assert.equal(f.plugin.diagnostics().summaryTimer, 0)
  assert.equal(observers.size, 1, 'The entry keeps its necessary native visibility observer to resume')
  const notifications = data.claims()
  await f.tick(); assert.ok(data.claims() > notifications)
  await act(async () => observer.callback([{ isIntersecting: true }]))
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0, 'Native visibility resumes the lightweight demand')
  assert.equal(f.plugin.diagnostics().summaryTimer, 1); assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  await f.destroy()
  assert.equal(observers.size, 0); assert.equal(f.plugin.diagnostics().entryObservers, 0)
})
test('an abort-aware GS summary Remote physically finishes before its hidden view resumes', async t => {
  const data = await observedTree(t, ['root']), read = data.service.gsSummary
  let hold = false, entered = 0, finished = 0, inFlight = 0, heldSignal
  data.service.gsSummary = async (sessionId, signal) => {
    entered++; inFlight++
    let aborted
    try {
      if (hold) { heldSignal = signal; await new Promise((resolve, reject) => { aborted = () => reject(signal.reason); signal.addEventListener('abort', aborted, { once: true }); if (signal.aborted) aborted() }) }
      return read(sessionId)
    } finally { if (aborted) signal.removeEventListener('abort', aborted); finished++; inFlight-- }
  }
  const f = await mountGS(t, data.service)
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0 && f.rpcActivity.methods.gsSummary === 1, 'Initial summary settles')
  hold = true; await f.tick(); assert.equal(inFlight, 1)
  await f.render('root', true, false)
  assert.equal(heldSignal.aborted, true); assert.equal(inFlight, 0); assert.equal(entered, finished)
  assert.equal(f.plugin.diagnostics().pendingSummary, 0); assert.equal(f.plugin.diagnostics().unfinishedRequests, 0)
  hold = false; await f.render('root', true, true)
  await f.until(() => f.plugin.diagnostics().pendingSummary === 0, 'Resumed current summary physically settles')
  assert.equal(entered, finished); assert.equal(entered, 3)
  t.diagnostic(JSON.stringify({ summaryIO: { entered, finished, inFlight }, carrier: f.rpcActivity }))
})
test('failed lightweight summaries keep explicit history counts, isolate a switched scope and recover without a full-tree fallback', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red']), read = data.service.gsSummary
  let refuse = false, full = 0
  data.service.gsSummary = async sessionId => { if (refuse) throw new Error('controlled GS summary unavailable'); return read(sessionId) }
  const snapshot = data.service.snapshot
  data.service.snapshot = async () => { full++; return snapshot() }
  const f = await mountGS(t, data.service)
  await f.until(() => f.element.querySelector('[data-gs-count=red]')?.textContent === '1', 'Initial authoritative summary is current')
  refuse = true; await f.tick()
  assert.ok(f.element.querySelector('[data-gs-entry]').textContent.includes('上次读取'))
  assert.deepEqual(counts(f.element.querySelector('[data-gs-entry]')), ['2', '0', '1', '0'])
  await f.render('red')
  assert.deepEqual(counts(f.element.querySelector('[data-gs-entry]')), ['0', '0', '0', '0'], 'A new scope does not inherit the old root counts')
  assert.ok(f.element.querySelector('[data-gs-entry]').textContent.includes('摘要不可用'))
  refuse = false; await f.tick()
  await f.until(() => f.element.querySelector('[data-gs-count=red]')?.textContent === '1', 'The current red scope summary recovers')
  assert.deepEqual(counts(f.element.querySelector('[data-gs-entry]')), ['0', '0', '1', '0'])
  assert.ok(!f.element.querySelector('[data-gs-entry]').textContent.includes('上次读取'))
  assert.equal(full, 0)
})
test('a disconnected source refreshes closed GS with core gray counts and an explicit source gap', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red'])
  const f = await mountGS(t, data.service)
  await f.until(() => f.element.querySelector('[data-gs-count=red]')?.textContent === '1', 'Connected source summary loaded')
  await data.core.unregisterSource(data.sourceKey)
  await f.tick()
  assert.deepEqual(counts(f.element.querySelector('[data-gs-entry]')), ['0', '0', '0', '3'])
  assert.ok(f.element.querySelector('[data-gs-entry]').textContent.includes('来源未连接'))
  assert.equal(f.rpcActivity.methods.snapshot || 0, 0)
  const current = await data.core.gsSummary({ sourceKey: data.sourceKey, sessionId: 'root' })
  assert.equal(current.sourceStatus, 'disconnected'); assert.equal(current.scopeKnown, true)
  assert.equal(current.coverage.complete, false)
})
test('native UI pause leaves public event collection active while analysis and notifications pause and resume once', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red'])
  data.service.claimNotifications = () => data.core.claimNotifications(data.sourceKey)
  data.service.ackNotification = (id, result) => data.core.ackNotification(id, result)
  data.service.pause = () => data.core.pause(); data.service.resume = () => data.core.resume()
  await data.core.pause()
  const f = await mountGS(t, data.service), before = data.modelCalls()
  data.advance(60001)
  await data.core.ingest(data.sourceKey, { events: [{ id: 'while-view-paused', taskId: 'red', taskLifecycleId: 'life', kind: 'output', capturedAt: data.now(), occurredAt: null, text: 'Collected while all full views are closed' }] })
  await f.tick()
  assert.equal(data.modelCalls(), before); assert.equal(f.delivered.length, 0)
  const collected = await data.core.snapshot()
  assert.ok(collected.tasks.find(task => task.id === 'red').events.some(event => event.id === 'while-view-paused'))
  assert.equal(collected.monitoring.state, 'paused')
  await f.renderOverview()
  await f.click([...f.element.querySelectorAll('button')].find(button => button.textContent === '继续分析'))
  await f.until(() => data.modelCalls() > before, 'Public UI resume gives new collected material its real analysis turn')
  await f.render('root')
  data.advance(3000); await f.tick()
  await f.until(() => f.delivered.length === 1, 'The resumed core delivers one current grouped attempt')
  const body = f.delivered[0].options.body
  assert.ok(body.includes(data.sourceKey) && body.includes(data.key('red')))
  assert.ok(body.length <= 1200)
  const after = data.modelCalls()
  await f.tick(); assert.equal(f.delivered.length, 1); assert.equal(data.modelCalls(), after)
  const current = await data.core.snapshot()
  assert.equal(current.monitoring.state, 'active')
  assert.equal(current.tasks.find(task => task.id === 'red').alerts[0].notification.seen, 'unknown')
})
async function addFlatSource(data) {
  const sourceKey = await data.core.registerSource({ registrationId: 'flat-ui', hostRunId: 'controlled-flat', name: 'Own flat Host' })
  const states = { red: 'running', 'flat-clear': 'running', 'flat-ended': 'disposed', 'directory-only': 'unknown' }
  await data.core.ingest(sourceKey, { tasks: Object.entries(states).map(([id, status]) => ({ id, lifecycleId: 'life', title: `Flat ${id}`, status })),
    events: Object.keys(states).filter(id => states[id] === 'running').flatMap(taskId => ['input', 'output'].map(kind => ({ id: kind, taskId, taskLifecycleId: 'life', kind, text: `${taskId} ${kind}`, capturedAt: data.snapshot.generatedAt, occurredAt: null }))) })
  let snapshot
  for (let attempt = 0; attempt < 100; attempt++) {
    snapshot = await data.core.snapshot()
    if (snapshot.tasks.filter(task => task.sourceKey === sourceKey && task.status === 'running').every(task => task.analysis.state === 'cooldown')) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.ok(snapshot.tasks.filter(task => task.sourceKey === sourceKey && task.status === 'running').every(task => task.analysis.state === 'cooldown'), 'Flat production analysis settles')
  const flat = snapshot.tasks.filter(task => task.sourceKey === sourceKey)
  assert.equal(flat.find(task => task.id === 'red').gs.light, 'red')
  assert.equal(flat.find(task => task.id === 'flat-clear').gs.light, 'green')
  assert.equal(flat.find(task => task.id === 'flat-ended').gs.light, null)
  assert.equal(flat.find(task => task.id === 'directory-only').gs.light, null)
  return { sourceKey, snapshot }
}

test('pure flat production actors contribute global activity and red counts without inventing session ownership', async t => {
  const data = await observedTree(t, [])
  await addFlatSource(data)
  const f = await mountGS(t, data.service)
  await f.renderOverview()
  const document = f.dom.window.document, header = document.querySelector('[data-gs-overview-counts]')
  assert.ok(header.textContent.includes('会话 0 · 活动代理 2 · 关注 1'))
  assert.deepEqual(counts(header), ['1', '0', '1', '0'])
  assert.ok(document.body.textContent.includes('会话及后代数量不可判断'))
  assert.equal(document.querySelector('[data-gs-empty]'), null, 'A visible flat red actor is not a no-problem empty state')
  await f.click(document.querySelector('[data-gs-hide-normal]'))
  assert.deepEqual(counts(header), ['1', '0', '1', '0'])
  assert.ok(document.querySelector('nav[aria-label="未分组观察任务"]').textContent.includes('Flat red'))
  assert.ok(!document.querySelector('nav[aria-label="未分组观察任务"]').textContent.includes('Flat flat-clear'))
  assert.equal(document.querySelector('[data-gs-empty]'), null)
})

test('mixed grouped and flat production actors are counted once by observation key and keep scope coverage through filtering', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red'])
  const flat = await addFlatSource(data)
  assert.equal(flat.snapshot.tasks.filter(task => task.id === 'red').length, 2, 'Same local ID belongs to two distinct public observation keys')
  const f = await mountGS(t, data.service)
  await f.renderOverview()
  const document = f.dom.window.document, header = document.querySelector('[data-gs-overview-counts]')
  assert.ok(header.textContent.includes('会话 1 · 活动代理 5 · 关注 2'))
  assert.deepEqual(counts(header), ['3', '0', '2', '0'])
  assert.equal(document.querySelectorAll('[data-sentinel-session-group]').length, 1)
  assert.ok(document.body.textContent.includes('会话及后代数量不可判断'))
  await f.click(document.querySelector('[data-gs-hide-normal]'))
  assert.deepEqual(counts(header), ['3', '0', '2', '0'])
  assert.ok(document.querySelector(`[data-gs-node="${data.key('red')}"]`), 'The grouped deep red path stays visible')
  assert.ok(document.querySelector('nav[aria-label="未分组观察任务"]').textContent.includes('Flat red'))
  assert.ok(document.body.textContent.includes('会话及后代数量不可判断'))
})

test('known flat running actors remain counted gray when unobservable while ended and directory-only objects stay out of activity totals', async t => {
  const data = await observedTree(t, [])
  const flat = await addFlatSource(data)
  await data.core.ingest(flat.sourceKey, { tasks: ['red', 'flat-clear'].map(id => ({ id, lifecycleId: 'life', title: `Flat ${id}`, status: 'unknown' })) })
  let snapshot = await data.core.snapshot()
  assert.deepEqual(snapshot.tasks.filter(task => task.sourceKey === flat.sourceKey && ['red', 'flat-clear'].includes(task.id)).map(task => task.gs.light), ['gray', 'gray'])
  const f = await mountGS(t, data.service)
  await f.renderOverview()
  const document = f.dom.window.document, header = document.querySelector('[data-gs-overview-counts]')
  assert.ok(header.textContent.includes('会话 0 · 活动代理 2 · 关注 0'))
  assert.deepEqual(counts(header), ['0', '0', '0', '2'])
  await data.core.ingest(flat.sourceKey, { tasks: ['red', 'flat-clear'].map(id => ({ id, lifecycleId: 'life', title: `Flat ${id}`, status: 'disposed' })) })
  snapshot = await data.core.snapshot()
  assert.ok(snapshot.tasks.filter(task => task.sourceKey === flat.sourceKey).every(task => task.gs.light === null))
  await f.tick()
  assert.ok(header.textContent.includes('会话 0 · 活动代理 0 · 关注 0'))
  assert.deepEqual(counts(header), ['0', '0', '0', '0'])
  assert.ok(document.body.textContent.includes('会话及后代数量不可判断'))
})

for (const waiting of [false, true]) test(`cached grouped ${waiting ? 'user-wait' : 'normal'} lights and counts are marked as prior observations before details are opened`, async t => {
  const data = await observedTree(t, ['root', 'gray'])
  await addFlatSource(data)
  if (waiting) await data.core.ingest(data.sourceKey, { events: [{ id: 'approval-open', taskId: 'root', taskLifecycleId: 'life', kind: 'status', capturedAt: data.snapshot.generatedAt, occurredAt: null,
    text: 'Own native approval is open', data: { nativeType: 'approval/asked', approvalId: 'approval-current' } }] })
  const before = await data.core.snapshot(), original = before.tasks.find(task => task.key === data.key('root')).gs
  assert.equal(original.light, 'green')
  assert.equal(original.label, waiting ? '等待你' : '正常')
  let refuse = false
  const read = data.service.snapshot, readSummary = data.service.gsSummary
  data.service.snapshot = async () => { if (refuse) throw new Error('controlled snapshot read refused'); return read() }
  data.service.gsSummary = async sessionId => { if (refuse) throw new Error('controlled summary read refused'); return readSummary(sessionId) }
  const f = await mountGS(t, data.service)
  await f.renderOverview()
  const document = f.dom.window.document, header = document.querySelector('[data-gs-overview-counts]')
  const globalBefore = counts(header)
  assert.deepEqual(globalBefore, ['2', '0', '1', '1'])
  assert.equal(document.querySelector('[data-gs-detail]'), null)
  refuse = true
  await f.tick()
  assert.ok(header.textContent.includes('上次读取计数 · 当前无法确认'), 'Global first-screen counts identify their cached observation context')
  assert.deepEqual(counts(header), globalBefore)
  const root = document.querySelector(`[data-gs-node="${data.key('root')}"]`)
  assert.ok(root.textContent.includes(`当前无法确认 · 上次观察：${original.label}`))
  assert.equal(root.querySelector('[data-gs-light]').dataset.gsLight, 'green', 'Cached core lamp is retained as a historical fact')
  const basis = document.querySelector(`[data-gs-basis="${data.key('gray')}"]`)
  assert.ok(basis.textContent.startsWith('上次观察依据：'))
  assert.equal(document.querySelector('[data-gs-detail]'), null, 'History qualifiers are visible without selecting details')
  assert.ok(document.body.textContent.includes('会话及后代数量不可判断'), 'The mixed ungrouped coverage gap remains')
  assert.deepEqual((await data.core.snapshot()).tasks.find(task => task.key === data.key('root')).gs, original, 'A read failure never rewrites the public core light or reason')
  await f.render('root')
  const entry = document.querySelector('[data-gs-entry]')
  assert.ok(entry.textContent.includes('上次读取'))
  assert.deepEqual(counts(entry), ['1', '0', '0', '1'])
  await f.click(entry)
  const popup = document.querySelector('[role=dialog]')
  assert.ok(popup.querySelector('[data-gs-count-context]').textContent.includes('上次读取计数 · 当前无法确认'))
  assert.ok(popup.querySelector(`[data-gs-node="${data.key('root')}"]`).textContent.includes(`当前无法确认 · 上次观察：${original.label}`))
  assert.ok(popup.querySelector(`[data-gs-basis="${data.key('gray')}"]`).textContent.startsWith('上次观察依据：'))
  assert.equal(popup.querySelector('[data-gs-detail]'), null)
  await f.render('not-observed')
  assert.equal(document.querySelector('[role=dialog]'), null)
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['0', '0', '0', '0'])
  await f.click(document.querySelector('[data-gs-entry]'))
  assert.equal(document.querySelectorAll('[data-gs-node]').length, 0, 'A session switch never presents the previous object as current')
  await f.render('root')
  await f.click(document.querySelector('[data-gs-entry]'))
  refuse = false
  await f.tick()
  const fresh = document.querySelector('[role=dialog]')
  assert.equal(fresh.querySelector('[data-gs-count-context]'), null)
  assert.ok(!fresh.querySelector(`[data-gs-node="${data.key('root')}"]`).textContent.includes('上次观察'))
  assert.ok(!fresh.querySelector(`[data-gs-basis="${data.key('gray')}"]`).textContent.includes('上次观察依据'))
  assert.deepEqual(counts(fresh), ['1', '0', '0', '1'])
  await f.renderOverview()
  assert.equal(document.querySelector('[data-gs-overview-counts] [data-gs-count-context]'), null)
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), globalBefore)
})

test('cached normal-only filter empty states describe the previous observation while current scope is unconfirmed', async t => {
  const data = await observedTree(t, ['root'])
  let refuse = false
  const read = data.service.snapshot
  data.service.snapshot = async () => { if (refuse) throw new Error('controlled normal-only snapshot refused'); return read() }
  const f = await mountGS(t, data.service), document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count=green]')?.textContent === '1', 'Production green scope loaded')
  await f.click(document.querySelector('[data-gs-entry]'))
  refuse = true
  await f.tick()
  await f.click(document.querySelector('[role=dialog] [data-gs-hide-normal]'))
  assert.equal(document.querySelector('[data-gs-empty]').textContent, '上次观察中没有需要关注的节点；当前无法确认。')
  assert.deepEqual(counts(document.querySelector('[role=dialog]')), ['1', '0', '0', '0'])
  await f.renderOverview()
  await f.click(document.querySelector('[data-gs-hide-normal]'))
  assert.equal(document.querySelector('[data-gs-empty]').textContent, '上次观察中没有需要关注的节点；当前无法确认。')
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), ['1', '0', '0', '0'])
  refuse = false
  await f.tick()
  assert.equal(document.querySelector('[data-gs-empty]').textContent, '没有需要关注的节点。')
})

test('actual GS hides only normal display paths and keeps core counts and each problem basis under its owner', async t => {
  const data = await observedTree(t)
  assert.deepEqual(data.snapshot.hierarchyContext.groups[0].counts, { green: 4, yellow: 1, red: 1, gray: 1 })
  const f = await mountGS(t, data.service)
  const document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count="red"]')?.textContent === '1', 'Production projection reaches native client')
  await f.click(document.querySelector('[data-gs-entry]'))
  const popup = document.querySelector('[role="dialog"]')
  const filter = popup.querySelector('[data-gs-hide-normal]')
  assert.ok(filter, 'The current GS provides hide normal')
  assert.equal(filter.checked, false)
  const normalChild = popup.querySelector(`[data-gs-node="${data.key('normal-child')}"]`)
  assert.equal(shown(normalChild), false, 'A pure normal branch starts collapsed')
  const red = popup.querySelector(`[data-gs-node="${data.key('red')}"]`)
  assert.equal(shown(red), true, 'The deep red path starts expanded through its green ancestor')
  const row = red.closest('[data-gs-row]')
  assert.ok(row.querySelector('[data-gs-basis]').textContent.includes('red own basis'))
  assert.ok(!row.querySelector('[data-gs-basis]').textContent.includes('gray own basis'))
  await f.click(red)
  assert.ok(row.querySelector('[data-gs-detail]'), 'Selected details stay directly under their own node')
  const before = counts(popup)
  const coreBefore = await data.core.snapshot()
  const claimsBefore = data.claims()
  await f.click(filter)
  assert.equal(popup.querySelector(`[data-gs-node="${data.key('normal')}"]`), null)
  const path = popup.querySelector(`[data-gs-path="${data.key('deep')}"]`)
  assert.ok(path, 'The green ancestor remains a path')
  assert.equal(path.querySelector('[data-gs-basis]'), null)
  assert.equal(path.querySelector('[data-gs-detail]'), null)
  assert.deepEqual(counts(popup), before)
  assert.deepEqual((await data.core.snapshot()).hierarchyContext.groups[0].counts, coreBefore.hierarchyContext.groups[0].counts)
  assert.deepEqual((await data.core.snapshot()).tasks.map(task => [task.analysis.state, task.events, task.alerts]), coreBefore.tasks.map(task => [task.analysis.state, task.events, task.alerts]))
  assert.equal(data.claims(), claimsBefore, 'Filtering does not request or alter notification delivery')
  assert.equal(document.getElementById('draft').value, 'keep draft')
})

test('actual global overview groups production sessions, keeps normal summaries closed and locates problems without changing the chat', async t => {
  const data = await observedTree(t, ['normal-root', 'normal-leaf', 'root', 'deep', 'red', 'yellow', 'gray', 'history'])
  const f = await mountGS(t, data.service)
  await f.renderOverview()
  const document = f.dom.window.document
  await f.until(() => document.querySelectorAll('[data-sentinel-session-group]').length === 2, 'Both production groups render')
  const groups = [...document.querySelectorAll('[data-sentinel-session-group]')]
  assert.ok(groups[0].querySelector('summary').textContent.includes('Name root'), 'Problem session appears before the normal session')
  assert.ok(document.querySelector('[data-gs-overview-counts]').textContent.includes('会话 2 · 活动代理 7 · 关注 1'))
  assert.equal(groups[1].querySelector('[data-gs-group-tree]').open, false, 'Normal session shows only its summary')
  assert.equal(groups[0].querySelector('[data-gs-history]').open, false)
  const red = groups[0].querySelector(`[data-gs-node="${data.key('red')}"]`)
  await f.click(red)
  assert.ok(red.closest('[data-gs-row]').querySelector('[data-gs-detail]'))
  assert.equal(document.getElementById('draft').value, 'keep draft')
  const allCounts = counts(document.querySelector('.sentinel-overview'))
  const filter = document.querySelector('[data-gs-hide-normal]')
  assert.equal(filter.checked, false)
  await f.click(filter)
  assert.equal(document.querySelectorAll('[data-sentinel-session-group]').length, 1)
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), allCounts.slice(0, 4))
  assert.equal(document.querySelector('[data-gs-settings]').open, false)
  assert.equal(red.closest('[data-gs-row]').querySelector('[data-gs-diagnostics]').open, false)
  await f.render('root')
  assert.equal(document.querySelector('[data-gs-entry]').dataset.gsEntry, 'root', 'Global selection never rebinds the current session')
})

test('normal-only and empty production scopes keep zero counts and show short filter empty states', async t => {
  const data = await observedTree(t, ['root', 'normal', 'normal-child'])
  const f = await mountGS(t, data.service)
  const document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count="green"]')?.textContent === '3', 'Normal production scope loaded')
  await f.click(document.querySelector('[data-gs-entry]'))
  let popup = document.querySelector('[role=dialog]')
  assert.deepEqual(counts(popup), ['3', '0', '0', '0'])
  await f.click(popup.querySelector('[data-gs-hide-normal]'))
  assert.equal(popup.querySelectorAll('[data-gs-node]').length, 0)
  assert.equal(popup.querySelector('[data-gs-empty]').textContent, '没有需要关注的节点。')
  assert.deepEqual(counts(popup), ['3', '0', '0', '0'])
  await f.renderOverview()
  assert.equal(document.querySelector('[data-gs-hide-normal]').checked, false, 'Each view starts with its own normal filter off')
  await f.click(document.querySelector('[data-gs-hide-normal]'))
  assert.equal(document.querySelectorAll('[data-sentinel-session-group]').length, 0)
  assert.equal(document.querySelector('[data-gs-empty]').textContent, '没有需要关注的节点。')
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), ['3', '0', '0', '0'])
  await f.render('not-observed')
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['0', '0', '0', '0'])
  await f.click(document.querySelector('[data-gs-entry]'))
  popup = document.querySelector('[role=dialog]')
  assert.ok(popup.textContent.includes('层级材料尚未取得'))
  assert.equal(popup.querySelectorAll('[data-gs-node]').length, 0, 'Unknown scope never flashes a different session tree')
})

test('disconnected production nodes share one source summary while history stays collapsed and available', async t => {
  const data = await observedTree(t)
  const f = await mountGS(t, data.service)
  const document = f.dom.window.document
  await data.core.unregisterSource(data.sourceKey)
  await f.tick()
  await f.click(document.querySelector('[data-gs-entry]'))
  let popup = document.querySelector('[role=dialog]')
  assert.equal(popup.querySelectorAll('[data-gs-disconnected]').length, 1)
  assert.ok([...popup.querySelectorAll('[data-gs-basis]')].every(node => !node.textContent.includes('来源失联')), 'The identical source-loss reason is summarized once')
  assert.deepEqual(counts(popup), ['0', '0', '0', '7'])
  const history = popup.querySelector('[data-gs-history]')
  assert.equal(history.open, false)
  await f.click(history.querySelector('summary'))
  assert.equal(history.open, true)
  assert.ok(history.querySelector(`[data-gs-node="${data.key('history')}"]`))
  await f.renderOverview()
  assert.equal(document.querySelectorAll('[data-gs-disconnected]').length, 1)
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), ['0', '0', '0', '7'])
  assert.ok(!document.body.textContent.includes('未知后代 0'), 'Unknown branches are never given an estimated count')
})

test('actual popup keyboard dismissal, text lamp labels and compact styles preserve the composer across session switches', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red', 'normal-root', 'normal-leaf'])
  const f = await mountGS(t, data.service)
  const document = f.dom.window.document, draft = document.getElementById('draft')
  await f.until(() => document.querySelector('[data-gs-count="red"]')?.textContent === '1', 'Mixed production scope loaded')
  const entry = document.querySelector('[data-gs-entry]')
  entry.focus()
  await f.click(entry)
  const popup = document.querySelector('[role=dialog]')
  assert.equal(document.activeElement.getAttribute('aria-label'), '关闭 GS')
  const red = popup.querySelector(`[data-gs-node="${data.key('red')}"]`), row = red.closest('[data-gs-row]')
  assert.equal(f.dom.window.getComputedStyle(red.querySelector('.sentinel-node-name')).fontSize, '13px')
  assert.equal(f.dom.window.getComputedStyle(row.querySelector('[data-gs-basis]')).fontSize, '12px')
  assert.equal(f.dom.window.getComputedStyle(row.querySelector('[data-gs-basis]')).opacity, '1')
  assert.equal(f.dom.window.getComputedStyle(popup).position, 'absolute', 'Popup does not become a composer flow row')
  assert.equal(f.dom.window.getComputedStyle(popup).boxSizing, 'border-box')
  entry.parentElement.getBoundingClientRect = () => ({ left: 20, right: 90 })
  popup.getBoundingClientRect = () => ({ left: -200, right: 160 })
  await f.resize()
  assert.equal(popup.style.left, '-12px', 'A left-edge anchor positions the popup at the eight-pixel viewport margin')
  for (const [color, label] of Object.entries({ green: '正常', yellow: '观察', red: '关注', gray: '不可判断' })) {
    assert.ok(popup.querySelector(`[data-gs-count="${color}"]`).parentElement.getAttribute('aria-label').startsWith(label))
  }
  await f.key('Escape')
  assert.equal(document.querySelector('[role=dialog]'), null)
  assert.equal(document.activeElement, entry)
  await f.click(entry)
  await f.render('normal-root')
  assert.equal(document.querySelector('[role=dialog]'), null, 'Session switching closes the previous scoped popup')
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['2', '0', '0', '0'])
  await f.click(document.querySelector('[data-gs-entry]'))
  assert.equal(document.querySelector(`[data-gs-node="${data.key('red')}"]`), null)
  assert.equal(document.getElementById('draft'), draft)
  assert.equal(draft.value, 'keep draft')
  await f.pointer(document.body)
  assert.equal(document.querySelector('[role=dialog]'), null)
})

test('an empty production core renders a zero-count global overview and an unknown current scope without guessed nodes', async t => {
  const data = await observedTree(t, [])
  const f = await mountGS(t, data.service)
  const document = f.dom.window.document
  await f.renderOverview()
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), ['0', '0', '0', '0'])
  assert.ok(document.querySelector('[data-gs-overview-counts]').textContent.includes('会话 0 · 活动代理 0 · 关注 0'))
  assert.equal(document.querySelector('[data-gs-empty]').textContent, '目前没有活动会话。')
  await f.render('root')
  await f.click(document.querySelector('[data-gs-entry]'))
  assert.deepEqual(counts(document.querySelector('[role=dialog]')), ['0', '0', '0', '0'])
  assert.equal(document.querySelectorAll('[data-gs-node]').length, 0)
})

test('an ended production ancestor remains a path while its own history can be inspected without rebinding the child', async t => {
  const data = await observedTree(t, ['root', 'deep', 'red'], new Set(['root']))
  const f = await mountGS(t, data.service, 'deep')
  const document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count="red"]')?.textContent === '1', 'Production child subtree loaded')
  await f.click(document.querySelector('[data-gs-entry]'))
  await f.click(document.querySelector('[data-gs-return-root]'))
  const path = document.querySelector(`[data-gs-path="${data.key('root')}"]`)
  const history = path.querySelector('[data-gs-history]')
  assert.equal(history.open, false)
  const historicalNode = path.querySelector('[data-gs-node]')
  assert.equal(shown(historicalNode), false)
  await f.click(history.querySelector('summary'))
  await f.click(historicalNode)
  assert.ok(history.querySelector('[data-gs-detail]').textContent.includes('已结束'))
  assert.equal(document.querySelector('[data-gs-entry]').dataset.gsEntry, 'deep')
  assert.deepEqual(counts(document.querySelector('[data-gs-entry]')), ['1', '0', '1', '0'])
})


test('inactive historical groups with gaps start closed while ended roots with active or unknown descendants remain visible', async t => {
  const group = (key, nodes, runningCount, counts) => ({ key, rootSessionId: nodes[0].sessionId, nodes, runningCount, counts,
    coverage: { complete: false, branchGaps: [{ sessionId: nodes[0].sessionId, reason: `${key} original coverage gap` }] } })
  const groups = [
    group('historical', [{ taskKey: 'old', sessionId: 'old', depth: 0, state: 'ended', light: null }], 0, { green: 0, yellow: 0, red: 0, gray: 0 }),
    group('active', [{ taskKey: 'ended-root', sessionId: 'ended-root', depth: 0, state: 'ended', light: null }, { taskKey: 'active-child', sessionId: 'active-child', depth: 1, state: 'running', light: 'gray' }], 1, { green: 0, yellow: 0, red: 0, gray: 1 }),
    group('uncertain', [{ taskKey: 'uncertain', sessionId: 'uncertain', depth: 0, state: 'unknown', light: 'gray' }], 0, { green: 0, yellow: 0, red: 0, gray: 1 }),
  ]
  const snapshot = { protocol: 1, generatedAt: Date.now(), adapter: { sourceKey: 'own' }, monitoring: { state: 'active' }, sources: [{ key: 'own', status: 'connected', gaps: ['Original source coverage gap'] }],
    hierarchyContext: { schemaVersion: 1, groups, scopes: [] }, tasks: groups.flatMap(item => item.nodes).map(node => ({ key: node.taskKey, id: node.sessionId, sourceKey: 'own', title: node.sessionId,
      status: node.state === 'ended' ? 'idle' : node.state, gs: { light: node.light, label: node.state === 'ended' ? '已结束' : '不可判断' }, analysis: { state: 'unknown', checks: [] } })) }
  const f = await mountGS(t, { snapshot: async () => snapshot, analysisConfig: async () => ({}), claimNotifications: async () => [] })
  await f.renderOverview()
  const document = f.dom.window.document
  await f.until(() => document.querySelectorAll('[data-sentinel-session-group]').length === 3, 'All original groups stay available')
  const details = key => document.querySelector(`[data-sentinel-session-group="${key}"] [data-gs-group-tree]`)
  assert.equal(details('historical').open, false, 'A coverage gap does not automatically expand an inactive historical group')
  assert.ok(details('historical').querySelector('summary').textContent.includes('层级覆盖不完整'))
  assert.equal(details('active').open, true, 'An ended root with an active descendant stays in the active view')
  assert.equal(details('uncertain').open, true, 'Unknown active status is not silently classified as ended')
  assert.ok(document.querySelector('[data-gs-coverage]').textContent.includes('未知后代数量不估算'))
  assert.deepEqual(counts(document.querySelector('[data-gs-overview-counts]')), ['0', '0', '0', '2'])
  await f.click(details('historical').querySelector('summary'))
  assert.equal(details('historical').open, true)
  assert.ok(details('historical').textContent.includes('historical original coverage gap'))
  await f.click(details('historical').querySelector('[data-gs-history] summary'))
  await f.click(details('historical').querySelector('[data-gs-node="old"]'))
  await f.tick()
  assert.equal(details('historical').open, true, 'The selected historical object remains inspectable through refresh')
  assert.ok(details('historical').querySelector('[data-gs-detail]'))
  assert.equal(document.getElementById('draft').value, 'keep draft')
})
