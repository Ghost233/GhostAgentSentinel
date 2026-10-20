import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { clientRequire } from './dsh-native-runtime.mjs'
import { createCore } from '../src/core/index.mjs'
import { mountGS } from './gs-session-client-runtime.mjs'

const { JSDOM } = clientRequire('jsdom'), { act } = clientRequire('react')
const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
test('twenty actual native clients keep summary, tree, notification and connection resources bounded through one hundred UI lifecycles', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-client-pressure-'))
  const dom = new JSDOM('<html><head></head><body><input id="draft" value="keep pressure input"></body></html>', { pretendToBeVisual: true })
  const original = { window: globalThis.window, document: globalThis.document, act: globalThis.IS_REACT_ACT_ENVIRONMENT }
  globalThis.window = dom.window; globalThis.document = dom.window.document; globalThis.IS_REACT_ACT_ENVIRONMENT = true
  let clock = Date.now(), analyses = 0
  const core = await createCore({ dataDir, now: () => clock, leaseMs: 172800000, analyzer: { async analyze(input) {
    analyses++
    return { summary: { currentTask: input.task.currentRequest, currentState: '', recentProgress: '', attention: '' },
      checks: checks.map(id => ({ id, status: id === 'request_drift' ? input.task.id === 'red' ? 'attention' : input.task.id === 'gray' ? 'unknown' : 'clear' : 'unknown', reason: `${input.task.id} own retained reason`,
        evidenceIds: id === 'request_drift' ? ['output'] : [], applicability: { state: id === 'request_drift' ? 'triggered' : 'not-triggered', reason: 'Complete controlled request and action', evidenceIds: ['input'] } })), coverage: input.coverage }
  } } })
  const clients = [], sources = [], samples = [], ids = ['root', 'red', 'gray', 'history']
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const event = (taskId, id, kind, data = {}) => ({ id, taskId, taskLifecycleId: 'life', kind, text: `${taskId} ${id}`, capturedAt: clock, occurredAt: null, data })
  t.after(async () => {
    const errors = []
    for (const f of clients) { try { await f.destroy() } catch (error) { errors.push(error) } }
    try { await core.close() } catch (error) { errors.push(error) }
    globalThis.window = original.window; globalThis.document = original.document; globalThis.IS_REACT_ACT_ENVIRONMENT = original.act
    dom.window.close(); await rm(dataDir, { recursive: true, force: true })
    if (errors.length) throw new AggregateError(errors, 'Native client pressure cleanup failed')
  })
  for (let index = 0; index < 20; index++) {
    const sourceKey = await core.registerSource({ registrationId: `native-client-${index}`, hostRunId: 'owned', name: 'Pressure source' }); sources.push(sourceKey)
    await core.ingest(sourceKey, { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: `${index} ${id}`, status: id === 'history' ? 'disposed' : 'running' })),
      events: ids.flatMap(id => [event(id, 'state', 'status', { nativeType: 'agent/status', status: id === 'history' ? 'disposed' : 'running' }),
        event(id, 'input', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: id === 'root' ? 'unknown' : 'delegated', inherited: false, messageId: `${id}-request` }),
        ...(id === 'history' ? [] : [event(id, 'output', 'output')])]).concat(event('root', 'claim', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'root-request' }), ...ids.slice(1).map(id => event('root', `catalog-${id}`, 'status', { nativeType: 'subagent/catalog', childSessionId: id }))),
      hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })), activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: taskId === 'history' ? 'ended' : 'running', stateRefs: [ref(taskId, 'state')] })),
        relations: ids.slice(1).map(childSessionId => ({ parentSessionId: 'root', childSessionId, kind: 'delegation', evidenceRefs: [ref('root', `catalog-${childSessionId}`)] })),
        instructionBindings: ids.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, 'input'), phase: 'consumed', origin: taskId === 'root' ? 'user-entry' : 'delegated', originRefs: [taskId === 'root' ? ref('root', 'claim') : ref('root', `catalog-${taskId}`)], ancestorLimitRefs: taskId === 'root' ? [] : [ref('root', 'input')] })), coverage: { complete: true, branchGaps: [] } } })
  }
  for (let attempt = 0; attempt < 300; attempt++) {
    if ((await core.snapshot()).tasks.filter(task => task.status === 'running').every(task => task.analysis.state === 'cooldown')) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  const projection = await core.snapshot()
  assert.equal(projection.hierarchyContext.groups.length, 20)
  assert.ok(projection.tasks.filter(task => task.status === 'running').every(task => task.analysis.state === 'cooldown'))
  clock += 3000
  for (const sourceKey of sources) {
    const element = dom.window.document.createElement('div'); dom.window.document.body.appendChild(element)
    clients.push(await mountGS(t, { snapshot: async () => ({ ...await core.snapshot(), adapter: { sourceKey } }), gsSummary: sessionId => core.gsSummary({ sourceKey, sessionId }),
      claimNotifications: () => core.claimNotifications(sourceKey), beginNotificationSubmission: id => core.beginNotificationSubmission(sourceKey, id), ackNotification: (id, result) => core.ackNotification(id, result), analysisConfig: async () => ({}) }, 'root', { dom, element }))
  }
  const idle = async () => {
    const end = Date.now() + 5000
    while (clients.some(f => f.rpcActivity.inFlight || f.plugin.diagnostics().pendingSnapshot || f.plugin.diagnostics().pendingSummary || f.plugin.diagnostics().pendingNotification)) {
      assert.ok(Date.now() < end, 'Actual native client RPCs settle within the controlled fixture deadline')
      await act(async () => { await new Promise(resolve => setTimeout(resolve, 5)) })
    }
  }
  const clickAll = async selector => { await act(async () => { for (const f of clients) f.element.querySelector(selector)?.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true })); await new Promise(resolve => setTimeout(resolve, 5)) }); await idle() }
  const cycle = async index => {
    await clickAll('[data-gs-entry]')
    await clickAll('[data-gs-tab=details]')
    await clickAll('[data-gs-scope-tree]:not([open]) > summary')
    await clickAll('[data-gs-hide-normal]')
    for (const f of clients) assert.ok(f.element.querySelector('[data-gs-path]'), 'Green ancestors remain display paths after filtering')
    await clickAll('[data-gs-light=red]')
    await clickAll('[data-gs-history] summary')
    if (index % 3 === 0) await act(async () => dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true })))
    else if (index % 3 === 1) await act(async () => dom.window.document.body.dispatchEvent(new dom.window.Event('pointerdown', { bubbles: true })))
    else await clickAll('[data-gs-entry]')
    await idle()
    for (const f of clients) { assert.equal(f.plugin.diagnostics().snapshotTimer, 0); assert.equal(f.plugin.diagnostics().summaryTimer, 1); assert.equal(f.element.querySelector('[role=dialog]'), null) }
    for (const f of clients) await f.render('root', true, false)
    for (const f of clients) { assert.equal(f.plugin.diagnostics().listeners, 0); assert.equal(f.plugin.diagnostics().summaryTimer, 0) }
    await act(async () => { for (const f of clients) f.fireTimers(); await new Promise(resolve => setTimeout(resolve, 5)) }); await idle()
    for (const f of clients) await f.render('red', false, true)
    for (const f of clients) { assert.ok(f.element.querySelector('header [data-gs-entry]')); assert.equal(f.element.querySelectorAll('[data-gs-entry]').length, 1) }
    for (const f of clients) await f.reinstall()
    for (const f of clients) await f.render('root', true, true)
    await idle()
    assert.equal(dom.window.document.getElementById('draft').value, 'keep pressure input')
  }
  const sample = async cycleCount => {
    global.gc?.()
    const memory = process.memoryUsage(), coreResources = await core.diagnostics()
    const system = process.platform === 'darwin' ? JSON.parse(execFileSync('python3', [fileURLToPath(new URL('../scripts/memory-footprint.py', import.meta.url)), String(process.pid)], { encoding: 'utf8', timeout: 2000 })) : { metric: 'unavailable on this platform' }
    const state = { cycleCount, rss: memory.rss, heapUsed: memory.heapUsed, system, nativeRPC: { entered: clients.reduce((n, f) => n + f.rpcActivity.entered, 0), finished: clients.reduce((n, f) => n + f.rpcActivity.finished, 0), inFlight: clients.reduce((n, f) => n + f.rpcActivity.inFlight, 0) }, clients: clients.map(f => f.plugin.diagnostics()), core: { runtime: coreResources.runtime, temporary: coreResources.temporary } }
    assert.equal(state.nativeRPC.inFlight, 0); assert.equal(state.nativeRPC.entered, state.nativeRPC.finished)
    for (const value of state.clients) { assert.equal(value.summaryTimer, 1); assert.equal(value.snapshotTimer, 0); assert.equal(value.unfinishedRequests, 0) }
    samples.push(state); console.log(JSON.stringify({ nativeClientCheckpoint: cycleCount, rss: state.rss, heapUsed: state.heapUsed, inFlight: state.nativeRPC.inFlight }))
  }
  await idle()
  for (let index = 0; index < 10; index++) await cycle(index)
  await sample(0)
  const analysisBaseline = analyses, deliveredBaseline = clients.map(f => f.delivered.length)
  assert.ok(deliveredBaseline.every(count => count === 1), 'Each source produces one owned grouped notification')
  for (let index = 1; index <= 100; index++) { await cycle(index); if (index % 20 === 0) await sample(index) }
  assert.equal(analyses, analysisBaseline, 'Unchanged UI refreshes do not create new model work')
  assert.deepEqual(clients.map(f => f.delivered.length), deliveredBaseline, 'Closing, restoring and re-enabling views cannot duplicate the original notification attempt')
  for (const f of clients) await f.destroy()
  assert.equal(dom.window.document.head.querySelectorAll('style').length, 0)
  for (const f of clients) { assert.ok(Object.values(f.plugin.diagnostics()).every(value => value === 0)); assert.equal(f.rpcActivity.inFlight, 0) }
  const report = { runtime: process.version, pid: process.pid, clients: 20, warmupCycles: 10, cycles: 100, gc: Boolean(global.gc), source: 'Actual native SlotCore/Remote/React + production public Core projection; controlled IO and visibility, not installed DSH pixels', samples, final: clients.map(f => ({ client: f.plugin.diagnostics(), nativeRPC: f.rpcActivity })) }
  if (process.env.SENTINEL_UI_RESOURCE_REPORT) { await mkdir(join(process.env.SENTINEL_UI_RESOURCE_REPORT, '..'), { recursive: true }); await writeFile(process.env.SENTINEL_UI_RESOURCE_REPORT, JSON.stringify(report, null, 2)) }
  t.diagnostic(JSON.stringify({ nativeClientPressure: { clients: 20, cycles: 100, analyses, notifications: deliveredBaseline, finalTemporaryZero: true, memoryAssessment: 'Continuous warm-process RSS/heap/macOS physical footprint samples recorded; no invented absolute memory budget' } }))
})
