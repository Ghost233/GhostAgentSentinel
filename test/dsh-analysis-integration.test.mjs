import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createServer } from 'node:http'
import { runInNewContext } from 'node:vm'
import { req, clientRequire, resolveGatewayBrowserClientPath } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { default: LlmRuntime, LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
const h = clientRequire('react').createElement
const { createRoot } = clientRequire('react-dom/client')
const { JSDOM } = clientRequire('jsdom')
const modules = ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(name => req(`@deepseek-ai/${name}`).default)
async function until(predicate, label) {
  const deadline = Date.now() + 5000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 10)) }
}
function assertClearTask(task) { assert.equal(task.analysis.checks.find(check => check.id === 'request_drift')?.status, 'clear') }
function completedUnknown(task, calls) {
  const inputId = task?.instructionContext?.binding?.inputRef?.eventId, analysis = task?.analysis
  const drift = analysis?.checks.find(check => check.id === 'request_drift')
  return !!inputId && analysis.completedAt != null && ['cooldown', 'idle'].includes(analysis.state)
    && ['current', 'reused'].includes(analysis.resultDisposition) && analysis.coverage?.complete === true
    && analysis.error === null && analysis.inputEventIds.includes(inputId) && analysis.through >= Math.max(...task.events.filter(event => event.kind === 'output').map(event => event.capturedAt))
    && drift?.status === 'unknown' && drift.judgmentState === 'insufficient' && drift.reason === 'JEV 返回未知，未将其视为正常或异常'
    && calls.some(call => call.path === '/v1/systemone' && call.body.state.requestId === inputId && call.body.state.request === task.currentRequest)
}

async function cleanupSteps(steps) {
  const errors = []
  for (const step of steps) { try { await step() } catch (error) { errors.push(error) } }
  if (errors.length) throw new AggregateError(errors, 'fixture cleanup failed')
}

async function fixture(t, faults = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-combined-'))
  let server, control, ctx, adapter, models, releaseHeld, notificationOffset = 0
  const streams = new Map(), handles = []
  t.after(() => cleanupSteps([
    () => faults.cleanupFault?.(),
    () => releaseHeld?.(),
    () => { for (const stream of streams.values()) { stream.done = true; stream.wake?.() } },
    ...handles.map(handle => () => handle.dispose()),
    () => adapter?.close(), () => ctx?.fiber.dispose(), () => control?.close(), () => server?.close(),
    async () => { if (models) { models.closeAllConnections(); if (models.listening) await new Promise(resolve => models.close(resolve)) } },
    () => rm(dataDir, { recursive: true, force: true }),
    () => { if (adapter) { assert.equal(adapter.diagnostics().adapter.activeRequests, 0); assert.equal(adapter.diagnostics().adapter.handlers, 0) } },
  ]))
  server = await startServer({ dataDir, cooldownMs: 30, now: () => Date.now() + notificationOffset })
  faults.afterCore?.({ dataDir, server })
  control = await connectCore({ dataDir }); await control.pause()
  ctx = new Context()
  class Actor extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream(options) {
      const pending = { queue: [], done: false }
      streams.set(options.model, pending)
      const abort = () => { pending.done = true; pending.wake?.() }
      options.signal.addEventListener('abort', abort, { once: true })
      try { while (!pending.done) { if (!pending.queue.length) await new Promise(resolve => { pending.wake = resolve }); for (const chunk of pending.queue.splice(0)) yield chunk } }
      finally { options.signal.removeEventListener('abort', abort) }
    }
  }
  await ctx.plugin(LlmRuntime)
  for (const module of modules) await ctx.plugin(module, { agents: [] })
  ctx.sessionProjections.register({ key: 'title', stateVersion: 1, stateSchema: { parse: value => value }, init: () => 'Same task title', apply: value => value, wire: { viewSchema: { parse: value => value }, view: value => value } })
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  ctx.llm.registerAdapter(['fixture'], new Actor())
  adapter = createAdapter(ctx, { dataDir, registrationId: 'combined-host' }, { flushMs: 5, heartbeatMs: 25 })
  await until(() => adapter.connected, 'native adapter connection')
  let mode = 'normal', held
  const modelCalls = []
  models = createServer(async (request, response) => {
    if (request.method === 'GET') return response.writeHead(404, { 'content-type': 'application/json' }).end('{}');
    let raw = ''; for await (const chunk of request) raw += chunk
    const wire = JSON.parse(raw), body = request.url === '/v1/systemone' ? decodeJevRequest(wire) : wire; modelCalls.push({ path: request.url, body })
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') {
      if (mode === 'hold') {
        held = response; releaseHeld = () => response.end(JSON.stringify({ model: 'select', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(wire, { selectedOptionIds: [], gaps: [] })) } }] }))
        return
      }
      response.end(JSON.stringify({ model: 'select', choices: [{ finish_reason: 'stop', message: { content: mode === 'invalid' ? '{"selectedOptionIds":["invalid"],"gaps":[]}' : JSON.stringify(withModelSummary(wire, { selectedOptionIds: [], gaps: [] })) } }] }))
    } else {
      const decision = faults.unknownB && body.state.request.includes('request B') ? 'unknown' : mode === 'unknown' ? 'unknown' : body.state.events.some(event => event.text.includes('unrelated-storage')) ? 'attention' : 'clear'
      response.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? decision : 'unknown', probabilities: { attention: decision === 'attention' ? 1 : 0, clear: decision === 'clear' ? 1 : 0, unknown: decision === 'unknown' ? 1 : 0 } }])) }))
    }
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  await configureInstallationModels(control, 'combined-host', { llm: { baseUrl, model: 'select' }, jev: { baseUrl, model: 'judge' } })
  ctx.tools.register({ name: 'combined_tool', description: 'fixed read-only output', parameters: {}, output: { schema: { type: 'string' }, render: (_, value) => [{ type: 'text', text: value }] }, execute: () => 'fixed-tool-result' })
  return { ctx, adapter, control, modelCalls,
    advanceNotificationWindow() { notificationOffset += 3_000 },
    mode(value) { mode = value }, get held() { return held }, release() { releaseHeld?.() },
    async create(id, request) {
      const handle = await ctx.agents.create({ sessionId: id, agentOptions: { provider: 'fixture', model: id } }); handles.push(handle)
      ctx.sessionProjections.snapshot(handle.agent.session, ['title'])
      handle.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: request }] }))
      await until(() => streams.has(id), 'native actor running')
      return handle
    },
    send(id, text) { const stream = streams.get(id); stream.queue.push({ type: 'text-delta', index: 0, text }); stream.wake?.() },
    finish(id) { const stream = streams.get(id); stream.done = true; stream.wake?.() },
  }
}

async function frontend(t, host, faults = {}) {
  const ctx = new Context(), intervals = new Map(), notices = [], dom = new JSDOM('<html><head></head><body><div id="view"></div></body></html>', { pretendToBeVisual: true })
  const oldWindow = globalThis.window, oldDocument = globalThis.document
  let factory, Panel, root, plugin, visible = true, id = 0, snapshotCalls = 0
  const settle = () => new Promise(resolve => setTimeout(resolve, 30))
  t.after(() => cleanupSteps([
    () => faults.cleanupFault?.(), () => root?.unmount(), () => ctx.fiber.dispose(), settle,
    () => { assert.equal(intervals.size, 0); if (plugin) assert.deepEqual(JSON.parse(JSON.stringify(plugin.diagnostics())), { viewInstances: 0, visibleViewers: 0, snapshotTimer: 0, backgroundTimer: 0, listeners: 0, pendingSnapshot: 0, pendingNotification: 0, callbacks: 0, activeRequests: 0, unfinishedRequests: 0, summaryViewers: 0, summaryTimer: 0, pendingSummary: 0, entryObservers: 0 }) },
    () => { globalThis.window = oldWindow; globalThis.document = oldDocument }, () => dom.window.close(),
  ]))
  globalThis.window = dom.window; globalThis.document = dom.window.document
  faults.afterGlobals?.({ dom })
  ctx.provide('slots', { inject(_, install) { install(); return () => {} }, register(_, component) { Panel = component; return () => {} } })
  ctx.provide('sidebarRightTabs', { register() { return () => {} } })
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  const { Peer } = req('@deepseek-ai/dsh-client-connection')
  const peer = new Peer(host.ctx, { principal: { kind: 'owner' }, interactions: 'none' })
  ctx.effect(() => () => peer.dispose())
  ctx.provide('connection', { registerGenerationSource: () => () => {}, start: () => ({ stop() {} }), rpc: { open: async function *() {}, call: async (_, endpoint, payload) => {
    const method = endpoint.split('/')[1]; if (method === 'snapshot') snapshotCalls++
    const value = await host.ctx.typertGateway.invoke({ namespace: 'sentinel', method, args: JSON.parse(JSON.stringify(payload.args)), peer })
    return { ok: true, value: JSON.parse(JSON.stringify(value)) }
  } } })
  let remoteFactory
  runInNewContext(await readFile(await resolveGatewayBrowserClientPath(), 'utf8'), { window: { crypto: globalThis.crypto, __ModuleLoader__: { load(value) { remoteFactory = value.factory } } }, crypto: globalThis.crypto, setTimeout, clearTimeout, AbortController, AbortSignal, console })
  const remote = remoteFactory(req); await ctx.plugin({ inject: remote.inject, apply: remote.apply })
  class Notification { static permission = 'granted'; constructor(title, options) { notices.push({ title, options }) } }
  runInNewContext(await readFile(new URL('../dsh/client.js', import.meta.url), 'utf8'), { window: { __ModuleLoader__: { load(value) { factory = value.factory } } }, document: dom.window.document, Notification, setInterval(callback) { intervals.set(++id, callback); return id }, clearInterval(key) { intervals.delete(key) }, setTimeout, clearTimeout, AbortController, Date, console })
  plugin = factory(name => name === 'react' ? clientRequire(name) : req(name)); await ctx.plugin({ inject: plugin.inject, apply: plugin.apply })
  const mount = async value => { visible = value; root ??= createRoot(dom.window.document.getElementById('view')); root.render(h(Panel, { useTabInfo: () => ({ tab: { visible } }) })); await settle() }
  await mount(true)
  return { plugin, notices, get snapshotCalls() { return snapshotCalls }, async hide() { await mount(false) }, async closePane() { root?.unmount(); root = undefined; await settle() }, async tick() { for (const callback of [...intervals.values()]) callback(); await settle() } }
}

test('native observation, real HTTP analysis and native Remote deliver isolated alerts while the pane is hidden or closed', async t => {
  const host = await fixture(t), view = await frontend(t, host)
  const a = await host.create('task-a', 'same-title request A'), b = await host.create('task-b', 'same-title request B')
  host.send('task-a', 'unrelated-storage action A'); host.send('task-b', 'requested-log action B')
  const outcome = await host.ctx.tools.execute({ callId: 'call-a', name: 'combined_tool', arguments: {}, agent: a.agent, signal: new AbortController().signal })
  assert.equal(outcome.value, 'fixed-tool-result')
  await until(async () => (await host.control.snapshot()).tasks.length === 2 && (await host.control.snapshot()).tasks.some(task => task.events.some(event => event.text.includes('unrelated-storage'))), 'both native task captures')
  await view.hide(); const reads = view.snapshotCalls
  await host.control.resume()
  await until(async () => (await host.control.snapshot()).tasks.some(task => task.alerts.some(alert => alert.checkId === 'request_drift' && alert.status === 'attention')), 'grounded native semantic alert')
  await until(async () => {
    const state = await host.control.snapshot(), taskB = state.tasks.find(task => task.id === 'task-b')
    return host.modelCalls.some(call => call.path === '/v1/systemone' && call.body.state.request === taskB.currentRequest
      && taskB.events.some(event => event.kind === 'input' && event.id === call.body.state.requestId))
      && taskB.analysis.checks.find(check => check.id === 'request_drift')?.status === 'clear'
      && taskB.analysis.through >= Math.max(...taskB.events.filter(event => event.text.includes('action B')).map(event => event.capturedAt))
  }, 'B current request actually judged clear')
  host.advanceNotificationWindow()
  await view.tick(); await until(() => view.notices.length === 1, 'global notification delivery')
  assert.equal(view.snapshotCalls, reads)
  assert.equal(view.plugin.diagnostics().snapshotTimer, 0)
  await view.closePane(); await view.tick(); assert.equal(view.snapshotCalls, reads)
  const state = await host.control.snapshot(), ta = state.tasks.find(task => task.id === 'task-a'), tb = state.tasks.find(task => task.id === 'task-b')
  assert.equal(ta.title, tb.title)
  assert.notEqual(ta.lifecycleId, tb.lifecycleId)
  assert.ok(ta.alerts.some(alert => alert.notification?.status === 'submitted'))
  assertClearTask(tb)
  assert.ok(tb.alerts.every(alert => alert.status !== 'attention' && alert.notification?.status !== 'submitted'))
  assert.ok(ta.events.every(event => !event.text.includes('action B'))); assert.ok(tb.events.every(event => !event.text.includes('action A')))
  assert.ok(host.modelCalls.some(call => call.path === '/v1/systemone'))
  host.finish('task-a'); await until(() => a.agent.status === 'idle', 'first A execution finishes naturally')
  await host.control.pause()
  a.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'A new request while the pane is closed' }] }))
  await until(() => a.agent.status === 'running', 'second native A execution')
  await until(async () => (await host.control.snapshot()).tasks.find(task => task.id === 'task-a').currentRequest.includes('A new request'), 'new request collected with pane closed')
  host.send('task-a', 'unrelated-storage second action')
  await until(async () => (await host.control.snapshot()).tasks.find(task => task.id === 'task-a').events.some(event => event.text.includes('second action')), 'closed-pane new activity')
  await host.control.resume()
  await until(async () => (await host.control.snapshot()).tasks.find(task => task.id === 'task-a').alerts.filter(alert => alert.status === 'attention').length > 0, 'new closed-pane analysis')
  host.advanceNotificationWindow()
  await view.tick(); await until(() => view.notices.length === 2, 'notification after pane destruction')
  assert.equal(view.snapshotCalls, reads)
  host.finish('task-a'); host.finish('task-b'); await until(() => a.agent.status === 'idle' && b.agent.status === 'idle', 'natural completion')
})

test('unknown or failed native analysis does not notify and cancellation seals late results', async t => {
  const host = await fixture(t), view = await frontend(t, host), actor = await host.create('task-error', 'same-title request')
  host.send('task-error', 'unrelated-storage activity')
  await until(async () => (await host.control.snapshot()).tasks.some(task => task.events.some(event => event.kind === 'output')), 'native output capture')
  host.mode('unknown'); await host.control.resume()
  await until(async () => completedUnknown((await host.control.snapshot()).tasks.find(task => task.id === 'task-error'), host.modelCalls), 'unknown analysis complete')
  await view.tick(); assert.equal(view.notices.length, 0)
  host.mode('invalid'); await configureInstallationModels(host.control, 'combined-host', (await host.control.installationAnalysisConfig('combined-host')).config)
  await until(async () => (await host.control.snapshot()).tasks.some(task => task.analysis.state === 'failed'), 'invalid selection rejected')
  await view.tick(); assert.equal(view.notices.length, 0)
  host.mode('hold'); await configureInstallationModels(host.control, 'combined-host', (await host.control.installationAnalysisConfig('combined-host')).config)
  await until(() => !!host.held, 'held real analysis request')
  await host.control.pause()
  const sealed = (await host.control.snapshot()).tasks[0].analysis
  host.release(); await new Promise(resolve => setTimeout(resolve, 50))
  const state = await host.control.snapshot()
  assert.deepEqual(state.tasks[0].analysis, sealed)
  assert.ok(state.tasks.every(task => task.analysis.checks.every(check => check.status !== 'attention')))
  await view.tick(); assert.equal(view.notices.length, 0)
  host.finish('task-error'); await until(() => actor.agent.status === 'idle', 'natural error task finish')
})

test('partial Host setup registers cleanup immediately and still releases resources after a cleanup error', async () => {
  let cleanup, captured
  const setupError = new Error('intentional partial Host setup'), cleanupError = new Error('intentional Host cleanup error')
  await assert.rejects(fixture({ after(fn) { cleanup = fn } }, { afterCore(state) { captured = state; throw setupError }, cleanupFault() { throw cleanupError } }), error => error === setupError)
  assert.equal(typeof cleanup, 'function')
  await assert.rejects(cleanup(), error => error instanceof AggregateError && error.errors.includes(cleanupError))
  await assert.rejects(stat(captured.dataDir), error => error.code === 'ENOENT')
  await assert.rejects(fetch(`${captured.server.url}/v1/health`))
})

test('partial frontend setup restores globals and closes its DOM despite cleanup failure', async () => {
  const oldWindow = globalThis.window, oldDocument = globalThis.document
  let cleanup, captured
  const setupError = new Error('intentional frontend setup'), cleanupError = new Error('intentional frontend cleanup error')
  await assert.rejects(frontend({ after(fn) { cleanup = fn } }, null, { afterGlobals(state) { captured = state; throw setupError }, cleanupFault() { throw cleanupError } }), error => error === setupError)
  assert.equal(typeof cleanup, 'function')
  await assert.rejects(cleanup(), error => error instanceof AggregateError && error.errors.includes(cleanupError))
  assert.equal(globalThis.window, oldWindow); assert.equal(globalThis.document, oldDocument)
  assert.equal(captured.dom.window.document, undefined)
})

test('an unknown B decision is rejected by the same-name task-clear acceptance assertion', async t => {
  const host = await fixture(t, { unknownB: true })
  const b = await host.create('task-b', 'same-title request B')
  host.send('task-b', 'requested-log action B')
  await until(async () => (await host.control.snapshot()).tasks.some(task => task.events.some(event => event.text.includes('action B'))), 'B native activity')
  await host.control.resume()
  await until(() => host.modelCalls.some(call => call.path === '/v1/systemone' && call.body.state.request.includes('request B')), 'B actual JEV invocation')
  await until(async () => completedUnknown((await host.control.snapshot()).tasks.find(task => task.id === 'task-b'), host.modelCalls), 'B completed result')
  const task = (await host.control.snapshot()).tasks.find(task => task.id === 'task-b')
  assert.throws(() => assertClearTask(task), assert.AssertionError)
  host.finish('task-b'); await until(() => b.agent.status === 'idle', 'B finishes naturally')
})

test('a current unknown completion remains observable after its short cooldown becomes idle reuse', async t => {
  const host = await fixture(t), actor = await host.create('task-error', 'same-title request')
  host.send('task-error', 'unrelated-storage activity')
  await until(async () => (await host.control.snapshot()).tasks.some(task => task.events.some(event => event.kind === 'output')), 'native output capture')
  host.mode('unknown'); await host.control.resume()
  let before, after
  t.after(async () => { if (process.env.SENTINEL_COMPLETION_PAIR_RECEIPT) await writeFile(process.env.SENTINEL_COMPLETION_PAIR_RECEIPT, JSON.stringify({ scope: 'public controlled unknown completion diagnostic; actual SDK text output, no native tool/call path', before, after, modelCalls: host.modelCalls, originalCooldownOnlyPredicateAfter: after?.analysis.state === 'cooldown', originalWaitMs: 5000 }, null, 2)) })
  await until(async () => { before = (await host.control.snapshot()).tasks.find(task => task.id === 'task-error'); return completedUnknown(before, host.modelCalls) }, 'current unknown completion exists')
  const calls = host.modelCalls.length
  await until(async () => { after = (await host.control.snapshot()).tasks.find(task => task.id === 'task-error'); return Date.now() >= before.analysis.nextEligibleAt && after.analysis.state === 'idle' }, 'public completion remains after cooldown eligibility')
  assert.equal(after.analysis.state === 'cooldown', false, 'old transient-only predicate rejects this valid later read')
  assert.equal(completedUnknown(after, host.modelCalls), true)
  assert.equal(after.analysis.completedAt, before.analysis.completedAt)
  assert.equal(after.analysis.through, before.analysis.through)
  assert.equal(host.modelCalls.length, calls, 'unchanged input is reused without another model call')
  assert.ok(after.alerts.every(alert => alert.status !== 'attention'))
  host.finish('task-error'); await until(() => actor.agent.status === 'idle', 'natural unknown task finish')
})
