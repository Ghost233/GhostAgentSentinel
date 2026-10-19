import { req, clientRequire, resolveGatewayBrowserClientPath } from './dsh-native-runtime.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { runInNewContext } from 'node:vm'
import descriptors from '../dsh/remote.cjs'
import { createCore } from '../src/core/index.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { createServer } from 'node:http'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const { Context } = req('@deepseek-ai/cordis')
const { createElement: h, act } = clientRequire('react')
const { renderToStaticMarkup } = clientRequire('react-dom/server')
const { createRoot } = clientRequire('react-dom/client')
const { JSDOM } = clientRequire('jsdom')

async function client(t, snapshot, permission = 'granted', constructorError = null, nativeRemote = false, mountPane = true, overrides = {}) {
  const ctx = new Context(), registered = [], delivered = [], acknowledged = [], invoked = []
  let factory, mounted, Panel, plugin, viewRoot, clientFiber
  const intervals = new Map(); let intervalId = 0
  const dom = new JSDOM('<html><head></head><body><div id="view"></div></body></html>', { pretendToBeVisual: true })
  const originalWindow = globalThis.window, originalDocument = globalThis.document
  globalThis.window = dom.window; globalThis.document = dom.window.document
  const tabController = new AbortController(), tabSignal = tabController.signal
  let visible = true
  const useTabInfo = () => ({ tab: { visible, signal: tabSignal } })
  const settle = async () => { await new Promise(resolve => setTimeout(resolve, 20)) }
  const waitUntil = async (predicate, message) => {
    const deadline = Date.now() + 1000
    while (!predicate()) { assert.ok(Date.now() < deadline, message); await settle() }
  }
  const commit = async action => {
    const previous = globalThis.IS_REACT_ACT_ENVIRONMENT
    globalThis.IS_REACT_ACT_ENVIRONMENT = true
    try { await act(action) } finally { globalThis.IS_REACT_ACT_ENVIRONMENT = previous }
  }
  const dataReady = async () => {
    await waitUntil(() => plugin.diagnostics().pendingSnapshot === 0 && plugin.diagnostics().pendingNotification === 0
      && dom.window.document.body.textContent.includes('保存分析连接'), 'Actual overview snapshot/configuration did not finish rendering')
    await commit(async () => {})
  }
  const mount = async (nextVisible = true, count = 1) => {
    visible = nextVisible
    if (!viewRoot) viewRoot = createRoot(document.getElementById('view'))
    await commit(async () => { viewRoot.render(h('div', null, ...Array.from({ length: count }, (_, index) => h(Panel, { useTabInfo, key: index })))) })
    await waitUntil(() => plugin.diagnostics().viewInstances === count
      && plugin.diagnostics().visibleViewers === (nextVisible && document.visibilityState !== 'hidden' ? count : 0), 'Actual sidebar viewer effects did not commit')
  }
  const unmount = async () => {
    await commit(async () => { viewRoot?.unmount(); viewRoot = undefined })
    if (plugin) await waitUntil(() => plugin.diagnostics().viewInstances === 0 && plugin.diagnostics().pendingSnapshot === 0, 'Actual sidebar unmount did not release its viewers and snapshot')
  }
  t.after(async () => {
    const errors = []
    try {
      for (const operation of [
        unmount,
        () => ctx.fiber.dispose(),
        () => plugin && waitUntil(() => plugin.diagnostics().activeRequests === 0 && plugin.diagnostics().pendingNotification === 0, 'Disposed connection still owns requests'),
        () => assert.equal(intervals.size, 0),
        () => assert.equal(dom.window.document.head.querySelectorAll('style').length, 0),
        () => { if (plugin) assert.deepEqual(JSON.parse(JSON.stringify(plugin.diagnostics())), { viewInstances: 0, visibleViewers: 0, snapshotTimer: 0, backgroundTimer: 0, listeners: 0, pendingSnapshot: 0, pendingNotification: 0, callbacks: 0, activeRequests: 0, unfinishedRequests: 0, summaryViewers: 0, summaryTimer: 0, pendingSummary: 0, entryObservers: 0 }) },
      ]) { try { await operation() } catch (error) { errors.push(error) } }
    } finally {
      globalThis.window = originalWindow; globalThis.document = originalDocument
      try { dom.window.close() } catch (error) { errors.push(error) }
    }
    if (errors.length) throw new AggregateError(errors, 'Client fixture cleanup failed')
  })
  class Notification {
    static permission = permission
    static async requestPermission() { Notification.permission = 'granted'; return 'granted' }
    constructor(title, options) { if (constructorError) throw constructorError; delivered.push({ title, options, instance: this }) }
  }
  const methods = {
    async snapshot() { invoked.push('snapshot'); return { ok: true, value: snapshot } },
    async claimNotifications() { invoked.push('claim'); return { ok: true, value: [{ id: 'notice-one', taskKey: 'task-one', title: '关注任务', body: '有依据的疑点' }] } },
    async ackNotification(id, result) { acknowledged.push({ id, result }); return { ok: true, value: {} } },
    async analysisConfig() { return { ok: true, value: {} } },
    async analysisConfigState() { return { ok: true, value: { registrationId: 'fixture', config: {}, revision: 'fixture-revision' } } },
    async pause() { invoked.push('pause'); return { ok: true, value: {} } },
    async resume() { invoked.push('resume'); return { ok: true, value: {} } },
    async configureAnalysis() { return { ok: true, value: {} } },
    async configureAnalysisIfCurrent(config) { return { ok: true, value: { registrationId: 'fixture', config, revision: 'fixture-saved' } } },
    async sourcePlan() { return { ok: true, value: snapshot.enrollment || { entries: [] } } },
    async configureSources(entries) { invoked.push('configureSources'); return { ok: true, value: { entries } } },
    async diagnostics() { return { ok: true, value: {} } },
  }
  Object.assign(methods, overrides)
  ctx.provide('slots', {
    inject(_name, install) { install(); return () => {} },
    register(_options, component) { Panel = component; return () => {} },
  })
  ctx.provide('sidebarRightTabs', { register(value) { registered.push(value); return () => {} } })
  if (nativeRemote) {
    await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
    ctx.provide('connection', {
      registerGenerationSource: () => () => {}, start: () => ({ stop() {} }),
      rpc: { open: async function *() {}, call: async (_route, endpoint, payload, signal) => {
        if (overrides.$dispatch) return overrides.$dispatch(endpoint, payload, signal)
        const method = endpoint.split('/')[1]
        const descriptor = descriptors().find(item => item.method === method)
        return methods[method](...descriptor.parameters.map(parameter => payload.args[parameter.wire]), signal)
      } },
    })
    let remoteFactory
    runInNewContext(await readFile(await resolveGatewayBrowserClientPath(), 'utf8'), {
      window: { crypto: globalThis.crypto, __ModuleLoader__: { load(value) { remoteFactory = value.factory } } },
      crypto: globalThis.crypto, setTimeout, clearTimeout, AbortController, AbortSignal, console,
    })
    const remotePlugin = remoteFactory(req)
    await ctx.plugin({ inject: remotePlugin.inject, apply: remotePlugin.apply })
  } else {
    ctx.provide('typert', {})
    ctx.provide('remote', { sentinel: methods, async $mount(value) { mounted = value; return () => {} } })
    ctx.provide('remote.sentinel', methods)
  }
  runInNewContext(await readFile(new URL('../dsh/client.js', import.meta.url), 'utf8'), {
    window: { __ModuleLoader__: { load(value) { factory = value.factory } } },
    document: dom.window.document,
    Notification, setInterval(callback) { const key = ++intervalId; intervals.set(key, callback); return key }, clearInterval(key) { intervals.delete(key) }, setTimeout, clearTimeout, AbortController, Date, console,
  })
  plugin = factory(name => name === 'react' ? clientRequire(name) : req(name))
  clientFiber = ctx.plugin({ inject: plugin.inject, apply: plugin.apply })
  await clientFiber.await()
  await waitUntil(() => plugin.diagnostics().backgroundTimer === 1, 'Actual Remote connection did not start background monitoring')
  if (mountPane) { await mount(); await dataReady() }
  else if (!overrides.claimNotifications) await waitUntil(() => plugin.diagnostics().pendingNotification === 0, 'Initial notification claim did not finish')
  return { ctx, Panel, mounted, registered, delivered, acknowledged, invoked, Notification, plugin, intervals, dom, mount, unmount, settle, waitUntil, dataReady, interact: commit,
    async tick() { await commit(async () => { for (const callback of [...intervals.values()]) callback(); await settle() }) },
    async visibility(value) { await commit(async () => { Object.defineProperty(dom.window.document, 'visibilityState', { value, configurable: true }); dom.window.document.dispatchEvent(new dom.window.Event('visibilitychange')); await settle() }) },
    async abortView() { await commit(async () => { tabController.abort(); await settle() }) },
    async dispose() { await ctx.fiber.dispose(); await waitUntil(() => plugin.diagnostics().activeRequests === 0 && plugin.diagnostics().pendingSnapshot === 0 && plugin.diagnostics().pendingNotification === 0, 'Disposed connection still owns requests') },
    async reinstall(ready = true) { await commit(async () => { await clientFiber.dispose() }); clientFiber = ctx.plugin({ inject: plugin.inject, apply: plugin.apply }); await clientFiber.await(); await waitUntil(() => plugin.diagnostics().backgroundTimer === 1, 'Replacement Remote connection did not start'); if (ready) await dataReady(); else await settle() },
  }
}

async function waitForDisplayedTask(fixture, title) {
  const deadline = Date.now() + 1000
  while (!fixture.dom.window.document.body.textContent.includes(title)) {
    assert.ok(Date.now() < deadline, `Actual overview did not display ${title}`)
    await fixture.settle()
  }
}

test('the actual DSH settings form saves its installation CAS through Native Remote and drives only its selected models', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-dsh-installation-ui-')), host = new Context(), requests = []
  let server, core, adapter, actor, f, clockOffset = 0
  const models = createServer(async (request, response) => {
    let raw = ''; for await (const bytes of request) raw += bytes
    const body = JSON.parse(raw); requests.push({ path: request.url, model: body.model })
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') {
      const evidence = JSON.parse(body.messages.find(message => message.role === 'user').content).evidence
      const option = evidence.find(entry => entry.origin === 'output')?.options.find(option => option.kind === 'action') ?? evidence[0].options[0]
      const selectedOptionIds = option.kind === 'action' ? [option.optionId] : []
      response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [], summary: { text: 'The current request has observed execution activity.', evidenceOptionIds: [option.optionId] } }) } }] }))
    } else {
      const input = decodeJevRequest(body)
      response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(input.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) }))
    }
  })
  t.after(async () => { await actor?.dispose(); await adapter?.close(); await host.fiber.dispose(); await core?.close(); await server?.close(); models.closeAllConnections(); if (models.listening) await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }) })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  server = await startServer({ dataDir, cooldownMs: 0, now: () => Date.now() + clockOffset }); core = await connectCore({ dataDir })
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  const config = name => ({ llm: { baseUrl, model: name + '-summary' }, jev: { baseUrl, model: name + '-judge' } })
  const formal = await core.configureInstallationAnalysis('chatgpt:formal', config('formal'), (await core.installationAnalysisConfig('chatgpt:formal')).revision)
  const candidate = await core.configureInstallationAnalysis('chatgpt:test', config('test'), (await core.installationAnalysisConfig('chatgpt:test')).revision)
  await host.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await host.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  for (const name of ['dsh-typert-registry', 'dsh-api-remote-access', 'dsh-api-gateway']) await host.plugin(req(`@deepseek-ai/${name}`).default)
  const { LlmAdapter, createUserMessage } = req('@deepseek-ai/dsh-llm')
  class NativeModel extends LlmAdapter {
    async resolveModel(provider, id) { return { provider, id, name: id } }
    async *stream({ signal }) { yield { type: 'text-delta', index: 0, text: 'Inspecting the DSH request evidence' }; if (!signal.aborted) await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })) }
  }
  host.llm.registerAdapter(['isolation-fixture'], new NativeModel())
  adapter = createAdapter(host, { dataDir, registrationId: 'dsh:stable' }, { flushMs: 10, heartbeatMs: 1000 })
  const { Peer } = req('@deepseek-ai/dsh-client-connection'), peer = new Peer(host, { principal: { kind: 'owner' }, interactions: 'none' })
  f = await client(t, null, 'denied', null, true, true, { $dispatch: (endpoint, payload, signal) => host.typertGateway.dispatchRpc(endpoint, JSON.parse(JSON.stringify(payload)), signal, peer) })
  const { Simulate } = clientRequire('react-dom/test-utils'), values = [baseUrl, 'dsh-summary', baseUrl, 'dsh-judge']
  const inputs = [...f.dom.window.document.querySelectorAll('input')].filter(input => input.type !== 'checkbox')
  assert.equal(inputs.length, 4)
  await f.interact(async () => { for (let index = 0; index < inputs.length; index++) { inputs[index].value = values[index]; Simulate.change(inputs[index]) }; await f.settle() })
  await f.interact(async () => { Simulate.submit(inputs[0].closest('form')); await f.settle() })
  await f.waitUntil(() => f.plugin.diagnostics().activeRequests === 0, 'the actual settings save finishes')
  let saved = await core.installationAnalysisConfig('dsh:stable')
  assert.deepEqual(saved.config, config('dsh'), 'the DSH form must configure its stable installation rather than the shared legacy slot')
  assert.match(inputs[0].closest('form').textContent, /已保存；下一轮采用新配置/)
  await core.configureInstallationAnalysis('dsh:stable', config('dsh'), saved.revision)
  await f.interact(async () => { Simulate.submit(inputs[0].closest('form')); await f.settle() })
  assert.match(inputs[0].closest('form').textContent, /conflict|changed|变化|冲突/i, 'a stale panel save exposes the conflict and preserves its draft')
  assert.equal(inputs[1].value, 'dsh-summary')
  await f.interact(async () => { Simulate.click([...inputs[0].closest('form').querySelectorAll('button')].find(button => button.textContent === '读取当前配置')); await f.settle() })
  await f.interact(async () => { Simulate.submit(inputs[0].closest('form')); await f.settle() })
  saved = await core.installationAnalysisConfig('dsh:stable')
  assert.match(inputs[0].closest('form').textContent, /已保存；下一轮采用新配置/)
  assert.deepEqual(await core.installationAnalysisConfig('chatgpt:formal'), formal)
  assert.deepEqual(await core.installationAnalysisConfig('chatgpt:test'), candidate)
  actor = await host.agents.create({ sessionId: 'dsh-observed', agentOptions: { provider: 'isolation-fixture', model: 'native-agent' } })
  actor.agent.followup(createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect the DSH log only' }] }))
  const deadline = Date.now() + 3000
  while (!requests.some(request => request.model === 'dsh-judge')) { assert.ok(Date.now() < deadline, 'the saved DSH settings reach both actual model HTTP requests'); await f.settle() }
  assert.ok(requests.some(request => request.path === '/v1/chat/completions' && request.model === 'dsh-summary'))
  assert.equal(requests.some(request => request.model.startsWith('formal-') || request.model.startsWith('test-')), false)
  const task = (await core.snapshot()).tasks.find(task => task.id === 'dsh-observed')
  assert.deepEqual(task.analysis.configScope, { registrationId: 'dsh:stable', revision: saved.revision })
  await f.tick()
  await f.interact(async () => { Simulate.click([...f.dom.window.document.querySelectorAll('button')].find(button => button.textContent === '暂停分析')); await f.settle() })
  assert.equal((await core.installationMonitoring('dsh:stable')).paused, true)
  assert.equal((await core.snapshot()).monitoring.state, 'active', 'the DSH pause leaves shared monitoring active')
  const peers = new Map()
  for (const [registrationId, id] of [['chatgpt:formal', 'formal'], ['chatgpt:test', 'test']]) {
    const sourceKey = await core.registerSource({ registrationId, hostRunId: id + '-host', name: id }); peers.set(id, sourceKey)
    await core.ingest(sourceKey, { tasks: [{ id, lifecycleId: 'run', title: id, status: 'running' }], events: [
      { id: id + '-input', kind: 'input', text: 'Inspect ' + id + ' only' }, { id: id + '-output', kind: 'output', text: 'Inspecting ' + id + ' evidence' },
    ].map(event => ({ ...event, taskId: id, taskLifecycleId: 'run', capturedAt: Date.now(), occurredAt: null })) })
  }
  const peersDeadline = Date.now() + 3000
  while (!(await core.snapshot()).tasks.filter(task => peers.has(task.id)).every(task => task.analysis.checks.some(check => check.id === 'request_drift' && check.status === 'attention'))) { assert.ok(Date.now() < peersDeadline, 'both Codex installations continue their own model analyses during the DSH pause'); await f.settle() }
  for (const id of peers.keys()) assert.ok(requests.some(request => request.model === id + '-summary') && requests.some(request => request.model === id + '-judge'))
  clockOffset = 10000
  const snapshot = await core.snapshot()
  for (const [id, sourceKey] of peers) {
    const notices = await core.claimNotifications(sourceKey)
    assert.equal(notices.length, 1, id + ' retains its own notification')
    assert.ok(notices.every(notice => snapshot.tasks.find(task => task.key === notice.taskKey)?.sourceKey === sourceKey))
  }
  assert.deepEqual(await adapter.claimNotifications(), [], 'DSH pause suppresses only the DSH installation notices')
  const sharedInstance = (await core.health()).instanceId
  await adapter.close()
  assert.equal(actor.agent.status, 'running')
  assert.equal((await core.health()).instanceId, sharedInstance)
  assert.deepEqual(await core.installationAnalysisConfig('chatgpt:formal'), formal)
  assert.deepEqual(await core.installationAnalysisConfig('chatgpt:test'), candidate)
  for (const sourceKey of peers.values()) assert.equal((await core.snapshot()).sources.find(source => source.key === sourceKey).status, 'connected')
})

async function publicNotificationClient(t, memberCount, overflow = false) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-consumer-'))
  let time = Date.now(), core
  t.after(async () => { try { await core?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  const sourceName = '来源显示'.repeat(10), ids = ['one', 'two'].slice(0, memberCount)
  const names = { one: '第一代理长名字'.repeat(8), two: '第二代理短名' }
  const reasons = { one: overflow ? '单成员超长原因'.repeat(200) : memberCount === 1 ? '单成员兼容原因' : '核对第一代理请求偏离。'.repeat(12), two: '第二代理必须核对的独立原因' }
  const checkIds = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
  core = await createCore({ dataDir, now: () => time, leaseMs: 172800000, analyzer: { async analyze(input) {
    return { summary: { currentTask: input.task.currentRequest, currentState: '', recentProgress: '', attention: '' },
      checks: checkIds.map(id => ({ id, status: id === 'request_drift' ? 'attention' : 'unknown', reason: id === 'request_drift' ? reasons[input.task.id] : 'Complete scope has no trigger',
        evidenceIds: id === 'request_drift' ? ['output'] : [], applicability: { state: id === 'request_drift' ? 'triggered' : 'not-triggered', reason: 'Controlled complete request and action', evidenceIds: ['input'] } })), coverage: input.coverage }
  } } })
  const sourceKey = await core.registerSource({ registrationId: 'notification-consumer', hostRunId: 'owned', name: sourceName })
  const otherSourceKey = await core.registerSource({ registrationId: 'other-consumer-source', hostRunId: 'owned', name: sourceName })
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const event = (taskId, id, kind, data = {}) => ({ id, taskId, taskLifecycleId: 'life', kind, capturedAt: time, occurredAt: null, text: `${taskId} ${id}`, data })
  await core.ingest(sourceKey, { tasks: [{ id: 'root', lifecycleId: 'life', sessionId: 'root', title: 'Notification root', status: 'idle' }, ...ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: names[id], status: 'running' }))],
    events: [event('root', 'state', 'status', { nativeType: 'agent/status', status: 'idle' }), event('root', 'claim', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'root-request' }),
      event('root', 'input', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'unknown', inherited: false, messageId: 'root-request' }),
      ...ids.flatMap(id => [event('root', `catalog-${id}`, 'status', { nativeType: 'subagent/catalog', childSessionId: id }), event(id, 'state', 'status', { nativeType: 'agent/status', status: 'running' }),
        event(id, 'input', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', inherited: false, messageId: `${id}-request` }), event(id, 'output', 'output')])],
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ['root', ...ids].map(sessionId => ({ sessionId })),
      activations: ['root', ...ids].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: taskId === 'root' ? 'ended' : 'running', stateRefs: [ref(taskId, 'state')] })),
      relations: ids.map(childSessionId => ({ parentSessionId: 'root', childSessionId, kind: 'delegation', evidenceRefs: [ref('root', `catalog-${childSessionId}`)] })),
      instructionBindings: [{ taskId: 'root', lifecycleId: 'life', inputRef: ref('root', 'input'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('root', 'claim')], ancestorLimitRefs: [] },
        ...ids.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, 'input'), phase: 'consumed', origin: 'delegated', originRefs: [ref('root', `catalog-${taskId}`)], ancestorLimitRefs: [ref('root', 'input')] }))], coverage: { complete: true, branchGaps: [] } } })
  let snapshot
  for (let attempt = 0; attempt < 100; attempt++) {
    snapshot = await core.snapshot()
    if (snapshot.tasks.filter(task => ids.includes(task.id)).every(task => task.gs.light === 'red')) break
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.equal(snapshot.tasks.filter(task => task.gs.light === 'red').length, memberCount, 'Public grounded producer alerts settle before delivery')
  time += 3000
  assert.deepEqual(await core.claimNotifications(otherSourceKey), [], 'An unrelated source cannot claim these notifications')
  const claims = [], acknowledgements = [], wireBodies = []
  const f = await client(t, snapshot, 'granted', null, true, false, {
    async claimNotifications() {
      const notices = await core.claimNotifications(sourceKey); claims.push(...notices)
      // Fault injection at external Remote IO checks the consumer limit independently of the bounded producer.
      const wire = overflow ? notices.map(notice => ({ ...notice, body: notice.body + '\ncontrolled transport overflow' })) : notices
      wireBodies.push(...wire.map(notice => notice.body)); return { ok: true, value: wire }
    },
    async ackNotification(id, result) { const receipt = await core.ackNotification(id, result); acknowledgements.push({ id, result, receipt }); return { ok: true, value: receipt } },
  })
  await f.waitUntil(() => f.delivered.length === 1 && acknowledgements.length === 1 && f.plugin.diagnostics().pendingNotification === 0, 'Actual Remote delivers and acknowledges the public core claim')
  return { core, sourceKey, otherSourceKey, sourceName, claims, acknowledgements, wireBodies, f }
}

test('actual native Remote passes both grouped public notification members to the system constructor beyond the first 240 characters', async t => {
  const data = await publicNotificationClient(t, 2), { f } = data
  assert.equal(data.claims.length, 1)
  const notice = data.claims[0], submitted = f.delivered[0].options.body, second = notice.members[1]
  assert.equal(notice.members.length, 2)
  assert.ok(notice.body.length > 240 && notice.body.length <= 1200)
  assert.ok(notice.body.indexOf(second.name) > 240, 'The legal second member starts beyond the old consumer cutoff')
  t.diagnostic(JSON.stringify({ publicClaim: { id: notice.id, taskKey: notice.taskKey, sourceKey: notice.sourceKey, body: notice.body, members: notice.members }, systemConstructor: f.delivered[0].options, acknowledgement: data.acknowledgements[0].receipt }))
  assert.equal(submitted, notice.body, 'The system constructor must receive the complete bounded producer body')
  assert.ok(submitted.includes(second.name) && submitted.includes(second.taskKey.slice(0, 8)) && submitted.includes(second.reasons[0]))
  assert.ok(submitted.slice(0, 240).includes(notice.taskKey) && submitted.slice(0, 240).includes(data.sourceKey))
  assert.ok(submitted.includes(data.sourceName))
  assert.equal(f.delivered[0].options.tag, `sentinel:${notice.id}`)
  assert.equal(data.acknowledgements[0].id, notice.id)
  assert.equal(data.acknowledgements[0].result.status, 'submitted')
  assert.equal(data.acknowledgements[0].receipt.seen, 'unknown')
  await f.tick()
  assert.equal(f.delivered.length, 1); assert.equal(data.acknowledgements.length, 1)
  assert.equal(f.plugin.diagnostics().viewInstances, 0, 'Background delivery does not require a mounted pane')
  const snapshot = await data.core.snapshot()
  for (const member of notice.members) assert.equal(snapshot.tasks.find(task => task.key === member.taskKey).alerts[0].notification.id, notice.id)
})

for (const overflow of [false, true]) test(`actual native Remote keeps single-member notification compatibility${overflow ? ' and caps a transport overflow at 1200 characters' : ''}`, async t => {
  const data = await publicNotificationClient(t, 1, overflow), notice = data.claims[0], submitted = data.f.delivered[0].options.body
  assert.equal(data.claims.length, 1); assert.equal(notice.members.length, 1)
  if (overflow) {
    assert.equal(notice.body.length, 1200, 'The public producer already caps the long legal body')
    assert.ok(data.wireBodies[0].length > 1200, 'Only the controlled external Remote fault exceeds the contract')
    assert.equal(submitted.length, 1200, 'The consumer retains its own existing bounded-body protection')
  } else assert.ok(submitted.includes('单成员兼容原因'))
  assert.equal(submitted, notice.body)
  assert.equal(data.f.delivered[0].title, notice.title)
  assert.ok(submitted.slice(0, 240).includes(notice.taskKey) && submitted.slice(0, 240).includes(data.sourceKey))
  assert.ok(!submitted.includes(data.otherSourceKey))
  assert.equal(data.acknowledgements.length, 1); assert.equal(data.acknowledgements[0].id, notice.id)
  assert.deepEqual(JSON.parse(JSON.stringify(data.acknowledgements[0].result)), { status: 'submitted' })
  assert.equal(data.acknowledgements[0].receipt.seen, 'unknown')
  t.diagnostic(JSON.stringify({ publicClaim: { id: notice.id, sourceKey: notice.sourceKey, bodyLength: notice.body.length }, wireBodyLength: data.wireBodies[0].length, systemConstructor: { title: data.f.delivered[0].title, ...data.f.delivered[0].options }, acknowledgement: data.acknowledgements[0].receipt }))
  await data.f.tick()
  assert.equal(data.f.delivered.length, 1); assert.equal(data.acknowledgements.length, 1)
})

test('a real DOM unmount failure still disposes the Native Context and retains later cleanup errors', async t => {
  let cleanup, f, disposed = false, cleanupAttempted = false
  const originalWindow = globalThis.window, originalDocument = globalThis.document
  t.after(async () => { if (!cleanupAttempted && cleanup) await cleanup() })
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  f = await client({ after(callback) { cleanup = callback } }, snapshot, 'denied', null, true)
  const container = f.dom.window.document.getElementById('view'), removeChild = container.removeChild
  const dispose = f.ctx.fiber.dispose.bind(f.ctx.fiber)
  container.removeChild = () => { throw new Error('injected DOM unmount failure') }
  f.ctx.fiber.dispose = async () => { await dispose(); disposed = true; throw new Error('injected Native cleanup acknowledgement failure') }
  let failure
  try {
    cleanupAttempted = true
    try { await cleanup() } catch (error) { failure = error }
    assert.equal(disposed, true, 'the original unmount failure must not skip real Native disposal')
    assert.ok(failure instanceof AggregateError)
    assert.equal(failure.errors.length, 2, 'resource verification must add no uninjected failures')
    assert.ok(failure.errors.some(error => error.message.includes('injected DOM unmount failure')))
    assert.ok(failure.errors.some(error => error.message.includes('injected Native cleanup acknowledgement failure')))
    assert.equal(f.plugin.diagnostics().backgroundTimer, 0)
    assert.equal(f.plugin.diagnostics().activeRequests, 0)
    assert.equal(f.intervals.size, 0)
    assert.equal(globalThis.window, originalWindow)
    assert.equal(globalThis.document, originalDocument)
  } finally {
    container.removeChild = removeChild
    f.ctx.fiber.dispose = dispose
    await dispose()
  }
})

test('client registers the right overview and submits notifications even when no pane is mounted', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: 'active', adapter: { connected: true, gaps: [] },
    overview: { runningTasks: 1, knownSources: 2, connectedSources: 1, attentionTasks: 1, coverage: 'unknown' }, sources: [],
    tasks: [{ key: 'task-one', title: '并行任务', sourceKey: 'source-one', status: 'running', events: [], analysis: { state: 'queued', checks: [] } }] }
  const f = await client(t, snapshot, 'granted', null, false, false)
  assert.ok(!f.invoked.includes('snapshot'))
  assert.equal(f.registered[0].title(), 'Sentinel')
  assert.deepEqual(JSON.parse(JSON.stringify(f.mounted.descriptors)), JSON.parse(JSON.stringify(descriptors())))
  assert.equal(f.delivered.length, 1); assert.equal(f.acknowledged[0].result.status, 'submitted')
  assert.equal(f.acknowledged[0].result.seen, undefined)
  await f.mount()
  await waitForDisplayedTask(f, '并行任务')
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('并行任务')); assert.ok(html.includes('排队')); assert.ok(html.includes('覆盖'))
  assert.ok(html.includes('推进不足')); assert.ok(html.includes('证据缺口'))
  assert.ok(!f.invoked.includes('followup'))
})

test('the existing overview groups verified parent and child while keeping flat legacy tasks accessible', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [{ key: 'own', descriptor: { name: 'Native Host' }, status: 'connected' }],
    tasks: [
      { key: 'p', id: 'parent', title: 'Main conversation', sourceKey: 'own', status: 'idle', events: [], analysis: { checks: [] } },
      { key: 'c', id: 'child', title: 'Child actor', sourceKey: 'own', status: 'running', events: [], analysis: { checks: [] }, hierarchy: { parentSessionId: 'parent', instructionOrigin: 'delegated' } },
      { key: 'legacy', id: 'legacy', title: 'Legacy flat task', sourceKey: 'own', status: 'running', events: [], analysis: { checks: [] } },
    ], hierarchyContext: { schemaVersion: 1, groups: [{ key: 'g', sourceKey: 'own', rootSessionId: 'parent', runningCount: 1, coverage: { complete: false, branchGaps: [{ reason: 'unread descendants' }] }, nodes: [{ taskKey: 'p', sessionId: 'parent', parentSessionId: null }, { taskKey: 'c', sessionId: 'child', parentSessionId: 'parent' }] }] } }
  const f = await client(t, snapshot, 'denied', null, true)
  const group = f.dom.window.document.querySelector('[data-sentinel-session-group="g"]')
  assert.ok(group, 'parent and child have one visible session group')
  assert.match(group.textContent, /Main conversation/)
  assert.match(group.textContent, /Child actor/)
  assert.match(group.textContent, /活动代理 1/)
  assert.match(group.textContent, /层级覆盖不完整/)
  assert.match(f.dom.window.document.body.textContent, /Legacy flat task/)
  group.querySelector('button[data-task-key="c"]').click(); await f.settle()
  assert.match(f.dom.window.document.querySelector('article').textContent, /尚未完成此项判断/)
})

test('overview assertion waits for an actual first snapshot response', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, adapter: { connected: true, gaps: [] },
    overview: { runningTasks: 1 }, sources: [], tasks: [{ key: 'delayed', title: '实际响应后才显示的任务', sourceKey: 'own', status: 'running', events: [], analysis: { checks: [] } }] }
  let release, received = false
  const pending = new Promise(resolve => { release = resolve })
  const f = await client(t, snapshot, 'denied', null, false, false, { snapshot: () => { received = true; return pending } })
  try {
    await f.mount()
    const deadline = Date.now() + 1000
    while (!received) { assert.ok(Date.now() < deadline, 'Actual snapshot request was not received'); await f.settle() }
    assert.ok(!renderToStaticMarkup(h(f.Panel)).includes('实际响应后才显示的任务'))
    release({ ok: true, value: snapshot })
    await waitForDisplayedTask(f, '实际响应后才显示的任务')
    assert.ok(renderToStaticMarkup(h(f.Panel)).includes('实际响应后才显示的任务'))
  } finally { release({ ok: true, value: snapshot }); await f.settle() }
})

test('fixture HTML readiness follows a delayed actual Native Remote response rather than the React mount timer', async t => {
  const through = 1700000010000
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'paused' }, overview: { runningTasks: 1 }, sources: [],
    tasks: [{ key: 'delayed-native', sourceKey: 'own', title: '延迟原生快照', status: 'running', events: [], analysis: { through, checks: [] } }] }
  let responded = false
  const f = await client(t, snapshot, 'denied', null, true, true, {
    async snapshot() { await new Promise(resolve => setTimeout(resolve, 80)); responded = true; return { ok: true, value: snapshot } },
  })
  assert.equal(responded, true, 'the fixture must wait for its actual data before snapshot-dependent HTML assertions')
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('继续分析')); assert.ok(html.includes('分析覆盖截止'))
  assert.ok(html.includes(new Date(through).toLocaleString())); assert.ok(html.includes('延迟原生快照'))
})

test('denied desktop permission does not claim or falsely acknowledge a notification', async t => {
  const f = await client(t, { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'paused' }, overview: {}, sources: [], tasks: [] }, 'denied')
  assert.equal(f.delivered.length, 0); assert.equal(f.acknowledged.length, 0); assert.ok(!f.invoked.includes('claim'))
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('通知被拒绝')); assert.ok(html.includes('继续分析')); assert.ok(html.includes('暂停期间仍采集'))
})

test('a failed desktop submission is reported as failed while the observation overview remains available', async t => {
  const f = await client(t, { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }, 'granted', new Error('desktop not available'))
  assert.equal(f.delivered.length, 0); assert.equal(f.acknowledged[0].result.status, 'failed')
  assert.equal(f.acknowledged[0].result.error, 'desktop not available')
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('投递失败')); assert.ok(html.includes('已接入来源目前没有已知任务'))
})

test('ended tasks display alert history, original and recovery evidence, and failed notification status without reviving old attention', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'source-history', descriptor: { name: '历史后台' }, status: 'connected', gaps: [] }],
    tasks: [{ key: 'task-history', sourceKey: 'source-history', id: 'ended', title: '已结束观察任务', status: 'disposed',
      lastActivity: 1700000030000, events: [{ id: 'original-evidence', text: '原失败证据' }, { id: 'recovery-evidence', text: '新验证通过' }],
      analysis: { state: 'interrupted', through: 1700000010000, completedAt: 1700000020000,
        summary: { attention: '上轮运行中的疑点' }, checks: [{ id: 'failure_loop', status: 'attention', reason: '上轮运行中的疑点', evidenceIds: ['original-evidence'] }] },
      alerts: [{ id: 'alert-history', checkId: 'failure_loop', firstAt: 1700000000000, status: 'recovered', reason: '历史重复失败',
        evidenceIds: ['original-evidence'], recoveryEvidenceIds: ['recovery-evidence'],
        notification: { id: 'notice-history', status: 'failed', delivery: 'failed', seen: 'unknown', error: '系统拒绝投递', at: 1700000025000 } }] }] }
  const f = await client(t, snapshot, 'denied')
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('告警记录')); assert.ok(html.includes('已恢复')); assert.ok(html.includes('首次发现'))
  assert.ok(html.includes('原失败证据')); assert.ok(html.includes('新验证通过')); assert.ok(html.includes('系统拒绝投递'))
  assert.ok(html.includes('用户是否看见未知'))
  assert.ok(html.includes('当前判断已失效'))
  assert.ok(!html.includes('需要关注：上轮运行中的疑点'))
})

test('analysis coverage timestamps and actual activity times are shown separately from snapshot update time', async t => {
  const through = 1700000010000, lastActivity = 1700000030000, updatedAt = 1700000040000
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'source-times', descriptor: { name: '后台' }, status: 'connected', gaps: [] }],
    tasks: [{ key: 'task-times', sourceKey: 'source-times', id: 'times', title: '时间覆盖', status: 'running', lastActivity, updatedAt, events: [],
      analysis: { state: 'cooldown', through, completedAt: 1700000020000, checks: [] } }] }
  const f = await client(t, snapshot, 'denied'), html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('分析覆盖截止')); assert.ok(html.includes(new Date(through).toLocaleString()))
  assert.ok(html.includes('最近实际活动')); assert.ok(html.includes(new Date(lastActivity).toLocaleString()))
  assert.ok(html.includes('任务资料更新时间')); assert.ok(html.includes(new Date(updatedAt).toLocaleString()))
  assert.ok(!html.includes('事件序号')); assert.ok(!html.includes(String(through)))
})

test('the existing overview renders backend GS counts, waiting duration and separate check applicability without deriving colors', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'source-gs', descriptor: { name: 'Own fixture' }, status: 'connected', gaps: [] }],
    hierarchyContext: { schemaVersion: 1, groups: [{ key: 'group-gs', sourceKey: 'source-gs', rootSessionId: 'root', runningCount: 4, counts: { green: 1, yellow: 1, red: 1, gray: 1 }, coverage: { complete: false }, nodes: [{ taskKey: 'actor-gs', sessionId: 'root', light: 'green' }] }] },
    tasks: [{ key: 'actor-gs', id: 'actor', sourceKey: 'source-gs', title: 'Native wait display', status: 'running', events: [], alerts: [], gs: { light: 'green', label: '等待你', reason: '真实审批开闭依据', waiting: { durationMs: 125_000 }, coverage: { complete: true, gaps: [] } }, analysis: { state: 'failed', checks: [
      { id: 'failure_loop', status: 'unknown', judgmentState: 'not-triggered', applicability: { state: 'not-triggered' }, reason: '没有重复失败' },
      { id: 'request_drift', status: 'unknown', judgmentState: 'insufficient', applicability: { state: 'triggered' }, reason: '缺少当前结果' },
      { id: 'validation_cost', status: 'unknown', judgmentState: 'failed', applicability: { state: 'unknown' }, reason: '调用失败' },
    ] } }] }
  const f = await client(t, snapshot, 'denied')
  const document = f.dom.window.document
  assert.equal(document.querySelector('[data-sentinel-light="green"]').textContent.includes('等待你'), true)
  assert.ok(document.body.textContent.includes('125 秒'))
  document.querySelector('[data-gs-node=actor-gs]').click(); await f.settle()
  assert.ok(document.body.textContent.includes('真实审批开闭依据'))
  for (const color of ['green', 'yellow', 'red', 'gray']) assert.equal(document.querySelector(`[data-gs-count="${color}"]`).textContent, '1')
  assert.ok(document.body.textContent.includes('未触发'))
  assert.ok(document.body.textContent.includes('材料不足'))
  assert.ok(document.body.textContent.includes('调用失败'))
})

test('an older core with hierarchy support explicitly reports missing GS status and waiting capability', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [], adapter: { error: null, gsStatus: { supported: false, code: 'gs_status_unsupported' } } }
  const f = await client(t, snapshot, 'denied')
  assert.ok(f.dom.window.document.body.textContent.includes('共享核心尚不支持 GS 状态与等待规则'))
})

test('a later browser notification error updates a successful submission to failed without claiming that the user saw it', async t => {
  const f = await client(t, { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] })
  assert.equal(f.acknowledged[0].result.status, 'submitted')
  f.delivered[0].instance.onerror()
  await new Promise(resolve => setTimeout(resolve, 0))
  assert.equal(f.acknowledged.at(-1).result.status, 'failed')
  assert.equal(f.acknowledged.at(-1).result.error, '浏览器报告通知投递失败')
  assert.ok(f.acknowledged.every(ack => ack.result.seen === undefined))
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('桌面通知投递失败'))
})

test('the production client mounts the real native Remote namespace and reads the overview through its RPC carrier', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, adapter: { connected: true },
    overview: { runningTasks: 4, knownSources: 2, connectedSources: 2, attentionTasks: 0 }, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'denied', null, true)
  await new Promise(resolve => setTimeout(resolve, 20))
  assert.ok(f.invoked.includes('snapshot'), renderToStaticMarkup(h(f.Panel)))
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('活动代理 4')); assert.ok(!html.includes('正在连接共享核心'))
})


test('native sidebar visibility separates one shared view refresh from independent background notification work over 100 cycles', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'denied', null, true, false)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  assert.equal(f.plugin.diagnostics().backgroundTimer, 1)
  for (let cycle = 0; cycle < 100; cycle++) {
    await f.mount(true, 2)
    await f.dataReady()
    assert.equal(f.plugin.diagnostics().viewInstances, 2)
    assert.equal(f.plugin.diagnostics().snapshotTimer, 1)
    assert.equal(f.plugin.diagnostics().pendingSnapshot, 0)
    const before = f.invoked.filter(method => method === 'snapshot').length
    await f.mount(false, 2)
    assert.equal(f.plugin.diagnostics().visibleViewers, 0)
    assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
    await f.tick()
    assert.equal(f.invoked.filter(method => method === 'snapshot').length, before)
    assert.equal(f.plugin.diagnostics().backgroundTimer, 1)
    await f.mount(true, 2)
    await f.dataReady()
    assert.equal(f.invoked.filter(method => method === 'snapshot').length, before + 1, 'restore immediately refreshes once for both viewers')
    await f.unmount()
    assert.equal(f.plugin.diagnostics().viewInstances, 0)
    assert.equal(f.plugin.diagnostics().listeners, 0)
    assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
    assert.equal(f.plugin.diagnostics().activeRequests, 0)
  }
})

test('hidden native sidebar detaches its dedicated subscription and listeners while notification work continues', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'granted', null, true)
  await f.mount(false)
  assert.equal(f.plugin.diagnostics().visibleViewers, 0)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  assert.equal(f.plugin.diagnostics().listeners, 0, 'The hidden tab releases its own state subscription and visibility/abort listeners')
  const reads = f.invoked.filter(method => method === 'snapshot').length
  const notices = f.delivered.length
  await f.tick()
  assert.equal(f.invoked.filter(method => method === 'snapshot').length, reads)
  assert.equal(f.delivered.length, notices + 1, 'Background notification polling remains independent')
  await f.mount(true)
  await f.dataReady()
  assert.equal(f.invoked.filter(method => method === 'snapshot').length, reads + 1)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 1)
  await f.mount(false)
  assert.equal(f.plugin.diagnostics().listeners, 0)
})

test('a destroyed native tab releases its binding and its abort-aware Remote snapshot actually finishes', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  let hold = false, entered = 0, finished = 0, inFlight = 0, heldSignal
  const f = await client(t, snapshot, 'granted', null, true, true, {
    async snapshot(signal) {
      entered++; inFlight++
      let aborted
      try {
        if (hold) {
          heldSignal = signal
          await new Promise((resolve, reject) => { aborted = () => reject(signal.reason); signal.addEventListener('abort', aborted, { once: true }); if (signal.aborted) aborted() })
        }
        return { ok: true, value: snapshot }
      } finally { if (aborted) signal.removeEventListener('abort', aborted); finished++; inFlight-- }
    },
  })
  hold = true
  await f.tick()
  assert.equal(inFlight, 1)
  await f.abortView()
  assert.equal(heldSignal.aborted, true)
  assert.equal(inFlight, 0); assert.equal(entered, finished, 'The controlled RPC IO really settled after cancellation')
  assert.equal(f.plugin.diagnostics().pendingSnapshot, 0)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  assert.equal(f.plugin.diagnostics().listeners, 0, 'An aborted tab signal can never resume and must release its dedicated bindings')
  const reads = entered, notices = f.delivered.length
  await f.tick()
  assert.equal(entered, reads); assert.equal(f.delivered.length, notices + 1)
  t.diagnostic(JSON.stringify({ remoteSnapshot: { entered, finished, inFlight }, client: f.plugin.diagnostics() }))
})

test('native snapshot IO ignoring abort stays in a bounded global unfinished pool across view and connection generations', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: { runningTasks: 3 }, sources: [], tasks: [] }
  const held = []; let hold = false, inFlight = 0, peak = 0, finished = 0, claims = 0
  t.after(() => { for (const entry of held) entry.finish() })
  const f = await client(t, snapshot, 'granted', null, true, true, {
    async snapshot(signal) {
      if (!hold) return { ok: true, value: snapshot }
      inFlight++; peak = Math.max(peak, inFlight)
      try {
        await new Promise(resolve => held.push({ signal, finish: resolve }))
        return { ok: true, value: { ...snapshot, overview: { runningTasks: 999 } } }
      } finally { inFlight--; finished++ }
    },
    async claimNotifications() { claims++; return { ok: true, value: [] } },
  })
  hold = true
  for (let cycle = 0; cycle < 5; cycle++) { await f.tick(); await f.mount(false); await f.mount(true); await f.settle() }
  await f.reinstall(false)
  await f.reinstall(false)
  assert.ok(peak <= 2, 'Cancelled unresolved native snapshot reads must not accumulate across repeated close/open or new connection scopes')
  assert.equal(inFlight, 2)
  assert.equal(f.plugin.diagnostics().unfinishedRequests, 2)
  assert.ok(f.dom.window.document.body.textContent.includes('先前读取尚未收尾'))
  const before = claims
  await f.tick()
  assert.ok(claims > before, 'Read admission cannot stop the independent background notification channel')
  hold = false
  for (const entry of held) entry.finish()
  await f.waitUntil(() => inFlight === 0 && f.plugin.diagnostics().unfinishedRequests === 0, 'Actual native RPC completions drain the bounded old pool')
  assert.equal(finished, 2)
  await f.tick(); await f.dataReady()
  assert.ok(!f.dom.window.document.body.textContent.includes('活动代理 999'), 'Late cancelled snapshots never overwrite the resumed current observation')
  assert.ok(f.dom.window.document.body.textContent.includes('活动代理 3'))
  t.diagnostic(JSON.stringify({ nativeRPC: { peak, finished, inFlight }, client: f.plugin.diagnostics() }))
})

test('document hidden state also pauses view refresh and native sidebar restore does not overlap pending snapshots', async t => {
  let resolveSnapshot, heldSignal, calls = 0
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: { runningTasks: 8 }, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'denied', null, true, false, {
    snapshot(signal) {
      calls++
      if (calls === 1) { heldSignal = signal; return new Promise(resolve => { resolveSnapshot = resolve }) }
      return Promise.resolve({ ok: true, value: snapshot })
    },
  })
  await f.mount()
  await f.waitUntil(() => calls === 1, 'Held snapshot did not enter the actual Native RPC carrier')
  assert.equal(calls, 1); assert.equal(f.plugin.diagnostics().pendingSnapshot, 1)
  for (let index = 0; index < 5; index++) await f.tick()
  assert.equal(calls, 1)
  await f.visibility('hidden')
  assert.equal(heldSignal.aborted, true); assert.equal(f.plugin.diagnostics().activeRequests, 0)
  assert.equal(f.plugin.diagnostics().pendingSnapshot, 0); assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  resolveSnapshot({ ok: true, value: { ...snapshot, overview: { runningTasks: 999 } } })
  await f.settle()
  assert.ok(!renderToStaticMarkup(h(f.Panel)).includes('活动代理 999'))
  await f.visibility('visible')
  assert.equal(calls, 2); assert.equal(f.plugin.diagnostics().snapshotTimer, 1)
  assert.ok(renderToStaticMarkup(h(f.Panel)).includes('活动代理 8'))
})

test('connection disposal aborts its own native pending notification call and ignores a late notice', async t => {
  let heldSignal, resolveClaim
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'granted', null, true, false, {
    claimNotifications(signal) { heldSignal = signal; return new Promise(resolve => { resolveClaim = resolve }) },
  })
  await f.waitUntil(() => heldSignal !== undefined, 'Held notification did not enter the actual Native RPC carrier')
  assert.equal(f.plugin.diagnostics().pendingNotification, 1)
  await f.dispose()
  assert.equal(heldSignal.aborted, true)
  assert.equal(f.plugin.diagnostics().pendingNotification, 0)
  assert.equal(f.plugin.diagnostics().activeRequests, 0)
  resolveClaim({ ok: true, value: [{ id: 'late', title: '不应提交' }] }); await f.settle()
  assert.equal(f.delivered.length, 0); assert.equal(f.acknowledged.length, 0)
  await f.dispose()
})

test('disposal clears browser notification error callbacks and saved callbacks cannot mutate or acknowledge after disposal', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'granted', null, true, false)
  await f.settle()
  assert.equal(f.delivered.length, 1)
  const callback = f.delivered[0].instance.onerror
  const before = f.acknowledged.length
  await f.dispose()
  assert.equal(f.delivered[0].instance.onerror, null)
  assert.equal(f.plugin.diagnostics().callbacks, 0)
  callback(); await f.settle()
  assert.equal(f.acknowledged.length, before)
})

test('the expected background list reports configuration counts and Host counts separately without claiming whole machine discovery', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'live', status: 'connected', descriptor: { name: '桌面' } }], tasks: [],
    enrollment: { configured: 2, connected: 1, entries: [
      { registrationId: 'desktop', name: '桌面后台', enabled: true, status: 'connected', connectedSources: 1 },
      { registrationId: 'other', name: '另一个后台', enabled: true, status: 'pending', connectedSources: 0 },
      { registrationId: 'off', name: '暂不观察', enabled: false, status: 'disabled', connectedSources: 0 },
    ], discovery: { complete: false } },
  }
  const f = await client(t, snapshot, 'denied', null, true)
  const html = renderToStaticMarkup(h(f.Panel))
  assert.ok(html.includes('启用配置 2')); assert.ok(html.includes('已连接配置 1')); assert.ok(html.includes('已连接 Host 1'))
  assert.ok(html.includes('等待首次连接')); assert.ok(html.includes('清单项已停用')); assert.ok(html.includes('不代表整机发现完整'))
  const codec = descriptors().find(method => method.method === 'configureSources').parameters[0].codec.create()
  assert.throws(() => codec.parse(Array.from({ length: 101 }, (_, index) => ({ registrationId: String(index), name: '后台', enabled: true }))))
  assert.throws(() => codec.parse([{ registrationId: 'a', name: '后台', enabled: 'true' }]))
  assert.throws(() => codec.parse([{ registrationId: 'a', name: '后台', enabled: true, token: 'unrecognized' }]))
  assert.throws(() => codec.parse([{ registrationId: 'a', name: '后台', enabled: true }, { registrationId: 'a', name: '另一个', enabled: false }]))
  assert.equal(codec.parse([{ registrationId: 'a', name: '后台', enabled: true }]).length, 1)
})


test('native connection disposal removes mounted pane subscriptions and DOM listeners before the view itself unmounts', async t => {
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'denied', null, true)
  assert.equal(f.plugin.diagnostics().listeners, 3)
  await f.dispose()
  assert.equal(f.plugin.diagnostics().listeners, 0)
  assert.equal(f.plugin.diagnostics().visibleViewers, 0)
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
  assert.equal(f.plugin.diagnostics().backgroundTimer, 0)
  assert.equal(f.plugin.diagnostics().activeRequests, 0)
  await f.visibility('hidden'); await f.visibility('visible')
  assert.equal(f.plugin.diagnostics().snapshotTimer, 0)
})

test('source plan heartbeats retain the mounted editor, unsaved draft, and operation result; saved configuration refreshes editable data', async t => {
  const entry = { registrationId: 'desktop', name: '桌面后台', enabled: true, status: 'connected', connectedSources: 1, lastSeenAt: 1000 }
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [], enrollment: { configured: 1, connected: 1, entries: [entry] } }
  const f = await client(t, snapshot, 'denied', null, true, true, {
    async configureSources(entries) {
      snapshot.enrollment = { ...snapshot.enrollment, entries: entries.map(value => ({ ...value, status: 'connected', connectedSources: 1, lastSeenAt: 3000 })) }
      return { ok: true, value: snapshot.enrollment }
    },
  })
  const { Simulate } = clientRequire('react-dom/test-utils')
  const { act } = clientRequire('react')
  const interact = async action => {
    const previous = globalThis.IS_REACT_ACT_ENVIRONMENT
    globalThis.IS_REACT_ACT_ENVIRONMENT = true
    try { await act(async () => { action(); await f.settle() }) }
    finally { globalThis.IS_REACT_ACT_ENVIRONMENT = previous }
  }
  const editor = f.dom.window.document.querySelector('textarea'), form = editor.closest('form')
  const draft = '{未保存的编辑'
  await interact(() => { editor.value = draft; Simulate.change(editor) })
  assert.equal(editor.value, draft, 'the public editor must first receive the unsaved user draft')
  Simulate.submit(form); await f.settle()
  const result = form.querySelector('[role="status"]').textContent
  assert.ok(result.length > 0)
  snapshot.enrollment = { ...snapshot.enrollment, entries: [{ ...entry, status: 'connecting', connectedSources: 0, lastSeenAt: 2000 }] }
  await f.tick()
  assert.ok(f.dom.window.document.querySelector('textarea') === editor, 'heartbeat metadata must not remount the editor')
  assert.equal(editor.value, draft)
  assert.equal(form.querySelector('[role="status"]').textContent, result)
  assert.ok(f.dom.window.document.body.textContent.includes('已连接，等待基线'))
  const saved = [{ registrationId: 'desktop', name: '用户修改的名称', enabled: true }]
  await interact(() => { editor.value = JSON.stringify(saved); Simulate.change(editor) })
  Simulate.submit(form); await f.settle()
  assert.ok(f.dom.window.document.querySelector('textarea') === editor)
  assert.deepEqual(JSON.parse(editor.value), saved)
  assert.ok(form.querySelector('[role="status"]').textContent.includes('已保存'))
  snapshot.enrollment = { ...snapshot.enrollment, entries: snapshot.enrollment.entries.map(value => ({ ...value, lastSeenAt: 4000 })) }
  await f.tick()
  assert.ok(f.dom.window.document.querySelector('textarea') === editor)
  assert.deepEqual(JSON.parse(editor.value), saved)
  assert.ok(form.querySelector('[role="status"]').textContent.includes('已保存'))
})


test('the real Remote namespace cancels in-flight calls and survives re-enablement without accepting late results', async t => {
  const signals = [], heldCalls = []
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: { runningTasks: 4 }, sources: [], tasks: [] }
  let holdNext = false, currentSnapshot = snapshot
  t.after(() => { for (const call of heldCalls) call.release(); })
  const f = await client(t, snapshot, 'granted', null, true, true, {
    async snapshot(signal) {
      signals.push(signal)
      if (holdNext) {
        holdNext = false
        let release
        const pending = new Promise(resolve => { release = resolve; })
        heldCalls.push({ signal, release, completed: false })
        const call = heldCalls.at(-1)
        await pending
        call.completed = true
        return { ok: true, value: { ...snapshot, overview: { runningTasks: 999 } } }
      }
      return { ok: true, value: currentSnapshot }
    },
  })
  assert.ok(signals.length > 0)
  assert.ok(renderToStaticMarkup(h(f.Panel)).includes('活动代理 4'))
  for (let cycle = 0; cycle < 3; cycle++) {
    holdNext = true
    await f.tick()
    await f.waitUntil(() => heldCalls.length === cycle + 1, 'Held replacement snapshot did not enter the actual Native RPC carrier')
    const old = heldCalls.at(-1)
    assert.equal(heldCalls.length, cycle + 1, 'the old snapshot must actually enter the RPC carrier')
    assert.equal(old.signal.aborted, false)
    assert.equal(f.plugin.diagnostics().pendingSnapshot, 1)
    assert.equal(f.plugin.diagnostics().activeRequests, 1)
    const before = signals.length, runningTasks = cycle + 5
    currentSnapshot = { ...snapshot, overview: { runningTasks } }
    await f.reinstall()
    assert.equal(old.signal.aborted, true, 'disposal must cancel the actual in-flight native RPC')
    assert.ok(signals.length > before, 're-enabled native namespace must perform its new RPC')
    assert.notEqual(signals.at(-1), old.signal)
    assert.equal(signals.at(-1).aborted, false, 'the replacement namespace must own a fresh cancellation token')
    assert.equal(f.plugin.diagnostics().visibleViewers, 1)
    assert.equal(f.plugin.diagnostics().snapshotTimer, 1)
    assert.equal(f.plugin.diagnostics().backgroundTimer, 1)
    assert.equal(f.plugin.diagnostics().pendingSnapshot, 0)
    assert.equal(f.plugin.diagnostics().pendingNotification, 0)
    assert.equal(f.plugin.diagnostics().activeRequests, 0)
    assert.ok(renderToStaticMarkup(h(f.Panel)).includes(`活动代理 ${runningTasks}`))
    const notifications = f.delivered.length
    old.release(); await f.settle()
    assert.equal(old.completed, true)
    assert.ok(renderToStaticMarkup(h(f.Panel)).includes(`活动代理 ${runningTasks}`))
    assert.ok(!renderToStaticMarkup(h(f.Panel)).includes('活动代理 999'))
    assert.equal(f.delivered.length, notifications, 'a late old snapshot cannot trigger a notification')
    assert.equal(f.plugin.diagnostics().activeRequests, 0)
  }
})

test('a temporary notification claim failure clears after a successful empty claim instead of leaving a stale connection warning', async t => {
  let claims = 0
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'granted', null, true, true, {
    async claimNotifications() {
      claims++
      return claims === 1 ? { ok: false, error: { code: 'CORE_STARTING', message: '正在连接共享核心' } } : { ok: true, value: [] }
    },
  })
  const notificationText = () => f.dom.window.document.querySelector('[data-gs-settings] > small').textContent
  assert.equal(notificationText(), '正在连接共享核心')
  await f.tick()
  assert.ok(notificationText().includes('桌面通知已获授权'), 'a healthy empty claim restores permission/check state')
  assert.equal(f.delivered.length, 0); assert.equal(f.acknowledged.length, 0)
})

test('notification transport recovery retains an earlier browser delivery failure instead of replacing it with generic permission state', async t => {
  let claims = 0
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [], tasks: [] }
  const f = await client(t, snapshot, 'granted', null, true, true, {
    async claimNotifications() {
      claims++
      if (claims === 1) return { ok: true, value: [{ id: 'persistent-delivery', title: '执行疑点' }] }
      return claims === 2 ? { ok: false, error: { code: 'TEMPORARY', message: '通知连接临时失联' } } : { ok: true, value: [] }
    },
  })
  const notificationText = () => f.dom.window.document.querySelector('[data-gs-settings] > small').textContent
  assert.ok(notificationText().includes('通知已提交给系统'))
  f.delivered[0].instance.onerror(); await f.settle()
  const failed = notificationText()
  assert.ok(failed.includes('桌面通知投递失败'))
  await f.tick()
  assert.equal(notificationText(), '通知连接临时失联')
  await f.tick()
  assert.equal(notificationText(), failed, 'healthy empty claims must not erase actual delivery outcomes')
  assert.equal(f.delivered.length, 1)
  assert.equal(f.acknowledged.at(-1).result.status, 'failed')
})

test('current observed activity remains visible while analysis is paused, unavailable, or waiting and updates independently of old analysis coverage', async t => {
  const through = 1700000000000
  const task = { key: 'activity-task', sourceKey: 'activity-source', id: 'activity', title: '活动观察', status: 'running', currentRequest: '修复并验证',
    events: [{ id: 'tool-start', kind: 'tool-start', capturedAt: through + 1000, text: 'npm test' }],
    activity: { summary: '已开始执行 npm test，等待工具返回', observedAt: through + 1000, evidenceIds: ['tool-start'] },
    analysis: { state: 'paused', through, completedAt: through, lagMs: 1000,
      summary: { currentState: '模型上轮认为验证已完成', attention: '模型上轮记录的疑点' },
      checks: [{ id: 'failure_loop', status: 'attention', reason: '旧轮次失败', evidenceIds: ['expired-evidence'] }] },
    alerts: [{ id: 'expired-alert', checkId: 'failure_loop', firstAt: through, status: 'evidence-expired', reason: '历史疑点', evidenceIds: ['expired-evidence'] }],
  }
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'paused' }, overview: {},
    sources: [{ key: 'activity-source', descriptor: { name: '测试后台' }, status: 'connected', gaps: [] }], tasks: [task] }
  const f = await client(t, snapshot, 'denied', null, true)
  const currentState = () => [...f.dom.window.document.querySelectorAll('.sentinel-overview article > p')].find(item => item.textContent.startsWith('当前状态：')).textContent
  assert.ok(currentState().includes('运行中'))
  assert.ok(currentState().includes('已开始执行 npm test，等待工具返回'), 'observed work must be visible without expanding evidence or enabling models')
  assert.ok(!currentState().includes('模型上轮认为验证已完成'))
  snapshot.monitoring.state = 'active'; task.analysis.state = 'unavailable'
  task.activity = { summary: '工具返回退出码 1，已收到验证失败结果', observedAt: through + 2000, evidenceIds: ['tool-result'] }
  await f.tick()
  assert.ok(currentState().includes('工具返回退出码 1，已收到验证失败结果'))
  assert.ok(!currentState().includes('已开始执行 npm test'))
  task.analysis.state = 'analyzing'; task.analysis.lagMs = 3000
  task.activity = { summary: '已收到代理输出：正在处理刚才的失败', observedAt: through + 3000, evidenceIds: ['new-output'] }
  await f.tick()
  assert.ok(currentState().includes('正在处理刚才的失败'))
  assert.ok(!currentState().includes('模型上轮认为验证已完成'), 'model currentState must not replace current observed work while it waits')
  const rendered = f.dom.window.document.body.textContent
  assert.ok(rendered.includes('分析覆盖截止：' + new Date(through).toLocaleString()))
  assert.ok(rendered.includes('当前结论尚未覆盖新活动'))
  assert.ok(rendered.includes('证据已到期，原疑点失效'))
})

test('the actual DSH detail shows the current model short summary with direct owned citations and hides it while new material waits or fails', async t => {
  const at = Date.now(), task = { key: 'summary-task', sourceKey: 'summary-source', id: 'main', lifecycleId: 'run', title: '总结观察', status: 'running',
    currentRequest: 'Inspect the current execution', activity: { summary: '工具已返回一个结果', observedAt: at },
    events: [{ id: 'basis', taskId: 'main', taskLifecycleId: 'run', kind: 'output', capturedAt: at, text: '本轮实际执行依据' }],
    analysis: { state: 'cooldown', resultDisposition: 'current', completedAt: at, summary: { generated: { text: '已观察到当前执行结果。', evidenceIds: ['basis'], evidenceRefs: [{ taskId: 'main', lifecycleId: 'run', eventId: 'basis' }] } }, checks: [] } }
  const snapshot = { protocol: 1, generatedAt: at, monitoring: { state: 'active' }, overview: {}, sources: [{ key: 'summary-source', status: 'connected', descriptor: { name: '本安装' } }], tasks: [task] }
  const f = await client(t, snapshot, 'denied', null, true)
  const summary = () => f.dom.window.document.querySelector('[data-sentinel-model-summary]')
  assert.ok(summary(), 'the current model-generated summary is visible in the actual task detail')
  assert.ok(summary().textContent.includes('已观察到当前执行结果。'))
  const citation = summary().querySelector('small[data-sentinel-citation="basis"]')
  assert.ok(citation.textContent.includes('本轮实际执行依据'))
  assert.equal(citation.dataset.sourceKey, 'summary-source')
  assert.equal(summary().closest('[data-gs-checks]'), null, 'summary and citations are directly visible without opening full checks')
  task.analysis.resultDisposition = 'reused'; await f.tick()
  assert.ok(summary(), 'a valid reused result stays visible')
  assert.ok(summary().textContent.includes('复用有效总结：已观察到当前执行结果。'), 'a valid reused result stays visible without claiming a new model call')
  task.analysis.resultDisposition = 'current'; await f.tick()
  assert.ok(summary().textContent.includes('模型总结：已观察到当前执行结果。'))
  assert.equal(summary().textContent.includes('复用有效总结'), false)
  task.analysis.state = 'analyzing'; task.analysis.resultDisposition = 'pending'; task.activity.summary = '已采集新材料，等待本轮模型结果'
  await f.tick()
  assert.equal(summary(), null, 'a prior generated summary does not cover pending material')
  assert.ok(f.dom.window.document.querySelector('[data-gs-detail]').textContent.includes(task.activity.summary))
  for (const state of ['failed', 'stale', 'unavailable', 'interrupted']) {
    task.analysis.state = state; task.analysis.resultDisposition = 'current'; await f.tick()
    assert.equal(summary(), null, state + ' never publishes a complete model summary')
  }
  task.analysis.state = 'cooldown'; await f.tick()
  assert.ok(summary().textContent.includes('已观察到当前执行结果。'))
})

test('a failed or pending DSH detail keeps only its last successful summary as folded owned history and rejects late expired or foreign records', async t => {
  const at = Date.now(), completedAt = at - 2000
  const generated = { text: '上次已完成日志检查。', evidenceIds: ['basis'], evidenceRefs: [{ taskId: 'main', lifecycleId: 'run', eventId: 'basis' }] }
  const successful = { generated, completedAt, configScope: { registrationId: 'dsh:summary', revision: 'prior-settings' }, expiresAt: completedAt + 86400000 }
  const task = { key: 'summary-history-task', sourceKey: 'summary-source', id: 'main', lifecycleId: 'run', title: '历史总结观察', status: 'running',
    currentRequest: 'Continue inspecting', activity: { summary: '新一轮结果不可用', observedAt: at },
    events: [{ id: 'basis', taskId: 'main', taskLifecycleId: 'run', kind: 'output', capturedAt: at - 3000, text: '本来源的原始完成依据' }],
    analysis: { state: 'failed', resultDisposition: 'invalid', completedAt: at - 1000, summary: { generated: { ...generated, text: '不可发布的当前旧总结' } },
      currentConfigScope: { registrationId: 'dsh:summary', revision: 'current-settings' }, lastSuccessfulSummary: successful, checks: [] } }
  const foreign = { key: 'foreign-task', sourceKey: 'foreign-source', id: 'foreign', lifecycleId: 'foreign-run', title: '另一个来源', status: 'idle',
    events: [{ id: 'basis', taskId: 'foreign', taskLifecycleId: 'foreign-run', kind: 'output', capturedAt: at - 3000, text: 'FOREIGN-SOURCE-PRIVATE-BASIS' }], analysis: { state: 'unknown', checks: [] } }
  const snapshot = { protocol: 1, generatedAt: at, monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'summary-source', status: 'connected', descriptor: { name: '本安装', registrationId: 'dsh:summary' } }, { key: 'foreign-source', status: 'connected', descriptor: { name: '另一安装', registrationId: 'dsh:foreign' } }], tasks: [task, foreign] }
  const f = await client(t, snapshot, 'denied', null, true)
  const current = () => f.dom.window.document.querySelector('[data-sentinel-model-summary]'), history = () => f.dom.window.document.querySelector('[data-sentinel-summary-history]')
  assert.equal(current(), null)
  assert.ok(history(), 'a failed new round preserves access to its earlier successful model summary')
  assert.equal(history().open, false)
  assert.equal(history().querySelector('summary').textContent, '上次成功总结')
  assert.ok(history().textContent.includes('历史总结'))
  assert.ok(history().textContent.includes(new Date(completedAt).toLocaleString()))
  assert.ok(history().textContent.includes(generated.text))
  assert.ok(history().querySelector('small[data-sentinel-citation="basis"]').textContent.includes('本来源的原始完成依据'))
  assert.equal(history().textContent.includes('不可发布的当前旧总结'), false)
  for (const state of ['analyzing', 'failed', 'stale', 'unavailable', 'interrupted']) {
    task.analysis.state = state; task.analysis.resultDisposition = state === 'analyzing' ? 'pending' : 'invalid'; await f.tick()
    assert.equal(current(), null)
    assert.ok(history().textContent.includes(new Date(completedAt).toLocaleString()), 'history keeps its original completion time')
  }
  task.analysis.lastSuccessfulSummary = { ...successful, completedAt: at - 500, expiresAt: at - 500 + 86400000 }; await f.tick()
  assert.equal(history(), null, 'a late result cannot replace history after a more recent settled round')
  task.analysis.lastSuccessfulSummary = { ...successful, completedAt: at + 10000 }; await f.tick()
  assert.equal(history(), null, 'a future completion is unconfirmed')
  task.analysis.lastSuccessfulSummary = { ...successful, expiresAt: at - 1 }; await f.tick()
  assert.equal(history(), null, 'expired historical text is no longer displayed')
  task.analysis.lastSuccessfulSummary = { ...successful, configScope: { registrationId: 'dsh:foreign', revision: 'other' } }; await f.tick()
  assert.equal(history(), null, 'another source installation cannot contribute the historical summary')
  task.analysis.lastSuccessfulSummary = { ...successful, generated: { ...generated, evidenceRefs: [{ taskId: 'foreign', lifecycleId: 'foreign-run', eventId: 'basis' }] } }; await f.tick()
  assert.ok(history().querySelector('[data-sentinel-citation-gap]'))
  assert.equal(history().textContent.includes('FOREIGN-SOURCE-PRIVATE-BASIS'), false, 'missing own references never resolve through another source')
})

test('current activity handles older snapshots without the field and preserves the core disconnected-source boundary', async t => {
  const task = { key: 'compat-activity', sourceKey: 'compat-source', id: 'compat', title: '兼容后台', status: 'running', events: [],
    analysis: { state: 'cooldown', summary: { currentState: '模型上轮认为一切正常' }, checks: [] } }
  const source = { key: 'compat-source', descriptor: { name: '旧版本后台' }, status: 'connected', gaps: [] }
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {}, sources: [source], tasks: [task] }
  const f = await client(t, snapshot, 'denied', null, true)
  const currentState = () => [...f.dom.window.document.querySelectorAll('.sentinel-overview article > p')].find(item => item.textContent.startsWith('当前状态：')).textContent
  assert.ok(currentState().includes('运行中'))
  assert.ok(currentState().includes('活动摘要未提供'), 'an older core cannot be treated as evidence of healthy activity')
  assert.ok(!currentState().includes('一切正常'))
  source.status = 'disconnected'; task.status = 'unknown'
  task.activity = { summary: '来源失联；最后观察到工具开始，当前是否仍执行不可确认', observedAt: 1700000000000, evidenceIds: ['past-start'] }
  await f.tick()
  assert.ok(currentState().includes('状态未知'))
  assert.ok(currentState().includes('来源失联；最后观察到工具开始，当前是否仍执行不可确认'))
  assert.ok(f.dom.window.document.body.textContent.includes('当前判断已失效或不可判断'))
})

test('same-name tasks expose their separate observed activity in the list and selecting a detail shows its stable observation identity', async t => {
  const tasks = [
    { key: 'observation-alpha', sourceKey: 'same-name-source', id: 'agent-alpha', title: '修复测试', status: 'running', events: [],
      activity: { summary: '已开始执行 unit-alpha 验证', observedAt: 1700000000000, evidenceIds: ['start-alpha'] }, analysis: { state: 'unavailable', checks: [] } },
    { key: 'observation-beta', sourceKey: 'same-name-source', id: 'agent-beta', title: '修复测试', status: 'running', events: [],
      activity: { summary: '已收到 unit-beta 的失败结果', observedAt: 1700000001000, evidenceIds: ['result-beta'] }, analysis: { state: 'unavailable', checks: [] } },
  ]
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'active' }, overview: {},
    sources: [{ key: 'same-name-source', descriptor: { name: '并行后台' }, status: 'connected', gaps: [] }], tasks }
  const f = await client(t, snapshot, 'denied', null, true)
  const rows = [...f.dom.window.document.querySelectorAll('.sentinel-overview nav button')]
  assert.equal(rows.length, 2)
  assert.ok(rows[0].textContent.includes('已开始执行 unit-alpha 验证'), 'each task list row must expose its own recent observed work')
  assert.ok(rows[1].textContent.includes('已收到 unit-beta 的失败结果'))
  assert.ok(!rows[0].textContent.includes('unit-beta'))
  assert.ok(!rows[1].textContent.includes('unit-alpha'))
  assert.equal(f.dom.window.document.querySelector('article code').textContent, 'observation-alpha')
  const { Simulate } = clientRequire('react-dom/test-utils'), { act } = clientRequire('react')
  const previous = globalThis.IS_REACT_ACT_ENVIRONMENT
  globalThis.IS_REACT_ACT_ENVIRONMENT = true
  try { await act(async () => { Simulate.click(rows[1]); await f.settle() }) }
  finally { globalThis.IS_REACT_ACT_ENVIRONMENT = previous }
  assert.equal(f.dom.window.document.querySelector('article code').textContent, 'observation-beta')
  const currentState = [...f.dom.window.document.querySelectorAll('article > p')].find(item => item.textContent.startsWith('当前状态：')).textContent
  assert.ok(currentState.includes('已收到 unit-beta 的失败结果'))
  assert.ok(!currentState.includes('unit-alpha'))
  assert.deepEqual(f.invoked, ['snapshot'], 'selection only updates observation detail; it must not send, start, or stop an original task')
})

test('a failed snapshot read marks cached task status and activity as historical until a fresh successful observation arrives', async t => {
  let reads = 0
  const through = 1700000000000
  const task = { key: 'cached-observation', sourceKey: 'cached-source', id: 'cached-agent', title: '读取故障后的观察', status: 'running',
    events: [{ id: 'open-tool', kind: 'tool-start', text: 'npm test 工具已开始', capturedAt: through + 1000 }],
    activity: { summary: '已开始 npm test；等待工具返回', observedAt: through + 1000, evidenceIds: ['open-tool'] },
    analysis: { state: 'unavailable', through, summary: { currentState: '旧模型结论', recentProgress: '旧轮次保留的推进信息' },
      checks: [{ id: 'failure_loop', status: 'unknown', reason: '旧判断依据保留', evidenceIds: ['open-tool'] }] } }
  const snapshot = { protocol: 1, generatedAt: Date.now(), monitoring: { state: 'paused' }, overview: {},
    sources: [{ key: 'cached-source', descriptor: { name: '读取故障后台' }, status: 'connected', gaps: [] }], tasks: [task] }
  const f = await client(t, snapshot, 'denied', null, true, true, {
    async snapshot() { reads++; if (reads === 2) throw new Error('核心读取临时失败'); return { ok: true, value: snapshot } },
  })
  const row = () => f.dom.window.document.querySelector('nav button').textContent
  const detail = () => [...f.dom.window.document.querySelectorAll('article > p')].find(item => item.textContent.startsWith('当前状态：')).textContent
  assert.ok(detail().includes('运行中'))
  assert.ok(detail().includes('已开始 npm test；等待工具返回'))
  assert.ok(!detail().includes('当前无法确认'))
  const timers = f.intervals.size
  await f.tick()
  assert.ok(row().includes('当前无法确认'), 'the cached list must not advertise a current running task after a refused read')
  assert.ok(row().includes('历史活动：已开始 npm test；等待工具返回'))
  assert.ok(detail().includes('当前无法确认'))
  assert.ok(detail().includes('历史活动：已开始 npm test；等待工具返回'))
  assert.ok(f.dom.window.document.body.textContent.includes('旧轮次保留的推进信息'))
  assert.ok(f.dom.window.document.body.textContent.includes('旧判断依据保留'))
  assert.equal(f.intervals.size, timers)
  task.activity = { summary: '已收到 npm test 成功结果', observedAt: through + 2000, evidenceIds: ['tool-success'] }
  task.events.push({ id: 'tool-success', kind: 'tool-result', text: 'npm test 返回退出码 0', capturedAt: through + 2000 })
  await f.tick()
  assert.ok(row().includes('运行中'))
  assert.ok(row().includes('已收到 npm test 成功结果'))
  assert.ok(!row().includes('当前无法确认'))
  assert.ok(!row().includes('历史活动：'))
  assert.ok(detail().includes('已收到 npm test 成功结果'))
  assert.ok(!detail().includes('当前无法确认'))
  assert.ok(!detail().includes('历史活动：'))
  assert.ok(f.dom.window.document.body.textContent.includes('分析覆盖截止：' + new Date(through).toLocaleString()))
  assert.ok(f.dom.window.document.body.textContent.includes('npm test 工具已开始'))
  assert.equal(reads, 3)
  assert.equal(f.intervals.size, timers)
})
