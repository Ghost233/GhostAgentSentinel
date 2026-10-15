import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'

const reference = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
const until = async (predicate, label) => { const deadline = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 5)) } }
async function fixture(t, { models = false, hold = false, drift = 'attention' } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-gs-review-')), requests = [], held = [], cleanup = []
  let server, modelServer, clock = Date.now()
  t.after(async () => {
    for (const release of [() => { for (const finish of held.splice(0)) finish() }, () => server?.close(), async () => { if (modelServer) { modelServer.closeAllConnections(); await new Promise(resolve => modelServer.close(resolve)) } }, () => rm(dataDir, { recursive: true, force: true })]) { try { await release() } catch (error) { cleanup.push(error) } }
    if (cleanup.length) throw new AggregateError(cleanup, 'GS public fixture cleanup failed')
  })
  if (models) {
    modelServer = createServer(async (req, res) => {
      let raw = ''; for await (const chunk of req) raw += chunk
      const body = JSON.parse(raw); requests.push({ path: req.url, body })
      res.setHeader('content-type', 'application/json')
      const finish = () => res.end(JSON.stringify(req.url === '/v1/chat/completions'
        ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }
        : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? drift : 'unknown' }])) }))
      if (hold && req.url === '/v1/chat/completions') held.push(finish)
      else finish()
    })
    await new Promise(resolve => modelServer.listen(0, '127.0.0.1', resolve))
    const baseUrl = `http://127.0.0.1:${modelServer.address().port}`
    await writeFile(join(dataDir, 'analysis-config.json'), JSON.stringify({ llm: { baseUrl, model: 'controlled-extractor' }, jev: { baseUrl, model: 'controlled-judge' } }))
  }
  server = await startServer({ dataDir, now: () => clock, cooldownMs: 60_000, leaseMs: 172_800_000 })
  async function request(path, body) {
    const response = await fetch(`${server.url}/v1/${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    const value = await response.json(); assert.equal(response.status, 200, JSON.stringify(value)); return value
  }
  const sourceKey = await request('source/register', { registrationId: 'gs-review', hostRunId: 'own-run', name: 'Own public regression' })
  return { request, sourceKey, requests, held, now: () => clock, advance: value => clock += value,
    event(taskId, id, kind, data = {}, at = clock, text = id) { return { id, taskId, taskLifecycleId: 'life', kind, occurredAt: at, capturedAt: at, text, data } },
    ingest(batch) { return request('source/ingest', { sourceKey, batch }) }, snapshot() { return request('snapshot') },
  }
}

test('F1 public snapshots ignore missing or unknown native wait types without losing other tasks or inventing a wait', async t => {
  const f = await fixture(t)
  await f.ingest({ tasks: ['opaque', 'healthy'].map(id => ({ id, lifecycleId: 'life', status: 'running', title: id })), events: [
    f.event('opaque', 'opaque-request', 'input'), f.event('healthy', 'healthy-request', 'input'), f.event('healthy', 'healthy-output', 'output'),
    f.event('opaque', 'missing-type', 'status', { waitId: 'opaque' }), f.event('opaque', 'unknown-type', 'status', { nativeType: 'custom/status', waitId: 'opaque' }),
  ] })
  let snapshot = await f.snapshot()
  assert.equal(snapshot.tasks.length, 2)
  assert.equal(snapshot.tasks.find(task => task.id === 'opaque').gs.waiting, null)
  assert.ok(snapshot.tasks.find(task => task.id === 'healthy').events.some(event => event.id === 'healthy-output'))
  await f.ingest({ events: [f.event('healthy', 'asked', 'status', { nativeType: 'approval/asked', approvalId: 'actual-audit' })] })
  snapshot = await f.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === 'healthy').gs.waiting.kind, 'user')
  await f.ingest({ events: [f.event('healthy', 'decided', 'status', { nativeType: 'approval/decided', approvalId: 'actual-audit' })] })
  assert.equal((await f.snapshot()).tasks.find(task => task.id === 'healthy').gs.waiting, null)
})

function actorGraph(f) {
  const rootInput = f.event('parent', 'parent-request', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: 'original', inherited: false })
  return { tasks: ['parent', 'child'].map(id => ({ id, lifecycleId: 'life', sessionId: id, status: 'running', title: id })), events: [
    ...['parent', 'child'].map(id => f.event(id, `${id}-state`, 'status', { nativeType: 'agent/status', status: 'running' })),
    f.event('parent', 'catalog', 'status', { nativeType: 'subagent/catalog', childSessionId: 'child' }),
    f.event('parent', 'claim', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'original' }), rootInput,
    f.event('parent', 'parent-action', 'output'), f.event('child', 'child-request', 'input', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'delegated', sourceKind: 'user', messageId: 'delegated', inherited: false }), f.event('child', 'child-action', 'output'),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ['parent', 'child'].map(sessionId => ({ sessionId })),
    activations: ['parent', 'child'].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [reference(taskId, `${taskId}-state`)] })),
    relations: [{ parentSessionId: 'parent', childSessionId: 'child', kind: 'delegation', evidenceRefs: [reference('parent', 'catalog')] }],
    instructionBindings: [{ taskId: 'parent', lifecycleId: 'life', inputRef: reference('parent', 'parent-request'), phase: 'consumed', origin: 'user-entry', originRefs: [reference('parent', 'claim')], ancestorLimitRefs: [] }, { taskId: 'child', lifecycleId: 'life', inputRef: reference('child', 'child-request'), phase: 'consumed', origin: 'delegated', originRefs: [reference('parent', 'catalog')], ancestorLimitRefs: [reference('parent', 'parent-request')] }],
    coverage: { complete: true, branchGaps: [] },
  } }
}

test('F2 incomplete activation keeps retained red evidence ahead of gray in task, node and scope projections', async t => {
  const f = await fixture(t, { models: true }), batch = actorGraph(f)
  await f.ingest(batch)
  let snapshot
  await until(async () => { snapshot = await f.snapshot(); return snapshot.tasks.every(task => task.alerts.some(alert => alert.status === 'attention')) }, 'both first grounded judgments')
  const old = snapshot.tasks.find(task => task.id === 'child'), oldAlert = old.alerts[0]
  const update = structuredClone(batch); update.events = []; update.hierarchyContext.revision++
  update.hierarchyContext.activations[1].state = 'unknown'
  update.hierarchyContext.coverage = { complete: false, branchGaps: [{ sessionId: 'child', reason: 'Current activation observation is incomplete' }] }
  await f.ingest(update)
  snapshot = await f.snapshot()
  const child = snapshot.tasks.find(task => task.id === 'child'), group = snapshot.hierarchyContext.groups[0]
  assert.equal(child.status, 'running')
  assert.deepEqual(child.instructionContext.binding, old.instructionContext.binding)
  assert.equal(child.alerts[0].id, oldAlert.id)
  assert.deepEqual(child.alerts[0].evidenceIds, oldAlert.evidenceIds)
  assert.equal(child.alerts[0].status, 'attention')
  assert.equal(child.gs.light, 'red')
  assert.equal(group.nodes.find(node => node.taskKey === child.key).light, 'red')
  assert.deepEqual(group.counts, { green: 0, yellow: 0, red: 2, gray: 0 })
  assert.equal(group.coverage.complete, false)
  assert.equal(group.coverage.branchGaps[0].reason, 'Current activation observation is incomplete')
  await f.request('source/unregister', { sourceKey: f.sourceKey })
  snapshot = await f.snapshot()
  assert.ok(snapshot.tasks.every(task => task.gs.light === 'gray'))
  assert.ok(snapshot.tasks.every(task => !task.alerts.some(alert => alert.status === 'attention')))
  assert.deepEqual(snapshot.hierarchyContext.groups[0].counts, { green: 0, yellow: 0, red: 0, gray: 2 })
})

test('F2 actual expiry or request replacement invalidates old red despite the same incomplete activation', async t => {
  for (const boundary of ['expiry', 'replacement']) {
    const f = await fixture(t, { models: true }), batch = actorGraph(f)
    await f.ingest(batch)
    await until(async () => (await f.snapshot()).tasks.every(task => task.alerts.some(alert => alert.status === 'attention')), 'initial red before actual invalidation')
    await f.request('monitoring/pause', {})
    const update = structuredClone(batch); update.events = []; update.hierarchyContext.revision++
    update.hierarchyContext.activations[1].state = 'unknown'
    update.hierarchyContext.coverage = { complete: false, branchGaps: [{ sessionId: 'child', reason: 'Current activation observation is incomplete' }] }
    await f.ingest(update)
    if (boundary === 'expiry') f.advance(86_400_001)
    else {
      f.advance(1)
      update.hierarchyContext.revision++
      update.events = [f.event('child', 'new-scope', 'input', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'delegated', sourceKind: 'user', messageId: 'new-delegated', inherited: false })]
      update.hierarchyContext.instructionBindings[1].inputRef = reference('child', 'new-scope')
      await f.ingest(update)
    }
    const snapshot = await f.snapshot(), child = snapshot.tasks.find(task => task.id === 'child')
    assert.equal(child.gs.light, 'gray', boundary)
    assert.equal(snapshot.hierarchyContext.groups[0].nodes.find(node => node.taskKey === child.key).light, 'gray', boundary)
    assert.ok(!child.alerts.some(alert => ['attention', 'recovered'].includes(alert.status)), boundary)
    if (boundary === 'replacement') assert.equal(child.alerts[0].status, 'superseded')
  }
})

test('F3 a held model result cannot exempt a tool timeout already established before the current user wait', async t => {
  const f = await fixture(t, { models: true, hold: true, drift: 'clear' }), started = f.now() - 400_000
  await f.ingest({ tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Held timeout' }], events: [f.event('actor', 'request', 'input', {}, started - 100_000), f.event('actor', 'tool-start', 'tool-start', { callId: 'matching-call' }, started)] })
  await until(() => f.held.length === 1, 'analysis captured the already-overdue tool before asking the user')
  assert.ok(JSON.parse(f.requests[0].body.messages[1].content).evidence.some(item => item.text === 'tool-start'))
  const waitingSince = f.advance(1)
  await f.ingest({ events: [f.event('actor', 'asked', 'status', { nativeType: 'approval/asked', approvalId: 'current-wait', callId: 'matching-call' })] })
  f.held.shift()()
  let actor
  await until(async () => { actor = (await f.snapshot()).tasks[0]; return actor.analysis.state === 'cooldown' }, 'held result completed once')
  const alert = actor.alerts.find(alert => alert.checkId === 'long_wait')
  assert.equal(alert.status, 'attention')
  assert.ok(alert.firstAt >= waitingSince, 'the alert was created after waiting began')
  assert.equal(actor.gs.waiting.kind, 'user')
  assert.equal(actor.gs.light, 'red', 'completion order cannot erase the independent earlier timeout')
  f.advance(3_000)
  const notices = await f.request('notifications/claim', { sourceKey: f.sourceKey })
  assert.equal(notices.length, 1)
  assert.ok(notices[0].checkIds.includes('long_wait'), 'the first sufficiently grounded round remains eligible to notify')
  assert.equal(f.requests.filter(request => request.path === '/v1/chat/completions').length, 1, 'no second vote is needed')
})

test('F3 actual pre-wait overdue facts stay red while time caused by the current user wait stays exempt', async t => {
  for (const preWaitOverdue of [true, false]) {
    const f = await fixture(t, { models: true, drift: preWaitOverdue ? 'attention' : 'clear' }), started = f.now() - (preWaitOverdue ? 700_000 : 400_000), waitingSince = f.now() - 300_000
    await f.ingest({ tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Paired timeout facts' }], events: [
      f.event('actor', 'request', 'input', {}, started - 100_000), f.event('actor', 'start', 'tool-start', { callId: 'matching-call' }, started), f.event('actor', 'asked', 'status', { nativeType: 'approval/asked', approvalId: 'audit', callId: 'matching-call' }, waitingSince),
    ] })
    let actor
    await until(async () => { actor = (await f.snapshot()).tasks[0]; return actor.analysis.state === 'cooldown' }, 'one paired round completed')
    assert.equal(actor.gs.waiting.kind, 'user')
    f.advance(3_000)
    const notices = await f.request('notifications/claim', { sourceKey: f.sourceKey })
    if (preWaitOverdue) {
      const longWait = actor.alerts.find(alert => alert.checkId === 'long_wait')
      assert.equal(longWait.observedAt, started + 300_000, 'the independent timeout crossed its threshold before the user wait')
      assert.ok(longWait.observedAt < waitingSince)
      assert.equal(actor.gs.light, 'red')
      assert.deepEqual(new Set(notices[0].checkIds), new Set(['request_drift', 'long_wait']), 'the user wait hides neither independent issue')
    } else {
      assert.equal(actor.analysis.checks.find(check => check.id === 'long_wait').status, 'clear')
      assert.ok(!actor.alerts.some(alert => alert.checkId === 'long_wait'))
      assert.equal(actor.gs.light, 'green')
      assert.deepEqual(notices, [])
    }
    assert.equal(f.requests.filter(request => request.path === '/v1/chat/completions').length, 1)
  }
})
