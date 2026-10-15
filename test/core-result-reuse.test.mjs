import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createCore } from '../src/core/index.mjs'
import { createAnalyzer } from '../src/analysis/index.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const until = async (predicate, label) => { const deadline = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 5)) } }
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-result-reuse-')), requests = [], held = []
  let core, modelServer, clock = Date.now(), holding = false, decision = 'clear'
  t.after(async () => {
    const failures = []
    for (const close of [() => { for (const finish of held.splice(0)) finish() }, () => core?.close(), async () => { if (modelServer) await new Promise(resolve => modelServer.close(resolve)) }, () => rm(dataDir, { recursive: true, force: true })]) { try { await close() } catch (error) { failures.push(error) } }
    assert.equal(modelServer?.listening, false)
    assert.equal((await core.diagnostics()).temporary.timers.total, 0)
    if (failures.length) throw new AggregateError(failures, 'Public Core/model HTTP cleanup failed')
  })
  modelServer = createServer(async (req, res) => {
    let raw = ''; for await (const chunk of req) raw += chunk
    const wire = JSON.parse(raw), body = req.url === '/v1/systemone' ? decodeJevRequest(wire) : wire; requests.push({ path: req.url, body })
    res.setHeader('content-type', 'application/json')
    if (decision === 'fail') { res.statusCode = 500; res.end(JSON.stringify({ error: { message: 'Controlled inference failure' } })); return }
    const finish = () => res.end(JSON.stringify(req.url === '/v1/chat/completions'
      ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: decision }])) }))
    if (holding && req.url === '/v1/chat/completions') held.push(finish)
    else finish()
  })
  await new Promise(resolve => modelServer.listen(0, '127.0.0.1', resolve))
  const baseUrl = `http://127.0.0.1:${modelServer.address().port}`
  const analyzer = options => createAnalyzer({ llm: { baseUrl, model: 'controlled-extractor' }, jev: { baseUrl, model: 'controlled-judge' }, now: () => clock, ...options })
  core = await createCore({ dataDir, analyzer: analyzer(), now: () => clock, leaseMs: 172_800_000 })
  const source = await core.registerSource({ registrationId: 'result-reuse', hostRunId: 'own', name: 'Own Core/HTTP model boundary' })
  return { core, source, requests, held, analyzer, hold(value) { holding = value }, decision(value) { decision = value }, now: () => clock, advance: value => clock += value,
    event(id, kind, at = clock, data = {}, taskId = 'actor') { return { id, taskId, taskLifecycleId: 'life', kind, occurredAt: at, capturedAt: at, text: id, data } },
    async actor() { return (await core.snapshot()).tasks[0] },
  }
}

test('an unchanged successful result is reused after cooldown inside the same time-condition interval without another model call', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', f.now() - 600_000), f.event('output', 'output', f.now() - 400_000)] })
  let first
  await until(async () => { first = await f.actor(); return first.analysis.state === 'cooldown' }, 'first actual HTTP model round completed')
  assert.deepEqual(f.requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone'])
  const originalCompletion = first.analysis.completedAt, originalThrough = first.analysis.through
  f.advance(60_000)
  await f.core.heartbeat(f.source)
  await f.core.snapshot()
  await until(async () => { const snapshot = await f.core.snapshot(); return snapshot.monitoring.activeTaskKey === null && snapshot.monitoring.queueLength === 0 }, 'reuse or unexpected second round settled')
  const reused = await f.actor()
  assert.equal(f.requests.length, 2, 'identical material and already-crossed time thresholds must not invoke either model again')
  assert.equal(reused.analysis.completedAt, originalCompletion)
  assert.equal(reused.analysis.through, originalThrough)
  assert.equal(reused.analysis.resultDisposition, 'reused')
  assert.equal(reused.gs.silence.durationMs, first.gs.silence.durationMs + 60_000, 'execution observation keeps advancing independently')
  assert.equal(reused.analysis.checks.find(check => check.id === 'long_wait').status, 'attention')
})

test('crossing silence and long-wait thresholds schedules a new actual model round without renewing input timestamps', async t => {
  const f = await fixture(t), at = f.now()
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', at), f.event('output', 'output', at)] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'initial normal round completed')
  assert.equal((await f.actor()).gs.light, 'green')
  f.advance(59_999)
  assert.equal((await f.actor()).gs.light, 'green')
  assert.equal(f.requests.length, 2)
  f.advance(1)
  await f.core.snapshot()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'silence-boundary round settled')
  assert.equal(f.requests.length, 4, 'the sixty-second time condition is new even with identical events')
  assert.equal((await f.actor()).gs.light, 'yellow')
  f.advance(240_000)
  await f.core.snapshot()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'long-wait-boundary round settled')
  const actor = await f.actor()
  assert.equal(f.requests.length, 6)
  assert.equal(actor.analysis.checks.find(check => check.id === 'long_wait').status, 'attention')
  assert.equal(actor.events.find(event => event.id === 'output').capturedAt, at)
  assert.equal(actor.analysis.through, at)
})

test('replacing the actual analyzer invalidates unchanged cached judgments while preserving the object completed-round cooldown', async t => {
  const f = await fixture(t), at = f.now()
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', at - 500_000), f.event('output', 'output', at - 400_000), f.event('asked', 'status', at - 300_000, { nativeType: 'approval/asked', approvalId: 'audit' })] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'normal waiting round completed')
  assert.equal((await f.actor()).gs.light, 'green')
  const nextEligibleAt = (await f.actor()).analysis.nextEligibleAt
  f.advance(1000)
  await f.core.setAnalyzer(f.analyzer({ inputBudgetChars: 25_000 }))
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'replacement at current cooldown observed')
  assert.equal(f.requests.length, 2, 'replacement must not start inside this object completed-round cooldown')
  assert.equal((await f.actor()).analysis.nextEligibleAt, nextEligibleAt)
  f.advance(59_000)
  await f.core.snapshot()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'replacement round after cooldown settled')
  assert.equal(f.requests.length, 4, 'the new analyzer cannot reuse the prior analyzer judgment')
})

test('same-scope normal results stay green and explicitly pending while new retained material cools or is analyzed', async t => {
  const f = await fixture(t), at = f.now()
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', at), f.event('output', 'output', at)] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'initial normal round completed')
  const before = await f.actor()
  f.hold(true); f.advance(1)
  await f.core.ingest(f.source, { events: [f.event('new-output', 'output'), f.event('new-result', 'tool-result', f.now(), { callId: 'result', exitCode: 0 })] })
  let actor = await f.actor()
  assert.equal(actor.analysis.resultDisposition, 'pending')
  assert.equal(actor.gs.light, 'green')
  assert.equal(actor.gs.label, '上轮正常 · 待分析')
  assert.equal(actor.analysis.through, before.analysis.through)
  assert.equal(actor.analysis.completedAt, before.analysis.completedAt)
  f.advance(59_999); await f.core.snapshot()
  await until(() => f.held.length === 1, 'new round actually started at cooldown boundary')
  actor = await f.actor()
  assert.equal(actor.analysis.state, 'analyzing')
  assert.equal(actor.gs.light, 'green')
  assert.equal(actor.gs.label, '上轮正常 · 待分析')
  assert.ok(actor.analysis.lagMs > 0)
  f.hold(false); f.held.shift()()
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'new material judgment completed')
  actor = await f.actor()
  assert.equal(actor.analysis.resultDisposition, 'current')
  assert.equal(actor.gs.label, '正常')
  assert.ok(actor.analysis.inputEventIds.includes('new-result'))
})

test('a successfully returned unknown judgment is reused without becoming normal or clear', async t => {
  const f = await fixture(t), at = f.now(); f.decision('unknown')
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', at - 500_000), f.event('output', 'output', at - 400_000), f.event('asked', 'status', at - 300_000, { nativeType: 'approval/asked', approvalId: 'audit' })] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'actual unknown judgment returned')
  const unknown = await f.actor()
  assert.equal(unknown.gs.light, 'gray')
  f.advance(60_000); await f.core.heartbeat(f.source)
  const reused = await f.actor()
  assert.equal(f.requests.length, 2)
  assert.equal(reused.analysis.resultDisposition, 'reused')
  assert.equal(reused.analysis.checks.find(check => check.id === 'request_drift').status, 'unknown')
  assert.equal(reused.analysis.completedAt, unknown.analysis.completedAt)
  assert.equal(reused.gs.light, 'gray')
})

test('a new capture gap or request cannot extend an old normal judgment while the completed cooldown remains', async t => {
  for (const boundary of ['gap', 'request', 'disconnect']) {
    const f = await fixture(t)
    await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input'), f.event('output', 'output')] })
    await until(async () => (await f.actor()).analysis.state === 'cooldown', 'normal judgment before invalidation')
    assert.equal((await f.actor()).gs.light, 'green')
    f.advance(1)
    if (boundary === 'gap') await f.core.ingest(f.source, { gaps: ['Actual current capture gap'] })
    if (boundary === 'request') await f.core.ingest(f.source, { events: [f.event('replacement', 'input')] })
    if (boundary === 'disconnect') await f.core.unregisterSource(f.source)
    const actor = await f.actor()
    assert.equal(actor.analysis.resultDisposition, 'invalid', boundary)
    assert.notEqual(actor.gs.light, 'green', boundary)
    assert.notEqual(actor.gs.label, '上轮正常 · 待分析', boundary)
    assert.equal(f.requests.length, 2)
  }
})

test('actual failed inference retries only after each independent completed-failure cooldown while collection continues', async t => {
  const f = await fixture(t); f.decision('fail')
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input'), f.event('output', 'output')] })
  await until(async () => (await f.actor()).analysis.state === 'failed', 'first actual HTTP failure completed')
  assert.equal(f.requests.length, 1)
  assert.equal((await f.actor()).analysis.resultDisposition, 'invalid')
  f.advance(30_000)
  await f.core.ingest(f.source, { events: [f.event('during-failure', 'output')] })
  assert.ok((await f.actor()).events.some(event => event.id === 'during-failure'))
  f.advance(29_999); await f.core.snapshot()
  assert.equal(f.requests.length, 1)
  f.advance(1); await f.core.snapshot()
  await until(async () => f.requests.length === 2 && (await f.actor()).analysis.state === 'failed', 'second HTTP failure retried at sixty seconds')
  const secondEligibility = (await f.actor()).analysis.nextEligibleAt
  f.decision('clear'); f.advance(59_999); await f.core.snapshot()
  assert.equal(f.requests.length, 2)
  f.advance(1)
  await f.core.ingest(f.source, { events: [f.event('recovery-output', 'output'), f.event('recovery-result', 'tool-result', f.now(), { callId: 'recovery', exitCode: 0 })] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'successful actual retry completed')
  assert.equal(f.requests.length, 4)
  assert.equal((await f.actor()).analysis.completedAt, secondEligibility)
  assert.equal((await f.actor()).gs.light, 'green')
})

test('root and delegated descendants share FIFO, repeated reads do not duplicate queued work, and dequeue takes latest retained material', async t => {
  const f = await fixture(t), at = f.now(), order = ['child', 'grandchild', 'root']
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const ev = (taskId, id, kind, data = {}) => f.event(id, kind, at, data, taskId)
  const edges = [['root', 'child'], ['child', 'grandchild']]
  f.hold(true)
  await f.core.ingest(f.source, { tasks: order.map(id => ({ id, lifecycleId: 'life', sessionId: id, status: 'running', title: id })), events: [
    ...order.flatMap(id => [ev(id, `${id}-state`, 'status', { nativeType: 'agent/status', status: 'running' }), ev(id, `${id}-request`, 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: `${id}-message`, inherited: false }), ev(id, `${id}-output`, 'output')]),
    ev('root', 'root-claim', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'root-message' }),
    ...edges.map(([parent, child]) => ev(parent, `${parent}-${child}`, 'status', { nativeType: 'subagent/catalog', childSessionId: child })),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: order.map(sessionId => ({ sessionId })), activations: order.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, `${taskId}-state`)] })),
    relations: edges.map(([parentSessionId, childSessionId]) => ({ parentSessionId, childSessionId, kind: 'delegation', evidenceRefs: [ref(parentSessionId, `${parentSessionId}-${childSessionId}`)] })),
    instructionBindings: order.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, `${taskId}-request`), phase: 'consumed', origin: taskId === 'root' ? 'user-entry' : 'delegated', originRefs: [taskId === 'root' ? ref('root', 'root-claim') : taskId === 'child' ? ref('root', 'root-child') : ref('child', 'child-grandchild')], ancestorLimitRefs: taskId === 'root' ? [] : [ref('root', 'root-request')] })), coverage: { complete: true, branchGaps: [] },
  } })
  await until(() => f.held.length === 1, 'first actual child HTTP extraction held')
  f.advance(1000)
  for (let read = 0; read < 5; read++) {
    const snapshot = await f.core.snapshot()
    assert.ok(snapshot.hierarchyContext.scopes.some(scope => scope.sessionId === 'root'))
    assert.equal(snapshot.monitoring.queueLength, 2)
  }
  await f.core.ingest(f.source, { events: [f.event('latest-grandchild-output', 'output', f.now(), {}, 'grandchild'), f.event('latest-grandchild-result', 'tool-result', f.now(), { callId: 'grandchild-result', exitCode: 0 }, 'grandchild')] })
  assert.equal((await f.core.snapshot()).monitoring.queueLength, 2)
  f.hold(false); f.held.shift()()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null && f.requests.filter(request => request.path === '/v1/systemone').length === 3, 'all three actual HTTP rounds completed in order')
  const judged = f.requests.filter(request => request.path === '/v1/systemone').map(request => request.body.state)
  assert.deepEqual(judged.map(state => state.instruction.inputRef.taskId), order)
  assert.ok(judged[1].events.some(event => event.id === 'latest-grandchild-result'))
  assert.equal(f.requests.filter(request => request.path === '/v1/chat/completions').length, 3)
  assert.ok((await f.core.snapshot()).tasks.every(task => task.analysis.nextEligibleAt === f.now() + 60_000))
})

test('reusing a valid alert still permits its timed reminder without another actual inference call', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input', f.now() - 600_000), f.event('output', 'output', f.now() - 400_000)] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'first grounded wait alert completed')
  f.advance(3_000)
  const [first] = await f.core.claimNotifications(f.source)
  assert.ok(first)
  await f.core.ackNotification(first.id, { status: 'submitted' })
  f.advance(600_000)
  await f.core.heartbeat(f.source)
  assert.deepEqual(await f.core.claimNotifications(f.source), [])
  f.advance(3_000)
  const repeated = await f.core.claimNotifications(f.source)
  assert.equal(f.requests.length, 2, 'notification timing is independent of result reuse')
  assert.equal(repeated.length, 1)
  assert.notEqual(repeated[0].id, first.id)
  assert.deepEqual(repeated[0].checkIds, ['long_wait'])
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'idle', title: 'Own actor' }] })
  f.advance(600_000)
  assert.deepEqual(await f.core.claimNotifications(f.source), [], 'ended scope cannot be revived by the notification tick')
})

test('each object cooldown begins at its actual completion rather than another queued actor completion', async t => {
  const f = await fixture(t), at = f.now(); f.hold(true)
  await f.core.ingest(f.source, { tasks: ['a', 'b'].map(id => ({ id, lifecycleId: 'life', status: 'running', title: id })), events: ['a', 'b'].flatMap(id => [f.event(`${id}-request`, 'input', at, {}, id), f.event(`${id}-output`, 'output', at, {}, id)]) })
  await until(() => f.held.length === 1, 'a extraction held')
  f.held.shift()()
  await until(() => f.held.length === 1 && f.requests.filter(request => request.path === '/v1/systemone').length === 1, 'a finished and b extraction held')
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'a').analysis.nextEligibleAt, at + 60_000)
  f.advance(10_000); f.held.shift()()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'b completed ten seconds later')
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'b').analysis.nextEligibleAt, at + 70_000)
  f.hold(false)
  await f.core.ingest(f.source, { events: ['a', 'b'].map(id => f.event(`${id}-fresh`, 'output', f.now(), {}, id)) })
  f.advance(50_000); await f.core.snapshot()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'a eligible round settled')
  assert.equal(f.requests.length, 6)
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'b').analysis.completedAt, at + 10_000)
  f.advance(10_000); await f.core.snapshot()
  await until(async () => (await f.core.snapshot()).monitoring.activeTaskKey === null, 'b independently eligible round settled')
  assert.equal(f.requests.length, 8)
})

test('cancelling an old actual HTTP round leaves the model endpoint alive and seals late output from the replaced request', async t => {
  const f = await fixture(t); f.hold(true)
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input'), f.event('output', 'output')] })
  await until(() => f.held.length === 1, 'old HTTP model response held')
  const oldFinish = f.held.shift(); f.advance(1)
  await f.core.ingest(f.source, { events: [f.event('replacement', 'input'), f.event('replacement-output', 'output'), f.event('replacement-result', 'tool-result', f.now(), { callId: 'new-result', exitCode: 0 })] })
  assert.equal((await f.core.snapshot()).monitoring.pendingCancelledRequests, 1)
  assert.equal((await f.actor()).currentRequest, 'replacement')
  assert.equal((await f.actor()).analysis.state, 'queued')
  oldFinish()
  await until(() => f.held.length === 1, 'replacement HTTP request reached the same live endpoint')
  assert.equal((await f.actor()).analysis.completedAt, null, 'late old response is sealed before the replacement is admitted')
  f.hold(false); f.held.shift()()
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'new request completed')
  const current = await f.actor()
  assert.equal(current.currentRequest, 'replacement')
  assert.equal(current.analysis.summary.currentTask, 'replacement')
  await until(async () => (await f.core.snapshot()).monitoring.pendingCancelledRequests === 0, 'cancelled public round naturally settled')
  const after = await f.actor()
  assert.equal(after.currentRequest, 'replacement')
  assert.equal(after.analysis.summary.currentTask, 'replacement')
  assert.equal(after.analysis.completedAt, current.analysis.completedAt)
  assert.equal(f.requests.filter(request => request.path === '/v1/chat/completions').length, 2)
})

test('twenty-four-hour expiry removes old normal applicability instead of extending it through reuse', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.source, { tasks: [{ id: 'actor', lifecycleId: 'life', status: 'running', title: 'Own actor' }], events: [f.event('request', 'input'), f.event('output', 'output')] })
  await until(async () => (await f.actor()).analysis.state === 'cooldown', 'initial normal completion')
  await f.core.pause(); f.advance(86_400_001)
  const expired = await f.actor()
  assert.equal(expired.analysis.resultDisposition, 'invalid')
  assert.notEqual(expired.gs.light, 'green')
  assert.notEqual(expired.gs.label, '上轮正常 · 待分析')
  assert.equal(expired.analysis.summary, null)
  assert.deepEqual(expired.events, [])
  assert.equal(f.requests.length, 2)
})
