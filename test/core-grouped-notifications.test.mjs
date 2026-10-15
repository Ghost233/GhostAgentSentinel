import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
async function until(predicate, label) {
  const deadline = Date.now() + 3000
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await delay(5) }
}

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-grouped-notifications-'))
  const capturedAt = Date.now() - 10_000, requests = [], sourceKeys = []
  let clock = Date.now(), server, client, modelRequests = 0, decision = () => 'attention'
  const models = createServer(async (request, response) => {
    modelRequests++
    try {
      let raw = ''; for await (const chunk of request) raw += chunk
      const wire = JSON.parse(raw), body = request.url === '/v1/systemone' ? decodeJevRequest(wire) : wire; requests.push({ path: request.url, body })
      response.setHeader('content-type', 'application/json')
      if (request.url === '/v1/chat/completions') {
        response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
      } else {
        response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? decision(body.state) : 'unknown' }])) }))
      }
    } finally { modelRequests-- }
  })
  t.after(async () => {
    const failures = [], cleanup = {}
    for (const step of [
      async () => {
        for (const sourceKey of sourceKeys) await client.unregisterSource(sourceKey)
        await until(async () => {
          const value = await client.diagnostics()
          cleanup.core = { ...value.runtime, queueKeys: value.temporary.queueKeys, queueMemberships: value.temporary.queueMemberships, httpRequests: value.http.activeRequests }
          return value.runtime.activeAnalyses === 0 && value.runtime.cancelledUnsettledRequests === 0
        }, 'notification fixture analysis settles naturally')
        assert.deepEqual(cleanup.core, { connectedSources: 0, activeAnalyses: 0, cancelledUnsettledRequests: 0, unsettledAnalysisCalls: 0, analysisAdmissionLimit: 1, analysisAdmissionBlocked: false, queueKeys: 0, queueMemberships: 0, httpRequests: 0 })
      },
      async () => { await client?.close(); if (client) { cleanup.client = client.transportDiagnostics(); assert.deepEqual(cleanup.client, { closed: true, pendingRequests: 0, deadlineTimers: 0 }) } },
      async () => { await server?.close(); if (server) { await assert.rejects(stat(join(dataDir, 'runtime.json')), { code: 'ENOENT' }); await assert.rejects(stat(join(dataDir, 'core.lock')), { code: 'ENOENT' }); cleanup.coreClosed = true } },
      async () => { if (models.listening) await new Promise((resolve, reject) => models.close(error => error ? reject(error) : resolve())); cleanup.modelRequests = modelRequests; cleanup.modelListening = models.listening; assert.equal(modelRequests, 0); assert.equal(models.listening, false) },
      () => rm(dataDir, { recursive: true, force: true }),
    ]) { try { await step() } catch (error) { failures.push(error) } }
    t.diagnostic(`cleanup: ${JSON.stringify(cleanup)}`)
    if (failures.length) throw new AggregateError(failures, 'grouped notification fixture cleanup failed')
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  server = await startServer({ dataDir, now: () => clock, leaseMs: 172_800_000 })
  client = await connectCore({ dataDir })
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  await client.configureAnalysis({ llm: { baseUrl, model: 'notification-extractor' }, jev: { baseUrl, model: 'notification-judge' } })
  const register = async (registrationId = 'grouped-source') => {
    const sourceKey = await client.registerSource({ registrationId, hostRunId: 'host', name: '同名来源' })
    sourceKeys.push(sourceKey); return sourceKey
  }
  const event = (taskId, id, kind, text, data = {}, offset = 0) => ({ id, taskId, taskLifecycleId: 'life', kind, text, capturedAt: capturedAt + offset, occurredAt: null, data })
  const batch = {
    tasks: ['root', 'one', 'two'].map(id => ({ id, lifecycleId: 'life', sessionId: id, title: '同名代理', status: id === 'root' ? 'idle' : 'running' })),
    events: [
      ...['root', 'one', 'two'].map(id => event(id, `${id}-state`, 'status', '', { nativeType: 'agent/status', status: id === 'root' ? 'idle' : 'running' })),
      ...['one', 'two'].map(id => event('root', `${id}-catalog`, 'status', '', { nativeType: 'subagent/catalog', childSessionId: id })),
      event('root', 'root-claim', 'input', 'Inspect repository without modifying files.', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'root-message' }),
      event('root', 'root-request', 'input', 'Inspect repository without modifying files.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'unknown', inherited: false, messageId: 'root-message' }),
    ],
    hierarchyContext: {
      schemaVersion: 1, revision: 1, sessions: ['root', 'one', 'two'].map(sessionId => ({ sessionId })),
      activations: ['root', 'one', 'two'].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: taskId === 'root' ? 'ended' : 'running', stateRefs: [ref(taskId, `${taskId}-state`)] })),
      relations: ['one', 'two'].map(childSessionId => ({ parentSessionId: 'root', childSessionId, kind: 'delegation', evidenceRefs: [ref('root', `${childSessionId}-catalog`)] })),
      instructionBindings: [{ taskId: 'root', lifecycleId: 'life', inputRef: ref('root', 'root-request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('root', 'root-claim')], ancestorLimitRefs: [] }],
      coverage: { complete: true, branchGaps: [] },
    },
  }
  const addProblem = (taskId, offset = 0) => {
    batch.events.push(event(taskId, `${taskId}-request`, 'input', 'Inspect only.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', inherited: false, messageId: `${taskId}-message` }, offset), event(taskId, `${taskId}-action`, 'output', `Modified unrelated storage for ${taskId}.`, {}, offset))
    batch.hierarchyContext.instructionBindings.push({ taskId, lifecycleId: 'life', inputRef: ref(taskId, `${taskId}-request`), phase: 'consumed', origin: 'delegated', originRefs: [ref('root', `${taskId}-catalog`)], ancestorLimitRefs: [ref('root', 'root-request')] })
  }
  return { client, batch, requests, register, addProblem, event, decide(value) { decision = value }, advance(value) { clock += value }, ingest(sourceKey) { return client.ingest(sourceKey, batch) } }
}

test('same-conversation alerts update the panel immediately and share a non-sliding three-second notification window', async t => {
  const f = await fixture(t), sourceKey = await f.register()
  f.addProblem('one')
  await f.ingest(sourceKey)
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'one')?.gs.light === 'red', 'first grounded alert is visible in the public panel snapshot')
  const first = (await f.client.snapshot()).tasks.find(task => task.id === 'one').alerts[0].id
  assert.deepEqual(await f.client.claimNotifications(sourceKey), [], 'the first candidate opens a bounded grouping window')

  f.advance(2000)
  f.addProblem('two', 2000); f.batch.hierarchyContext.revision++
  await f.ingest(sourceKey)
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'two')?.gs.light === 'red', 'second independent alert appears before desktop notification')
  const snapshot = await f.client.snapshot(), actors = snapshot.tasks.filter(task => ['one', 'two'].includes(task.id))
  assert.equal(actors.find(task => task.id === 'one').alerts[0].id, first, 'an additional node does not replace the first problem record')
  assert.notEqual(actors[0].alerts[0].id, actors[1].alerts[0].id)
  assert.equal(snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'root').counts.red, 2, 'hierarchy summary does not become a third alerting execution')
  f.advance(999)
  assert.deepEqual(await f.client.claimNotifications(sourceKey), [])
  f.advance(1)
  const notices = await f.client.claimNotifications(sourceKey)
  assert.equal(notices.length, 1, 'the second alert cannot move the deadline beyond three seconds from the first')
  for (const actor of actors) {
    const member = notices[0].members.find(member => member.taskKey === actor.key)
    assert.ok(member, 'each same-name agent is traceable by complete observation identity')
    assert.equal(member.name, actor.title)
    assert.deepEqual(member.checkIds, ['request_drift'])
    assert.deepEqual(member.alertIds, [actor.alerts[0].id])
    assert.ok(member.reasons.includes(actor.alerts[0].reason), 'each agent retains its independent concern')
    assert.ok(notices[0].body.includes(actor.title))
    assert.ok(notices[0].body.includes(actor.key.slice(0, 8)), 'same-name agents retain a distinguishing system-visible identity')
    assert.ok(notices[0].body.includes(actor.alerts[0].reason))
  }
  assert.equal(notices[0].rootSessionId, 'root')
  assert.equal(notices[0].sourceKey, sourceKey)
  assert.ok(notices[0].body.slice(0, 240).includes(notices[0].taskKey), 'the first surviving observation identity remains system-visible before client truncation')
  assert.ok(notices[0].body.slice(0, 240).includes(sourceKey))
  assert.deepEqual(await f.client.claimNotifications(sourceKey), [], 'the grouped attempt is only claimed once')
  const ack = await f.client.ackNotification(notices[0].id, { status: 'submitted' })
  assert.equal(ack.delivery, 'system-accepted')
  assert.equal(ack.seen, 'unknown', 'system submission is not evidence that the user saw the notification')
  const after = await f.client.snapshot()
  for (const actor of after.tasks.filter(task => ['one', 'two'].includes(task.id))) assert.equal(actor.alerts[0].notification.id, notices[0].id)
})

test('different sources and different confirmed root conversations have separate grouped delivery attempts', async t => {
  const f = await fixture(t), sources = [await f.register('first-source'), await f.register('second-source')]
  f.addProblem('one'); f.addProblem('two')
  const names = { root: 'other-root', one: 'other-one', two: 'other-two' }
  const rename = value => Array.isArray(value) ? value.map(rename) : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, rename(item)])) : names[value] ?? value
  const other = rename(f.batch)
  f.batch.tasks.push(...other.tasks); f.batch.events.push(...other.events)
  for (const key of ['sessions', 'activations', 'relations', 'instructionBindings']) f.batch.hierarchyContext[key].push(...other.hierarchyContext[key])
  for (const source of sources) await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 8, 'all four independent actors in each source have grounded alerts')
  f.advance(3000)
  const all = []
  for (const source of sources) {
    const notices = await f.client.claimNotifications(source)
    assert.equal(notices.length, 2)
    assert.deepEqual(new Set(notices.map(notice => notice.rootSessionId)), new Set(['root', 'other-root']))
    for (const notice of notices) {
      assert.equal(notice.sourceKey, source)
      assert.equal(notice.members.length, 2)
      const actors = (await f.client.snapshot()).tasks.filter(task => notice.members.some(member => member.taskKey === task.key))
      assert.ok(actors.every(task => task.sourceKey === source && task.hierarchy.rootSessionId === notice.rootSessionId))
      await f.client.ackNotification(notice.id, { status: 'submitted' })
    }
    assert.deepEqual(await f.client.claimNotifications(source), [])
    all.push(...notices)
  }
  assert.equal(new Set(all.map(notice => notice.id)).size, 4)
})

test('an older descendant keeps its verified root execution when the same root session starts a new native run', async t => {
  const f = await fixture(t), source = await f.register()
  const laterTask = f.batch.tasks.find(task => task.id === 'two')
  const laterActivation = f.batch.hierarchyContext.activations.find(activation => activation.taskId === 'two')
  const laterRelation = f.batch.hierarchyContext.relations.find(relation => relation.childSessionId === 'two')
  f.batch.tasks = f.batch.tasks.filter(task => task.id !== 'two')
  f.batch.events = f.batch.events.filter(event => !['two-state', 'two-catalog'].includes(event.id))
  f.batch.hierarchyContext.sessions = f.batch.hierarchyContext.sessions.filter(session => session.sessionId !== 'two')
  f.batch.hierarchyContext.activations = f.batch.hierarchyContext.activations.filter(activation => activation.taskId !== 'two')
  f.batch.hierarchyContext.relations = f.batch.hierarchyContext.relations.filter(relation => relation.childSessionId !== 'two')
  f.batch.tasks.push({ id: 'dispatcher', lifecycleId: 'life', title: 'Native start carrier', status: 'idle' })
  f.batch.events.push(f.event('dispatcher', 'root-one', 'status', '', { nativeType: 'subagent/start', childSessionId: 'root', runId: 'root-one' }))
  const root = f.batch.hierarchyContext.activations.find(activation => activation.taskId === 'root')
  root.runId = 'root-one'; root.stateRefs.push(ref('dispatcher', 'root-one'))
  f.addProblem('one'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'one')?.gs.light === 'red', 'old descendant is observed under root-one')
  const older = (await f.client.snapshot()).tasks.find(task => task.id === 'one')
  assert.equal(older.notificationScope.rootIdentity.runId, 'root-one')
  f.advance(2000)
  f.batch.events.push(f.event('dispatcher', 'root-two', 'status', '', { nativeType: 'subagent/start', childSessionId: 'root', runId: 'root-two' }, 2000))
  root.runId = 'root-two'; root.stateRefs = [ref('dispatcher', 'root-two')]; root.state = 'running'
  f.batch.tasks.find(task => task.id === 'root').status = 'running'
  f.batch.tasks.push(laterTask)
  f.batch.events.push(f.event('two', 'two-state', 'status', '', { nativeType: 'agent/status', status: 'running' }, 2000), f.event('root', 'two-catalog', 'status', '', { nativeType: 'subagent/catalog', childSessionId: 'two' }, 2000))
  f.batch.hierarchyContext.sessions.push({ sessionId: 'two' })
  f.batch.hierarchyContext.activations.push(laterActivation)
  f.batch.hierarchyContext.relations.push(laterRelation)
  f.addProblem('two', 2000); f.batch.hierarchyContext.revision++
  await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'two')?.gs.light === 'red', 'new descendant is observed under root-two')
  const snapshot = await f.client.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === 'one').notificationScope.rootIdentity.runId, 'root-one')
  assert.equal(snapshot.tasks.find(task => task.id === 'two').notificationScope.rootIdentity.runId, 'root-two')
  f.advance(1000)
  const [first] = await f.client.claimNotifications(source)
  assert.equal(first.rootIdentity.runId, 'root-one')
  assert.deepEqual(first.members.map(member => member.taskKey), [older.key])
  await f.client.ackNotification(first.id, { status: 'submitted' })
  f.advance(2000)
  const [second] = await f.client.claimNotifications(source)
  assert.equal(second.rootIdentity.runId, 'root-two')
  assert.notEqual(second.id, first.id)
  assert.equal(second.members.length, 1)
  assert.notEqual(second.members[0].taskKey, older.key)
})

test('unconfirmed root activations remain explicit coverage gaps and cannot merge separate actors', async t => {
  const f = await fixture(t), source = await f.register()
  f.batch.hierarchyContext.activations = f.batch.hierarchyContext.activations.filter(activation => activation.taskId !== 'root')
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'grounded actor concerns remain independent of missing root-generation proof')
  for (const actor of (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.notificationScope.coverage.complete, false)
    assert.ok(actor.notificationScope.coverage.gaps.includes('notification_root_activation_unconfirmed'))
  }
  f.advance(3000)
  const notices = await f.client.claimNotifications(source)
  assert.equal(notices.length, 2)
  for (const notice of notices) {
    assert.equal(notice.rootSessionId, null)
    assert.equal(notice.coverage.complete, false)
    assert.ok(notice.coverage.gaps.includes('notification_root_activation_unconfirmed'))
    assert.equal(notice.members.length, 1)
  }
})

test('a delayed old delegation cannot acquire the latest root native run without generation proof', async t => {
  const f = await fixture(t), source = await f.register()
  f.batch.tasks.push({ id: 'dispatcher', lifecycleId: 'life', title: 'Native start carrier', status: 'idle' })
  f.batch.events.push(f.event('dispatcher', 'latest-root-run', 'status', '', { nativeType: 'subagent/start', childSessionId: 'root', runId: 'latest-root-run' }, 2000))
  const root = f.batch.hierarchyContext.activations.find(activation => activation.taskId === 'root')
  root.runId = 'latest-root-run'; root.stateRefs.push(ref('dispatcher', 'latest-root-run'))
  f.batch.events.find(event => event.id === 'two-catalog').capturedAt += 2000
  f.addProblem('one'); f.addProblem('two', 2000); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'both concerns are grounded, independently from root-generation proof')
  const snapshot = await f.client.snapshot(), old = snapshot.tasks.find(task => task.id === 'one'), current = snapshot.tasks.find(task => task.id === 'two')
  assert.equal(old.notificationScope.rootIdentity, null, 'the older catalog predates the current root execution and cannot be rebound to it')
  assert.equal(old.notificationScope.coverage.complete, false)
  assert.equal(current.notificationScope.rootIdentity.runId, 'latest-root-run')
  f.advance(3000)
  const notices = await f.client.claimNotifications(source)
  assert.equal(notices.length, 2)
  assert.equal(notices.find(notice => notice.members[0].taskKey === old.key).coverage.complete, false)
})

test('expiry of root-generation proof splits current actor concerns without renewing their first candidate deadline', async t => {
  const f = await fixture(t), source = await f.register()
  f.batch.events.find(event => event.id === 'root-state').capturedAt = Date.now() - 86_400_000 + 2000
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'current actor evidence is grounded while its root proof is still retained')
  f.advance(3000)
  const snapshot = await f.client.snapshot()
  for (const actor of snapshot.tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.gs.light, 'red', 'expiry of unrelated root-state proof does not repair the actor concern')
    assert.equal(actor.notificationScope.coverage.complete, false)
    assert.ok(actor.notificationScope.coverage.gaps.includes('notification_root_evidence_expired'))
  }
  const notices = await f.client.claimNotifications(source)
  assert.equal(notices.length, 2, 'unknown root associations use independent fallback groups at the original deadline')
  for (const notice of notices) {
    assert.equal(notice.members.length, 1)
    assert.equal(notice.rootIdentity, null)
    assert.equal(notice.coverage.complete, false)
    assert.ok(notice.coverage.gaps.includes('notification_root_evidence_expired'))
  }
})

test('an invalid first member is removed before claim without moving the surviving member deadline or identity', async t => {
  const f = await fixture(t), source = await f.register()
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'both independent candidates are visible')
  const before = await f.client.snapshot(), old = before.tasks.find(task => task.id === 'one'), survivor = before.tasks.find(task => task.id === 'two')
  f.advance(2000)
  f.batch.tasks.find(task => task.id === 'one').status = 'idle'
  f.batch.events.push(f.event('one', 'one-ended', 'status', '', { nativeType: 'agent/status', status: 'idle' }, 2000))
  const activation = f.batch.hierarchyContext.activations.find(item => item.taskId === 'one')
  activation.state = 'ended'; activation.stateRefs = [ref('one', 'one-ended')]; f.batch.hierarchyContext.revision++
  await f.ingest(source)
  f.advance(999); assert.deepEqual(await f.client.claimNotifications(source), [])
  f.advance(1)
  const notices = await f.client.claimNotifications(source)
  assert.equal(notices.length, 1)
  assert.equal(notices[0].taskKey, survivor.key)
  assert.deepEqual(notices[0].members.map(member => member.taskKey), [survivor.key])
  assert.ok(notices[0].body.slice(0, 240).includes(survivor.key))
  assert.ok(!notices[0].body.includes(old.key))
  assert.equal((await f.client.snapshot()).tasks.find(task => task.id === 'two').gs.light, 'red')
})

test('yellow and completed unknown nodes do not become desktop anomaly candidates', async t => {
  const f = await fixture(t), source = await f.register()
  await f.ingest(source)
  assert.equal((await f.client.snapshot()).hierarchyContext.groups[0].counts.yellow, 2)
  f.advance(3000); assert.deepEqual(await f.client.claimNotifications(source), [])
  f.decide(() => 'unknown'); f.addProblem('one', 3000); f.addProblem('two', 3000); f.batch.hierarchyContext.revision++
  await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id)).every(task => task.analysis.completedAt != null), 'unknown semantic judgments complete')
  assert.ok((await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id)).every(task => task.gs.light === 'gray' && task.alerts.length === 0))
  f.advance(3000); assert.deepEqual(await f.client.claimNotifications(source), [])
})

for (const boundary of ['request-replaced', 'ended', 'source-disconnected', 'evidence-expired']) {
  test(`${boundary} invalidates all old candidates before delivery and stops their later reminders`, async t => {
    const f = await fixture(t), source = await f.register()
    f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
    await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'old candidates are initially grounded')
    f.advance(2000)
    if (boundary === 'source-disconnected') await f.client.unregisterSource(source)
    else if (boundary === 'evidence-expired') f.advance(86_400_000)
    else {
      for (const id of ['one', 'two']) {
        if (boundary === 'ended') f.batch.tasks.find(task => task.id === id).status = 'idle'
        else {
          f.batch.events.push(f.event(id, `${id}-replacement`, 'input', 'Inspect the new scope only.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', inherited: false, inputSequence: 2 }, 2000))
          const original = f.batch.hierarchyContext.instructionBindings.find(binding => binding.taskId === id)
          f.batch.hierarchyContext.instructionBindings.push({ ...original, inputRef: ref(id, `${id}-replacement`) })
        }
      }
      f.batch.hierarchyContext.revision++; await f.ingest(source)
    }
    f.advance(1000)
    if (boundary === 'source-disconnected') await assert.rejects(f.client.claimNotifications(source), /not registered or connected/)
    else assert.deepEqual(await f.client.claimNotifications(source), [])
    f.advance(600_000)
    if (boundary !== 'source-disconnected') assert.deepEqual(await f.client.claimNotifications(source), [], 'a timer must not resurrect the old problem')
    const snapshot = await f.client.snapshot()
    assert.ok(snapshot.tasks.filter(task => ['one', 'two'].includes(task.id)).every(task => !task.alerts.some(alert => alert.status === 'attention')))
  })
}

test('repeat grouping is due ten minutes after successful submission rather than candidate creation or claim', async t => {
  const f = await fixture(t), source = await f.register()
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'the first two alerts complete')
  f.advance(3000)
  const [first] = await f.client.claimNotifications(source)
  f.advance(7000)
  await f.client.ackNotification(first.id, { status: 'submitted' })
  f.advance(599_999)
  assert.deepEqual(await f.client.claimNotifications(source), [], 'ten minutes from candidate or claim is still too early')
  await until(async () => (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id)).every(task => ['idle', 'cooldown'].includes(task.analysis.state)), 'any crossed analysis time condition completes independently')
  const calls = f.requests.length
  f.advance(1); assert.deepEqual(await f.client.claimNotifications(source), [], 'the reminder has its own bounded grouping window')
  f.advance(2999); assert.deepEqual(await f.client.claimNotifications(source), [])
  f.advance(1)
  const notices = await f.client.claimNotifications(source)
  assert.equal(notices.length, 1)
  assert.equal(notices[0].members.length, 2)
  assert.notEqual(notices[0].id, first.id)
  assert.equal(f.requests.length, calls, 'a due reminder does not require an additional model judgment')
  await f.client.ackNotification(notices[0].id, { status: 'submitted' })
  await f.client.ackNotification(first.id, { status: 'failed', error: 'late earlier system error' })
  for (const actor of (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.alerts[0].notification.id, notices[0].id)
    assert.equal(actor.alerts[0].notification.status, 'submitted')
    assert.equal(actor.gs.light, 'red')
  }
})

test('a new problem starts an independent window while a different node retains its previous reminder interval', async t => {
  const f = await fixture(t), source = await f.register()
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'original concerns are grounded')
  const old = (await f.client.snapshot()).tasks.find(task => task.id === 'one').alerts[0].id
  f.advance(3000)
  const [first] = await f.client.claimNotifications(source)
  await f.client.ackNotification(first.id, { status: 'submitted' })
  f.advance(60_000)
  f.batch.events.push(f.event('one', 'one-new-request', 'input', 'Inspect new storage only.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', inherited: false, inputSequence: 2 }, 4000), f.event('one', 'one-new-action', 'output', 'Modified unrelated network.', {}, 4000))
  f.batch.hierarchyContext.instructionBindings.push({ ...f.batch.hierarchyContext.instructionBindings.find(binding => binding.taskId === 'one'), inputRef: ref('one', 'one-new-request') }); f.batch.hierarchyContext.revision++
  await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'one').alerts.some(alert => alert.id !== old && alert.status === 'attention'), 'new request has its own independent problem record')
  assert.deepEqual(await f.client.claimNotifications(source), [])
  f.advance(3000)
  const [notice] = await f.client.claimNotifications(source)
  assert.equal(notice.members.length, 1)
  assert.notEqual(notice.members[0].alertIds[0], old)
  assert.notEqual(notice.id, first.id)
  const unchanged = (await f.client.snapshot()).tasks.find(task => task.id === 'two')
  assert.equal(unchanged.alerts[0].notification.id, first.id)
})

test('failed system submission preserves each red alert and does not masquerade as seen or immediately retry', async t => {
  const f = await fixture(t), source = await f.register()
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'grounded concerns are ready')
  f.advance(3000)
  const [notice] = await f.client.claimNotifications(source)
  const submitted = await f.client.ackNotification(notice.id, { status: 'submitted' })
  assert.equal(submitted.delivery, 'system-accepted'); assert.equal(submitted.seen, 'unknown')
  const failed = await f.client.ackNotification(notice.id, { status: 'failed', error: 'native permission failure' })
  assert.equal(failed.delivery, 'failed'); assert.equal(failed.seen, 'unknown')
  for (const actor of (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.gs.light, 'red')
    assert.equal(actor.alerts[0].notification.status, 'failed')
    assert.equal(actor.alerts[0].notification.error, 'native permission failure')
  }
  f.advance(60_000); assert.deepEqual(await f.client.claimNotifications(source), [])
})

test('an abandoned grouped claim remains delivery unknown and late receipts cannot alter a newer member attempt', async t => {
  const f = await fixture(t), source = await f.register()
  f.addProblem('one'); f.addProblem('two'); await f.ingest(source)
  await until(async () => (await f.client.snapshot()).tasks.filter(task => task.gs.light === 'red').length === 2, 'original grouped attempt is grounded')
  f.advance(3000)
  const [old] = await f.client.claimNotifications(source)
  f.advance(60_000)
  for (const actor of (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.alerts[0].notification.status, 'expired')
    assert.equal(actor.alerts[0].notification.delivery, 'unknown')
    assert.equal(actor.alerts[0].notification.seen, 'unknown')
    assert.equal(actor.gs.light, 'red')
  }
  assert.equal((await f.client.ackNotification(old.id, { status: 'submitted' })).status, 'expired')
  assert.deepEqual(await f.client.claimNotifications(source), [])
  f.advance(540_000); assert.deepEqual(await f.client.claimNotifications(source), [])
  f.advance(3000)
  const [next] = await f.client.claimNotifications(source)
  assert.notEqual(next.id, old.id)
  await f.client.ackNotification(next.id, { status: 'submitted' })
  await f.client.ackNotification(old.id, { status: 'failed', error: 'late old client error' })
  for (const actor of (await f.client.snapshot()).tasks.filter(task => ['one', 'two'].includes(task.id))) {
    assert.equal(actor.alerts[0].notification.id, next.id)
    assert.equal(actor.alerts[0].notification.status, 'submitted')
    assert.equal(actor.alerts[0].notification.error, null)
  }
})
