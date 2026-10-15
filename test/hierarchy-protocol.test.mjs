import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-hierarchy-protocol-')), server = await startServer({ dataDir })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  async function request(path, body) {
    const response = await fetch(`${server.url}/v1/${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    return { status: response.status, body: await response.json() }
  }
  const register = (registrationId = 'a', hostRunId = 'run-1') => request('source/register', { registrationId, hostRunId, name: 'Same name' }).then(result => result.body)
  const sourceKey = await register(), capturedAt = Date.now()
  const ref = (taskId, eventId, lifecycleId = 'life') => ({ taskId, lifecycleId, eventId })
  const event = (id, taskId, kind, data) => ({ id, taskId, taskLifecycleId: 'life', kind, text: 'fixed evidence', occurredAt: null, capturedAt, data })
  const batch = {
    tasks: ['parent', 'child'].map(id => ({ id, lifecycleId: 'life', sessionId: id, title: 'Same name', status: 'running' })),
    events: [event('parent-state', 'parent', 'status', { status: 'running', nativeType: 'agent/status' }), event('child-state', 'child', 'status', { status: 'running', nativeType: 'agent/status' }), event('catalog', 'parent', 'status', { nativeType: 'subagent/catalog', childSessionId: 'child' }), event('input', 'child', 'input', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'delegated', messageId: 'child-input' })],
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'parent' }, { sessionId: 'child' }],
      activations: ['parent', 'child'].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, `${taskId}-state`)] })),
      relations: [{ parentSessionId: 'parent', childSessionId: 'child', kind: 'delegation', evidenceRefs: [ref('parent', 'catalog')] }],
      instructionBindings: [{ taskId: 'child', lifecycleId: 'life', inputRef: ref('child', 'input'), phase: 'consumed', origin: 'delegated', originRefs: [ref('parent', 'catalog')], ancestorLimitRefs: [] }], coverage: { complete: false, branchGaps: [{ sessionId: 'child', reason: 'unread descendants' }] } },
  }
  return { request, register, sourceKey, batch, ingest: batch => request('source/ingest', { sourceKey, batch }) }
}

test('hierarchy capabilities, accepted revision, idempotent retry and stale batches preserve the newer whole observation', async t => {
  const f = await fixture(t)
  assert.ok((await f.request('health')).body.features.includes('hierarchy-context/1'))
  assert.ok((await f.request('health')).body.features.includes('gs-status/1'), 'hierarchy support alone does not prove GS status and wait support')
  assert.deepEqual((await f.ingest(f.batch)).body.hierarchyContext, { schemaVersion: 1, acceptedRevision: 1 })
  assert.equal((await f.ingest(f.batch)).body.duplicate, 4)
  const newer = structuredClone(f.batch); newer.hierarchyContext.revision = 2; newer.hierarchyContext.coverage = { complete: true, branchGaps: [] }
  assert.equal((await f.ingest(newer)).status, 200)
  const stale = structuredClone(f.batch); stale.tasks[0].title = 'stale overwrite'
  assert.equal((await f.ingest(stale)).body.hierarchyContext.acceptedRevision, 2)
  const snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').title, 'Same name')
  assert.equal(snapshot.hierarchyContext.sources[0].coverage.complete, true)
})

test('unsupported versions and invalid reference or conflicting revision reject the whole batch without changing source/task facts', async t => {
  const f = await fixture(t)
  await f.ingest(f.batch)
  const before = (await f.request('snapshot')).body
  for (const mutate of [batch => { batch.hierarchyContext.schemaVersion = 2 }, batch => { batch.hierarchyContext.revision = -1 }, batch => { batch.hierarchyContext.revision = 2; batch.hierarchyContext.relations[0].evidenceRefs[0].lifecycleId = 'wrong' }, batch => { batch.hierarchyContext.coverage.branchGaps[0].reason = 'conflict' }]) {
    const invalid = structuredClone(f.batch); invalid.tasks[0].title = 'must not apply'; mutate(invalid)
    const result = await f.ingest(invalid)
    assert.equal(result.status, invalid.hierarchyContext.schemaVersion === 2 ? 409 : 422)
    assert.equal(result.body.code, invalid.hierarchyContext.schemaVersion === 2 ? 'hierarchy_version_unsupported' : 'hierarchy_context_invalid')
    const after = (await f.request('snapshot')).body
    assert.equal(after.tasks.find(task => task.id === 'parent').title, 'Same name')
    assert.equal(after.sources[0].lastSeenAt, before.sources[0].lastSeenAt)
  }
})

test('same names across source and host epochs remain separate, and parent fields, branches or tool ancestry cannot establish delegation', async t => {
  const f = await fixture(t)
  await f.ingest(f.batch)
  const other = await f.register('other'), restarted = await f.register('a', 'run-2')
  for (const sourceKey of [other, restarted]) assert.equal((await f.request('source/ingest', { sourceKey, batch: f.batch })).status, 200)
  const snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.hierarchyContext.groups.length, 3)
  assert.equal(new Set(snapshot.hierarchyContext.groups.map(group => group.key)).size, 3)
  const noDelegation = structuredClone(f.batch); noDelegation.hierarchyContext.revision = 2; noDelegation.hierarchyContext.relations[0].kind = 'branch'; noDelegation.hierarchyContext.instructionBindings[0].origin = 'unknown'; noDelegation.tasks[1].parentId = 'parent'
  assert.equal((await f.ingest(noDelegation)).status, 200)
  assert.equal((await f.request('snapshot')).body.hierarchyContext.groups.filter(group => group.sourceKey === f.sourceKey).length, 2)
  const forged = structuredClone(noDelegation); forged.hierarchyContext.revision = 3; forged.hierarchyContext.relations[0].kind = 'delegation'; forged.events.find(event => event.id === 'catalog').data = { nativeType: 'tool/ptc-dispatch-start', parentCallId: 'parent', childSessionId: 'child' }
  forged.hierarchyContext.relations[0].evidenceRefs[0].eventId = 'fake'; forged.events.find(event => event.id === 'catalog').id = 'fake'
  assert.equal((await f.ingest(forged)).status, 422)
})

test('native role/user labels and rpcId do not by themselves grant permission eligibility', async t => {
  const f = await fixture(t)
  f.batch.events.find(event => event.id === 'input').data = { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'unknown', messageId: 'real-message', sourceKind: 'user', rpcId: 'browser-looking-label' }
  f.batch.hierarchyContext.instructionBindings[0].origin = 'unknown'
  assert.equal((await f.ingest(f.batch)).status, 200)
  assert.equal((await f.request('snapshot')).body.tasks.find(task => task.id === 'child').events.find(event => event.id === 'input').authorizationEligible, false)
  const forged = structuredClone(f.batch); forged.hierarchyContext.revision = 2; forged.hierarchyContext.instructionBindings[0].origin = 'user-entry'
  assert.equal((await f.ingest(forged)).status, 422)
})

test('a native activation run cannot borrow another run identity, and invalid native state proof is atomic', async t => {
  const f = await fixture(t)
  f.batch.hierarchyContext.activations[1].runId = 'forged-native-run'
  assert.equal((await f.ingest(f.batch)).status, 422)
  assert.equal((await f.request('snapshot')).body.tasks.length, 0)
})

test('an accepted source-scoped native run stays bound to its task lifecycle across later hierarchy revisions', async t => {
  const f = await fixture(t)
  const start = { id: 'native-start-r', taskId: 'parent', taskLifecycleId: 'life', kind: 'status', occurredAt: null, capturedAt: Date.now(), text: 'native start', data: { nativeType: 'subagent/start', childSessionId: 'child', runId: 'run-r' } }
  f.batch.events.push(start)
  f.batch.hierarchyContext.activations[1].runId = 'run-r'
  f.batch.hierarchyContext.activations[1].stateRefs.push({ taskId: 'parent', lifecycleId: 'life', eventId: start.id })
  assert.equal((await f.ingest(f.batch)).status, 200)
  const replacement = structuredClone(f.batch)
  replacement.hierarchyContext.revision = 2
  replacement.tasks[1].lifecycleId = 'new-life'
  replacement.events.find(event => event.id === 'child-state').taskLifecycleId = 'new-life'
  replacement.events.find(event => event.id === 'input').taskLifecycleId = 'new-life'
  replacement.hierarchyContext.activations[1].lifecycleId = 'new-life'
  replacement.hierarchyContext.activations[1].stateRefs[0].lifecycleId = 'new-life'
  replacement.hierarchyContext.instructionBindings[0].lifecycleId = 'new-life'
  replacement.hierarchyContext.instructionBindings[0].inputRef.lifecycleId = 'new-life'
  assert.equal((await f.ingest(replacement)).status, 422, 'new lifecycle cannot reuse the accepted old run and start reference')
  let snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.tasks.filter(task => task.id === 'child').length, 1)
  assert.equal(snapshot.tasks.find(task => task.id === 'child').lifecycleId, 'life')
  assert.equal(snapshot.hierarchyContext.sources[0].revision, 1)
  replacement.events = replacement.events.filter(event => event.id !== 'native-start-r')
  replacement.events.push({ ...start, id: 'native-start-next', data: { ...start.data, runId: 'run-next' } })
  replacement.hierarchyContext.activations[1].runId = 'run-next'
  replacement.hierarchyContext.activations[1].stateRefs[1].eventId = 'native-start-next'
  assert.equal((await f.ingest(replacement)).status, 200, 'a different actual native run admits the new lifecycle')
  snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.tasks.filter(task => task.id === 'child').length, 2)
  assert.equal(snapshot.tasks.find(task => task.lifecycleId === 'new-life').hierarchy.runId, 'run-next')
  const independent = await f.register('independent-host')
  assert.equal((await f.request('source/ingest', { sourceKey: independent, batch: f.batch })).status, 200, 'run identities are source-scoped')
})

test('an accepted inbox input or runtime notice remains evidence without replacing the consumed task anchor', async t => {
  const f = await fixture(t)
  f.batch.hierarchyContext.instructionBindings[0].phase = 'accepted'
  f.batch.events.find(event => event.id === 'input').data.nativeType = 'agent/inbox/claimed'
  f.batch.events.find(event => event.id === 'input').data.inputPhase = 'accepted'
  assert.equal((await f.ingest(f.batch)).status, 200)
  assert.equal((await f.request('snapshot')).body.tasks.find(task => task.id === 'child').currentRequest, '')
  const notice = structuredClone(f.batch); notice.hierarchyContext.revision = 2; notice.hierarchyContext.instructionBindings[0].phase = 'consumed'; notice.hierarchyContext.instructionBindings[0].origin = 'runtime-notice'
  notice.events.push({ ...structuredClone(notice.events.find(event => event.id === 'input')), id: 'notice', data: { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'runtime-notice', messageId: 'notice' } })
  notice.hierarchyContext.instructionBindings[0].inputRef.eventId = 'notice'
  assert.equal((await f.ingest(notice)).status, 200)
  assert.equal((await f.request('snapshot')).body.tasks.find(task => task.id === 'child').currentRequest, '')
})


test('known native accepted or claimed records cannot be promoted to consumed by sender phase and rejection preserves current public state', async t => {
  const f = await fixture(t)
  assert.equal((await f.ingest(f.batch)).status, 200)
  const before = (await f.request('snapshot')).body
  for (const nativeType of ['agent/inbox/claimed', 'agent/inbox/spliced']) for (const forgedPhase of [undefined, 'consumed']) {
    const bad = structuredClone(f.batch); bad.hierarchyContext.revision++
    const original = bad.events.find(event => event.id === 'input')
    original.id = 'forged-input'; original.data = { nativeType, ...(forgedPhase ? { inputPhase: forgedPhase } : {}), inputOrigin: 'delegated', messageId: 'forged' }
    bad.hierarchyContext.instructionBindings[0].inputRef.eventId = original.id
    const response = await f.ingest(bad)
    assert.equal(response.status, 422)
    assert.equal(response.body.code, 'hierarchy_context_invalid')
    const after = (await f.request('snapshot')).body
    assert.equal(after.sources[0].hierarchyContext.revision, before.sources[0].hierarchyContext.revision)
    assert.equal(after.tasks.find(task => task.id === 'child').currentRequest, before.tasks.find(task => task.id === 'child').currentRequest)
    assert.ok(!after.tasks.some(task => task.events.some(event => event.id === 'forged-input')))
  }
})

test('a parent wait needs its own retained native audit and a bound active descendant; it never hides child judgments', async t => {
  const f = await fixture(t), at = f.batch.events[0].capturedAt
  const reference = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const native = (id, taskId, kind, data) => ({ id, taskId, taskLifecycleId: 'life', kind, text: id, occurredAt: at, capturedAt: at, data })
  f.batch.events.push(native('parent-claim', 'parent', 'input', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'original-user', inherited: false }), native('parent-request', 'parent', 'input', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: 'original-user', inherited: false }), native('child-output', 'child', 'output', {}), native('wait-start', 'parent', 'status', { nativeType: 'execution/wait-start', waitId: 'wait-one' }))
  f.batch.hierarchyContext.instructionBindings.push({ taskId: 'parent', lifecycleId: 'life', inputRef: reference('parent', 'parent-request'), phase: 'consumed', origin: 'user-entry', originRefs: [reference('parent', 'parent-claim')], ancestorLimitRefs: [] })
  f.batch.hierarchyContext.coverage = { complete: true, branchGaps: [] }
  f.batch.hierarchyContext.waitBindings = [{ taskId: 'parent', lifecycleId: 'life', waitId: 'wait-one', phase: 'started', childRefs: [{ taskId: 'child', lifecycleId: 'life' }], evidenceRefs: [reference('parent', 'wait-start')] }]
  const accepted = await f.ingest(f.batch)
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body))
  let snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').gs.label, '等待子代理')
  assert.equal(snapshot.tasks.find(task => task.id === 'child').gs.light, 'gray')
  assert.deepEqual(snapshot.hierarchyContext.groups[0].counts, { green: 1, yellow: 0, red: 0, gray: 1 })
  for (const mutate of [
    batch => { batch.hierarchyContext.waitBindings[0].evidenceRefs[0].taskId = 'child' },
    batch => { batch.hierarchyContext.waitBindings[0].childRefs[0].lifecycleId = 'foreign-life' },
    batch => { batch.hierarchyContext.relations[0].kind = 'branch'; batch.hierarchyContext.instructionBindings[0].origin = 'unknown' },
    batch => { batch.events.find(event => event.id === 'fresh-bad-wait').data.nativeType = 'tool/call' },
  ]) {
    const bad = structuredClone(f.batch); bad.hierarchyContext.revision = 2; bad.events.find(event => event.id === 'wait-start').id = 'fresh-bad-wait'; bad.hierarchyContext.waitBindings[0].evidenceRefs[0].eventId = 'fresh-bad-wait'; mutate(bad)
    assert.equal((await f.ingest(bad)).status, 422)
    assert.equal((await f.request('snapshot')).body.sources[0].hierarchyContext.revision, 1)
  }
  const ended = structuredClone(f.batch); ended.hierarchyContext.revision = 2; ended.hierarchyContext.waitBindings[0].phase = 'ended'
  ended.events.push(native('wait-end', 'parent', 'status', { nativeType: 'execution/wait-end', waitId: 'wait-one' })); ended.hierarchyContext.waitBindings[0].evidenceRefs.push(reference('parent', 'wait-end'))
  assert.equal((await f.ingest(ended)).status, 200)
  snapshot = (await f.request('snapshot')).body
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').gs.light, 'yellow')
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').gs.waiting, null)
})
