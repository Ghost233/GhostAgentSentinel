import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { DatabaseSync } from 'node:sqlite'

const DAY = 86_400_000
const ref = (taskId, eventId, lifecycleId = 'life') => ({ taskId, lifecycleId, eventId })
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-identity-retention-'))
  let clock = 1_000_000, server, legacyContext
  t.after(async () => { try { await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  const open = async () => { server = await startServer({ dataDir, now: () => clock, leaseMs: DAY * 10 }); return server }
  await open()
  async function request(path, body) {
    const response = await fetch(`${server.url}/v1/${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    return { status: response.status, body: await response.json() }
  }
  const register = (registrationId = 'context', hostRunId = 'host') => request('source/register', { registrationId, hostRunId, name: 'Same name' }).then(result => result.body)
  const sourceKey = await register()
  const event = (taskId, id, kind, text, data = {}, lifecycleId = 'life') => ({ id, taskId, taskLifecycleId: lifecycleId, kind, text, occurredAt: null, capturedAt: clock, data })
  const batch = (taskId = 'root', lifecycleId = 'life', revision = 1, suffix = '') => ({
    tasks: [{ id: taskId, lifecycleId, sessionId: taskId, title: 'Same name', status: 'running' }],
    events: [event(taskId, `state${suffix}`, 'status', '', { nativeType: 'agent/status', status: 'running' }, lifecycleId), event(taskId, `claim${suffix}`, 'input', 'Read only.', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: `message${suffix}` }, lifecycleId), event(taskId, `input${suffix}`, 'input', 'Read only.', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'unknown', sourceKind: 'user', messageId: `message${suffix}`, inherited: false }, lifecycleId)],
    hierarchyContext: { schemaVersion: 1, revision, sessions: [{ sessionId: taskId }], activations: [{ taskId, lifecycleId, sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, `state${suffix}`, lifecycleId)] }], relations: [], instructionBindings: [{ taskId, lifecycleId, inputRef: ref(taskId, `input${suffix}`, lifecycleId), phase: 'consumed', origin: 'user-entry', originRefs: [ref(taskId, `claim${suffix}`, lifecycleId)], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } },
  })
  const ingest = async (value, source = sourceKey) => {
    const response = await request('source/ingest', { sourceKey: source, batch: value })
    if (source === sourceKey && response.status === 200 && value.hierarchyContext && response.body.hierarchyContext?.acceptedRevision === value.hierarchyContext.revision) legacyContext = structuredClone(value.hierarchyContext)
    return response
  }
  const snapshot = () => request('snapshot').then(result => result.body)
  const unknown = original => ({ tasks: structuredClone(original.tasks), events: [], hierarchyContext: { ...structuredClone(original.hierarchyContext), revision: original.hierarchyContext.revision + 1, activations: original.hierarchyContext.activations.map(activation => ({ ...activation, state: 'unknown', runId: null, stateRefs: [] })), relations: [], instructionBindings: [], coverage: { complete: false, branchGaps: [{ sessionId: original.tasks[0].sessionId, reason: 'Current material missing' }] } } })
  return { request, register, sourceKey, event, batch, ingest, snapshot, unknown, advance: value => { clock += value }, restart: async () => { await server.close(); await open() },
    legacyRestart: async advanceAfterClose => {
      await server.close()
      // Legacy source format is the v1 frozen core (SHA-256 62f292c6342fe60927609c9f1bbb4dcbcbc813abf01c15f3b72758dec8ee5b6c).
      // That writer retained its accepted graph when bodies expired. Restore that
      // exact graph before removing later fields; all business assertions use HTTP.
      const db = new DatabaseSync(join(dataDir, 'observations.sqlite'))
      try {
        const source = JSON.parse(db.prepare("SELECT value FROM records WHERE collection='sources' AND key=?").get(sourceKey).value)
        assert.ok(legacyContext, 'legacy serialization uses a previously accepted whole HTTP context')
        source.hierarchyContext = structuredClone(legacyContext)
        delete source.hierarchyMaterialKey
        delete source.hierarchyMaterialAt
        delete source.hierarchyEvidenceCaptures
        delete source.hierarchyCaptureBudgetExceeded
        delete source.hierarchyCaptureMigrationBlocked
        db.prepare("UPDATE records SET value=? WHERE collection='sources' AND key=?").run(JSON.stringify(source), sourceKey)
      } finally { db.close() }
      clock += advanceAfterClose
      await open()
      assert.equal(await register(), sourceKey)
    },
  }
}

test('expired first captures survive an accepted empty hierarchy replacement and reject retiming atomically over HTTP', async t => {
  const f = await fixture(t), original = f.batch()
  assert.equal((await f.ingest(original)).status, 200)
  assert.equal((await f.snapshot()).tasks[0].events.find(event => event.id === 'input').authorizationEligible, true)
  f.advance(DAY + 1)
  assert.equal((await f.request('source/heartbeat', { sourceKey: f.sourceKey })).status, 200)
  assert.equal((await f.snapshot()).tasks[0].events.length, 0)
  assert.equal((await f.ingest(f.unknown(original))).status, 200)
  const before = await f.snapshot(), replay = f.batch('root', 'life', 3)
  replay.tasks[0].title = 'Must not apply'
  const result = await f.ingest(replay)
  assert.equal(result.status, 422, JSON.stringify(result.body))
  assert.equal(result.body.code, 'hierarchy_context_invalid')
  const after = await f.snapshot()
  assert.equal(after.sources[0].hierarchyContext.revision, 2)
  assert.equal(after.sources[0].lastSeenAt, before.sources[0].lastSeenAt)
  assert.equal(after.tasks[0].title, 'Same name')
  assert.equal(after.tasks[0].currentRequest, '')
  assert.deepEqual(after.tasks[0].events, [])
})

test('first captures remain protected after a core restart and source reconnection', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.ingest(f.unknown(original))
  await f.restart()
  assert.equal(await f.register(), f.sourceKey)
  assert.equal((await f.ingest(f.batch('root', 'life', 3))).status, 422)
  assert.ok((await f.snapshot()).tasks.every(task => task.currentRequest === '' && task.events.length === 0))
})

test('legacy persisted hierarchy seeds first captures before initial expiry and optional HTTP traffic', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  f.advance(DAY - 500)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.legacyRestart(501)
  const before = await f.snapshot()
  assert.equal(before.tasks.reduce((sum, task) => sum + task.events.length, 0), 0)
  const replay = f.batch(); delete replay.hierarchyContext; replay.tasks[0].title = 'Must not apply'
  const response = await f.ingest(replay)
  assert.equal(response.status, 422, JSON.stringify(response.body))
  assert.equal(response.body.code, 'hierarchy_context_invalid')
  const after = await f.snapshot()
  assert.equal(after.sources[0].lastSeenAt, before.sources[0].lastSeenAt)
  assert.deepEqual(after.tasks, before.tasks)
  assert.equal((await f.request('diagnostics')).body.retained.hierarchyEvidenceIdentities, 3)
  assert.equal((await f.ingest(f.batch('root', 'life', 2, '-fresh'))).status, 200)
  const freshSource = await f.register('fresh-source')
  assert.equal((await f.ingest(f.batch(), freshSource)).status, 200)
  assert.equal((await f.snapshot()).tasks.find(task => task.sourceKey === freshSource).events.find(event => event.id === 'input').authorizationEligible, true)
})

test('legacy identities stay expired across a first empty graph and another restart', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  f.advance(DAY - 500)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.legacyRestart(501)
  assert.equal((await f.ingest(f.unknown(original))).status, 200)
  await f.restart(); assert.equal(await f.register(), f.sourceKey)
  const response = await f.ingest({ events: f.batch().events })
  assert.equal(response.status, 422, JSON.stringify(response.body))
  assert.ok((await f.snapshot()).tasks.every(task => task.currentRequest === '' && task.events.length === 0))
})

test('legacy context references whose bodies already expired remain closed to optional retiming', async t => {
  const f = await fixture(t)
  await f.ingest(f.batch())
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.legacyRestart(1)
  const response = await f.ingest({ events: f.batch().events })
  assert.equal(response.status, 422, JSON.stringify(response.body))
  assert.equal((await f.request('diagnostics')).body.retained.events, 0)
})

test('omitting the optional hierarchy field cannot reinsert an expired guarded proof body', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.ingest(f.unknown(original))
  const result = await f.ingest({ events: f.batch().events })
  assert.equal(result.status, 422)
  assert.equal(result.body.code, 'hierarchy_context_invalid')
  assert.deepEqual((await f.snapshot()).tasks[0].events, [])
})

test('fresh event, task, lifecycle, and source identities remain admitted beside an expired original', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.ingest(f.unknown(original))
  assert.equal((await f.ingest(f.batch('root', 'life', 3, '-new'))).status, 200)
  assert.equal((await f.snapshot()).tasks.find(task => task.id === 'root').events.find(event => event.id === 'input-new').authorizationEligible, true)
  assert.equal((await f.ingest(f.batch('other-task', 'life', 4))).status, 200)
  assert.equal((await f.ingest(f.batch('root', 'new-life', 5))).status, 200)
  for (const source of [await f.register('other-source'), await f.register('context', 'new-host')]) {
    assert.notEqual(source, f.sourceKey)
    assert.equal((await f.ingest(f.batch(), source)).status, 200)
  }
  const snapshot = await f.snapshot()
  assert.equal(snapshot.sources.length, 3)
  assert.equal(snapshot.tasks.filter(task => task.currentRequest === 'Read only.').length, 5)
  assert.equal((await f.ingest(f.batch('root', 'life', 6))).status, 422, 'the original task/lifecycle identity remains expired')
})

test('a genuinely new native execution admits new proof identities on the same task lifecycle', async t => {
  const f = await fixture(t), original = f.batch()
  original.events.push(f.event('root', 'run-one', 'status', '', { nativeType: 'subagent/start', childSessionId: 'root', runId: 'one' }))
  original.hierarchyContext.activations[0].runId = 'one'
  original.hierarchyContext.activations[0].stateRefs.push(ref('root', 'run-one'))
  assert.equal((await f.ingest(original)).status, 200)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  await f.ingest(f.unknown(original))
  const next = f.batch('root', 'life', 3, '-two')
  next.events.push(f.event('root', 'run-two', 'status', '', { nativeType: 'subagent/start', childSessionId: 'root', runId: 'two' }))
  next.hierarchyContext.activations[0].runId = 'two'
  next.hierarchyContext.activations[0].stateRefs.push(ref('root', 'run-two'))
  assert.equal((await f.ingest(next)).status, 200)
  const task = (await f.snapshot()).tasks[0]
  assert.equal(task.hierarchy.runId, 'two')
  assert.equal(task.instructionContext.binding.inputRef.eventId, 'input-two')
  assert.equal(task.events.find(event => event.id === 'input-two').authorizationEligible, true)
})

test('ended tasks retain replay guards until the inactive source and its retained history are reclaimed', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  const ended = f.batch('root', 'life', 2, '-end')
  ended.tasks[0].status = 'idle'
  ended.events[0].data.status = 'idle'
  ended.hierarchyContext.activations[0].state = 'ended'
  ended.hierarchyContext.instructionBindings = []
  ended.events = [ended.events[0]]
  assert.equal((await f.ingest(ended)).status, 200)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  assert.equal((await f.request('diagnostics')).body.retained.events, 0)
  assert.equal((await f.request('diagnostics')).body.retained.hierarchyEvidenceIdentities, 4)
  assert.equal((await f.ingest(f.batch('root', 'life', 3))).status, 422)
  assert.equal((await f.request('source/unregister', { sourceKey: f.sourceKey })).status, 200)
  f.advance(DAY + 1)
  const snapshot = await f.snapshot(), diagnostics = (await f.request('diagnostics')).body
  assert.equal(snapshot.sources.length, 0)
  assert.equal(snapshot.tasks.length, 0)
  assert.equal(diagnostics.retained.hierarchyEvidenceIdentities, 0)
})

test('identity budget overflow records a persistent gap, preserves old guards, and leaves a new source usable', async t => {
  const f = await fixture(t), original = f.batch()
  await f.ingest(original)
  const limit = (await f.request('diagnostics')).body.retained.hierarchyEvidenceIdentityLimitPerSource
  assert.equal(limit, 10_000)
  let revision = 2, rejected
  while (!rejected) {
    const next = f.batch('root', 'life', revision++, `-${revision}`)
    for (let index = 0; index < 300; index++) {
      const id = `${revision}-${index}`
      next.events.push(f.event('root', `claim-${id}`, 'input', 'Read only.', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: id }), f.event('root', `input-${id}`, 'input', 'Read only.', { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'unknown', sourceKind: 'user', messageId: id, inherited: false }))
      next.hierarchyContext.instructionBindings.push({ taskId: 'root', lifecycleId: 'life', inputRef: ref('root', `input-${id}`), phase: 'consumed', origin: 'user-entry', originRefs: [ref('root', `claim-${id}`)], ancestorLimitRefs: [] })
    }
    const before = await f.snapshot(), result = await f.ingest(next)
    assert.ok([200, 422].includes(result.status), JSON.stringify(result.body))
    if (result.status === 422) {
      rejected = next
      const after = await f.snapshot()
      assert.equal(after.sources[0].hierarchyContext.revision, before.sources[0].hierarchyContext.revision)
      assert.equal(after.tasks[0].currentRequest, before.tasks[0].currentRequest)
      assert.equal(after.tasks[0].requestId, before.tasks[0].requestId)
      assert.equal(after.tasks[0].events.length, before.tasks[0].events.length)
      assert.ok(after.tasks[0].coverage.gaps.includes('Hierarchy evidence identity retention budget exceeded'))
    }
    assert.ok(revision < 30, 'the fixed capacity must stop unbounded identity growth')
  }
  assert.ok((await f.request('diagnostics')).body.retained.hierarchyEvidenceIdentities <= limit)
  f.advance(DAY + 1)
  await f.request('source/heartbeat', { sourceKey: f.sourceKey })
  const gap = f.unknown(original); gap.hierarchyContext.revision = revision++
  assert.equal((await f.ingest(gap)).status, 200)
  assert.ok((await f.snapshot()).sources[0].gaps.includes('Hierarchy evidence identity retention budget exceeded'))
  assert.ok((await f.snapshot()).tasks[0].coverage.gaps.includes('Hierarchy evidence identity retention budget exceeded'))
  assert.equal((await f.ingest(f.batch('root', 'life', revision))).status, 422)
  const other = await f.register('fresh-source')
  assert.equal((await f.ingest(f.batch(), other)).status, 200)
  assert.equal((await f.snapshot()).tasks.find(task => task.sourceKey === other).events.find(event => event.id === 'input').authorizationEligible, true)
})
