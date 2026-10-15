import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createCore } from '../src/core/index.mjs'

const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })

test('public projections retain source-owned instructions and read new evidence, requests and expiry afresh', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'projection-read-'))
  let clock = 1_001_000
  const core = await createCore({ dataDir, now: () => clock, leaseMs: 172_800_000 })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  const event = (taskId, id, kind, text, data = {}) => ({ id, taskId, taskLifecycleId: 'life', kind, text, capturedAt: clock, occurredAt: null, data })
  async function source(label) {
    const key = await core.registerSource({ registrationId: label, hostRunId: label, name: label })
    const context = { schemaVersion: 1, revision: 1, sessions: ['root', 'child'].map(sessionId => ({ sessionId })), activations: ['root', 'child'].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, 'state')] })), relations: [{ parentSessionId: 'root', childSessionId: 'child', kind: 'delegation', evidenceRefs: [ref('root', 'catalog')] }], instructionBindings: [
      { taskId: 'root', lifecycleId: 'life', inputRef: ref('root', 'request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('root', 'claim')], ancestorLimitRefs: [] },
      { taskId: 'child', lifecycleId: 'life', inputRef: ref('child', 'request'), phase: 'consumed', origin: 'delegated', originRefs: [ref('root', 'catalog')], ancestorLimitRefs: [ref('root', 'request')] },
    ], coverage: { complete: true, branchGaps: [] } }
    await core.ingest(key, { tasks: ['root', 'child'].map(id => ({ id, lifecycleId: 'life', sessionId: id, title: id, status: 'running' })), events: [
      ...['root', 'child'].map(id => event(id, 'state', 'status', '', { nativeType: 'agent/status', status: 'running' })),
      event('root', 'catalog', 'status', '', { nativeType: 'subagent/catalog', childSessionId: 'child' }),
      event('root', 'claim', 'input', `${label} limits`, { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: label }),
      event('root', 'request', 'input', `${label} limits`, { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: label, inherited: false }),
      event('child', 'request', 'input', `${label} delegated`, { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: `${label}-child`, inherited: false, inputOrigin: 'delegated' }),
    ], hierarchyContext: context })
    return { key, context }
  }
  const a = await source('A'), b = await source('B')
  const actor = (snapshot, source, id) => snapshot.tasks.find(task => task.sourceKey === source.key && task.id === id)
  const first = await core.snapshot()
  assert.equal(actor(first, a, 'root').events.find(e => e.id === 'request').authorizationEligible, true)
  assert.equal(actor(first, a, 'child').events.find(e => e.id === 'request').authorizationEligible, false)
  assert.deepEqual(actor(first, a, 'child').instructionContext.ancestorLimitRefs, [ref('root', 'request')])
  assert.equal(actor(first, b, 'root').currentRequest, 'B limits')
  actor(first, a, 'root').events.find(e => e.id === 'request').text = 'Caller mutation'
  assert.equal(actor(await core.snapshot(), a, 'root').events.find(e => e.id === 'request').text, 'A limits')
  await core.ingest(a.key, { events: [event('child', 'private-output', 'output', '', { redacted: true })] })
  const updated = await core.snapshot()
  assert.ok(actor(updated, a, 'child').coverage.gaps.includes('Some observed content was omitted for privacy'))
  assert.equal(actor(updated, b, 'child').coverage.gaps.includes('Some observed content was omitted for privacy'), false)
  clock += 10
  a.context.revision++
  a.context.instructionBindings.push({ taskId: 'root', lifecycleId: 'life', inputRef: ref('root', 'next-request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('root', 'next-claim')], ancestorLimitRefs: [] })
  await core.ingest(a.key, { events: [
    event('root', 'next-claim', 'input', 'New A limits', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'next' }),
    event('root', 'next-request', 'input', 'New A limits', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: 'next', inherited: false }),
  ], hierarchyContext: a.context })
  const next = await core.snapshot()
  assert.equal(actor(next, a, 'root').currentRequest, 'New A limits')
  assert.ok(actor(next, a, 'child').instructionContext.gaps.includes('ancestor_user_scope_update_unconfirmed'))
  assert.equal(actor(next, b, 'child').instructionContext.gaps.includes('ancestor_user_scope_update_unconfirmed'), false)
  const summary = await core.gsSummary({ sourceKey: a.key, sessionId: 'root' })
  assert.deepEqual(summary.counts, next.hierarchyContext.scopes.find(scope => scope.sourceKey === a.key && scope.sessionId === 'root').counts)
  await core.unregisterSource(b.key)
  const disconnected = await core.gsSummary({ sourceKey: b.key, sessionId: 'root' })
  assert.equal(disconnected.sourceStatus, 'disconnected')
  assert.equal(disconnected.coverage.complete, false)
  assert.ok(disconnected.coverage.gaps.includes('source_disconnected'))
  clock += 86_400_000 - 5
  const expired = await core.snapshot()
  assert.equal(actor(expired, a, 'root').events.some(e => e.id === 'request'), false)
  assert.equal(actor(expired, a, 'root').events.some(e => e.id === 'next-request'), true)
  assert.equal(expired.tasks.some(task => task.sourceKey === b.key), false)
  assert.equal((await core.gsSummary({ sourceKey: b.key, sessionId: 'root' })).sourceStatus, 'unknown')
})

for (const [kind, expected] of [
  ['future-entry', [false, false, true, true]],
  ['expiry-exit', [true, true, false, false]],
  ['rollback', [true, false, false, false]],
]) test(`a synchronous public projection preserves instruction eligibility across ${kind}`, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'projection-clock-'))
  let clock = 1_000_000, armed = false, calls = 0
  const now = () => {
    if (!armed) return clock
    calls++
    if (kind === 'rollback') return calls < 6 ? 1_000_002 : 999_998
    return clock++
  }
  const core = await createCore({ dataDir, now, leaseMs: 172_800_000 })
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }) })
  const source = await core.registerSource({ registrationId: kind, hostRunId: kind, name: kind }), ids = ['one', 'two', 'three', 'four']
  await core.ingest(source, { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, status: 'running', title: id })), events: ids.flatMap(taskId => [
    { id: 'state', taskId, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: clock, data: { nativeType: 'agent/status', status: 'running' } },
    { id: 'claim', taskId, taskLifecycleId: 'life', kind: 'input', text: 'Read only', capturedAt: clock, data: { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: taskId } },
    { id: 'request', taskId, taskLifecycleId: 'life', kind: 'input', text: 'Read only', capturedAt: clock, data: { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', messageId: taskId, inherited: false } },
  ]), hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })), activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, 'state')] })), relations: [], instructionBindings: ids.map(taskId => ({ taskId, lifecycleId: 'life', inputRef: ref(taskId, 'request'), phase: 'consumed', origin: 'user-entry', originRefs: [ref(taskId, 'claim')], ancestorLimitRefs: [] })), coverage: { complete: true, branchGaps: [] } } })
  clock = kind === 'expiry-exit' ? 1_000_000 + 86_400_000 - 8 : 1_000_000 - 8
  armed = true
  const snapshot = await core.snapshot()
  armed = false
  assert.deepEqual(snapshot.tasks.map(task => task.events.find(event => event.id === 'request').authorizationEligible), expected)
})
