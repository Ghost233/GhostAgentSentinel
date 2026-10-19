import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createCore } from '../src/core/index.mjs'

const DAY = 86_400_000
const CHECKS = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const ref = (taskId, eventId, lifecycleId = 'life') => ({ taskId, lifecycleId, eventId })
const until = async predicate => { const deadline = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < deadline, 'Expected public analysis state'); await new Promise(resolve => setTimeout(resolve, 5)) } }
async function fixture(t, generated) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-citation-ownership-'))
  let clock = 1_001_000, core
  t.after(async () => { try { await core?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  core = await createCore({ dataDir, now: () => clock, cooldownMs: 1000, leaseMs: DAY * 2, analyzer: { async analyze(input) {
    const child = input.task.id === 'child', recovered = input.events.some(event => event.id === 'repair')
    const evidenceIds = child ? [recovered ? 'repair' : 'action'] : []
    const evidenceRefs = child ? recovered ? [ref('parent', 'same-input'), ref('child', 'repair')] : [ref('parent', 'same-input'), ref('child', 'same-input'), ref('child', 'action')] : []
    return { summary: { currentTask: input.task.currentRequest, currentState: 'Observed', recentProgress: '', attention: '', ...(generated ? { generated: generated(input) } : {}) }, checks: CHECKS.map(id => ({ id, status: id === 'request_drift' && child ? recovered ? 'clear' : 'attention' : 'unknown', reason: 'Controlled ownership boundary', evidenceIds: id === 'request_drift' ? evidenceIds : [], evidenceRefs: id === 'request_drift' ? evidenceRefs : [], ...(id === 'request_drift' && recovered && input.task.activeAlerts?.[0] ? { recoveryFor: input.task.activeAlerts[0].id, recoveryEvidenceIds: ['repair'] } : {}) })), coverage: input.coverage, quality: 'unvalidated' }
  } } })
  const sourceKey = await core.registerSource({ registrationId: 'citations', hostRunId: 'host', name: 'Same name' })
  const event = (taskId, id, kind, text, data = {}, capturedAt = clock) => ({ id, taskId, taskLifecycleId: 'life', kind, text, occurredAt: null, capturedAt, data })
  const batch = { tasks: ['parent', 'child'].map(id => ({ id, lifecycleId: 'life', sessionId: id, title: 'Same name', status: 'running' })), events: [
    ...['parent', 'child'].map(id => event(id, 'state', 'status', '', { nativeType: 'agent/status', status: 'running' })),
    event('parent', 'catalog', 'status', '', { nativeType: 'subagent/catalog', childSessionId: 'child' }),
    event('parent', 'claim', 'input', 'Read only.', { nativeType: 'agent/inbox/claimed', inputPhase: 'accepted', sourceKind: 'user', messageId: 'root' }, clock - 1000),
    event('parent', 'same-input', 'input', 'Read only.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'unknown', messageId: 'root', inherited: false }, clock - 1000),
    event('child', 'same-input', 'input', 'Inspect tests.', { nativeType: 'user/message', inputPhase: 'consumed', sourceKind: 'user', inputOrigin: 'delegated', messageId: 'child', inherited: false }),
    event('child', 'action', 'output', 'Editing unrelated files.'),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ['parent', 'child'].map(sessionId => ({ sessionId })), activations: ['parent', 'child'].map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, 'state')] })), relations: [{ parentSessionId: 'parent', childSessionId: 'child', kind: 'delegation', evidenceRefs: [ref('parent', 'catalog')] }], instructionBindings: [
    { taskId: 'parent', lifecycleId: 'life', inputRef: ref('parent', 'same-input'), phase: 'consumed', origin: 'user-entry', originRefs: [ref('parent', 'claim')], ancestorLimitRefs: [] },
    { taskId: 'child', lifecycleId: 'life', inputRef: ref('child', 'same-input'), phase: 'consumed', origin: 'delegated', originRefs: [ref('parent', 'catalog')], ancestorLimitRefs: [ref('parent', 'same-input')] },
  ], coverage: { complete: true, branchGaps: [] } } }
  const child = async () => (await core.snapshot()).tasks.find(task => task.id === 'child')
  return { core, sourceKey, batch, event, child, advance: value => { clock += value } }
}

test('public alerts and recovery retain verified ancestor owners when local event IDs collide', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.sourceKey, f.batch)
  await until(async () => (await f.child()).alerts.some(alert => alert.status === 'attention'))
  let child = await f.child(), alert = child.alerts[0]
  const originalRefs = [ref('parent', 'same-input'), ref('child', 'same-input'), ref('child', 'action')]
  assert.deepEqual(child.analysis.checks.find(check => check.id === 'request_drift').evidenceRefs, originalRefs)
  assert.deepEqual(alert.evidenceRefs, originalRefs)
  assert.deepEqual(alert.evidenceIds, ['action'])
  f.advance(1001)
  await f.core.ingest(f.sourceKey, { events: [f.event('child', 'repair', 'tool-result', 'Completed read-only inspection and verified the requested scope.', { callId: 'scoped-repair', exitCode: 0 })] })
  await until(async () => (await f.child()).alerts.some(alert => alert.status === 'recovered'))
  alert = (await f.child()).alerts[0]
  assert.deepEqual(alert.evidenceRefs, originalRefs)
  assert.deepEqual(alert.recoveryEvidenceRefs, [ref('parent', 'same-input'), ref('child', 'repair')])
  assert.deepEqual(alert.recoveryEvidenceIds, ['repair'])
  f.advance(DAY - 1501)
  await f.core.heartbeat(f.sourceKey)
  child = await f.child(); alert = child.alerts[0]
  const snapshot = await f.core.snapshot()
  assert.ok(!snapshot.tasks.find(task => task.id === 'parent').events.some(event => event.id === 'same-input'))
  assert.ok(child.events.some(event => event.id === 'same-input'), 'the colliding child event is still retained')
  assert.equal(alert.status, 'evidence-expired')
  assert.deepEqual(alert.evidenceRefs, originalRefs, 'the missing ancestor remains explicitly scoped, never replaced by its child collision')
  assert.deepEqual(alert.recoveryEvidenceRefs[0], ref('parent', 'same-input'))
  assert.ok(child.instructionContext.gaps.includes('ancestor_limit_origin_or_retention_missing'))
})

test('the public core rejects a generated summary that borrows an unbound task reference with a colliding local event ID', async t => {
  const f = await fixture(t, input => ({ text: '受控模型总结。', evidenceIds: ['state'], evidenceRefs: [ref(input.task.id === 'child' ? 'parent' : 'child', 'state')] }));
  await f.core.ingest(f.sourceKey, f.batch);
  await until(async () => (await f.child()).analysis.completedAt !== null);
  const child = await f.child();
  assert.equal(child.analysis.state, 'failed');
  assert.equal(child.analysis.error.code, 'invalid_summary');
  assert.equal(child.analysis.summary?.generated, undefined);
  assert.equal(child.analysis.lastSuccessfulSummary, undefined);
});

test('the public core retains generated summaries with matching own and verified inherited references', async t => {
  const f = await fixture(t, input => input.task.id === 'child'
    ? { text: '已检查当前动作及继承限制。', evidenceIds: ['action', 'same-input'], evidenceRefs: [ref('child', 'action'), ref('parent', 'same-input')] }
    : { text: '已观察到原始请求。', evidenceIds: ['same-input'], evidenceRefs: [ref('parent', 'same-input')] });
  await f.core.ingest(f.sourceKey, f.batch);
  await until(async () => (await f.child()).analysis.completedAt !== null);
  const child = await f.child();
  assert.equal(child.analysis.error, null);
  assert.deepEqual(child.analysis.summary.generated.evidenceRefs, [ref('child', 'action'), ref('parent', 'same-input')]);
});
