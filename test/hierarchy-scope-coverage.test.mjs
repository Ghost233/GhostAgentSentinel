import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, stat, writeFile } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

async function fixture(t, { missingChild = false, orphanSession = false } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'scope-coverage-')), observations = []
  let clock = Date.now()
  let server, client
  t.after(async () => {
    const failures = []
    for (const release of [() => client?.close(), () => server?.close()]) try { await release() } catch (error) { failures.push(error.message) }
    if (client) assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
    for (const path of ['runtime.json', 'core.lock']) await assert.rejects(stat(join(dataDir, path)), { code: 'ENOENT' })
    await rm(dataDir, { recursive: true, force: true })
    if (process.env.SENTINEL_NATIVE_WAIT_RECEIPTS) await writeFile(join(process.env.SENTINEL_NATIVE_WAIT_RECEIPTS, 'scope-' + randomUUID() + '.json'), JSON.stringify({ scope: 'isolated authenticated public Core HTTP snapshot/summary; no native actors/Host/model', observations, cleanup: { failures, transport: client?.transportDiagnostics(), serverCloseCompleted: failures.length === 0 } }, null, 2))
    assert.deepEqual(failures, [])
  })
  server = await startServer({ dataDir, now: () => clock }); client = await connectCore({ dataDir }); await client.pause()
  const sourceKey = await client.registerSource({ registrationId: 'scope-coverage', hostRunId: 'synthetic', name: 'Scope coverage' })
  const ids = ['rootA', 'localA', 'siblingA', 'rootB', 'foreignB'], edges = [['rootA', 'localA'], ['rootA', 'siblingA'], ['rootB', 'foreignB']]
  if (missingChild) edges.push(['localA', 'missingLocal'])
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  const batch = { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: id, status: 'running' })),
    events: [...ids.map(id => ({ id: id + '-state', taskId: id, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: clock, occurredAt: null, data: { nativeType: 'agent/status', status: 'running' } })),
      ...edges.map(([parent, child]) => ({ id: parent + '-' + child, taskId: parent, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: clock, occurredAt: null, data: { nativeType: 'subagent/catalog', childSessionId: child } }))],
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [...ids, ...(missingChild ? ['missingLocal'] : []), ...(orphanSession ? ['unproven-history'] : [])].map(sessionId => ({ sessionId })),
      activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: 'running', stateRefs: [ref(taskId, taskId + '-state')] })),
      relations: edges.map(([parentSessionId, childSessionId]) => ({ parentSessionId, childSessionId, kind: 'delegation', evidenceRefs: [ref(parentSessionId, parentSessionId + '-' + childSessionId)] })), instructionBindings: [], coverage: { complete: true, branchGaps: [] } },
  }
  await client.ingest(sourceKey, batch)
  const read = async () => { const snapshot = await client.snapshot(), summary = await client.gsSummary(sourceKey, 'localA'); const value = { snapshot, summary }; observations.push(value); return value }
  return { read, batch, client, sourceKey, now: () => clock, advance(ms) { clock += ms }, async gap(owner) { batch.hierarchyContext.revision++; batch.hierarchyContext.coverage = { complete: false, branchGaps: owner ? [{ sessionId: owner, reason: 'Missing coverage for ' + owner }] : [] }; await client.ingest(sourceKey, batch); return read() } }
}
const scope = (snapshot, id) => snapshot.hierarchyContext.scopes.find(item => item.sessionId === id)
const group = (snapshot, root) => snapshot.hierarchyContext.groups.find(item => item.rootSessionId === root)

for (const owner of ['foreignB', 'siblingA']) test(`known ${owner} coverage remains outside the local subtree summary while its owner stays incomplete`, async t => {
  const f = await fixture(t), baseline = await f.read(), next = await f.gap(owner)
  assert.equal(next.snapshot.hierarchyContext.sources[0].coverage.complete, false)
  assert.equal(scope(next.snapshot, owner).coverage.complete, false)
  assert.deepEqual(scope(next.snapshot, 'localA').coverage, { complete: true, branchGaps: [] })
  assert.deepEqual(next.summary.coverage, { complete: true, gaps: [] })
  assert.deepEqual(next.summary.counts, baseline.summary.counts)
  assert.deepEqual(scope(next.snapshot, 'localA').path, scope(baseline.snapshot, 'localA').path)
  assert.deepEqual(next.snapshot.tasks.find(task => task.id === 'localA').gs, baseline.snapshot.tasks.find(task => task.id === 'localA').gs)
  assert.equal(group(next.snapshot, 'rootA').coverage.complete, owner === 'foreignB')
  assert.equal(group(next.snapshot, owner === 'foreignB' ? 'rootB' : 'rootA').coverage.complete, false)
})

test('a current branch gap remains on its subtree, root and summary without changing source or known counts', async t => {
  const f = await fixture(t), baseline = await f.read(), next = await f.gap('localA')
  assert.equal(scope(next.snapshot, 'localA').coverage.complete, false)
  assert.equal(group(next.snapshot, 'rootA').coverage.complete, false)
  assert.equal(group(next.snapshot, 'rootB').coverage.complete, true)
  assert.equal(next.summary.coverage.complete, false)
  assert.ok(next.summary.coverage.gaps.includes('Missing coverage for localA'))
  assert.deepEqual(next.summary.counts, baseline.summary.counts)
  assert.deepEqual(next.snapshot.hierarchyContext.sources[0].coverage, f.batch.hierarchyContext.coverage)
})

for (const setup of [{}, { orphanSession: true }]) test(`unproven ${setup.orphanSession ? 'listed session' : 'gap owner'} remains conservative across root and local scopes`, async t => {
  const f = await fixture(t, setup), owner = setup.orphanSession ? 'unproven-history' : 'unknown-owner', next = await f.gap(owner)
  for (const id of ['rootA', 'localA', 'rootB', 'foreignB']) {
    assert.equal(scope(next.snapshot, id).coverage.complete, false, id)
    assert.ok(scope(next.snapshot, id).coverage.branchGaps.some(gap => gap.sessionId === owner))
  }
  assert.ok(next.snapshot.hierarchyContext.groups.every(value => value.coverage.complete === false))
  assert.equal(next.summary.coverage.complete, false)
  assert.deepEqual(next.snapshot.hierarchyContext.sources[0].coverage, f.batch.hierarchyContext.coverage)
})

test('incomplete context without attributable reasons never becomes complete in a root, local scope or summary', async t => {
  const f = await fixture(t), next = await f.gap(null)
  for (const value of [...next.snapshot.hierarchyContext.groups, ...next.snapshot.hierarchyContext.scopes]) assert.deepEqual(value.coverage, { complete: false, branchGaps: [] })
  assert.deepEqual(next.summary.coverage, { complete: false, gaps: [] })
  assert.deepEqual(next.snapshot.hierarchyContext.sources[0].coverage, { complete: false, branchGaps: [] })
})

test('a verified missing descendant with no activation still marks its owning empty range and current subtree incomplete', async t => {
  const f = await fixture(t, { missingChild: true }), next = await f.gap('missingLocal')
  for (const id of ['missingLocal', 'localA', 'rootA']) assert.equal(scope(next.snapshot, id).coverage.complete, false, id)
  assert.equal(group(next.snapshot, 'rootA').coverage.complete, false)
  assert.deepEqual(scope(next.snapshot, 'foreignB').coverage, { complete: true, branchGaps: [] })
  assert.equal(group(next.snapshot, 'rootB').coverage.complete, true)
  assert.equal(next.summary.coverage.complete, false)
})

test('an empty verified scope keeps effective global source loss even after foreign branch gaps are filtered', async t => {
  const f = await fixture(t, { missingChild: true })
  await f.client.ingest(f.sourceKey, { gaps: ['Global capture loss'] })
  const next = await f.gap('foreignB'), summary = await f.client.gsSummary(f.sourceKey, 'missingLocal')
  assert.equal(scope(next.snapshot, 'missingLocal').counts.gray, 0)
  assert.equal(summary.scopeKnown, true)
  assert.deepEqual(summary.counts, { green: 0, yellow: 0, red: 0, gray: 0 })
  assert.equal(summary.coverage.complete, false)
  assert.ok(summary.coverage.gaps.includes('Global capture loss'))
  assert.equal(scope(next.snapshot, 'missingLocal').coverage.complete, false)
  assert.ok(scope(next.snapshot, 'missingLocal').coverage.branchGaps.some(gap => gap.reason === 'Global capture loss'))
  assert.ok(next.snapshot.sources.find(source => source.key === f.sourceKey).gaps.includes('Global capture loss'))
})

test('an empty verified scope without global loss can stay complete after filtering a known foreign branch', async t => {
  const f = await fixture(t, { missingChild: true }), next = await f.gap('foreignB')
  assert.deepEqual(scope(next.snapshot, 'missingLocal').coverage, { complete: true, branchGaps: [] })
  const summary = await f.client.gsSummary(f.sourceKey, 'missingLocal')
  assert.equal(summary.scopeKnown, true)
  assert.deepEqual(summary.coverage, { complete: true, gaps: [] })
  assert.deepEqual(summary.counts, { green: 0, yellow: 0, red: 0, gray: 0 })
})

test('effective global loss remains on populated current scopes, groups and summaries independently of a foreign owner', async t => {
  const f = await fixture(t)
  await f.client.ingest(f.sourceKey, { gaps: ['Effective global loss'] })
  const next = await f.gap('foreignB')
  for (const id of ['rootA', 'localA', 'siblingA', 'rootB', 'foreignB']) {
    assert.equal(scope(next.snapshot, id).coverage.complete, false)
    assert.ok(scope(next.snapshot, id).coverage.branchGaps.some(gap => gap.reason === 'Effective global loss'))
  }
  assert.ok(next.snapshot.hierarchyContext.groups.every(value => value.coverage.complete === false))
  assert.equal(next.summary.coverage.complete, false)
  assert.ok(next.summary.coverage.gaps.includes('Effective global loss'))
})

test('an older time-scoped source gap stays in source history but does not repollute a covered new nonempty request', async t => {
  const f = await fixture(t)
  await f.client.ingest(f.sourceKey, { gaps: ['Earlier time-scoped loss'] })
  f.advance(1000)
  const input = { id: 'new-local-request', taskId: 'localA', taskLifecycleId: 'life', kind: 'input', text: 'Inspect only.', capturedAt: f.now(), occurredAt: f.now(), data: { nativeType: 'user/message', inputPhase: 'consumed', inputOrigin: 'delegated', sourceKind: 'user', messageId: 'new-local-message', inherited: false } }
  f.batch.events.push(input)
  f.batch.hierarchyContext.instructionBindings = [{ taskId: 'localA', lifecycleId: 'life', inputRef: { taskId: 'localA', lifecycleId: 'life', eventId: input.id }, phase: 'consumed', origin: 'delegated', originRefs: [{ taskId: 'rootA', lifecycleId: 'life', eventId: 'rootA-localA' }], ancestorLimitRefs: [] }]
  const next = await f.gap('foreignB')
  assert.ok(next.snapshot.sources.find(source => source.key === f.sourceKey).gaps.includes('Earlier time-scoped loss'))
  assert.equal(next.snapshot.tasks.find(task => task.id === 'localA').currentRequest, 'Inspect only.')
  assert.equal(next.snapshot.tasks.find(task => task.id === 'localA').gs.coverage.complete, true)
  assert.deepEqual(scope(next.snapshot, 'localA').coverage, { complete: true, branchGaps: [] })
  assert.deepEqual(next.summary.coverage, { complete: true, gaps: [] })
})
