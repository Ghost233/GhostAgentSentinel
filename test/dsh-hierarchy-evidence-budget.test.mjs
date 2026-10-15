import test from 'node:test'
import { randomUUID } from 'node:crypto'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

// Synthetic collector inputs through actual public Core HTTP; no executed SDK caller is claimed.
async function fixture(t) {
  let clock = Date.now(), collector, server, client, sourceKey, serverClosed = false
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-evidence-budget-')), samples = []
  t.after(async () => {
    const failures = []
    for (const release of [() => collector?.close(), () => client?.close(), async () => { await server?.close(); serverClosed = true }, () => rm(dataDir, { recursive: true, force: true })]) try { await release() } catch (error) { failures.push(error.message) }
    const closed = collector?.diagnostics()
    if (closed) for (const [key, value] of Object.entries(closed)) if (key !== 'closed') assert.equal(value, 0, key)
    const receipt = process.env.SENTINEL_EVIDENCE_GC_RECEIPT ?? (process.env.SENTINEL_NATIVE_WAIT_RECEIPTS && join(process.env.SENTINEL_NATIVE_WAIT_RECEIPTS, 'evidence-' + randomUUID() + '.json'))
    if (receipt) await writeFile(receipt, JSON.stringify({ scope: 'synthetic public collector and authenticated Core HTTP, no SDK/Host/model', samples, cleanup: { failures, collector: closed, serverCloseCompleted: serverClosed } }, null, 2))
    if (server) await assert.rejects(fetch(server.url + '/v1/health'))
    assert.deepEqual(failures, [])
  })
  const parent = { id: 'parent', status: 'running', session: { id: 'parent', seq: 2, header: { id: 'parent' } } }
  const child = { id: 'child', status: 'running', session: { id: 'child', seq: 2, header: { id: 'child', parentSession: 'parent' } } }
  collector = createObservation({ identity: hostIdentity({}), hierarchy: true, now: () => clock, readCatalog: session => session.id === 'parent' ? [{ id: 'child', mode: 'one-shot' }] : [] })
  server = await startServer({ dataDir, now: () => clock }); client = await connectCore({ dataDir }); await client.pause()
  sourceKey = await client.registerSource({ registrationId: 'evidence-budget', hostRunId: 'synthetic-host', name: 'Evidence budget fixture', capabilities: [], enroll: true, gaps: [] })
  const publish = async () => { const batch = collector.batch(); try { await client.ingest(sourceKey, batch) } catch (error) { samples.push({ phase: 'public-ingest-rejected', diagnostics: collector.diagnostics(), referenced: referencedIds(batch.hierarchyContext).size, gaps: batch.gaps, error: { message: error.message, code: error.code, status: error.status } }); throw error }; collector.acknowledge(batch.events.map(event => event.id), batch.tasks); return batch }
  for (const agent of [parent, child]) {
    collector.observe('agent/created', { agent })
    const message = { id: 'request-' + agent.id, source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only.' }] }
    collector.observe('agent/inbox/claimed', { agent, message })
    collector.observe('session/event', { session: agent.session, event: { seq: 0, time: clock, type: 'user/message', data: message } })
  }
  collector.observe('subagent/start', { parent, child, activation: { id: 'child', runId: 'child-run' }, capturedAt: clock })
  collector.observe('agent/assistant-stream', { agent: child, frame: { type: 'chunk', revision: 1, chunk: { text: 'Inspecting the delegated request.' } } })
  await publish()
  return { collector, client, sourceKey, parent, child, publish, samples, advance(ms = 1) { clock += ms }, now: () => clock,
    beginWait() {
      const execution = { agent: parent, token: {}, callId: 'current-caller', name: 'read', arguments: { prompt: 'Inspect only.' } }
      collector.observe('tools/execute', { exec: execution })
      collector.observe('subagent/wait-start', { waitId: 'current-wait', parent, parentStatusAtStart: 'running', parentSeqAtStart: 2, purpose: 'join-operation', targets: [{ id: 'child', runId: 'child-run', agent: child }], coverage: { complete: true, gaps: [] }, timestamp: clock })
      collector.observe('subagent/wait-caller-start', { waitId: 'current-wait', parent, execution, timestamp: clock })
      return execution
    } }
}

const referencedIds = context => new Set([...context.activations.flatMap(item => item.stateRefs), ...context.relations.flatMap(item => item.evidenceRefs), ...context.instructionBindings.flatMap(item => [item.inputRef, ...item.originRefs, ...item.ancestorLimitRefs]), ...(context.waitBindings ?? []).flatMap(item => item.evidenceRefs)].map(ref => ref.eventId))

test('acknowledged obsolete hierarchy proof versions cannot exhaust the500budget or erase a fresh exact wait', async t => {
  const f = await fixture(t), original = f.collector.batch()
  const originalInput = original.hierarchyContext.instructionBindings.find(binding => binding.taskId === 'parent' && binding.phase === 'consumed').inputRef
  for (let index = 0; index < 520; index++) {
    f.advance(); f.collector.observe('agent/status', { agent: f.parent, status: 'running' }); await f.publish()
  }
  const afterChurn = f.collector.batch()
  f.samples.push({ phase: 'acknowledged520versions', diagnostics: f.collector.diagnostics(), referenced: referencedIds(afterChurn.hierarchyContext).size, gaps: afterChurn.gaps })
  f.beginWait(); const current = await f.publish(), snapshot = await f.client.snapshot()
  f.samples.push({ phase: 'fresh-current-wait', diagnostics: f.collector.diagnostics(), hierarchy: current.hierarchyContext, publicParent: snapshot.tasks.find(task => task.id === 'parent') })
  assert.ok(current.hierarchyContext.waitBindings?.some(wait => wait.waitId === 'current-wait' && wait.phase === 'started'))
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').gs.waiting?.kind, 'children')
  assert.equal(snapshot.tasks.find(task => task.id === 'parent').gs.light, 'green')
  assert.ok(!current.gaps.some(gap => gap.includes('层级依据索引')))
  assert.deepEqual(current.hierarchyContext.instructionBindings.find(binding => binding.taskId === 'parent' && binding.phase === 'consumed').inputRef, originalInput)
  assert.ok(f.collector.diagnostics().hierarchyEvidence < 20)
})

test('acknowledgement keeps original input, native run and independent wait start/end refs until their real24h expiry', async t => {
  const f = await fixture(t), execution = f.beginWait(), opened = await f.publish()
  const binding = opened.hierarchyContext.waitBindings[0], first = opened.events.find(event => event.id === binding.evidenceRefs[0].eventId)
  for (let index = 0; index < 20; index++) { f.advance(); f.collector.observe('agent/status', { agent: f.parent, status: 'running' }); await f.publish() }
  const retained = f.collector.batch()
  assert.ok(binding.evidenceRefs.every(ref => retained.events.some(event => event.id === ref.eventId)))
  assert.equal(retained.events.find(event => event.id === first.id).capturedAt, first.capturedAt)
  assert.equal(retained.hierarchyContext.activations.find(item => item.taskId === 'child').runId, 'child-run')
  f.collector.observe('subagent/wait-caller-end', { parent: f.parent, execution, waitId: 'current-wait', timestamp: f.now() })
  const closed = await f.publish()
  assert.equal(closed.hierarchyContext.waitBindings[0].phase, 'ended')
  assert.ok(closed.hierarchyContext.waitBindings[0].evidenceRefs.every(ref => closed.events.some(event => event.id === ref.eventId)))
  assert.equal((await f.client.snapshot()).tasks.find(task => task.id === 'parent').gs.waiting, null)
  f.advance(86_400_001)
  const expired = f.collector.batch(), counts = f.collector.diagnostics()
  assert.equal(counts.hierarchyEvidence, 0)
  assert.equal(counts.hierarchyInputs, 0)
  assert.equal(counts.hierarchyRuns, 0)
  assert.equal(counts.hierarchyWaits, 0)
  assert.equal(expired.hierarchyContext.waitBindings?.length ?? 0, 0)
  assert.equal(expired.tasks.find(task => task.id === 'parent').lifecycleId, opened.tasks.find(task => task.id === 'parent').lifecycleId)
  f.samples.push({ phase: 'original-proof-retained-then-expired', originalStart: first, closedBinding: closed.hierarchyContext.waitBindings[0], expiredCounts: counts })
})

test('more than500 genuinely referenced proof bodies still hit the cap and cannot show a valid wait as green', async t => {
  const f = await fixture(t); f.beginWait(); await f.publish()
  assert.equal((await f.client.snapshot()).tasks.find(task => task.id === 'parent').gs.light, 'green')
  for (let index = 0; index < 210; index++) {
    const agent = { id: 'active-' + index, status: 'running', session: { id: 'active-' + index, seq: 2, header: { id: 'active-' + index } } }
    f.advance(); f.collector.observe('agent/created', { agent })
    const message = { id: 'input-' + index, source: { kind: 'user' }, content: [{ type: 'text', text: 'Inspect only.' }] }
    f.collector.observe('agent/inbox/claimed', { agent, message })
    f.collector.observe('session/event', { session: agent.session, event: { seq: 0, time: f.now(), type: 'user/message', data: message } })
    const delivered = f.collector.batch(); f.collector.acknowledge(delivered.events.map(event => event.id), delivered.tasks)
  }
  const lost = f.collector.batch(), counts = f.collector.diagnostics()
  assert.ok(counts.hierarchyEvidence <= 500)
  assert.ok(counts.hierarchyInputs > 200 && counts.hierarchyStates + counts.hierarchyInputBindings > 500, 'current state and distinct input refs alone exceed the fixed cap')
  assert.ok(lost.gaps.some(gap => gap.includes('层级依据索引')))
  assert.equal(counts.bufferedEvents, 0, 'overflow persists with all outgoing events acknowledged')
  // The public gap contract must keep a formerly valid wait gray. This does not assert the incomplete graph is acceptable.
  await f.client.ingest(f.sourceKey, { gaps: lost.gaps })
  const row = (await f.client.snapshot()).tasks.find(task => task.id === 'parent')
  assert.equal(row.gs.light, 'gray')
  assert.equal(row.gs.coverage.complete, false)
  f.samples.push({ phase: 'essential-reference-overflow', diagnostics: counts, gaps: lost.gaps, publicParent: row })
})

test('refreshing one current state at exactly500acknowledged references never evicts another actor proof', t => {
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, readCatalog: () => [] })
  t.after(() => { collector.close(); for (const [key, value] of Object.entries(collector.diagnostics())) if (key !== 'closed') assert.equal(value, 0, key) })
  const actors = Array.from({ length: 500 }, (_, index) => ({ id: 'boundary-' + index, status: 'running', session: { id: 'boundary-' + index, seq: 0, header: {} } }))
  collector.baseline(actors)
  const before = collector.batch()
  collector.acknowledge(before.events.map(event => event.id), before.tasks)
  assert.equal(collector.diagnostics().hierarchyEvidence, 500)
  collector.observe('agent/status', { agent: actors.at(-1), status: 'running' })
  const after = collector.batch()
  assert.equal(collector.diagnostics().hierarchyEvidence, 500)
  assert.equal(after.hierarchyContext.activations.filter(activation => activation.state === 'running' && activation.stateRefs.length === 1).length, 500)
  assert.equal(after.hierarchyContext.activations.find(activation => activation.taskId === actors[0].id).state, 'running')
  assert.deepEqual(after.gaps, [])
  const last = after.hierarchyContext.activations.find(activation => activation.taskId === actors.at(-1).id)
  assert.notDeepEqual(last.stateRefs, before.hierarchyContext.activations.find(activation => activation.taskId === actors.at(-1).id).stateRefs)
})
