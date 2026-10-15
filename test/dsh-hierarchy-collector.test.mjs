import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createObservation, hostIdentity } from '../dsh/observation.mjs'
import { startServer } from '../src/server.mjs'

const DAY = 86_400_000
const actor = (id, status = 'running') => ({ id, status, session: { id, header: {} } })

test('expired directory proof retains an explicit hierarchy gap through the public HTTP snapshot without renewing its evidence', async t => {
  let clock = 1_800_000_000_000
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, now: () => clock,
    readCatalog: session => session.id === 'parent' ? [{ id: 'child', createdAt: clock, mode: 'continuable' }] : [] })
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-hierarchy-expiry-'))
  const server = await startServer({ dataDir, now: () => clock, leaseMs: DAY * 2 })
  t.after(async () => { collector.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const request = async (path, body) => {
    const response = await fetch(`${server.url}/v1/${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    assert.equal(response.status, 200)
    return response.json()
  }
  const sourceKey = await request('source/register', { registrationId: 'expiry-host', hostRunId: 'host-run', name: 'Expiry Host' })
  collector.baseline([actor('parent'), actor('child')])
  const first = collector.batch(), catalog = first.events.find(event => event.data?.nativeType === 'subagent/catalog')
  assert.equal(first.hierarchyContext.coverage.complete, true)
  await request('source/ingest', { sourceKey, batch: first })
  collector.acknowledge(first.events.map(event => event.id), first.tasks)
  clock += DAY + 1
  const expired = collector.batch()
  assert.equal(expired.hierarchyContext.coverage.complete, false, 'expired parent catalog cannot become an empty complete tree')
  assert.ok(expired.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'parent'))
  assert.ok(!expired.events.some(event => event.id === catalog.id))
  assert.equal(expired.hierarchyContext.relations.length, 0)
  await request('source/ingest', { sourceKey, batch: expired })
  const snapshot = await request('snapshot')
  assert.equal(snapshot.hierarchyContext.sources[0].coverage.complete, false)
  assert.ok(snapshot.hierarchyContext.sources[0].coverage.branchGaps.some(gap => gap.sessionId === 'parent'))
})

test('evicted catalog evidence leaves a coverage gap instead of an empty complete directory', () => {
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, maxEvents: 4,
    readCatalog: session => session.id === 'parent' ? [{ id: 'child', mode: 'continuable' }] : [] })
  collector.baseline([actor('parent'), actor('child')])
  const first = collector.batch()
  assert.equal(first.hierarchyContext.coverage.complete, true)
  collector.acknowledge(first.events.map(event => event.id), first.tasks)
  for (let index = 0; index < 4; index++) collector.observe('agent/created', { agent: actor(`extra-${index}`) })
  const evicted = collector.batch()
  assert.equal(evicted.hierarchyContext.coverage.complete, false)
  assert.ok(evicted.hierarchyContext.coverage.branchGaps.some(gap => gap.sessionId === 'parent'))
  assert.equal(evicted.hierarchyContext.relations.length, 0)
  collector.close()
})

test('an end edge for a pruned child whose state was evicted remains a gap rather than crashing batch publication', () => {
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, maxEvents: 1 })
  const child = actor('child', 'idle'), parent = actor('parent')
  collector.baseline([child, parent])
  collector.observe('subagent/end', { parent, activation: { id: 'child', runId: 'unbound-run' } })
  const batch = collector.batch()
  assert.equal(batch.hierarchyContext.activations.find(activation => activation.sessionId === 'child').state, 'unknown')
  assert.equal(batch.hierarchyContext.coverage.complete, false)
  assert.ok(batch.gaps.some(gap => gap.includes('执行代次归属未核对')))
  collector.close()
})

test('a known child retains its own state and native run proof when its execution ends', () => {
  const collector = createObservation({ identity: hostIdentity({}), hierarchy: true, maxEvents: 8, readCatalog: () => [] })
  const parent = actor('parent'), child = actor('child')
  collector.baseline([parent, child])
  collector.observe('subagent/start', { parent, child, activation: { id: 'child', runId: 'known-run' } })
  child.status = 'idle'; collector.observe('agent/status', { agent: child, status: 'idle' })
  collector.observe('subagent/end', { parent, activation: { id: 'child', runId: 'known-run' } })
  const batch = collector.batch(), node = batch.hierarchyContext.activations.find(activation => activation.sessionId === 'child')
  assert.equal(node.state, 'ended')
  assert.equal(node.runId, 'known-run')
  assert.ok(node.stateRefs.some(reference => batch.events.some(event => event.id === reference.eventId && event.data?.nativeType === 'subagent/end')))
  assert.ok(!batch.gaps.some(gap => gap.includes('执行代次归属未核对')))
  collector.close()
})
