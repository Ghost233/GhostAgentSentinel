import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-projection-'))
  const server = await startServer({ dataDir })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const request = async (path, body) => {
    const response = await fetch(`${server.url}/v1/${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
    const value = await response.json(); assert.equal(response.status, 200, JSON.stringify(value)); return value
  }
  const sourceKey = await request('source/register', { registrationId: 'projection', hostRunId: 'run', name: 'Same label' })
  return { request, sourceKey }
}
function graph() {
  const ids = ['root', 'child', 'grandchild', 'deep', 'sibling', 'plain-branch'], time = Date.now()
  const edges = [['root', 'child'], ['child', 'grandchild'], ['grandchild', 'deep'], ['root', 'sibling']]
  const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
  return {
    tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: id, status: id === 'root' ? 'idle' : 'running' })),
    events: [...ids.map(id => ({ id: `${id}-state`, taskId: id, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: time, occurredAt: null, data: { nativeType: 'agent/status', status: id === 'root' ? 'idle' : 'running' } })),
      ...edges.map(([parent, child]) => ({ id: `${parent}-${child}`, taskId: parent, taskLifecycleId: 'life', kind: 'status', text: '', capturedAt: time, occurredAt: null, data: { nativeType: 'subagent/catalog', childSessionId: child } }))],
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })),
      activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: taskId === 'root' ? 'ended' : 'running', stateRefs: [ref(taskId, `${taskId}-state`)] })),
      relations: [...edges.map(([parentSessionId, childSessionId]) => ({ parentSessionId, childSessionId, kind: 'delegation', evidenceRefs: [ref(parentSessionId, `${parentSessionId}-${childSessionId}`)] })), { parentSessionId: 'root', childSessionId: 'plain-branch', kind: 'branch', evidenceRefs: [] }],
      instructionBindings: [], coverage: { complete: true, branchGaps: [] } },
  }
}

test('the public snapshot projects a deep current subtree, ancestor path and independent active counts after the root ends', async t => {
  const f = await fixture(t)
  await f.request('source/ingest', { sourceKey: f.sourceKey, batch: graph() })
  const snapshot = await f.request('snapshot')
  const projection = snapshot.hierarchyContext
  const scope = projection.scopes?.find(item => item.sourceKey === f.sourceKey && item.sessionId === 'child')
  assert.ok(scope, 'the current child must have a core-projected scope')
  const group = projection.groups.find(item => item.key === scope.groupKey)
  assert.deepEqual(group.nodes.slice(scope.start, scope.end).map(node => node.sessionId), ['child', 'grandchild', 'deep'])
  assert.deepEqual(scope.path.map(node => node.sessionId), ['root', 'child'])
  assert.deepEqual(scope.counts, { green: 0, yellow: 3, red: 0, gray: 0 })
  assert.deepEqual(group.counts, { green: 0, yellow: 4, red: 0, gray: 0 })
  assert.equal(group.nodes.find(node => node.sessionId === 'deep').depth, 3)
  assert.ok(!group.nodes.some(node => node.sessionId === 'plain-branch'))
  assert.equal(group.runningCount, 4)
  assert.equal(group.nodes.find(node => node.sessionId === 'root').light, null)
  const actor = snapshot.tasks.find(task => task.id === 'child')
  assert.equal(group.nodes.find(node => node.sessionId === 'child').light, actor.gs.light)
  assert.equal(group.nodes.find(node => node.sessionId === 'child').gs.label, actor.gs.label)
  await f.request('source/unregister', { sourceKey: f.sourceKey })
  const disconnected = await f.request('snapshot')
  const lostGroup = disconnected.hierarchyContext.groups.find(item => item.key === group.key)
  assert.deepEqual(lostGroup.counts, { green: 0, yellow: 0, red: 0, gray: 4 }, 'known active but unobservable descendants remain gray and the ended parent remains outside counts')
})

test('a known external execution with unavailable internal activity counts gray without manufacturing initial silence', async t => {
  const f = await fixture(t), batch = graph(), at = batch.events[0].capturedAt
  batch.tasks.push({ id: 'external', lifecycleId: 'external-life', title: 'External actor', status: 'running', gaps: ['内部执行不可观察'] })
  batch.events.push({ id: 'external-catalog', taskId: 'root', taskLifecycleId: 'life', kind: 'status', text: '', occurredAt: null, capturedAt: at, data: { nativeType: 'subagent/catalog', childSessionId: 'external' } }, { id: 'external-start', taskId: 'root', taskLifecycleId: 'life', kind: 'status', text: '', occurredAt: null, capturedAt: at, data: { nativeType: 'subagent/start', childSessionId: 'external', runId: 'external-run' } })
  batch.hierarchyContext.activations.push({ taskId: 'external', lifecycleId: 'external-life', sessionId: null, runId: 'external-run', state: 'running', stateRefs: [{ taskId: 'root', lifecycleId: 'life', eventId: 'external-start' }] })
  batch.hierarchyContext.relations.push({ parentSessionId: 'root', childActivationRef: { taskId: 'external', lifecycleId: 'external-life' }, kind: 'delegation', evidenceRefs: [{ taskId: 'root', lifecycleId: 'life', eventId: 'external-catalog' }] })
  batch.hierarchyContext.coverage = { complete: false, branchGaps: [{ sessionId: 'external', reason: '内部执行不可观察' }] }
  await f.request('source/ingest', { sourceKey: f.sourceKey, batch })
  const snapshot = await f.request('snapshot'), external = snapshot.tasks.find(task => task.id === 'external')
  assert.equal(external.gs.light, 'gray')
  assert.equal(external.gs.silence.exempt, false)
  assert.deepEqual(snapshot.hierarchyContext.groups.find(group => group.rootSessionId === 'root').counts, { green: 0, yellow: 4, red: 0, gray: 1 })
})
