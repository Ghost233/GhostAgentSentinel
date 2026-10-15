import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, stat, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { GS_SUMMARY_FEATURE } from '../src/protocol.mjs'

const ref = (taskId, eventId) => ({ taskId, lifecycleId: 'life', eventId })
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-summary-'))
  let clock = Date.now()
  const server = await startServer({ dataDir, now: () => clock, leaseMs: 172_800_000 })
  const client = await connectCore({ dataDir })
  t.after(async () => {
    await client.close(); await server.close()
    assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
    await assert.rejects(stat(join(dataDir, 'runtime.json')), { code: 'ENOENT' })
    await assert.rejects(stat(join(dataDir, 'core.lock')), { code: 'ENOENT' })
    await rm(dataDir, { recursive: true, force: true })
  })
  const sourceKey = await client.registerSource({ registrationId: 'summary', hostRunId: 'host', name: 'PRIVATE SOURCE NAME' })
  const ids = ['root', 'child', 'grandchild', 'sibling'], edges = [['root', 'child'], ['child', 'grandchild'], ['root', 'sibling']]
  const batch = { tasks: ids.map(id => ({ id, lifecycleId: 'life', sessionId: id, title: 'PRIVATE TASK TITLE', status: id === 'root' ? 'idle' : 'running' })),
    events: [...ids.map(id => ({ id: `${id}-state`, taskId: id, taskLifecycleId: 'life', kind: 'status', text: 'PRIVATE BODY', capturedAt: clock - 100, occurredAt: null, data: { nativeType: 'agent/status', status: id === 'root' ? 'idle' : 'running' } })),
      ...edges.map(([parent, child]) => ({ id: `${parent}-${child}`, taskId: parent, taskLifecycleId: 'life', kind: 'status', text: 'PRIVATE RELATION BODY', capturedAt: clock - 100, occurredAt: null, data: { nativeType: 'subagent/catalog', childSessionId: child } }))],
    hierarchyContext: { schemaVersion: 1, revision: 1, sessions: ids.map(sessionId => ({ sessionId })),
      activations: ids.map(taskId => ({ taskId, lifecycleId: 'life', sessionId: taskId, runId: null, state: taskId === 'root' ? 'ended' : 'running', stateRefs: [ref(taskId, `${taskId}-state`)] })),
      relations: edges.map(([parentSessionId, childSessionId]) => ({ parentSessionId, childSessionId, kind: 'delegation', evidenceRefs: [ref(parentSessionId, `${parentSessionId}-${childSessionId}`)] })), instructionBindings: [], coverage: { complete: true, branchGaps: [] } },
  }
  await client.ingest(sourceKey, batch)
  return { client, server, sourceKey, batch, advance(value) { clock += value }, now: () => clock }
}

test('closed-view GS summaries read fresh authoritative subtree counts without reading or returning a full snapshot', async t => {
  const f = await fixture(t)
  const first = await f.client.gsSummary(f.sourceKey, 'child')
  assert.equal(first.protocol, 1)
  assert.equal(first.sourceKey, f.sourceKey); assert.equal(first.sessionId, 'child')
  assert.equal(first.sourceStatus, 'connected'); assert.equal(first.scopeKnown, true)
  assert.equal(first.gsStatus.supported, true)
  assert.deepEqual(first.counts, { green: 0, yellow: 2, red: 0, gray: 0 })
  assert.equal((await f.client.diagnostics()).http.snapshotReads, 0)
  assert.ok((await f.client.health()).features.includes(GS_SUMMARY_FEATURE))
  assert.ok(!JSON.stringify(first).includes('PRIVATE'))
  assert.deepEqual(Object.keys(first).sort(), ['protocol', 'generatedAt', 'sourceKey', 'sessionId', 'sourceStatus', 'scopeKnown', 'counts', 'coverage', 'gsStatus'].sort())
  f.advance(1000)
  f.batch.tasks.find(task => task.id === 'grandchild').status = 'idle'
  f.batch.events.push({ id: 'grandchild-ended', taskId: 'grandchild', taskLifecycleId: 'life', kind: 'status', text: 'PRIVATE END BODY', capturedAt: f.now() - 100, occurredAt: null, data: { nativeType: 'agent/status', status: 'idle' } })
  const ended = f.batch.hierarchyContext.activations.find(item => item.taskId === 'grandchild')
  ended.state = 'ended'; ended.stateRefs = [ref('grandchild', 'grandchild-ended')]; f.batch.hierarchyContext.revision++
  await f.client.ingest(f.sourceKey, f.batch)
  const next = await f.client.gsSummary(f.sourceKey, 'child')
  assert.equal(next.generatedAt, f.now()); assert.ok(next.generatedAt > first.generatedAt)
  assert.deepEqual(next.counts, { green: 0, yellow: 1, red: 0, gray: 0 })
  assert.equal((await f.client.diagnostics()).http.snapshotReads, 0)
  const full = await f.client.snapshot()
  assert.deepEqual(next.counts, full.hierarchyContext.scopes.find(scope => scope.sessionId === 'child').counts)
})

test('unknown summary scopes remain unconfirmed while disconnected known actors are counted gray at the current read', async t => {
  const f = await fixture(t)
  const missing = await f.client.gsSummary(f.sourceKey, 'not-an-observed-session')
  assert.equal(missing.scopeKnown, false)
  assert.equal(missing.coverage.complete, false)
  assert.ok(missing.coverage.gaps.includes('gs_scope_unconfirmed'))
  assert.deepEqual(missing.counts, { green: 0, yellow: 0, red: 0, gray: 0 })
  const noSource = await f.client.gsSummary('not-an-observed-source', 'child')
  assert.equal(noSource.sourceStatus, 'unknown'); assert.equal(noSource.scopeKnown, false)
  assert.ok(noSource.coverage.gaps.includes('source_unknown'))
  await f.client.unregisterSource(f.sourceKey)
  f.advance(1000)
  const disconnected = await f.client.gsSummary(f.sourceKey, 'root')
  assert.equal(disconnected.generatedAt, f.now())
  assert.equal(disconnected.sourceStatus, 'disconnected'); assert.equal(disconnected.scopeKnown, true)
  assert.deepEqual(disconnected.counts, { green: 0, yellow: 0, red: 0, gray: 3 })
  assert.equal(disconnected.coverage.complete, false)
  assert.ok(disconnected.coverage.gaps.includes('source_disconnected'))
  assert.equal((await f.client.diagnostics()).http.snapshotReads, 0)
})

test('summary queries keep private HTTP authentication and reject missing, duplicated, or extra scope parameters', async t => {
  const f = await fixture(t)
  const validPath = `/v1/gs/summary?sourceKey=${encodeURIComponent(f.sourceKey)}&sessionId=child`
  assert.equal((await fetch(`${f.server.url}${validPath}`)).status, 401)
  assert.equal((await fetch(`${f.server.url}${validPath}`, { headers: { authorization: `Bearer ${f.server.token}`, origin: 'https://untrusted.example' } })).status, 401)
  for (const path of ['/v1/gs/summary', '/v1/gs/summary?sourceKey=x', '/v1/gs/summary?sourceKey=x&sessionId=a&sessionId=b', '/v1/gs/summary?sourceKey=x&sessionId=a&extra=b', '/v1/gs/summary?sourceKey=&sessionId=a']) {
    const response = await fetch(`${f.server.url}${path}`, { headers: { authorization: `Bearer ${f.server.token}` } })
    assert.equal(response.status, 400, path)
  }
  await assert.rejects(f.client.gsSummary('', 'child'), error => error.status === 400 && error.code !== 'gs_summary_unsupported')
  assert.equal((await f.client.diagnostics()).http.snapshotReads, 0)
})

test('a compatible old core without the summary endpoint reports unsupported rather than reading a full snapshot', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'gs-summary-old-core-'))
  let snapshots = 0, client
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/health') response.end(JSON.stringify({ protocol: 1, instanceId: 'old-core', features: ['gs-status/1'] }))
    else { if (request.url === '/v1/snapshot') snapshots++; response.writeHead(404); response.end(JSON.stringify({ error: 'unknown endpoint' })) }
  })
  t.after(async () => { await client?.close(); if (server.listening) await new Promise(resolve => server.close(resolve)); await rm(dataDir, { recursive: true, force: true }) })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  await writeFile(join(dataDir, 'runtime.json'), JSON.stringify({ protocol: 1, instanceId: 'old-core', url: `http://127.0.0.1:${server.address().port}`, token: 'a'.repeat(64) }))
  client = await connectCore({ dataDir })
  await assert.rejects(client.gsSummary('source', 'session'), error => error.status === 404 && error.code === 'gs_summary_unsupported')
  assert.equal(snapshots, 0)
})
