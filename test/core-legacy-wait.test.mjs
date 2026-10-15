import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { randomUUID, randomBytes, createHash } from 'node:crypto'
import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createCore } from '../src/core/index.mjs'
import { connectCore } from '../src/core-client.mjs'
import { normalizeSource, normalizeBatch } from '../src/protocol.mjs'

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const until = async (predicate, label) => { const deadline = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < deadline, label); await new Promise(resolve => setTimeout(resolve, 5)) } }

// The public Core permits legacy analyzers. This own HTTP carrier exercises its
// normalized ingress with the real client; it is not an installed Host/server.
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-legacy-wait-')), token = randomBytes(32).toString('hex'), instanceId = randomUUID(), transcript = []
  let core, server, client, clock = Date.now(), calls = 0, citation = 'start', conditionAt
  t.after(async () => {
    const errors = []
    for (const close of [() => client?.close(), async () => { if (server) await new Promise(resolve => server.close(resolve)) }, () => core?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await close() } catch (error) { errors.push(error.message) } }
    const transport = client?.transportDiagnostics(), diagnostics = await core?.diagnostics()
    if (process.env.GS_LEGACY_EVIDENCE_DIR) {
      await mkdir(process.env.GS_LEGACY_EVIDENCE_DIR, { recursive: true })
      const evidence = { name: t.name, instanceId, scope: 'Public createCore legacy analyzer + own authenticated HTTP carrier + real connectCore client', transcript, calls, cleanup: { errors, httpListening: server?.listening ?? false, transport, temporary: diagnostics?.temporary } }
      await writeFile(join(process.env.GS_LEGACY_EVIDENCE_DIR, `${instanceId}.json`), JSON.stringify(evidence, null, 2) + '\n')
    }
    assert.deepEqual(errors, []); assert.equal(server?.listening, false)
    assert.equal(transport.pendingRequests, 0); assert.equal(transport.deadlineTimers, 0); assert.equal(diagnostics.temporary.timers.total, 0)
  })
  core = await createCore({ dataDir, now: () => clock, leaseMs: 172_800_000, analyzer: { async analyze(input) {
    calls++
    return { summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: id === 'long_wait' ? 'attention' : 'unknown', reason: id === 'long_wait' ? 'Legacy long-wait observation' : 'Absent in this complete scope', evidenceIds: id === 'long_wait' ? Array.isArray(citation) ? citation : [citation] : [], ...(id === 'long_wait' && conditionAt !== undefined ? { conditionAt } : {}), applicability: { state: id === 'long_wait' ? 'triggered' : 'not-triggered', reason: 'Controlled legacy scope', evidenceIds: [input.events.find(event => event.id === input.task.currentRequestId).id] } })), coverage: input.coverage }
  } } })
  server = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk
    const body = raw ? JSON.parse(raw) : undefined
    response.setHeader('content-type', 'application/json')
    let value, status = 200
    try {
      assert.equal(request.headers.authorization, `Bearer ${token}`)
      switch (request.url) {
        case '/v1/health': value = { protocol: 1, instanceId }; break
        case '/v1/source/register': value = await core.registerSource(normalizeSource(body)); break
        case '/v1/source/ingest': value = await core.ingest(body.sourceKey, normalizeBatch(body.batch)); break
        case '/v1/snapshot': value = await core.snapshot(); break
        case '/v1/monitoring/pause': value = await core.pause(); break
        case '/v1/notifications/claim': value = await core.claimNotifications(body.sourceKey); break
        default: throw new Error('Unsupported own fixture route')
      }
    } catch (error) { status = 400; value = { error: error.message } }
    transcript.push({ path: request.url, request: body, inputSha256: createHash('sha256').update(JSON.stringify(body ?? null)).digest('hex'), status, response: value })
    response.writeHead(status); response.end(JSON.stringify(value))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  await writeFile(join(dataDir, 'runtime.json'), JSON.stringify({ protocol: 1, url: `http://127.0.0.1:${server.address().port}`, token, instanceId }), { mode: 0o600 })
  client = await connectCore({ dataDir })
  const source = await client.registerSource({ registrationId: 'legacy-wait', hostRunId: 'own', name: 'Own legacy boundary' })
  const event = (id, kind, data = {}, at = clock) => ({ id, taskId: 'actor', taskLifecycleId: 'life', kind, occurredAt: at, capturedAt: at, text: id, data })
  await client.ingest(source, { tasks: [{ id: 'actor', lifecycleId: 'life', title: 'Own legacy actor', status: 'running' }], events: [event('request', 'input', {}, clock - 500_000), event('start', 'tool-start', { callId: 'call' }, clock - 400_000)] })
  const actor = async () => (await client.snapshot()).tasks[0]
  await until(async () => (await actor()).analysis.state === 'cooldown', 'first legacy round completed')
  return { client, source, event, actor, now: () => clock, advance: value => clock += value, get calls() { return calls }, select(id, at) { citation = id; conditionAt = at } }
}

test('legacy attention with the same evidence preserves its pre-wait time across a complete cooldown and remains notice-eligible', async t => {
  const f = await fixture(t), before = await f.actor(), first = before.alerts.find(alert => alert.checkId === 'long_wait')
  assert.equal(before.gs.light, 'red')
  f.advance(1)
  await f.client.ingest(f.source, { events: [f.event('asked', 'status', { nativeType: 'approval/asked', approvalId: 'approval', callId: 'call' })] })
  assert.equal((await f.actor()).gs.light, 'red')
  f.advance(60_000)
  await f.client.snapshot()
  await until(async () => f.calls === 2 && (await f.actor()).analysis.state === 'cooldown', 'same legacy evidence repeated after cooldown')
  const after = await f.actor(), repeated = after.alerts.find(alert => alert.checkId === 'long_wait')
  assert.equal(repeated.id, first.id)
  assert.deepEqual(repeated.evidenceIds, first.evidenceIds)
  assert.equal(repeated.observedAt, first.observedAt)
  assert.equal(after.gs.light, 'red')
  assert.equal((await f.client.claimNotifications(f.source)).length, 1)
})

test('reordered evidence remains the same legacy issue, while a new tool issue takes its own observation time', async t => {
  const f = await fixture(t)
  await f.client.ingest(f.source, { events: [f.event('diagnosis', 'output')] })
  f.select(['start', 'diagnosis']); f.advance(60_000)
  await f.client.snapshot()
  await until(async () => f.calls === 2 && (await f.actor()).analysis.state === 'cooldown', 'expanded evidence established before the wait')
  const expanded = (await f.actor()).alerts[0]
  f.advance(1)
  await f.client.ingest(f.source, { events: [f.event('asked', 'status', { nativeType: 'approval/asked', approvalId: 'old-approval', callId: 'call' })] })
  f.select(['diagnosis', 'start']); f.advance(60_000)
  await f.client.snapshot()
  await until(async () => f.calls === 3 && (await f.actor()).analysis.state === 'cooldown', 'same evidence in another order repeated')
  assert.equal((await f.actor()).alerts[0].observedAt, expanded.observedAt)
  assert.equal((await f.actor()).gs.light, 'red')
  f.advance(600_000)
  f.select('new-start')
  const newObservedAt = f.now()
  await f.client.ingest(f.source, { events: [
    f.event('old-result', 'tool-result', { callId: 'call', exitCode: 0 }, f.now() - 500_000),
    f.event('old-decided', 'status', { nativeType: 'approval/decided', approvalId: 'old-approval' }, f.now() - 500_000),
    f.event('new-start', 'tool-start', { callId: 'second-call' }, f.now() - 400_000),
    f.event('new-asked', 'status', { nativeType: 'approval/asked', approvalId: 'new-approval', callId: 'second-call' }, f.now() - 300_000),
  ] })
  await until(async () => f.calls === 4 && (await f.actor()).analysis.state === 'cooldown', 'new issue analyzed once')
  const fresh = await f.actor()
  assert.deepEqual(fresh.alerts[0].evidenceIds, ['new-start'])
  assert.equal(fresh.alerts[0].observedAt, newObservedAt, 'the old problem time does not attach to the new tool evidence')
  assert.equal(fresh.gs.light, 'green', 'only the current user-wait-caused legacy delay is exempt')
})

test('an explicit condition time takes precedence over retained legacy fallback time even for identical evidence', async t => {
  const f = await fixture(t), before = (await f.actor()).alerts[0]
  f.advance(1)
  await f.client.ingest(f.source, { events: [f.event('asked', 'status', { nativeType: 'approval/asked', approvalId: 'approval', callId: 'call' })] })
  const afterWaitCondition = f.advance(1)
  f.select('start', afterWaitCondition); f.advance(60_000)
  await f.client.snapshot()
  await until(async () => f.calls === 2 && (await f.actor()).analysis.state === 'cooldown', 'explicit later condition completed')
  assert.equal((await f.actor()).alerts[0].observedAt, afterWaitCondition)
  assert.equal((await f.actor()).gs.light, 'green')
  f.select('start', before.observedAt - 1); f.advance(60_000)
  await f.client.ingest(f.source, { events: [f.event('condition-diagnostic', 'status')] })
  await until(async () => f.calls === 3 && (await f.actor()).analysis.state === 'cooldown', 'explicit pre-wait condition completed')
  assert.equal((await f.actor()).alerts[0].observedAt, before.observedAt - 1)
  assert.equal((await f.actor()).gs.light, 'red')
})
