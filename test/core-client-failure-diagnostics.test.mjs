import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes, randomUUID } from 'node:crypto'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { connectCore } from '../src/core-client.mjs'

const { Context } = req('@deepseek-ai/cordis')
async function until(predicate) {
  const deadline = performance.now() + 5000
  while (!await predicate()) { assert.ok(performance.now() < deadline, 'controlled transport did not reach the expected public state'); await new Promise(resolve => setTimeout(resolve, 5)) }
}
async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-failure-diagnostics-'))
  const token = randomBytes(32).toString('hex'), instanceId = randomUUID(), ctx = new Context()
  const faults = new Map(), held = new Set(), sockets = new Set(), clients = []
  const server = createServer(async (request, response) => {
    for await (const _chunk of request) {}
    assert.equal(request.headers.authorization, `Bearer ${token}`)
    const fault = faults.get(request.url.split('?')[0])
    if (fault === 'held') { held.add(response); response.once('close', () => held.delete(response)); return }
    response.setHeader('content-type', 'application/json')
    if (fault) { response.writeHead(fault.status ?? 503); response.end(fault.raw ?? JSON.stringify(fault.body)); return }
    if (request.url === '/v1/health') response.end(JSON.stringify({ protocol: 1, instanceId, features: [] }))
    else if (request.url === '/v1/source/register') response.end(JSON.stringify('owned-source'))
    else if (request.url === '/v1/snapshot') response.end(JSON.stringify({ protocol: 1, generatedAt: Date.now(), monitoring: 'active', sources: [], tasks: [] }))
    else response.end(JSON.stringify({ accepted: 0 }))
  })
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)) })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  await writeFile(join(dataDir, 'runtime.json'), JSON.stringify({ protocol: 1, url: `http://127.0.0.1:${server.address().port}`, token, instanceId }), { mode: 0o600 })
  const adapter = createAdapter(ctx, { dataDir, registrationId: 'owned-diagnostic-source' }, { flushMs: 25, heartbeatMs: 50 })
  t.after(async () => {
    faults.clear()
    await adapter.close()
    for (const client of clients) await client.close()
    await ctx.fiber.dispose()
    server.closeIdleConnections()
    await new Promise(resolve => server.close(resolve))
    await until(() => sockets.size === 0 && held.size === 0)
    const closed = adapter.diagnostics().adapter
    assert.equal(closed.closed, true); assert.equal(closed.activeRequests, 0); assert.equal(closed.timers, 0); assert.equal(closed.tickInFlight, false)
    for (const client of clients) assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
    await rm(dataDir, { recursive: true, force: true })
  })
  await until(() => adapter.connected && !adapter.diagnostics().adapter.tickInFlight)
  return { adapter, faults, held, token, async client() { const value = await connectCore({ dataDir }); clients.push(value); return value } }
}

test('communication diagnostics retain the first read failure through later source failures and connection recovery', async t => {
  const f = await fixture(t), secret = `Bearer ${f.token}`
  // Keep a successful background tick from recovering the episode between faults.
  f.faults.set('/v1/source/heartbeat', 'held')
  await until(() => f.held.size === 1)
  f.faults.set('/v1/snapshot', { status: 503, body: { error: `${secret} https://private.invalid/${f.token}`, code: 'secretAlpha123', name: 'privateServerName' } })
  const startedAt = Date.now(), failed = await f.adapter.snapshot()
  assert.equal(failed.adapter.connected, false)
  assert.equal(failed.monitoring, 'unknown')
  const episode = f.adapter.diagnostics().adapter.communicationFailure
  assert.ok(episode, 'public diagnostics must identify the failed Core request')
  assert.equal(episode.first.operation, '/v1/snapshot')
  assert.equal(episode.first.kind, 'http')
  assert.equal(episode.first.status, 503)
  assert.equal(episode.first.name, 'Error')
  assert.equal(episode.first.timeoutMs, 5000)
  assert.ok(episode.first.startedAt >= startedAt && episode.first.failedAt >= episode.first.startedAt)
  assert.ok(episode.first.elapsedMs >= 0)
  assert.equal(episode.first.code, undefined)
  const first = structuredClone(episode.first)
  episode.first.operation = 'caller mutation'
  f.faults.delete('/v1/snapshot')
  f.faults.set('/v1/source/heartbeat', { body: { error: secret } })
  for (const response of f.held) { response.writeHead(503, { 'content-type': 'application/json' }); response.end(JSON.stringify({ error: secret })) }
  await until(() => f.adapter.diagnostics().adapter.communicationFailure?.latest.operation === '/v1/source/heartbeat')
  assert.deepEqual(f.adapter.diagnostics().adapter.communicationFailure.first, first)
  f.faults.delete('/v1/source/heartbeat')
  f.faults.set('/v1/source/ingest', { body: { error: secret } })
  await until(() => f.adapter.diagnostics().adapter.communicationFailure?.latest.operation === '/v1/source/ingest')
  assert.deepEqual(f.adapter.diagnostics().adapter.communicationFailure.first, first)
  f.faults.clear()
  await until(() => f.adapter.connected && f.adapter.diagnostics().adapter.communicationFailure.active === false)
  const recovered = f.adapter.diagnostics().adapter.communicationFailure
  assert.deepEqual(recovered.first, first)
  assert.equal(recovered.latest.operation, '/v1/source/ingest')
  assert.ok(recovered.connectionRecoveredAt >= recovered.latest.failedAt)
  assert.equal((await f.adapter.snapshot()).adapter.connected, true)
  const publicDiagnostic = JSON.stringify(recovered)
  for (const forbidden of [f.token, secret, 'https://', 'private.invalid', 'stack', 'body']) assert.equal(publicDiagnostic.includes(forbidden), false)
})

test('decode diagnostics expose the interface without private response or query data', async t => {
  const f = await fixture(t)
  f.faults.set('/v1/snapshot', { status: 200, raw: `private response ${f.token}` })
  assert.equal((await f.adapter.snapshot()).adapter.connected, false)
  const failure = f.adapter.diagnostics().adapter.communicationFailure.first
  assert.equal(failure.kind, 'decode'); assert.equal(failure.name, 'SyntaxError'); assert.equal(failure.status, 200)
  f.faults.set('/v1/gs/summary', { status: 503, body: { error: `https://private.invalid/${f.token}`, code: 'analysis_config_conflict', name: f.token } })
  const client = await f.client()
  const error = await client.gsSummary(f.token, `private-query-${f.token}`).catch(error => error)
  assert.equal(error.coreRequestFailure.operation, '/v1/gs/summary')
  assert.equal(error.coreRequestFailure.kind, 'http')
  assert.equal(error.coreRequestFailure.code, 'analysis_config_conflict')
  assert.equal(error.coreRequestFailure.name, 'Error')
  const publicDiagnostic = JSON.stringify({ failure, queryFailure: error.coreRequestFailure })
  for (const forbidden of [f.token, 'private-query', 'private response', 'private.invalid', 'https://', '?', 'stack', 'body']) assert.equal(publicDiagnostic.includes(forbidden), false)
})

test('a real Core deadline is retained while caller and close cancellation do not create source failures', async t => {
  const f = await fixture(t)
  f.faults.set('/v1/snapshot', 'held')
  const started = performance.now(), failed = await f.adapter.snapshot()
  assert.equal(failed.adapter.connected, false)
  assert.equal(failed.monitoring, 'unknown')
  const deadline = f.adapter.diagnostics().adapter.communicationFailure.first
  assert.equal(deadline.kind, 'timeout'); assert.equal(deadline.operation, '/v1/snapshot'); assert.equal(deadline.timeoutMs, 5000)
  assert.ok(deadline.elapsedMs >= 4900 && performance.now() - started >= 4900)
  await until(() => f.held.size === 0 && f.adapter.connected && !f.adapter.diagnostics().adapter.communicationFailure.active)
  const retained = f.adapter.diagnostics().adapter.communicationFailure
  const controller = new AbortController(), reason = new Error(`private cancellation ${f.token}`)
  reason.name = `privateCaller${f.token}`
  const pending = f.adapter.snapshot(controller.signal).catch(error => error)
  await until(() => f.held.size === 1)
  controller.abort(reason)
  const cancelled = await pending
  assert.equal(cancelled, reason)
  assert.equal(cancelled.coreRequestFailure.kind, 'cancelled')
  assert.equal(cancelled.coreRequestFailure.name, 'Error')
  assert.equal(JSON.stringify(cancelled.coreRequestFailure).includes(f.token), false)
  assert.equal(f.adapter.connected, true)
  assert.deepEqual(f.adapter.diagnostics().adapter.communicationFailure, retained)
  await until(() => f.held.size === 0)
  const primitiveClient = await f.client()
  for (const reason of ['caller stop', 7, null]) {
    const direct = new AbortController()
    const directRead = primitiveClient.snapshot(direct.signal).then(value => ({ value }), error => ({ error }))
    await until(() => f.held.size === 1)
    direct.abort(reason)
    assert.equal(Object.is((await directRead).error, reason), true, 'CoreClient must preserve primitive caller cancellation')
    await until(() => f.held.size === 0)
    const primitive = new AbortController()
    const read = f.adapter.snapshot(primitive.signal).then(value => ({ value }), error => ({ error }))
    await until(() => f.held.size === 1)
    primitive.abort(reason)
    const result = await read
    assert.equal(Object.is(result.error, reason), true, 'primitive caller cancellation must preserve the original rejection')
    assert.equal(f.adapter.connected, true)
    assert.deepEqual(f.adapter.diagnostics().adapter.communicationFailure, retained)
    await until(() => f.held.size === 0)
  }
  const client = await f.client(), closedRead = client.snapshot().catch(error => error)
  await until(() => f.held.size === 1)
  await client.close()
  assert.equal((await closedRead).coreRequestFailure.kind, 'cancelled')
  assert.deepEqual(f.adapter.diagnostics().adapter.communicationFailure, retained)
})
