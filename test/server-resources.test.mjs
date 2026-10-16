import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { request } from 'node:http'
import { channel } from 'node:diagnostics_channel'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

test('sequential core calls reuse loopback connections and explicit close releases their sockets', { timeout: 10000 }, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-server-keepalive-'))
  let server, client, closing
  const connections = new Map(), connected = channel('undici:client:connected')
  const observe = ({ socket }) => {
    if (socket?.remotePort !== Number(new URL(server.url).port)) return
    connections.set(socket, new Promise(resolve => socket.once('close', resolve)))
  }
  const close = () => closing ??= (async () => {
    await client?.close(); await server?.close()
    let timer
    try {
      await Promise.race([Promise.all(connections.values()), new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('owned HTTP sockets did not close')), 1000) })])
      assert.ok([...connections.keys()].every(socket => socket.destroyed))
      if (client) assert.deepEqual(client.transportDiagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0 })
    } finally { clearTimeout(timer); connected.unsubscribe(observe); await rm(dataDir, { recursive: true, force: true }) }
  })()
  t.after(close)
  server = await startServer({ dataDir })
  connected.subscribe(observe)
  client = await connectCore({ dataDir })
  for (let index = 0; index < 64; index++) assert.equal((await client.health()).instanceId, server.instanceId)
  assert.equal((await client.diagnostics()).http.activeRequests, 0)
  const count = connections.size
  await close()
  t.diagnostic(`64 sequential core calls used ${count} TCP connections; all owned sockets closed`)
  assert.ok(count > 0, 'the observer must have captured the owned HTTP connections')
  assert.ok(count <= 3, `64 sequential core calls opened ${count} TCP connections instead of reusing them`)
  await assert.rejects(fetch(`${server.url}/v1/health`, { signal: AbortSignal.timeout(1000) }), /fetch failed/)
})

test('authenticated diagnostics separate retained records from temporary HTTP requests and expose no token or task content', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-server-resources-'))
  const server = await startServer({ dataDir })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const client = await connectCore({ dataDir })
  t.after(() => client.close())
  const source = await client.registerSource({ registrationId: 'a', hostRunId: 'run', name: 'test' })
  await client.ingest(source, { tasks: [{ id: 'private task id', lifecycleId: 'l', title: 'private task body', status: 'idle' }] })
  const diagnostics = await client.diagnostics()
  assert.equal(diagnostics.pid, process.pid)
  assert.equal(diagnostics.runtime.connectedSources, 1)
  assert.equal(diagnostics.retained.tasks, 1)
  assert.equal(diagnostics.http.activeRequests, 0, 'the diagnostics read itself is excluded')
  assert.equal(JSON.stringify(diagnostics).includes(server.token), false)
  assert.equal(JSON.stringify(diagnostics).includes('private task'), false)
  const foreignHostStatus = await new Promise((resolve, reject) => {
    const req = request(`${server.url}/v1/health`, { headers: { authorization: `Bearer ${server.token}`, host: 'example.invalid' } }, response => { response.resume(); response.on('end', () => resolve(response.statusCode)) })
    req.on('error', reject); req.end()
  })
  assert.equal(foreignHostStatus, 401)
})

test('a pending valid source registration crosses idle grace and prevents shutdown after its body completes', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-slow-registration-'))
  let idleCalls = 0
  const server = await startServer({ dataDir, idleGraceMs: 30, onIdle: () => { idleCalls++ } })
  let req, client
  t.after(async () => { req?.destroy(); await client?.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  client = await connectCore({ dataDir })
  const seed = await client.registerSource({ registrationId: 'initial', hostRunId: 'initial-run', name: 'initial source' })
  const response = new Promise((resolve, reject) => {
    req = request(`${server.url}/v1/source/register`, { method: 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' } }, res => {
      let body = ''; res.on('data', chunk => { body += chunk }); res.on('end', () => resolve({ status: res.statusCode, source: JSON.parse(body) }))
    })
    req.on('error', reject)
    req.write('{"registrationId":"a",')
  })
  // Keep the original failure observable through await response while teardown may abort it.
  void response.catch(() => {})
  const enteredDeadline = Date.now() + 2000
  while ((await client.diagnostics()).http.activeRequests !== 1) {
    assert.ok(Date.now() < enteredDeadline, 'the partial registration must enter the server before source removal')
    await delay(5)
  }
  // Arm this window only after the server has observed the still-pending request.
  if (idleCalls) t.diagnostic(`legitimate pre-request startup idle callbacks: ${idleCalls}`)
  idleCalls = 0
  await client.unregisterSource(seed, 'initial source removed after the pending request entered')
  await delay(80)
  const pending = await client.diagnostics()
  assert.equal(pending.http.activeRequests, 1)
  assert.equal(pending.runtime.connectedSources, 0)
  assert.equal(idleCalls, 0, 'ordinary idle cannot close a source arriving in an active request')
  req.end('"hostRunId":"run","name":"test"}')
  const result = await response
  assert.equal(result.status, 200)
  assert.equal((await client.snapshot()).overview.connectedSources, 1)
  assert.equal(idleCalls, 0, 'ordinary idle cannot close a source arriving in an active request')
})
