import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { connectCore } from '../src/core-client.mjs'

test('closing one transport cancels and settles its pending read, releases deadlines, and leaves another client active', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-transport-resource-'))
  const held = []
  const server = createServer((req, res) => {
    if (req.url === '/v1/health') res.end(JSON.stringify({ protocol: 1, instanceId: 'fixture' }))
    else { const request = { res, closed: false }; held.push(request); res.on('close', () => { request.closed = true }) }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); await rm(dataDir, { recursive: true, force: true }) })
  await writeFile(join(dataDir, 'runtime.json'), JSON.stringify({ protocol: 1, url: `http://127.0.0.1:${server.address().port}`, token: 'a'.repeat(64), instanceId: 'fixture' }))
  const one = await connectCore({ dataDir }), two = await connectCore({ dataDir })
  t.after(async () => { await one.close(); await two.close() })
  const first = one.snapshot().then(() => ({ resolved: true }), error => ({ error }))
  const second = two.snapshot()
  for (let i = 0; i < 100 && held.length < 2; i++) await delay(5)
  assert.equal(held.length, 2)
  await one.close()
  assert.equal(one.transportDiagnostics().pendingRequests, 0)
  assert.equal(one.transportDiagnostics().deadlineTimers, 0)
  const cancelled = await first
  assert.equal(cancelled.resolved, undefined)
  assert.ok(cancelled.error)
  assert.equal(held[1].closed, false)
  held[1].res.end(JSON.stringify({ protocol: 1, tasks: [] }))
  assert.equal((await second).protocol, 1)
  assert.equal(two.transportDiagnostics().pendingRequests, 0)
  await one.close()
})

test('an already cancelled adapter bootstrap never starts a core', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-bootstrap-abort-'))
  t.after(() => rm(dataDir, { recursive: true, force: true }))
  const controller = new AbortController()
  controller.abort(new Error('adapter stopped'))
  await assert.rejects(connectCore({ dataDir, signal: controller.signal }), /adapter stopped/)
})
