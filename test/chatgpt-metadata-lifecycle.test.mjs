import test from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { createServer } from 'node:http'
import { randomUUID } from 'node:crypto'
import { rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { createMetadataClient } from '../chatgpt/native-io.mjs'
const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')

async function metadataFixture(t, mode) {
  const socketPath = join(tmpdir(), 'gs-m-' + randomUUID().slice(0, 12) + '.sock')
  const websocket = new WebSocketServer({ noServer: true }), server = createServer()
  server.on('upgrade', (req, socket, head) => websocket.handleUpgrade(req, socket, head, ws => websocket.emit('connection', ws, req)))
  let received, initialized
  const requestReceived = new Promise(resolve => { received = resolve })
  const initializationReceived = new Promise(resolve => { initialized = resolve })
  const methods = []
  websocket.on('connection', ws => ws.on('message', bytes => {
    const message = JSON.parse(bytes)
    methods.push(message.method)
    if (!message.id) return
    if (message.method === 'initialize') {
      initialized()
      if (mode === 'timeout') return
      ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, ...(mode === 'reject' ? { error: { code: -32603, message: 'controlled initialization rejection' } } : { result: { serverInfo: { name: 'controlled-metadata' } } }) }))
    } else if (mode === 'hold') received()
    else ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result: { thread: { id: 'confirmed-main' } } }))
  }))
  await new Promise(resolve => server.listen(socketPath, resolve))
  const client = createMetadataClient({ socketPath })
  t.after(async () => { await client.close(); for (const ws of websocket.clients) ws.terminate(); await new Promise(resolve => websocket.close(resolve)); await new Promise(resolve => server.close(resolve)); await rm(socketPath, { force: true }) })
  assert.equal(dirname(server.address()), tmpdir(), 'metadata sockets must use the platform temporary directory')
  assert.ok(Buffer.byteLength(server.address()) < 104, 'Unix socket endpoint must fit the supported macOS path limit')
  return { client, connections: () => websocket.clients.size, requestReceived, initializationReceived, methods, accept() { mode = 'accept' } }
}

test('each rejected initialization releases its websocket before a retry', async t => {
  const f = await metadataFixture(t, 'reject')
  for (let retry = 0; retry < 3; retry++) {
    await assert.rejects(f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false }), /metadata request failed/)
    assert.equal(f.connections(), 0)
    assert.equal(f.client.diagnostics().pendingRequests, 0)
    assert.equal(f.client.diagnostics().connected, false)
  }
})

test('a successful initialized connection is reused and explicitly closes without pending deadlines', async t => {
  const f = await metadataFixture(t, 'accept')
  assert.equal((await f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false })).thread.id, 'confirmed-main')
  assert.equal((await f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false })).thread.id, 'confirmed-main')
  assert.equal(f.connections(), 1)
  await f.client.close()
  assert.equal(f.connections(), 0)
  assert.deepEqual(f.client.diagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0, connected: false })
})

test('an initialization timeout releases its websocket before reporting failure', async t => {
  const f = await metadataFixture(t, 'timeout')
  await assert.rejects(f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false }), /metadata request timed out/)
  assert.equal(f.connections(), 0)
  assert.equal(f.client.diagnostics().pendingRequests, 0)
  assert.equal(f.client.diagnostics().connected, false)
})

test('metadata disconnect cancels an owned request and permits a fresh connection before terminal close', async t => {
  const f = await metadataFixture(t, 'hold')
  const rejected = assert.rejects(f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false }), { code: 'cancelled' })
  await f.requestReceived
  await f.client.disconnect()
  await rejected
  assert.equal(f.connections(), 0)
  assert.deepEqual(f.client.diagnostics(), { closed: false, pendingRequests: 0, deadlineTimers: 0, connected: false })
  await f.client.disconnect()
  f.accept()
  assert.equal((await f.client.request('thread/read', { threadId: 'confirmed-main', includeTurns: false })).thread.id, 'confirmed-main')
  assert.equal(f.methods.filter(method => method === 'initialize').length, 2)
  await f.client.close()
  await assert.rejects(f.client.request('thread/read', { threadId: 'confirmed-main' }), { code: 'cancelled' })
  assert.equal(f.connections(), 0)
})

test('metadata disconnect also settles an initializing connection before opening a fresh one', async t => {
  const f = await metadataFixture(t, 'timeout')
  const rejected = assert.rejects(f.client.request('thread/read', { threadId: 'confirmed-main' }), { code: 'cancelled' })
  await f.initializationReceived
  await f.client.disconnect()
  await rejected
  assert.equal(f.connections(), 0)
  assert.equal(f.client.diagnostics().deadlineTimers, 0)
  f.accept()
  assert.equal((await f.client.request('thread/read', { threadId: 'confirmed-main' })).thread.id, 'confirmed-main')
  assert.equal(f.methods.filter(method => method === 'initialize').length, 2)
})
