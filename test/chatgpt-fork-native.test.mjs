import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createNativeIO } from '../chatgpt/native-io.mjs'
const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')

test('an independently identified native fork obtains its own latest turn metadata without resuming any execution', async t => {
  const socketPath = join(tmpdir(), 'sf-' + randomUUID().slice(0, 8) + '.sock'), server = createServer(), ws = new WebSocketServer({ server }), calls = []
  ws.on('connection', connection => connection.on('message', bytes => {
    const message = JSON.parse(bytes); if (!message.id) return
    calls.push(message.method)
    const result = message.method === 'initialize' ? { serverInfo: { name: 'controlled' } } : message.method === 'thread/read' ? { thread: { id: 'fork-native', parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: 'original' } } : { data: [{ id: 'fork-own-turn', status: 'notLoaded' }], nextCursor: null }
    connection.send(JSON.stringify({ id: message.id, result }))
  }))
  await new Promise(resolve => server.listen(socketPath, resolve))
  const io = createNativeIO({ pluginId: 'sentinel@test', socketPath })
  t.after(async () => { await io.close(); await new Promise(resolve => ws.close(resolve)); await new Promise(resolve => server.close(resolve)); await rm(socketPath, { force: true }) })
  const metadata = await io.readThread('fork-native')
  assert.equal(metadata.thread.forkedFromId, 'original')
  assert.equal(metadata.anchorTurnId, 'fork-own-turn')
  assert.deepEqual(calls, ['initialize', 'thread/read', 'thread/turns/list'])
})
