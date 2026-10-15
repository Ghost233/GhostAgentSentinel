import test from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { createMetadataClient, createNativeIO } from '../chatgpt/native-io.mjs'
const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const app = '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT'
const nativePath = '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex'

test('production native IO qualifies actual app-server facts and readonly metadata before core and MCP publication', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-native-'))
  const socketPath = '/private/tmp/sentinel-native-' + randomUUID() + '.sock'
  const recordPath = join(dir, 'main.jsonl'), born = Date.now() - 10_000
  const started = new Date(born).toUTCString().replace(',', '').replace(/ GMT$/, '')
  await writeFile(recordPath, JSON.stringify({ type: 'session_meta', payload: { id: 'native-main' } }) + '\n' + JSON.stringify({ timestamp: new Date(born + 3000).toISOString(), type: 'event_msg', payload: { type: 'task_started', turn_id: 'native-turn' } }) + '\n')
  let actualCommand = nativePath + ' -c model_provider="opaque provider" --config policy=["opaque", "values"] app-server --analytics-default-enabled -c nested="opaque value" -c final=true'
  let enabled = true
  const calls = []
  const metadata = new WebSocketServer({ path: '/', noServer: true })
  const { createServer } = await import('node:http')
  const unix = createServer()
  unix.on('upgrade', (req, socket, head) => metadata.handleUpgrade(req, socket, head, ws => metadata.emit('connection', ws, req)))
  metadata.on('connection', ws => ws.on('message', bytes => {
    const message = JSON.parse(bytes)
    calls.push(message.method)
    if (!message.id) return
    let result
    if (message.method === 'initialize') result = { serverInfo: { name: 'controlled-readonly-metadata' } }
    if (message.method === 'thread/read') result = { thread: { id: 'native-main', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', forkedFromId: null, cwd: dir, path: recordPath, status: { type: 'notLoaded' } } }
    if (message.method === 'thread/turns/list') result = { data: [{ id: 'native-turn', status: 'interrupted' }], nextCursor: null }
    if (message.method === 'config/read') result = { config: { plugins: { 'sentinel@test': { enabled } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled } } } }] }
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }))
  }))
  await new Promise(resolve => unix.listen(socketPath, resolve))
  const native = createNativeIO({ pluginId: 'sentinel@test', socketPath, profileSample: async () => profile, processSample: async () => [{ pid: 11, ppid: 1, command: app, started }, { pid: 12, ppid: 11, command: actualCommand, started }] })
  const server = await startServer({ dataDir: join(dir, 'core') })
  const core = await connectCore({ dataDir: join(dir, 'core') })
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'native-io-test', token: 'd'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); for (const ws of metadata.clients) ws.terminate(); await new Promise(resolve => metadata.close(resolve)); await new Promise(resolve => unix.close(resolve)); await rm(socketPath, { force: true }); await rm(dir, { force: true, recursive: true }) })
  const result = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'd'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'native-main' } } }) }).then(response => response.json())
  assert.equal(result.result.structuredContent.observation.phase, 'started', 'saved notLoaded/interrupted metadata is not runtime truth')
  assert.equal((await core.snapshot()).tasks[0].status, 'running')
  const quotedPrefix = ' -c note="app-server with \\"quoted words\\"" --config other=\'opaque app-server words\''
  actualCommand = nativePath + quotedPrefix + ' app-server --analytics-default-enabled'
  assert.equal((await adapter.read({ thread_id: 'native-main' })).observation.phase, 'started')
  actualCommand = nativePath + quotedPrefix + ' exec-server --some-flag app-server'
  const wrongChild = await adapter.read({ thread_id: 'native-main' })
  assert.equal(wrongChild.observation.gap, 'execution-source-unavailable')
  assert.equal((await core.snapshot()).tasks[0].status, 'unknown')
  actualCommand = nativePath + ' app-server'
  enabled = false
  assert.equal((await adapter.read({ thread_id: 'native-main' })).observation.gap, 'plugin-disabled')
  assert.ok(calls.every(method => ['initialize', 'initialized', 'thread/read', 'thread/turns/list', 'config/read'].includes(method)))
  const isolated = createMetadataClient({ socketPath })
  await assert.rejects(isolated.request('thread/resume', {}), /non-observation method rejected/)
  await isolated.close()
  await adapter.close()
  assert.deepEqual(native.diagnostics(), { closed: true, pendingRequests: 0, deadlineTimers: 0, connected: false })
})
