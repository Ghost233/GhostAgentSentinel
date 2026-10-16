import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { randomUUID } from 'node:crypto'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { createNativeIO } from '../chatgpt/native-io.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')

for (const state of ['app-absent', 'host-unknown', 'plugin-disabled', 'plugin-unknown']) test(`standby ${state} never attaches Core or reads conversation metadata`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-qualification-')), coreCalls = [], metadata = []
  const host = state === 'app-absent' ? { available: false, reason: 'execution-source-unavailable' } : state === 'host-unknown' ? { available: null } : { available: true, hostRunId: 'owned-host', hostStartedAt: Date.now() - 10000, profile: supportedProfile }
  const core = { async health() { coreCalls.push('health'); return { features: ['source-observation/1'] } } }
  const native = {
    async sampleHost() { return host },
    async readEnablement() { metadata.push('config'); return { user: state === 'plugin-disabled' ? 'disabled' : 'unknown', effective: state === 'plugin-disabled' ? 'disabled' : 'unknown' } },
    async readThread() { metadata.push('thread'); throw new Error('standby must not read a thread') },
    async listThreads() { metadata.push('directory'); return { data: [], nextCursor: null } },
    async disconnectMetadata() {}, async close() {},
  }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'qualification', token: 'd'.repeat(64), pollMs: 0 })
  t.after(async () => { await adapter.close(); await rm(dir, { recursive: true, force: true }) })
  const view = await adapter.read({ thread_id: 'owned-main' })
  await adapter.refresh()
  assert.ok(view.observation.gap)
  assert.deepEqual(coreCalls, [], 'qualification must finish before any lazy Core health call')
  assert.equal(metadata.includes('thread') || metadata.includes('directory'), false)
  if (state.startsWith('app-') || state === 'host-unknown') assert.deepEqual(metadata, [], 'an absent or unknown Host must not initialize metadata')
})

async function qualifiedFixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-toggle-')), socketPath = join(tmpdir(), 'gs-q-' + randomUUID().slice(0, 12) + '.sock')
  const calls = [], coreCalls = [], releases = new Set(), websocket = new WebSocketServer({ noServer: true }), unix = createServer()
  let present = true, enabled = true, native, server, core, adapter, client, nextHealth = null, nextThread = null, nextIngest = null, hostPid = 12
  const born = Date.now() - 10000, started = new Date(born).toUTCString().replace(',', '').replace(/ GMT$/, ''), path = join(dir, 'main.jsonl')
  const thread = { id: 'owned-main', parentThreadId: null, source: 'vscode', originator: 'Codex Desktop', forkedFromId: null, cwd: dir, path }
  t.after(async () => {
    for (const release of releases) release()
    await adapter?.close(); await native?.close(); await core?.close(); await server?.close()
    for (const ws of websocket.clients) ws.terminate()
    await new Promise(resolve => websocket.close(resolve))
    if (unix.listening) await new Promise(resolve => unix.close(resolve))
    await rm(socketPath, { force: true }); await rm(dir, { recursive: true, force: true })
  })
  await writeFile(path, JSON.stringify({ type: 'session_meta', payload: { id: thread.id } }) + '\n' + JSON.stringify({ timestamp: new Date(born + 3000).toISOString(), type: 'event_msg', payload: { type: 'task_started', turn_id: 'owned-turn' } }) + '\n')
  unix.on('upgrade', (req, socket, head) => websocket.handleUpgrade(req, socket, head, ws => websocket.emit('connection', ws, req)))
  websocket.on('connection', ws => ws.on('message', bytes => {
    const message = JSON.parse(bytes); calls.push(message.method)
    if (!message.id) return
    let result
    if (message.method === 'initialize') result = { serverInfo: { name: 'owned-qualification' } }
    if (message.method === 'config/read') result = { config: { plugins: { 'sentinel@test': { enabled } } }, layers: [{ name: { type: 'user' }, config: { plugins: { 'sentinel@test': { enabled } } } }] }
    if (message.method === 'thread/list') result = { data: message.params.archived ? [] : [thread], nextCursor: null }
    if (message.method === 'thread/read') {
      result = { thread }
      if (nextThread) { const gate = nextThread; nextThread = null; gate.entered(); gate.wait.then(() => { if (ws.readyState === 1) ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result })) }); return }
    }
    if (message.method === 'thread/turns/list') result = { data: [{ id: 'owned-turn' }], nextCursor: null }
    ws.send(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }))
  }))
  await new Promise(resolve => unix.listen(socketPath, resolve))
  native = createNativeIO({ pluginId: 'sentinel@test', socketPath, profileSample: async () => supportedProfile, processSample: async () => present ? [{ pid: 11, ppid: 1, command: '/Applications/ChatGPT.app/Contents/MacOS/ChatGPT', started }, { pid: hostPid, ppid: 11, command: '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex app-server', started }] : [] })
  server = await startServer({ dataDir: join(dir, 'core') }); client = await connectCore({ dataDir: join(dir, 'core') })
  const health = async () => { coreCalls.push('health'); const value = await client.health(), gate = nextHealth; nextHealth = null; if (gate) { gate.entered(); await gate.wait }; return value }
  const ingest = async (...args) => { const value = await client.ingest(...args), gate = nextIngest; nextIngest = null; if (gate) { gate.entered(); await gate.wait }; return value }
  core = { ...client, health, ingest }
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'qualification-toggle', token: 'd'.repeat(64), pollMs: 0 })
  const hold = method => { let entered, release; const started = new Promise(resolve => { entered = resolve }), wait = new Promise(resolve => { release = resolve }); releases.add(release); if (method === 'health') nextHealth = { entered, wait }; else if (method === 'thread') nextThread = { entered, wait }; else nextIngest = { entered, wait }; return { started, release } }
  return { adapter, core, native, calls, coreCalls, setEnabled(value) { enabled = value }, setPresent(value) { present = value }, setGeneration(value) { hostPid = value }, holdHealth: () => hold('health'), holdThread: () => hold('thread'), holdIngest: () => hold('ingest'), async restartCore() { await client.close(); await server.close(); server = await startServer({ dataDir: join(dir, 'core') }); client = await connectCore({ dataDir: join(dir, 'core') }); Object.assign(core, client, { health, ingest }) } }
}

test('plugin off releases metadata and only its source, then reenables at the same panel endpoint', async t => {
  const f = await qualifiedFixture(t), url = f.adapter.url
  const dsh = await f.core.registerSource({ registrationId: 'owned-dsh', hostRunId: 'dsh-host', name: 'Owned DSH' })
  await f.core.ingest(dsh, { tasks: [{ id: 'dsh-task', lifecycleId: 'dsh-turn', title: 'Owned DSH', status: 'running' }] })
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.phase, 'started')
  assert.equal(f.native.diagnostics().connected, true)
  f.coreCalls.length = 0; f.setEnabled(false)
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.gap, 'plugin-disabled')
  assert.deepEqual(f.coreCalls, [])
  assert.equal(f.native.diagnostics().connected, false)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  assert.equal((await f.core.snapshot()).tasks.find(task => task.sourceKey === dsh).status, 'running')
  const before = f.calls.length
  f.setPresent(false)
  for (let attempt = 0; attempt < 3; attempt++) await f.adapter.refresh()
  assert.equal(f.calls.length, before, 'absent App polling must not initialize metadata again')
  assert.deepEqual(f.coreCalls, [])
  f.setPresent(true); f.setEnabled(true)
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.phase, 'started')
  assert.equal(f.adapter.url, url)
  assert.equal(f.native.diagnostics().connected, true)
  assert.equal(f.calls.filter(method => method === 'initialize').length, 2)
  assert.equal((await f.core.snapshot()).tasks.find(task => task.sourceKey === dsh).status, 'running')
})

test('authenticated helper health returns only runtime identity without waking collection or Core', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-health-')), token = 'e'.repeat(64)
  let statusCalls = 0, collectionCalls = 0
  const forbidden = async () => { collectionCalls++; throw new Error('health must not wake collection') }
  const identity = { protocol: 'ghost-sentinel-chatgpt-helper/1', installationId: 'owned-installation', runId: 'owned-run', pid: 1234, version: '0.1.2-dev.0' }
  const adapter = await startChatGPTAdapter({ core: { health: forbidden }, native: { sampleHost: forbidden, readEnablement: forbidden, async close() {} }, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: identity.installationId, token, pollMs: 0, runtimeStatus: () => { statusCalls++; return { ...identity, token, dataDir: '/private/owned-body', body: 'private text' } } })
  t.after(async () => { await adapter.close(); await rm(dir, { recursive: true, force: true }) })
  assert.equal((await fetch(adapter.url + '/health')).status, 401)
  assert.equal(statusCalls, 0)
  const response = await fetch(adapter.url + '/health', { headers: { authorization: 'Bearer ' + token } })
  assert.equal(response.status, 200)
  assert.deepEqual(await response.json(), identity)
  assert.equal(statusCalls, 1)
  assert.equal(collectionCalls, 0)
})

test('a changed Core instance requires fresh registration before a retained reader can publish', async t => {
  const f = await qualifiedFixture(t)
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.phase, 'started')
  const previous = await f.core.health()
  await f.restartCore()
  assert.notEqual((await f.core.health()).instanceId, previous.instanceId)
  await f.adapter.read({ thread_id: 'owned-main' })
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources.find(source => source.descriptor.registrationId === 'chatgpt:qualification-toggle').status, 'connected')
  assert.equal(snapshot.tasks.find(task => task.id === 'owned-main').status, 'running')
})

test('plugin loss while Core health is delayed seals the old batch and retains the other source', async t => {
  const f = await qualifiedFixture(t)
  const dsh = await f.core.registerSource({ registrationId: 'owned-delayed-dsh', hostRunId: 'dsh-host', name: 'Owned DSH' })
  await f.core.ingest(dsh, { tasks: [{ id: 'dsh-task', lifecycleId: 'dsh-turn', title: 'Owned DSH', status: 'running' }] })
  await f.adapter.read({ thread_id: 'owned-main' })
  const gate = f.holdHealth(), oldRead = f.adapter.read({ thread_id: 'owned-main' })
  await gate.started
  f.setEnabled(false)
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.gap, 'plugin-disabled')
  const afterLoss = f.calls.length
  gate.release()
  assert.equal((await oldRead).observation.gap, 'plugin-disabled')
  assert.equal(f.calls.length, afterLoss, 'a late healthy Core reply cannot resume an invalidated batch')
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources.find(source => source.descriptor.registrationId === 'chatgpt:qualification-toggle').status, 'disconnected')
  assert.equal(snapshot.tasks.find(task => task.sourceKey === dsh).status, 'running')
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('plugin off cancels pending metadata without a late cancellation overwriting the disabled view', async t => {
  const f = await qualifiedFixture(t)
  await f.adapter.read({ thread_id: 'owned-main' })
  const gate = f.holdThread(), oldRead = f.adapter.read({ thread_id: 'owned-main' })
  await gate.started
  f.setEnabled(false)
  const disabled = await f.adapter.read({ thread_id: 'owned-main' })
  gate.release()
  assert.equal(disabled.observation.gap, 'plugin-disabled')
  assert.equal((await oldRead).observation.gap, 'plugin-disabled')
  assert.deepEqual(f.native.diagnostics(), { closed: false, pendingRequests: 0, deadlineTimers: 0, connected: false })
  assert.equal(f.adapter.diagnostics().openReaders, 0)
})

test('plugin off remains visible after an already accepted Core ingest reply arrives late', async t => {
  const f = await qualifiedFixture(t)
  await f.adapter.read({ thread_id: 'owned-main' })
  const gate = f.holdIngest(), oldRead = f.adapter.read({ thread_id: 'owned-main' })
  await gate.started
  f.setEnabled(false)
  assert.equal((await f.adapter.read({ thread_id: 'owned-main' })).observation.gap, 'plugin-disabled')
  gate.release()
  assert.equal((await oldRead).observation.gap, 'plugin-disabled')
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources.find(source => source.descriptor.registrationId === 'chatgpt:qualification-toggle').status, 'disconnected')
  assert.equal(snapshot.tasks.find(task => task.id === 'owned-main').status, 'unknown')
})

test('a Host generation change during a qualified batch cannot publish the old running scope', async t => {
  const f = await qualifiedFixture(t)
  await f.adapter.read({ thread_id: 'owned-main' })
  const before = (await f.core.snapshot()).tasks.find(task => task.id === 'owned-main').events
  const gate = f.holdHealth(), read = f.adapter.read({ thread_id: 'owned-main' })
  await gate.started
  f.setGeneration(13); gate.release()
  assert.equal((await read).observation.gap, 'source-generation-changed')
  const snapshot = await f.core.snapshot(), task = snapshot.tasks.find(task => task.id === 'owned-main')
  assert.equal(task.status, 'unknown')
  assert.deepEqual(task.events, before)
  assert.equal(f.adapter.diagnostics().openReaders, 0)
  assert.equal(f.native.diagnostics().connected, false)
})
