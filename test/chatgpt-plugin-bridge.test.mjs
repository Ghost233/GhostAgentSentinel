import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, writeFile, readFile, rm, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { prepareInstallation, resolvePluginOwner, startFromInstallation } from '../chatgpt/runtime.mjs'
import { createStdioMetadataClient } from '../chatgpt/stdio-metadata.mjs'
import { startServer } from '../src/server.mjs'
import { startPluginBridge } from '../chatgpt/plugin-bridge.mjs'
import { PassThrough } from 'node:stream'
import { createServer } from 'node:http'
import { connectCore } from '../src/core-client.mjs'
import { resourceMaterial } from './fixtures/chatgpt-plugin-lazy/resource-material.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { Client } from '../chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js'
import { StdioClientTransport } from '../chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/stdio.js'

async function bounded(promise, label, timeoutMs = 5000) {
  const controller = new AbortController()
  try { return await Promise.race([promise, delay(timeoutMs, null, { signal: controller.signal }).then(() => assert.fail(label))]) }
  finally { controller.abort() }
}

async function setup(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-bridge-')), core = await startServer({ dataDir: join(dir, 'core') })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@bridge' })
  await writeFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查' } }), { mode: 0o600 })
  await writeFile(join(prepared.pluginDir, '.mcp.json'), JSON.stringify({ mcpServers: { sentinel: { command: process.execPath, args: [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', prepared.configPath], cwd: new URL('../', import.meta.url).pathname } } }), { mode: 0o600 })
  const statePath = join(dir, 'enabled.json'), callsPath = join(dir, 'calls.txt'), pidsPath = join(dir, 'owner-pids.txt'), faultPath = join(dir, 'fault.json'), auditPath = join(dir, 'audit.jsonl'), children = []
  await writeFile(statePath, 'true'); await writeFile(callsPath, ''); await writeFile(pidsPath, '')
  await writeFile(faultPath, '{}'); await writeFile(auditPath, '')
  t.after(async () => {
    await writeFile(statePath, 'false')
    for (const child of children) if (child.exitCode === null && child.signalCode === null) { const ended = once(child, 'close'); child.kill('SIGTERM'); await bounded(ended, 'test child cleanup failed') }
    await core.close(); await rm(dir, { recursive: true, force: true })
  })
  function track(child) { children.push(child); child.ended = once(child, 'close'); child.errors = ''; child.stderr.on('data', bytes => { child.errors += bytes }); return child }
  async function owner(timeoutMs = 5000) {
    const child = track(spawn(process.execPath, [new URL('./fixtures/chatgpt-plugin-bridge/owner.mjs', import.meta.url).pathname, prepared.configPath, statePath, callsPath], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] }))
    const [identity] = await bounded(once(child, 'message'), 'owner did not start: ' + child.errors, timeoutMs)
    return { child, identity }
  }
  function bridge({ launchOwner = false, faults = false } = {}) {
    const args = faults ? [new URL('./fixtures/chatgpt-plugin-bridge/fault-bridge.mjs', import.meta.url).pathname, prepared.configPath, faultPath, auditPath] : launchOwner ? [new URL('./fixtures/chatgpt-plugin-bridge/bridge.mjs', import.meta.url).pathname, prepared.configPath, statePath, callsPath, pidsPath] : [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', prepared.configPath]
    const child = track(spawn(process.execPath, args, { stdio: ['pipe', 'pipe', 'pipe'] }))
    let buffer = '', id = 0
    const pending = new Map(), messages = []
    child.stdout.on('data', bytes => {
      buffer += bytes
      while (buffer.includes('\n')) {
        const index = buffer.indexOf('\n'), line = buffer.slice(0, index); buffer = buffer.slice(index + 1)
        const message = JSON.parse(line); messages.push(message)
        if (pending.has(message.id)) { pending.get(message.id)(message); pending.delete(message.id) }
      }
    })
    function request(method, params = {}, requestId = ++id) {
      const response = new Promise(resolve => { pending.set(requestId, resolve) })
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params }) + '\n')
      return bounded(response, 'bridge request failed ' + method + ': ' + child.errors)
    }
    return { child, request, messages }
  }
  return { ...prepared, dir, statePath, callsPath, pidsPath, faultPath, auditPath, owner, bridge }
}

test('cold observation data does not block the MCP UI and later reads recover without reconnecting', async t => {
  const env = await setup(t), input = new PassThrough(), output = new PassThrough()
  const startupBarrier = join(env.dir, 'hold-startup')
  await writeFile(startupBarrier, 'held')
  await writeFile(join(env.dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: env.dir, producerVersion: supportedProfile.cliVersion }))
  let ownerStarting, bridge, launches = 0, sequence = 0, ownerReady = false
  const request = async (method, params = {}) => {
    const reply = once(output, 'data')
    input.write(JSON.stringify({ jsonrpc: '2.0', id: ++sequence, method, params }) + '\n')
    return JSON.parse((await bounded(reply, 'local MCP UI did not respond while data was pending'))[0])
  }
  try {
    bridge = await startPluginBridge({ configPath: env.configPath, input, output, startOwner() { launches++; ownerStarting = env.owner(15000).then(value => { ownerReady = true; return value }); ownerStarting.catch(() => {}); return Promise.resolve() } })
    const initialized = await request('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'pending-data-regression', version: '1' } })
    assert.equal(initialized.result.protocolVersion, '2025-06-18')
    input.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n')
    const resource = await request('resources/read', { uri: 'ui://ghost-sentinel/current-chat.html' })
    assert.match(resource.result.contents[0].text, /id="refresh"/)
    const waiting = await request('tools/call', { name: 'sentinel_current_chat', arguments: {} })
    assert.equal(waiting.result.structuredContent.signal, 'gray')
    assert.equal(waiting.result.structuredContent.observation.gap, 'observer-unavailable')
    const diagnostic = await request('tools/call', { name: 'sentinel_installation_diagnostics', arguments: {} })
    assert.deepEqual(diagnostic.result.structuredContent, { state: 'unavailable', reason: 'observer-unavailable' })
    assert.notEqual(diagnostic.result.isError, true, 'startup pending is not an MCP tool failure')
    assert.equal(ownerReady, false, 'handshake and page reads finish while observer startup is still held')
    await rm(startupBarrier)
    const owner = await ownerStarting
    const recovered = await request('tools/call', { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } })
    assert.equal(recovered.result.structuredContent.identity.threadId, 'resource-main')
    const paused = await request('tools/call', { name: 'sentinel_set_chat_analysis', arguments: { paused: true, expectedThreadId: 'resource-main', revision: recovered.result.structuredContent.monitoring.revision }, _meta: { thread_id: 'resource-main' } })
    assert.equal(paused.result.structuredContent.monitoring.paused, true)
    assert.equal(bridge.runId, owner.identity.runId)
    assert.equal(launches, 1)
  } finally { await rm(startupBarrier, { force: true }); await bridge?.close(); await ownerStarting?.catch(() => {}) }
})

test('normal metadata stderr cannot close a live MCP panel or its unique observer', async t => {
  const env = await setup(t), metadataChildren = []
  const metadata = createStdioMetadataClient({ executable: process.execPath, args: [new URL('./fixtures/chatgpt-stdio-metadata.mjs', import.meta.url).pathname, 'stderr-chatter'], spawn(...args) { const child = spawn(...args); metadataChildren.push(child); return child } })
  let noisy = false, runtime
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'metadata-stderr-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement(cwd) {
      const result = await metadata.request('config/read', { key: noisy && cwd === null ? 'stderr-burst' : 'quiet' })
      const enabled = result.config.plugins['sentinel@test'].enabled && JSON.parse(await readFile(env.statePath, 'utf8'))
      return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' }
    },
    async listThreads() { return { data: [], nextCursor: null } },
    async readThread(id) { return { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(env.dir, id + '.jsonl'), cwd: env.dir }, anchorTurnId: 'resource-turn' } },
    async close() { await metadata.close() },
  }
  const transport = new StdioClientTransport({ command: process.execPath, args: [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', env.configPath], stderr: 'pipe' })
  transport.stderr.on('data', () => {})
  const client = new Client({ name: 'metadata-stderr-panel', version: '1' })
  try {
    await writeFile(join(env.dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: env.dir, producerVersion: supportedProfile.cliVersion }))
    runtime = await startFromInstallation({ configPath: env.configPath, native, ownership: 'plugin', pollMs: 0 })
    await bounded(client.connect(transport), 'controlled MCP did not initialize')
    const initial = await client.callTool({ name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } })
    assert.equal(initial.structuredContent.identity.threadId, 'resource-main', 'the quiet production path is connected before diagnostic output')
    noisy = true
    const identities = []
    for (let n = 0; n < 8; n++) {
      const view = await bounded(client.callTool({ name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } }), 'valid panel read closed after normal metadata diagnostics')
      assert.notEqual(view.isError, true)
      identities.push(view.structuredContent.identity.threadId)
    }
    assert.deepEqual(identities, Array(8).fill('resource-main'))
    assert.ok((await client.readResource({ uri: 'ui://ghost-sentinel/current-chat.html' })).contents.length, 'switching back can still load the page resource on the same connection')
    assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, runtime.runId)
    assert.equal(metadataChildren.length, 1)
    await writeFile(env.statePath, 'false'); await runtime.refresh()
    assert.equal((await bounded(runtime.closed, 'actual disable must still reclaim the observer')).reason, 'plugin-disabled')
    await bounded((async () => { while (transport.pid !== null) await delay(10) })(), 'actual disable retained the MCP connection')
  } finally { await client.close(); await runtime?.close(); await metadata.close() }
})

test('an unavailable observer retains local resources, gray reads and rejected writes without launch loops', async t => {
  const env = await setup(t), input = new PassThrough(), output = new PassThrough()
  let launches = 0, sequence = 0
  const bridge = await startPluginBridge({ configPath: env.configPath, input, output, startOwner() { launches++; return Promise.resolve() } })
  const request = async (method, params = {}) => {
    const reply = once(output, 'data')
    input.write(JSON.stringify({ jsonrpc: '2.0', id: ++sequence, method, params }) + '\n')
    return JSON.parse((await bounded(reply, 'unavailable observer closed the local MCP UI', 2000))[0])
  }
  try {
    assert.ok((await request('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'missing-data-regression', version: '1' } })).result)
    input.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n')
    assert.ok((await request('resources/read', { uri: 'ui://ghost-sentinel/settings.html' })).result.contents.length)
    for (let n = 0; n < 2; n++) assert.equal((await request('tools/call', { name: 'sentinel_current_chat', arguments: {} })).result.structuredContent.signal, 'gray')
    const write = await request('tools/call', { name: 'sentinel_set_chat_analysis', arguments: { paused: true, expectedThreadId: 'unknown-main', revision: 'unknown' } })
    assert.equal(write.result.isError, true)
    assert.equal(launches, 1)
    input.end(); await bounded(bridge.closed, 'pending-data MCP retained its process on EOF')
  } finally { await bridge.close() }
})

for (const kind of ['control', 'health-timeout', 'health-network', 'health-expired-success', 'health-identity', 'health-401', 'health-403', 'health-404', 'health-503']) test(`initial handshake with ${kind} never launches a second healthy owner`, async t => {
  const env = await setup(t), owner = await env.owner()
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  await writeFile(env.faultPath, JSON.stringify({ startup: kind === 'control' ? null : kind }))
  const transport = new StdioClientTransport({ command: process.execPath, args: [new URL('./fixtures/chatgpt-plugin-bridge/fault-bridge.mjs', import.meta.url).pathname, env.configPath, env.faultPath, env.auditPath], stderr: 'pipe' })
  transport.stderr.on('data', () => {})
  const client = new Client({ name: 'initial-handshake-feedback', version: '1' })
  const started = performance.now(), soft = ['control', 'health-timeout', 'health-network'].includes(kind), pendingData = kind === 'health-expired-success'
  try {
    const connecting = bounded(client.connect(transport), 'initial handshake exceeded its original five-second startup budget')
    if (soft || pendingData) {
      await connecting
      assert.ok(performance.now() - started < 5000)
      assert.ok((await client.listTools()).tools.some(tool => tool.name === 'sentinel_current_chat'))
    } else await assert.rejects(connecting, /Connection closed/)
    const facts = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse)
    assert.equal(facts.filter(fact => fact.event === 'unexpected-owner-launch').length, 0)
    assert.equal(facts.filter(fact => fact.event === 'mcp-request' && fact.method === 'initialize').length, soft ? 1 : 0, 'the handshake request is sent once, only after owner qualification')
    assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
    assert.equal(owner.child.exitCode, null)
    if (!soft && !pendingData) {
      assert.ok(facts.some(fact => fact.event === 'bridge-start-failed'))
      assert.equal(facts.filter(fact => fact.event === 'bridge-ready').length, 0, 'an inadmissible owner cannot open stdio')
    }
    if (kind === 'health-expired-success') assert.ok(facts.some(fact => fact.event === 'healthy-owner-returned-after-budget'))
  } finally {
    await client.close()
    await bounded((async () => { while (transport.pid !== null) await delay(10) })(), 'initial handshake bridge did not exit')
    await writeFile(env.statePath, 'false')
    assert.deepEqual(await bounded(owner.child.ended, 'initial handshake fixture owner did not exit'), [0, null], owner.child.errors)
  }
})

for (const fault of [{ kind: 'persistent-health-network' }, { kind: 'persistent-health-timeout' }, { startup: ['health-network', 'health-503'] }]) test(`initial handshake separates unavailable data from untrusted identity: ${JSON.stringify(fault)}`, async t => {
  const env = await setup(t), owner = await env.owner()
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  await writeFile(env.faultPath, JSON.stringify(fault))
  const transport = new StdioClientTransport({ command: process.execPath, args: [new URL('./fixtures/chatgpt-plugin-bridge/fault-bridge.mjs', import.meta.url).pathname, env.configPath, env.faultPath, env.auditPath], stderr: 'pipe' })
  transport.stderr.on('data', () => {})
  const client = new Client({ name: 'initial-handshake-budget', version: '1' })
  try {
    // Process spawning is outside the bridge's measured five-second budget.
    const connecting = bounded(client.connect(transport), 'initial handshake did not settle', 8000)
    if (fault.startup) await assert.rejects(connecting, /Connection closed/)
    else {
      await connecting
      assert.ok((await client.readResource({ uri: 'ui://ghost-sentinel/current-chat.html' })).contents.length)
      assert.equal((await client.callTool({ name: 'sentinel_current_chat', arguments: {} })).structuredContent.signal, 'gray')
    }
    const facts = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse)
    const failure = facts.find(fact => fact.event === 'bridge-start-failed')
    if (fault.startup) assert.ok(failure.elapsedMs <= 5000, 'a hard identity failure retains its original ceiling')
    else assert.equal(failure, undefined, 'missing data no longer closes the local MCP entry')
    assert.equal(facts.filter(fact => fact.event === 'unexpected-owner-launch').length, 0)
    assert.equal(facts.filter(fact => fact.event === 'mcp-request').length, 0, 'initialize has not been forwarded or replayed')
    if (fault.startup) { assert.equal(facts.filter(fact => fact.event === 'health-fault').length, 2); assert.ok(failure.elapsedMs < 2000, 'a hard status after a soft failure is not retried') }
    else assert.ok(facts.filter(fact => fact.event === 'health-fault').length > 1)
    assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
    assert.equal(owner.child.exitCode, null)
  } finally {
    await client.close()
    await bounded((async () => { while (transport.pid !== null) await delay(10) })(), 'failed initial bridge did not exit')
    await writeFile(env.statePath, 'false')
    assert.deepEqual(await bounded(owner.child.ended, 'startup budget fixture owner did not exit'), [0, null], owner.child.errors)
  }
})

for (const kind of ['health-timeout', 'health-network', 'http-503', 'http-close']) test(`a committed pause survives ${kind} as an explicit unknown followed by a fresh read and explicit resume`, async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true }), core = await connectCore({ dataDir: join(env.dir, 'core') })
  t.after(() => core.close())
  await writeFile(join(env.dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: env.dir, producerVersion: supportedProfile.cliVersion }))
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'transient-io', version: '1' } })
  const call = (name, args = {}) => bridge.request('tools/call', { name, arguments: args, _meta: { thread_id: 'resource-main' } })
  const before = await call('sentinel_current_chat')
  assert.equal(before.result.structuredContent.monitoring.paused, false)
  await writeFile(env.faultPath, JSON.stringify({ kind }))
  const unconfirmed = await call('sentinel_set_chat_analysis', { paused: true, expectedThreadId: 'resource-main', revision: before.result.structuredContent.monitoring.revision })
  assert.deepEqual(unconfirmed.error, { code: -32603, message: 'Observation temporarily unconfirmed; request outcome may be unknown' })
  assert.equal(unconfirmed.result, undefined, 'no unqualified view is forwarded as success')
  assert.equal(owner.child.exitCode, null)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  assert.equal((await core.snapshot()).tasks.find(task => task.id === 'resource-main').conversationMonitoring.paused, true)
  await writeFile(env.faultPath, '{}')
  const paused = await call('sentinel_current_chat')
  assert.equal(paused.result.structuredContent.monitoring.paused, true)
  const resumed = await call('sentinel_set_chat_analysis', { paused: false, expectedThreadId: 'resource-main', revision: paused.result.structuredContent.monitoring.revision })
  assert.equal(resumed.result.structuredContent.monitoring.paused, false)
  const facts = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse)
  assert.deepEqual(facts.filter(fact => fact.event === 'control-committed').map(fact => fact.paused), [true, false], 'mutations are never replayed')
  assert.equal(facts.filter(fact => fact.event === 'mcp-request' && fact.method === 'initialize').length, 1, 'HTTP replacement cannot replay initialization')
  assert.ok(facts.filter(fact => fact.event === 'mcp-request' && fact.method === 'tools/call').every(fact => fact.protocolVersion === '2025-03-26'), 'a rebuilt HTTP transport retains the negotiated protocol')
  assert.equal(bridge.messages.filter(message => message.id === unconfirmed.id).length, 1, 'send and reply failure settle the request once')
  assert.doesNotMatch(JSON.stringify(bridge.messages), /secret-|Bearer|token/)
  bridge.child.stdin.end()
  assert.deepEqual(await bounded(bridge.child.ended, 'recovered bridge did not exit on EOF'), [0, null], bridge.child.errors)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'owned observer did not exit after disable'), [0, null], owner.child.errors)
})

test('persistent unconfirmed health remains bounded without emitting material and still exits after disable', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'persistent-unknown', version: '1' } })
  await writeFile(env.faultPath, JSON.stringify({ kind: 'persistent-health-network' }))
  for (let n = 0; n < 40; n++) {
    const response = await bridge.request('tools/list')
    assert.equal(response.error?.code, -32603)
    assert.equal(response.result, undefined)
  }
  await delay(1100)
  assert.equal(bridge.child.exitCode, null, 'unknown availability is distinct from confirmed owner revocation')
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  const facts = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse)
  assert.equal(facts.filter(fact => fact.event === 'mcp-request').length, 1, 'none of the unconfirmed requests reach the owner')
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'owner remained after disable during unknown health'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(bridge.child.ended, 'bridge remained after disabled owner ended'), [0, null], bridge.child.errors)
})

test('qualification revokes the owner endpoint before closing IO while its process lock fences cleanup', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'revocation-fence', version: '1' } })
  await writeFile(join(env.dir, 'hold-close'), 'held')
  try {
    await writeFile(env.statePath, 'false')
    for (let n = 0; n < 100 && !(await readFile(env.callsPath, 'utf8')).includes('native-close-start'); n++) await delay(10)
    assert.match(await readFile(env.callsPath, 'utf8'), /native-close-start/)
    assert.equal(owner.child.exitCode, null, 'cleanup remains in its real native IO stage')
    assert.equal(JSON.parse(await readFile(join(env.dir, 'adapter/runtime.lock/owner.json'), 'utf8')).runId, owner.identity.runId, 'the live owner lock remains a generation fence')
    assert.equal(await resolvePluginOwner({ configPath: env.configPath }), null, 'the stopped endpoint is no longer publishable before process cleanup finishes')
    assert.deepEqual(await bounded(bridge.child.ended, 'withdrawn endpoint retained a bridge'), [0, null], bridge.child.errors)
  } finally { await rm(join(env.dir, 'hold-close'), { force: true }); await bounded(owner.child.ended, 'fenced owner did not finish natural cleanup') }
})

test('a failed notification produces no invented response and does not close the next request', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'notification-failure', version: '1' } })
  await writeFile(env.faultPath, JSON.stringify({ kind: 'notification-network' }))
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n')
  for (let n = 0; n < 100 && !(await readFile(env.auditPath, 'utf8')).includes('notification-fault'); n++) await delay(10)
  assert.match(await readFile(env.auditPath, 'utf8'), /notification-fault/)
  await writeFile(env.faultPath, '{}')
  assert.ok((await bridge.request('tools/list')).result.tools.length)
  assert.equal(bridge.messages.length, 2, 'the failed notification has no reply ID')
  bridge.child.stdin.end()
  assert.deepEqual(await bounded(bridge.child.ended, 'notification failure prevented EOF cleanup'), [0, null], bridge.child.errors)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'notification fixture owner remained'), [0, null], owner.child.errors)
})

test('a late old HTTP response cannot satisfy a reused client ID after lazy transport replacement', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await writeFile(join(env.dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: env.dir, producerVersion: supportedProfile.cliVersion }))
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'late-http', version: '1' } })
  const params = { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } }
  const before = await bridge.request('tools/call', params)
  await writeFile(env.faultPath, JSON.stringify({ kind: 'http-close' }))
  const failed = await bridge.request('tools/call', { ...params, name: 'sentinel_set_chat_analysis', arguments: { paused: true, expectedThreadId: 'resource-main', revision: before.result.structuredContent.monitoring.revision } }, 'reused-id')
  assert.equal(failed.error.code, -32603)
  await writeFile(env.faultPath, '{}')
  const fresh = await bridge.request('tools/call', params, 'reused-id')
  assert.equal(fresh.result.structuredContent.monitoring.paused, true)
  for (let n = 0; n < 100 && !(await readFile(env.auditPath, 'utf8')).includes('late-old-transport-response'); n++) await delay(10)
  await delay(50)
  assert.match(await readFile(env.auditPath, 'utf8'), /late-old-transport-response/)
  assert.equal(bridge.messages.filter(message => message.id === 'reused-id').length, 2, 'only the first unknown and the new request settle')
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'late HTTP fixture owner remained'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(bridge.child.ended, 'late HTTP bridge remained'), [0, null], bridge.child.errors)
})

test('cancellation maps only an active client request to its unique upstream ID', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'cancel-mapping', version: '1' } })
  await writeFile(join(env.dir, 'hold-native'), 'held')
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 'cancel-me', method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'cancel-chat' } } }) + '\n')
  for (let n = 0; n < 100 && !(await readFile(env.callsPath, 'utf8')).includes('cancel-chat'); n++) await delay(10)
  const cancel = requestId => bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId } }) + '\n')
  cancel('cancel-me'); cancel('unknown-id'); cancel(1)
  for (let n = 0; n < 100 && !(await readFile(env.auditPath, 'utf8')).includes('notifications/cancelled'); n++) await delay(10)
  const facts = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse)
  const upstream = facts.find(fact => fact.method === 'tools/call'), cancellations = facts.filter(fact => fact.method === 'notifications/cancelled')
  assert.equal(cancellations.length, 1)
  assert.equal(cancellations[0].requestId, upstream.id)
  assert.notEqual(cancellations[0].requestId, 'cancel-me')
  await rm(join(env.dir, 'hold-native'))
  assert.ok((await bridge.request('tools/list')).result.tools.length)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'cancel fixture owner remained'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(bridge.child.ended, 'cancel bridge remained'), [0, null], bridge.child.errors)
})

for (const method of ['ping', '']) test(`upstream method ${JSON.stringify(method)} retains its server ID instead of being discarded as an unmatched response`, async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'server-message', version: '1' } })
  await writeFile(env.faultPath, JSON.stringify({ kind: 'server-request', method }))
  assert.ok((await bridge.request('tools/list')).result.tools.length)
  for (let n = 0; n < 100 && !bridge.messages.some(message => message.id === 'owned-server-request'); n++) await delay(10)
  assert.deepEqual(bridge.messages.find(message => message.id === 'owned-server-request'), { jsonrpc: '2.0', id: 'owned-server-request', method })
  await writeFile(env.faultPath, '{}')
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 'owned-server-request', result: {} }) + '\n')
  for (let n = 0; n < 100 && !(await readFile(env.auditPath, 'utf8')).includes('"method":"response"'); n++) await delay(10)
  const response = (await readFile(env.auditPath, 'utf8')).trim().split('\n').map(JSON.parse).find(fact => fact.method === 'response')
  assert.equal(response.id, 'owned-server-request')
  bridge.child.stdin.end()
  assert.deepEqual(await bounded(bridge.child.ended, 'server-request bridge remained after EOF'), [0, null], bridge.child.errors)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'server-request fixture owner remained'), [0, null], owner.child.errors)
})

test('an empty client method cannot collide with a normal request upstream ID', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'empty-method', version: '1' } })
  await writeFile(join(env.dir, 'hold-native'), 'held')
  try {
    bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 'normal-client', method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'held-method-chat' } } }) + '\n')
    for (let n = 0; n < 100 && !(await readFile(env.callsPath, 'utf8')).includes('held-method-chat'); n++) await delay(10)
    assert.match(await readFile(env.callsPath, 'utf8'), /held-method-chat/)
    bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 2, method: '', params: {} }) + '\n')
    for (let n = 0; n < 100 && !bridge.messages.some(message => message.id === 2 || message.id === 'normal-client'); n++) await delay(10)
    assert.equal(bridge.messages.some(message => message.id === 'normal-client'), false, 'an unknown empty method cannot settle the still-held normal request')
    assert.equal(bridge.messages.find(message => message.id === 2)?.error?.code, -32601)
    await rm(join(env.dir, 'hold-native'))
    for (let n = 0; n < 100 && !bridge.messages.some(message => message.id === 'normal-client'); n++) await delay(10)
    assert.equal(bridge.messages.find(message => message.id === 'normal-client')?.result?.structuredContent.identity.threadId, 'held-method-chat')
  } finally {
    await rm(join(env.dir, 'hold-native'), { force: true }); await writeFile(env.statePath, 'false')
    assert.deepEqual(await bounded(owner.child.ended, 'empty method owner remained'), [0, null], owner.child.errors)
    assert.deepEqual(await bounded(bridge.child.ended, 'empty method bridge remained'), [0, null], bridge.child.errors)
  }
})

for (const kind of ['health-401', 'health-403', 'health-404', 'health-503', 'health-identity']) test(`${kind} cannot retain a bridge or leak a committed pause result`, async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge({ faults: true })
  await writeFile(join(env.dir, 'resource-main.jsonl'), resourceMaterial({ at: Date.now(), cwd: env.dir, producerVersion: supportedProfile.cliVersion }))
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'identity-failure', version: '1' } })
  const before = await bridge.request('tools/call', { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'resource-main' } })
  await writeFile(env.faultPath, JSON.stringify({ kind }))
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 'blocked-result', method: 'tools/call', params: { name: 'sentinel_set_chat_analysis', arguments: { paused: true, expectedThreadId: 'resource-main', revision: before.result.structuredContent.monitoring.revision }, _meta: { thread_id: 'resource-main' } } }) + '\n')
  assert.deepEqual(await bounded(bridge.child.ended, 'untrusted owner retained the bridge'), [0, null], bridge.child.errors)
  assert.equal(bridge.messages.some(message => message.id === 'blocked-result'), false)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId, 'only this unhealthy connection exits')
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'identity fixture owner remained'), [0, null], owner.child.errors)
})

test('real stdio MCP initialization, app tools, resource and current-chat metadata reach one independently owned observation', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  const initialized = await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'bridge-test', version: '1' } })
  assert.equal(initialized.result.serverInfo.name, 'ghost-sentinel-chatgpt')
  const tools = await bridge.request('tools/list')
  const current = tools.result.tools.find(tool => tool.name === 'sentinel_current_chat'), settings = tools.result.tools.find(tool => tool.name === 'sentinel_model_settings')
  assert.deepEqual(current._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  assert.deepEqual(settings._meta['openai/ui'].entrypoints, [{ type: 'global' }])
  const resources = await bridge.request('resources/list')
  assert.deepEqual(resources.result.resources.map(resource => resource.uri).sort(), ['ui://ghost-sentinel/current-chat.html', 'ui://ghost-sentinel/settings.html'])
  const resource = await bridge.request('resources/read', { uri: current._meta.ui.resourceUri })
  assert.match(resource.result.contents[0].text, /Sentinel/)
  const settingsResource = await bridge.request('resources/read', { uri: settings._meta.ui.resourceUri })
  assert.equal(settingsResource.result.contents[0].mimeType, 'text/html;profile=mcp-app')
  const before = await bridge.request('tools/call', { name: settings.name, arguments: {} })
  const config = { llm: { baseUrl: 'http://127.0.0.1:1234', model: 'summary-a' }, jev: { baseUrl: 'http://127.0.0.1:2345', model: 'judge-a' } }
  const saved = await bridge.request('tools/call', { name: 'sentinel_save_model_settings', arguments: { revision: before.result.structuredContent.revision, config } })
  assert.deepEqual(saved.result.structuredContent.config, config)
  assert.deepEqual((await bridge.request('tools/call', { name: settings.name, arguments: {} })).result.structuredContent.config, config)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  await writeFile(join(env.dir, 'main-from-stdio.jsonl'), JSON.stringify({ type: 'session_meta', payload: { id: 'main-from-stdio' } }) + '\n' + JSON.stringify({ type: 'event_msg', timestamp: new Date().toISOString(), payload: { type: 'task_started', turn_id: 'turn' } }) + '\n')
  const result = await bridge.request('tools/call', { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main-from-stdio', 'openai/session': 'preserved-host-context' } })
  assert.equal(result.result.structuredContent.identity.threadId, 'main-from-stdio')
  assert.match(await readFile(env.callsPath, 'utf8'), /main-from-stdio/)
  bridge.child.stdin.end()
  assert.deepEqual(await bounded(bridge.child.ended, 'EOF did not close lightweight bridge'), [0, null], bridge.child.errors)
  assert.equal(owner.child.exitCode, null, 'closing one stdio connection cannot terminate the shared observer')
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'disabled observation remained resident'), [0, null], owner.child.errors)
})

test('two simultaneous stdio connections start one independent observer, retain it on EOF and both exit after disable', async t => {
  const env = await setup(t), first = env.bridge({ launchOwner: true }), second = env.bridge({ launchOwner: true })
  await Promise.all([first, second].map(bridge => bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'two-clients', version: '1' } })))
  let identity
  await bounded((async () => { while (!(identity = await resolvePluginOwner({ configPath: env.configPath }))) await delay(10) })(), 'shared observer did not publish after the UI handshake')
  assert.ok(identity)
  assert.notEqual(identity.pid, first.child.pid); assert.notEqual(identity.pid, second.child.pid)
  const connected = await Promise.all([first, second].map(bridge => bridge.request('tools/list')))
  for (const message of connected) assert.ok(message.result.tools.some(tool => tool.name === 'sentinel_current_chat'))
  first.child.stdin.end()
  assert.deepEqual(await bounded(first.child.ended, 'first bridge did not exit on EOF'), [0, null], first.child.errors)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, identity.runId)
  const third = env.bridge()
  await third.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'third-client', version: '1' } })
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).pid, identity.pid)
  await writeFile(env.statePath, 'false')
  for (const bridge of [second, third]) assert.deepEqual(await bounded(bridge.child.ended, 'disabled bridge stayed resident'), [0, null], bridge.child.errors)
  assert.equal(await resolvePluginOwner({ configPath: env.configPath }), null)
  const pids = (await readFile(env.pidsPath, 'utf8')).trim().split('\n').map(Number)
  assert.ok(pids.includes(identity.pid))
  for (const pid of pids) {
    for (let n = 0; n < 100; n++) { try { process.kill(pid, 0); await delay(10) } catch (error) { assert.equal(error.code, 'ESRCH'); break } }
    assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' }, 'every owner contender and the active observer actually exit')
  }
})

test('concurrent panel qualification seals an in-flight stdio response and remaining connection processes exit naturally', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'late-result', version: '1' } })
  await writeFile(join(env.dir, 'hold-native'), 'held')
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 900, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'late-chat' } } }) + '\n')
  for (let n = 0; n < 200 && !(await readFile(env.callsPath, 'utf8')).includes('late-chat'); n++) await delay(10)
  assert.match(await readFile(env.callsPath, 'utf8'), /late-chat/)
  await writeFile(env.statePath, 'false')
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 901, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'qualification-check' } } }) + '\n')
  assert.deepEqual(await bounded(owner.child.ended, 'observer did not close held native IO'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(bridge.child.ended, 'bridge retained late response after disable'), [0, null], bridge.child.errors)
  assert.match(await readFile(env.callsPath, 'utf8'), /completed:late-chat/, 'external IO actually completed after qualification was revoked')
  assert.equal(bridge.messages.some(message => message.id === 900), false, 'late native material is not emitted as a tool result')
  assert.equal(await resolvePluginOwner({ configPath: env.configPath }), null)
})

test('a finite native read does not require another panel request to discover disable and reclaim observation and bridge processes', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'finite-native', version: '1' } })
  await writeFile(join(env.dir, 'finite-native-ms'), '200')
  bridge.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: 800, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'finite-chat' } } }) + '\n')
  for (let n = 0; n < 200 && !(await readFile(env.callsPath, 'utf8')).includes('finite-chat'); n++) await delay(10)
  assert.match(await readFile(env.callsPath, 'utf8'), /finite-chat/)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'observer did not reclaim after bounded native return'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(bridge.child.ended, 'bridge required a new panel read to notice owner exit'), [0, null], bridge.child.errors)
  assert.deepEqual(JSON.parse(bridge.child.errors.trim()), { status: 'closed', code: 'plugin_bridge_closed', reason: 'owner-ended' }, 'stdio stderr reports the exit reason without configuration or chat data')
  assert.equal(await resolvePluginOwner({ configPath: env.configPath }), null)
})

test('a connection from an old observation generation exits instead of adopting the re-enabled observer', async t => {
  const env = await setup(t), firstOwner = await env.owner(), oldBridge = env.bridge()
  await oldBridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'old-generation', version: '1' } })
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(firstOwner.child.ended, 'old owner did not exit'), [0, null], firstOwner.child.errors)
  await writeFile(env.statePath, 'true')
  const nextOwner = await env.owner()
  assert.notEqual(nextOwner.identity.runId, firstOwner.identity.runId)
  assert.deepEqual(await bounded(oldBridge.child.ended, 'old connection followed a new observer generation'), [0, null], oldBridge.child.errors)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, nextOwner.identity.runId)
  const nextBridge = env.bridge()
  await nextBridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'new-generation', version: '1' } })
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(nextOwner.child.ended, 'new owner did not exit'), [0, null], nextOwner.child.errors)
  assert.deepEqual(await bounded(nextBridge.child.ended, 'new connection did not exit'), [0, null], nextBridge.child.errors)
})

test('excess stdio work closes only its bounded connection and does not terminate the shared observer', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'request-budget', version: '1' } })
  bridge.child.stdin.write(Array.from({ length: 64 }, (_, index) => JSON.stringify({ jsonrpc: '2.0', id: index + 100, method: 'ping' })).join('\n') + '\n')
  assert.deepEqual(await bounded(bridge.child.ended, 'overloaded connection did not exit'), [0, null], bridge.child.errors)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  const other = env.bridge()
  await other.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'unaffected-chat', version: '1' } })
  assert.ok((await other.request('tools/list')).result.tools.length)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'shared owner did not exit'), [0, null], owner.child.errors)
  assert.deepEqual(await bounded(other.child.ended, 'peer connection did not exit'), [0, null], other.child.errors)
})

test('private configuration and owner authentication failures never launch a replacement observer', async t => {
  const env = await setup(t)
  let launches = 0
  const connect = () => startPluginBridge({ configPath: env.configPath, input: new PassThrough(), output: new PassThrough(), startOwner: async () => { launches++ } })
  await chmod(env.configPath, 0o644)
  await assert.rejects(connect(), /private configuration required/)
  await chmod(env.configPath, 0o600)
  const owner = await env.owner()
  const path = join(env.dir, 'adapter/runtime.lock/owner.json'), saved = await readFile(path, 'utf8'), record = JSON.parse(saved)
  await writeFile(path, JSON.stringify({ ...record, ownership: undefined }))
  await assert.rejects(connect(), /different process ownership/)
  await writeFile(path, JSON.stringify({ ...record, runId: '00000000-0000-0000-0000-000000000000' }))
  await assert.rejects(connect(), /untrusted helper endpoint/)
  await writeFile(path, saved)
  const config = JSON.parse(await readFile(env.configPath, 'utf8'))
  await writeFile(env.configPath, JSON.stringify({ ...config, token: '0'.repeat(64) }))
  await assert.rejects(connect(), /identity mismatch/)
  await writeFile(env.configPath, JSON.stringify(config))
  const runtimePath = join(env.dir, 'adapter/runtime.json'), runtimeText = await readFile(runtimePath, 'utf8'), runtime = JSON.parse(runtimeText)
  const spoof = createServer((_req, res) => res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ ...runtime, runId: '00000000-0000-0000-0000-000000000000' })))
  await new Promise(resolve => spoof.listen(0, '127.0.0.1', resolve))
  try {
    await writeFile(env.configPath, JSON.stringify({ ...config, port: spoof.address().port }))
    await writeFile(runtimePath, JSON.stringify({ ...runtime, url: 'http://127.0.0.1:' + spoof.address().port }))
    await assert.rejects(connect(), /identity mismatch/, 'a healthy unrelated HTTP endpoint cannot be adopted')
  } finally {
    await writeFile(env.configPath, JSON.stringify(config)); await writeFile(runtimePath, runtimeText)
    spoof.closeAllConnections(); await new Promise(resolve => spoof.close(resolve))
  }
  assert.equal(launches, 0)
  assert.equal((await resolvePluginOwner({ configPath: env.configPath })).runId, owner.identity.runId)
  await writeFile(env.statePath, 'false')
  assert.deepEqual(await bounded(owner.child.ended, 'authenticated owner did not exit'), [0, null], owner.child.errors)
})
