import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, writeFile, readFile, rm, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { prepareInstallation, resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { startServer } from '../src/server.mjs'
import { startPluginBridge } from '../chatgpt/plugin-bridge.mjs'
import { PassThrough } from 'node:stream'
import { createServer } from 'node:http'

async function bounded(promise, label) {
  const controller = new AbortController()
  try { return await Promise.race([promise, delay(5000, null, { signal: controller.signal }).then(() => assert.fail(label))]) }
  finally { controller.abort() }
}

async function setup(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-bridge-')), core = await startServer({ dataDir: join(dir, 'core') })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@bridge' })
  const statePath = join(dir, 'enabled.json'), callsPath = join(dir, 'calls.txt'), pidsPath = join(dir, 'owner-pids.txt'), children = []
  await writeFile(statePath, 'true'); await writeFile(callsPath, ''); await writeFile(pidsPath, '')
  t.after(async () => {
    await writeFile(statePath, 'false')
    for (const child of children) if (child.exitCode === null && child.signalCode === null) { const ended = once(child, 'close'); child.kill('SIGTERM'); await bounded(ended, 'test child cleanup failed') }
    await core.close(); await rm(dir, { recursive: true, force: true })
  })
  function track(child) { children.push(child); child.ended = once(child, 'close'); child.errors = ''; child.stderr.on('data', bytes => { child.errors += bytes }); return child }
  async function owner() {
    const child = track(spawn(process.execPath, [new URL('./fixtures/chatgpt-plugin-bridge/owner.mjs', import.meta.url).pathname, prepared.configPath, statePath, callsPath], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] }))
    const [identity] = await bounded(once(child, 'message'), 'owner did not start: ' + child.errors)
    return { child, identity }
  }
  function bridge({ launchOwner = false } = {}) {
    const args = launchOwner ? [new URL('./fixtures/chatgpt-plugin-bridge/bridge.mjs', import.meta.url).pathname, prepared.configPath, statePath, callsPath, pidsPath] : [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', prepared.configPath]
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
    function request(method, params = {}) {
      const requestId = ++id, response = new Promise(resolve => { pending.set(requestId, resolve) })
      child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params }) + '\n')
      return bounded(response, 'bridge request failed ' + method + ': ' + child.errors)
    }
    return { child, request, messages }
  }
  return { ...prepared, dir, statePath, callsPath, pidsPath, owner, bridge }
}

test('real stdio MCP initialization, app tools, resource and current-chat metadata reach one independently owned observation', async t => {
  const env = await setup(t), owner = await env.owner(), bridge = env.bridge()
  const initialized = await bridge.request('initialize', { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'bridge-test', version: '1' } })
  assert.equal(initialized.result.serverInfo.name, 'ghost-sentinel-chatgpt')
  const tools = await bridge.request('tools/list')
  const current = tools.result.tools.find(tool => tool.name === 'sentinel_current_chat')
  assert.deepEqual(current._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  const resources = await bridge.request('resources/list')
  const resource = await bridge.request('resources/read', { uri: resources.result.resources[0].uri })
  assert.match(resource.result.contents[0].text, /Sentinel/)
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
  const identity = await resolvePluginOwner({ configPath: env.configPath })
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
