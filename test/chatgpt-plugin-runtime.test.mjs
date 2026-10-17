import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, readdir, rm, stat } from 'node:fs/promises'
import { createServer } from 'node:http'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { prepareInstallation, startFromInstallation, resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { notificationPrefix } from '../chatgpt/native-notifications.mjs'

async function completed(promise) {
  const controller = new AbortController()
  try { return await Promise.race([promise, delay(2000, null, { signal: controller.signal }).then(() => assert.fail('plugin-owned runtime did not finish cleanup'))]) }
  finally { controller.abort() }
}

test('plugin disable closes the observation runtime while retaining its installation and the DSH shared Core client', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-runtime-'))
  const server = await startServer({ dataDir: join(dir, 'core') }), dsh = await connectCore({ dataDir: join(dir, 'core') })
  let runtime
  t.after(async () => { await runtime?.close(); await dsh.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const dshSource = await dsh.registerSource({ registrationId: 'dsh-owned', hostRunId: 'dsh-host', name: 'DSH' })
  const instanceId = (await dsh.health()).instanceId
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@plugin-runtime' })
  let enabled = true, nativeClosed = 0
  const hostStartedAt = Date.now() - 1000
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'plugin-host', hostStartedAt, profile: supportedProfile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads() { return { data: [], nextCursor: null } },
    async close() { nativeClosed++ },
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  const before = await dsh.snapshot(), source = before.sources.find(value => value.descriptor.registrationId !== 'dsh-owned')
  assert.ok(source, 'collection starts without opening a panel')
  enabled = false
  await runtime.refresh()
  assert.deepEqual(await completed(runtime.closed), { reason: 'plugin-disabled' })
  assert.equal(nativeClosed, 1)
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(dir, 'adapter/runtime.json')), { code: 'ENOENT' })
  assert.equal(JSON.parse(await readFile(prepared.configPath, 'utf8')).installationId, runtime.installationId)
  const after = await dsh.snapshot()
  assert.equal(after.sources.find(value => value.key === source.key).status, 'disconnected')
  assert.equal(after.sources.find(value => value.key === dshSource).status, 'connected')
  await dsh.heartbeat(dshSource)
  assert.equal((await dsh.health()).instanceId, instanceId)
})

test('20 plugin connections and 100 reuse cycles retain one owner until plugin disable', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-connections-')); let owner
  t.after(async () => { await owner?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@connections' })
  let enabled = true
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } }, async close() {} }
  owner = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  const connections = await Promise.all(Array.from({ length: 20 }, () => startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })))
  for (const connection of connections) { assert.equal(connection.reused, true); assert.equal(connection.runId, owner.runId); await connection.close() }
  for (let cycle = 0; cycle < 100; cycle++) {
    const connection = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
    assert.equal(connection.runId, owner.runId); await connection.close()
  }
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const health = await fetch(owner.url + '/health', { headers: { authorization: 'Bearer ' + config.token } }).then(value => value.json())
  assert.equal(health.runId, owner.runId); assert.equal(health.ownership, 'plugin')
  assert.equal(owner.diagnostics().closed, false)
  assert.equal(owner.diagnostics().pendingOperations, 0)
  assert.equal(owner.diagnostics().transports, 0)
  assert.equal(owner.diagnostics().activeRequests, 0)
  assert.equal(owner.diagnostics().trackedThreads, 0)
  assert.equal(owner.diagnostics().openReaders, 0)
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 }), /different process ownership/)
  await assert.rejects(stat(join(prepared.pluginDir, '.mcp.json')), { code: 'ENOENT' })
  await assert.rejects(stat(join(prepared.pluginDir, '.codex-plugin/plugin.json')), { code: 'ENOENT' })
  enabled = false; await owner.refresh()
  assert.deepEqual(await completed(owner.closed), { reason: 'plugin-disabled' })
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
})

test('plugin-owned startup refuses a live helper standby instead of adopting its process', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-legacy-owner-')); let runtime
  t.after(async () => { await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@legacy-owner' })
  const native = { async sampleHost() { return { available: false } }, async close() {} }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 })
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' }), /different process ownership/)
  assert.equal(runtime.diagnostics().closed, false)
  assert.equal(JSON.parse(await readFile(join(dir, 'adapter/runtime.lock/owner.json'), 'utf8')).runId, runtime.runId)
})

for (const scenario of ['plugin-unknown', 'plugin-read-failed', 'host-lost']) test(`plugin-owned observation exits when global qualification is ${scenario}`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-qualification-')); let runtime, lost = false
  t.after(async () => { await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@qualification' })
  const native = {
    async sampleHost() { return lost && scenario === 'host-lost' ? { available: false, reason: 'execution-source-unavailable' } : { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { if (lost && scenario === 'plugin-read-failed') throw new Error('owned enablement read failure'); return { user: lost ? 'unknown' : 'enabled', effective: lost ? 'unknown' : 'enabled' } },
    async close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  lost = true
  await runtime.refresh()
  assert.deepEqual(await completed(runtime.closed), { reason: scenario === 'host-lost' ? 'execution-source-unavailable' : 'plugin-enablement-unconfirmed' })
  assert.equal(runtime.diagnostics().refreshTimers, 0)
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(dir, 'core')), { code: 'ENOENT' })
})

test('idle discovery and a failed chat read keep plugin observation alive after panel requests disconnect', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-idle-')), server = await startServer({ dataDir: join(dir, 'core') }); let runtime
  t.after(async () => { await runtime?.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@idle' })
  let directoryCalls = 0, directoryFails = false
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async listThreads() { directoryCalls++; if (directoryFails) throw new Error('owned directory failure'); return { data: [], nextCursor: null } },
    async readThread() { throw new Error('owned single chat failure') }, async close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 20, ownership: 'plugin' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'unavailable-chat' } } }) })
  assert.equal(response.status, 200); await response.json()
  const before = directoryCalls
  for (let n = 0; directoryCalls <= before && n < 100; n++) await delay(5)
  assert.ok(directoryCalls > before, 'discovery continues without any active panel requests or running chats')
  directoryFails = true; await runtime.refresh()
  assert.equal(runtime.diagnostics().closed, false)
  assert.ok(runtime.diagnostics().refreshTimers || runtime.diagnostics().pendingOperations)
  assert.equal((await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token } })).status, 200)
})

test('qualification loss aborts in-flight analysis, seals late output and retains history, cursor and manual pause', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-inflight-')), dataDir = join(dir, 'adapter')
  let runtime, modelReached, heldResponse, modelRequests = 0, modelConnectionClosed = false
  const reached = new Promise(resolve => { modelReached = resolve })
  const model = createServer(async (req, res) => { for await (const chunk of req) void chunk; modelRequests++; heldResponse = res; res.once('close', () => { modelConnectionClosed = true }); modelReached() })
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve))
  const server = await startServer({ dataDir: join(dir, 'core') }), client = await connectCore({ dataDir: join(dir, 'core') })
  t.after(async () => { await runtime?.close(); await client.close(); await server.close(); model.closeAllConnections(); await new Promise(resolve => model.close(resolve)); await rm(dir, { recursive: true, force: true }) })
  const baseUrl = 'http://127.0.0.1:' + model.address().port
  await client.configureAnalysis({ llm: { baseUrl, model: 'owned-extractor' }, jev: { baseUrl, model: 'owned-judge' } })
  const prepared = await prepareInstallation({ dataDir, coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@inflight' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8')), hostStartedAt = Date.now() - 10000, at = Date.now() - 1000, path = join(dir, 'main.jsonl')
  const line = (type, payload, offset = 0) => JSON.stringify({ type, timestamp: new Date(at + offset).toISOString(), payload }) + '\n'
  await writeFile(path, line('session_meta', { id: 'main', cli_version: supportedProfile.cliVersion, originator: 'Codex Desktop', source: 'vscode', cwd: dir }) + line('event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + line('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn', item: { type: 'UserMessage', id: 'message', content: [{ type: 'text', text: 'Read the requested log only' }] } }, 1)
    + line('response_item', { type: 'message', role: 'assistant', internal_chat_message_metadata_passthrough: { turn_id: 'turn' }, content: [{ type: 'output_text', text: 'Reading the requested log' }] }, 2))
  const thread = { id: 'main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }
  let enabled = true
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads() { return { data: [thread], nextCursor: null } }, async readThread() { return { thread, anchorTurnId: 'turn' } }, async close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  await completed(reached)
  const before = await client.snapshot(), task = before.tasks.find(task => task.id === 'main')
  assert.equal(task.currentRequest, 'Read the requested log only')
  assert.equal(task.analysis.state, 'analyzing')
  const cursors = await readdir(join(dataDir, 'cursors')), cursor = await readFile(join(dataDir, 'cursors', cursors[0]), 'utf8')
  await client.ingest(task.sourceKey, { tasks: [{ id: 'paused-peer', lifecycleId: 'peer-turn', title: 'Paused peer', status: 'running' }] })
  const pauseScope = { registrationId: 'chatgpt:' + config.installationId, taskId: 'paused-peer' }
  const setting = await client.conversationMonitoring(pauseScope)
  const paused = await client.setConversationMonitoring({ ...pauseScope, paused: true, revision: setting.revision })
  assert.equal((await client.snapshot()).tasks.find(value => value.key === task.key).analysis.state, 'analyzing', 'a different chat pause does not cancel this analysis')
  enabled = false; await runtime.refresh(); await completed(runtime.closed)
  for (let n = 0; !modelConnectionClosed && n < 100; n++) await delay(2)
  assert.equal(modelConnectionClosed, true, 'qualification revocation cancels the actual outstanding model HTTP request')
  heldResponse.end(JSON.stringify({ choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
  const after = await client.snapshot(), retained = after.tasks.find(value => value.key === task.key)
  assert.equal(after.sources.find(value => value.key === task.sourceKey).status, 'disconnected')
  assert.deepEqual(retained.events, task.events)
  assert.equal(retained.analysis.completedAt, null)
  assert.equal(modelRequests, 1, 'late extractor output does not start a JEV request')
  assert.deepEqual(await client.conversationMonitoring(pauseScope), paused)
  assert.equal(await readFile(join(dataDir, 'cursors', cursors[0]), 'utf8'), cursor)
})

test('plugin cleanup reports a native close failure but still closes its panel, outbox and owner records', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-cleanup-error-')); let runtime
  t.after(async () => { await runtime?.close().catch(() => {}); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@cleanup-error' })
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() { throw new Error('owned native close failed') } }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await assert.rejects(runtime.close(), /owned native close failed/)
  await assert.rejects(runtime.closed, /owned native close failed/)
  await assert.rejects(fetch(runtime.url + '/health', { signal: AbortSignal.timeout(1000) }))
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  assert.equal(runtime.diagnostics().transports, 0)
  assert.equal(runtime.diagnostics().healthOutbox.closed, true)
})

test('a pending chat metadata read is cancelled on disable and cannot re-register a late source', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-metadata-cancel-')), server = await startServer({ dataDir: join(dir, 'core') })
  const client = await connectCore({ dataDir: join(dir, 'core') }); let runtime, enabled = true, hold = false, release, entered
  const reached = new Promise(resolve => { entered = resolve }), path = join(dir, 'main.jsonl'), hostStartedAt = Date.now() - 10000
  await writeFile(path, JSON.stringify({ type: 'session_meta', payload: { id: 'main', cli_version: supportedProfile.cliVersion, originator: 'Codex Desktop', source: 'vscode', cwd: dir } }) + '\n' + JSON.stringify({ type: 'event_msg', timestamp: new Date().toISOString(), payload: { type: 'task_started', turn_id: 'turn' } }) + '\n')
  t.after(async () => { release?.(); await runtime?.close(); await client.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@metadata-cancel' })
  const thread = { id: 'main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } },
    async readEnablement(cwd) { return { user: cwd !== null || enabled ? 'enabled' : 'disabled', effective: cwd !== null || enabled ? 'enabled' : 'disabled' } },
    async readThread() { if (hold) { entered(); await new Promise(resolve => { release = resolve }) }; return { thread, anchorTurnId: 'turn' } },
    async close() { release?.() },
  }
  let registrations = 0
  const originalFetch = globalThis.fetch
  t.mock.method(globalThis, 'fetch', (url, ...args) => { if (String(url) === server.url + '/v1/source/register') registrations++; return originalFetch(url, ...args) })
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const read = () => fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, signal: AbortSignal.timeout(2000), body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) }).then(value => value.json())
  await read()
  const before = await client.snapshot()
  assert.equal(registrations, 1)
  hold = true
  const pending = read().catch(() => null)
  await completed(reached)
  enabled = false
  await read().catch(() => null)
  await completed(runtime.closed); await pending
  assert.equal(registrations, 1, 'a late chat read must not briefly reconnect its revoked source')
  const after = await client.snapshot()
  assert.equal(after.sources.find(value => value.key === before.sources[0].key).status, 'disconnected')
  assert.deepEqual(after.tasks[0].events, before.tasks[0].events)
})

for (const globalLoss of ['disabled', 'unknown']) test(`a project-only enablement read failure isolates its chat while global ${globalLoss} still closes the owner`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-project-gap-')), server = await startServer({ dataDir: join(dir, 'core') })
  const client = await connectCore({ dataDir: join(dir, 'core') }); let runtime, failProject = false, globalSetting = 'enabled'
  t.after(async () => { await runtime?.close(); await client.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const hostStartedAt = Date.now() - 10000, at = Date.now() - 1000
  const threads = ['a', 'b'].map(id => ({ id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(dir, id + '.jsonl'), cwd: join(dir, 'project-' + id) }))
  for (const thread of threads) await writeFile(thread.path, JSON.stringify({ type: 'session_meta', payload: { id: thread.id, cli_version: supportedProfile.cliVersion, originator: 'Codex Desktop', source: 'vscode', cwd: thread.cwd } }) + '\n' + JSON.stringify({ type: 'event_msg', timestamp: new Date(at).toISOString(), payload: { type: 'task_started', turn_id: 'turn-' + thread.id } }) + '\n')
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@project-gap' })
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } },
    async readEnablement(cwd) { if (cwd === threads[0].cwd && failProject) throw new Error('owned project RPC failure'); return { user: globalSetting, effective: globalSetting } },
    async listThreads() { return { data: threads, nextCursor: null } }, async readThread(id) { return { thread: threads.find(thread => thread.id === id), anchorTurnId: 'turn-' + id } }, async close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  assert.equal((await client.snapshot()).tasks.filter(task => task.status === 'running').length, 2)
  failProject = true; await runtime.refresh()
  assert.equal(runtime.diagnostics().closed, false, 'a failed project config RPC is not proof that the installation lost global eligibility')
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const read = async id => {
    const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) })
    return (await response.json()).result.structuredContent
  }
  assert.equal((await read('a')).observation.gap, 'plugin-enablement-unconfirmed')
  assert.equal((await read('b')).identity.threadId, 'b')
  const partial = await client.snapshot()
  assert.equal(partial.sources[0].status, 'connected')
  assert.equal(partial.tasks.find(task => task.id === 'a').status, 'unknown')
  assert.equal(partial.tasks.find(task => task.id === 'b').status, 'running')
  failProject = false; await runtime.refresh()
  assert.notEqual((await read('a')).observation.gap, 'plugin-enablement-unconfirmed')
  globalSetting = globalLoss; await runtime.refresh()
  assert.deepEqual(await completed(runtime.closed), { reason: globalLoss === 'disabled' ? 'plugin-disabled' : 'plugin-enablement-unconfirmed' })
  assert.ok((await client.snapshot()).sources.every(source => source.status === 'disconnected'))
})

for (const mode of ['snapshot-failed', 'cancel-failed', 'pending-remains', 'history-gap-only']) test(`plugin cleanup reports notification ${mode} accurately while releasing its other resources`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-notification-cleanup-')), server = await startServer({ dataDir: join(dir, 'core') })
  const client = await connectCore({ dataDir: join(dir, 'core') }); let runtime, stopping = false, nativeClosed = 0, notifierClosed = 0, settingsCalls = 0
  t.after(async () => { await runtime?.close().catch(() => {}); await client.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@notification-cleanup' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8')), requestId = notificationPrefix(config.installationId) + 'owned-pending'
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [], nextCursor: null } }, async close() { nativeClosed++ } }
  const notificationIO = {
    async settings() { settingsCalls++; return { authorization: 'denied', observedAt: Date.now() } },
    async snapshot() {
      if (stopping && mode === 'snapshot-failed') throw Object.assign(new Error('owned notification snapshot failure'), { code: 'native_unavailable' })
      return { pending: stopping && mode !== 'history-gap-only' ? [{ requestId }] : [], delivered: [], actions: [], observedAt: Date.now(), ...(mode === 'history-gap-only' ? { historyGap: true, deliveredHasMore: true } : {}) }
    },
    async cancel() { if (mode === 'cancel-failed') throw Object.assign(new Error('owned notification cancellation failure'), { code: 'native_unavailable' }); return { pendingIds: [requestId], observedAt: Date.now() } },
    cancelPendingOperations() { stopping = true }, close() { notifierClosed++ },
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, notificationIO, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  for (let n = 0; !settingsCalls && n < 100; n++) await delay(2)
  assert.equal(settingsCalls, 1)
  if (mode === 'history-gap-only') assert.deepEqual(await runtime.close(), { reason: 'requested' })
  else {
    await assert.rejects(runtime.close(), error => error.code === 'notification_cleanup_failed')
    await assert.rejects(runtime.closed, error => error.code === 'notification_cleanup_failed')
  }
  assert.equal(nativeClosed, 1); assert.equal(notifierClosed, 1)
  await assert.rejects(fetch(runtime.url + '/health', { signal: AbortSignal.timeout(1000) }))
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
  await assert.rejects(stat(join(dir, 'adapter/runtime.json')), { code: 'ENOENT' })
  assert.equal(runtime.diagnostics().healthOutbox.closed, true)
  assert.ok((await client.snapshot()).sources.every(source => source.status === 'disconnected'))
})

test('a real stdio installation starts and resolves one authenticated plugin owner using its private saved HTTP port', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-stdio-installation-')), server = await startServer({ dataDir: join(dir, 'core') })
  const client = await connectCore({ dataDir: join(dir, 'core') }); let runtime
  t.after(async () => { await runtime?.close(); await client.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@stdio-installation' })
  const manifestPath = join(prepared.pluginDir, '.codex-plugin/plugin.json'), connectionPath = join(prepared.pluginDir, '.mcp.json')
  const manifest = JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查' } })
  const connection = JSON.stringify({ mcpServers: { sentinel: { command: process.execPath, args: [new URL('../chatgpt/plugin-bridge.mjs', import.meta.url).pathname, '--config', prepared.configPath], cwd: new URL('../', import.meta.url).pathname } } })
  await writeFile(manifestPath, manifest, { mode: 0o600 }); await writeFile(connectionPath, connection, { mode: 0o600 })
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [], nextCursor: null } }, async close() {} }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh()
  assert.equal((await client.snapshot()).sources[0].status, 'connected', 'the installed stdio definition does not prevent complete observation startup')
  const resolved = await resolvePluginOwner({ configPath: prepared.configPath })
  assert.equal(resolved.runId, runtime.runId); assert.equal(resolved.ownership, 'plugin')
  const reused = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  assert.equal(reused.reused, true); assert.equal(reused.runId, runtime.runId); await reused.close()
  assert.equal(await readFile(manifestPath, 'utf8'), manifest)
  assert.equal(await readFile(connectionPath, 'utf8'), connection)
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8')), port = Number(new URL(runtime.url).port)
  assert.equal(config.port, port)
  try {
    await writeFile(prepared.configPath, JSON.stringify({ ...config, port: port === 65535 ? port - 1 : port + 1 }), { mode: 0o600 })
    await assert.rejects(resolvePluginOwner({ configPath: prepared.configPath }), /untrusted helper endpoint/)
    await assert.rejects(startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' }), /untrusted helper endpoint/)
  } finally { await writeFile(prepared.configPath, JSON.stringify(config), { mode: 0o600 }) }
  const first = runtime
  await first.close()
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0, ownership: 'plugin' })
  assert.equal(runtime.url, first.url); assert.notEqual(runtime.runId, first.runId)
  assert.equal((await resolvePluginOwner({ configPath: prepared.configPath })).runId, runtime.runId)
  await runtime.close()
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 }), /untrusted helper endpoint/)
})

for (const loss of ['disabled', 'unknown', 'enablement-failed', 'host-lost', 'generation-changed', 'identity-unconfirmed', 'start-time-unconfirmed']) test(`an unsupported material profile keeps one gray observer until ${loss} revokes global eligibility`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-unsupported-')); let runtime, changed = false, enablementReads = 0, materialReads = 0
  t.after(async () => { await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@unsupported' })
  const native = {
    async sampleHost() {
      if (changed && loss === 'host-lost') return { available: false, reason: 'execution-source-unavailable' }
      return { available: true, hostRunId: changed && loss === 'identity-unconfirmed' ? null : changed && loss === 'generation-changed' ? 'different-host' : 'host', hostStartedAt: changed && loss === 'start-time-unconfirmed' ? null : 1, profile: { appVersion: '26.1013.31256', appBuild: '20587', cliVersion: '0.163.0-alpha.10.2' } }
    },
    async readEnablement(cwd) { enablementReads++; assert.equal(cwd, null); if (changed && loss === 'enablement-failed') throw new Error('owned global configuration unavailable'); return { user: changed && ['disabled', 'unknown'].includes(loss) ? loss : 'enabled', effective: changed && ['disabled', 'unknown'].includes(loss) ? loss : 'enabled' } },
    async listThreads() { materialReads++; assert.fail('an unsupported material profile cannot read the chat directory') },
    async readThread() { materialReads++; assert.fail('an unsupported material profile cannot read a chat record') }, async close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 20, ownership: 'plugin' })
  await runtime.refresh()
  assert.equal(runtime.diagnostics().closed, false)
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const health = await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token } }).then(value => value.json())
  assert.equal(health.runId, runtime.runId); assert.equal(health.ownership, 'plugin')
  const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'unverified-chat' } } }) })
  const view = (await response.json()).result.structuredContent
  assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-host-version')
  assert.match(view.summary, /版本尚未适配.*材料采集已停止/)
  assert.equal(view.identity.threadId, null, 'unsupported material does not claim a verified current chat')
  const reads = enablementReads
  for (let n = 0; enablementReads <= reads && n < 100; n++) await delay(5)
  assert.ok(enablementReads > reads, 'safe global eligibility checks continue with the same observer')
  assert.equal(materialReads, 0)
  await assert.rejects(stat(join(dir, 'core')), { code: 'ENOENT' })
  changed = true; await runtime.refresh()
  assert.deepEqual(await completed(runtime.closed), { reason: loss === 'disabled' ? 'plugin-disabled' : ['unknown', 'enablement-failed'].includes(loss) ? 'plugin-enablement-unconfirmed' : loss === 'generation-changed' ? 'source-generation-changed' : ['identity-unconfirmed', 'start-time-unconfirmed'].includes(loss) ? 'source-generation-unverified' : 'execution-source-unavailable' })
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
})

for (const notificationFailure of [false, true]) test(`an unsupported profile revokes an existing source and in-flight analysis while preserving its gray owner${notificationFailure ? ' and notification cleanup error' : ''}`, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-plugin-profile-change-')), dataDir = join(dir, 'adapter')
  let runtime, profile = supportedProfile, enabled = true, modelReached, modelConnectionClosed = false, heldResponse, modelRequests = 0, materialReads = 0, notifierCancellations = 0, failNotifications = false
  const reached = new Promise(resolve => { modelReached = resolve })
  const model = createServer(async (req, res) => { for await (const chunk of req) void chunk; modelRequests++; heldResponse = res; res.once('close', () => { modelConnectionClosed = true }); modelReached() })
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve))
  const server = await startServer({ dataDir: join(dir, 'core') }), client = await connectCore({ dataDir: join(dir, 'core') })
  t.after(async () => { failNotifications = false; await runtime?.close(); await client.close(); await server.close(); model.closeAllConnections(); await new Promise(resolve => model.close(resolve)); await rm(dir, { recursive: true, force: true }) })
  const baseUrl = 'http://127.0.0.1:' + model.address().port
  await client.configureAnalysis({ llm: { baseUrl, model: 'owned-extractor' }, jev: { baseUrl, model: 'owned-judge' } })
  const prepared = await prepareInstallation({ dataDir, coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'sentinel@profile-change' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8')), hostStartedAt = Date.now() - 10000, at = Date.now() - 1000, path = join(dir, 'main.jsonl')
  const line = (type, payload, offset = 0) => JSON.stringify({ type, timestamp: new Date(at + offset).toISOString(), payload }) + '\n'
  await writeFile(path, line('session_meta', { id: 'main', cli_version: supportedProfile.cliVersion, originator: 'Codex Desktop', source: 'vscode', cwd: dir }) + line('event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + line('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn', item: { type: 'UserMessage', id: 'message', content: [{ type: 'text', text: 'Read the requested log only' }] } }, 1)
    + line('response_item', { type: 'message', role: 'assistant', internal_chat_message_metadata_passthrough: { turn_id: 'turn' }, content: [{ type: 'output_text', text: 'Reading the requested log' }] }, 2))
  const thread = { id: 'main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads() { materialReads++; return { data: [thread], nextCursor: null } }, async readThread() { materialReads++; return { thread, anchorTurnId: 'turn' } }, async close() {},
  }
  const notificationIO = {
    async settings() { return { authorization: 'denied', observedAt: Date.now() } },
    async snapshot() { if (failNotifications) throw Object.assign(new Error('owned notification cleanup unavailable'), { code: 'native_unavailable' }); return { pending: [], delivered: [], actions: [], observedAt: Date.now() } },
    async cancel() { return { pendingIds: [], observedAt: Date.now() } }, cancelPendingOperations() { notifierCancellations++ }, close() {},
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, notificationIO, pollMs: 0, ownership: 'plugin' })
  await runtime.refresh(); await completed(reached)
  const before = await client.snapshot(), task = before.tasks.find(task => task.id === 'main'), readCount = materialReads
  assert.equal(task.analysis.state, 'analyzing')
  failNotifications = notificationFailure
  profile = { appVersion: '26.1013.31256', appBuild: '20587', cliVersion: '0.163.0-alpha.10.2' }
  await runtime.refresh()
  assert.equal(runtime.diagnostics().closed, false); assert.equal(runtime.diagnostics().openReaders, 0)
  for (let n = 0; !modelConnectionClosed && n < 100; n++) await delay(2)
  assert.equal(modelConnectionClosed, true)
  heldResponse.end(JSON.stringify({ choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
  const read = async () => {
    const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) })
    return (await response.json()).result.structuredContent
  }
  let view = await read()
  if (notificationFailure) for (let n = 0; view.notifications.errorCode !== 'native_unavailable' && n < 100; n++) { await delay(2); view = await read() }
  assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-host-version')
  assert.match(view.summary, /版本尚未适配.*材料采集已停止/)
  assert.equal(view.notifications.errorCode, notificationFailure ? 'native_unavailable' : null)
  assert.ok(notifierCancellations > 0)
  assert.equal(materialReads, readCount, 'unsupported material never reaches directory or record metadata IO')
  assert.equal((await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token } }).then(response => response.json())).runId, runtime.runId)
  const after = await client.snapshot(), retained = after.tasks.find(value => value.key === task.key)
  assert.equal(after.sources.find(value => value.key === task.sourceKey).status, 'disconnected')
  assert.deepEqual(retained.events, task.events); assert.equal(retained.analysis.completedAt, null); assert.equal(modelRequests, 1)
  failNotifications = false; enabled = false; await runtime.refresh()
  assert.deepEqual(await completed(runtime.closed), { reason: 'plugin-disabled' })
})
