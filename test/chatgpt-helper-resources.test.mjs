import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Agent, request } from 'node:http'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('one production helper serves 20 panel clients and reclaims 100 plugin cycles while DSH remains usable', { timeout: 30000 }, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-resources-')), agents = []
  let server, core, runtime
  t.after(async () => { agents.forEach(agent => agent.destroy()); await runtime?.close(); await core?.close(); await server?.close(); await rm(dir, { recursive: true, force: true }) })
  server = await startServer({ dataDir: join(dir, 'core'), leaseMs: 60000 })
  core = await connectCore({ dataDir: join(dir, 'core') })
  const peer = await core.registerSource({ registrationId: 'dsh-resource-peer', hostRunId: 'dsh-host', name: 'DSH retained peer' })
  await core.ingest(peer, { tasks: [{ id: 'peer-task', lifecycleId: 'peer-turn', title: 'DSH retained task', status: 'running' }] })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'controlled.sock'), pluginId: 'ghost-sentinel@resources' })
  const started = Date.now() - 10000, path = join(dir, 'main.jsonl')
  const line = (type, payload, index) => JSON.stringify({ type, timestamp: new Date(started + 2000 + index).toISOString(), payload }) + '\n'
  await writeFile(path, line('session_meta', { id: 'resource-main', cli_version: supportedProfile.cliVersion }, 0)
    + line('event_msg', { type: 'task_started', turn_id: 'resource-turn', turn_attribution: { turn_id: 'resource-turn', turn_trigger: 'composer' } }, 1)
    + line('event_msg', { type: 'item_completed', thread_id: 'resource-main', turn_id: 'resource-turn', item: { type: 'UserMessage', id: 'resource-input', content: [{ type: 'text', text: 'Observe this resource fixture only' }] } }, 2))
  const thread = { id: 'resource-main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', cwd: dir, path }
  let enabled = true, metadataDisconnects = 0, closed = 0
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'controlled-host', hostStartedAt: started, profile: supportedProfile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async listThreads({ archived }) { return { data: archived ? [] : [thread], nextCursor: null } },
    async readThread(id) { return id === thread.id ? { thread, anchorTurnId: 'resource-turn' } : null },
    async disconnectMetadata() { metadataDisconnects++ },
    async close() { closed++ },
  }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8')), url = runtime.url, runId = runtime.runId, token = config.token
  let sequence = 0
  const list = agent => new Promise((resolve, reject) => {
    const req = request(url + '/mcp', { method: 'POST', agent, headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' } }, res => {
      let bytes = ''
      res.on('data', part => { bytes += part }); res.once('error', reject)
      res.once('end', () => { try { assert.equal(res.statusCode, 200); const value = JSON.parse(bytes); assert.ok(value.result.tools.some(tool => tool.name === 'sentinel_current_chat')); resolve() } catch (error) { reject(error) } })
    })
    req.setTimeout(5000, () => req.destroy(new Error('owned panel client deadline'))); req.once('error', reject)
    req.end(JSON.stringify({ jsonrpc: '2.0', id: ++sequence, method: 'tools/list', params: {} }))
  })
  for (let index = 0; index < 20; index++) agents.push(new Agent({ keepAlive: true, maxSockets: 1, maxFreeSockets: 1 }))
  await Promise.all(agents.map(list))
  assert.equal(runtime.diagnostics().sockets, 20)
  for (let cycle = 0; cycle < 100; cycle++) {
    enabled = true; await runtime.refresh()
    const active = (await core.snapshot()).sources.find(source => source.descriptor.registrationId === 'chatgpt:' + config.installationId)
    assert.equal(active.status, 'connected')
    assert.ok(runtime.diagnostics().openReaders > 0, 'each enabled cycle must exercise real file readers')
    assert.equal((await core.snapshot()).tasks.find(task => task.sourceKey === active.key).currentRequest, 'Observe this resource fixture only')
    enabled = false; await runtime.refresh()
    const snapshot = await core.snapshot(), own = snapshot.sources.find(source => source.key === active.key)
    assert.equal(own.status, 'disconnected')
    assert.equal(snapshot.sources.find(source => source.key === peer).status, 'connected')
    assert.equal(snapshot.tasks.find(task => task.sourceKey === peer).status, 'running')
    await core.heartbeat(peer)
    if (cycle % 10 === 0) await Promise.all(agents.map(list))
    const counts = runtime.diagnostics()
    assert.equal(counts.openReaders, 0); assert.equal(counts.pendingOperations, 0); assert.equal(counts.queuedBatches, 0)
    assert.equal(counts.activeRequests, 0); assert.equal(counts.transports, 0); assert.equal(counts.refreshTimers, 0)
    assert.equal(counts.trackedThreads, 1)
    assert.equal(runtime.url, url); assert.equal(runtime.runId, runId)
  }
  assert.equal(metadataDisconnects, 100)
  assert.equal((await core.snapshot()).sources.length, 2, 'plugin cycles must not accumulate source generations')
  agents.forEach(agent => agent.destroy())
  await runtime.close()
  const counts = runtime.diagnostics()
  assert.equal(closed, 1); assert.equal(counts.sockets, 0); assert.equal(counts.transports, 0)
  assert.equal(counts.trackedThreads, 0); assert.equal(counts.healthOutbox.pending, 0)
  assert.equal((await core.health()).instanceId, server.instanceId)
  assert.equal((await core.snapshot()).sources.find(source => source.key === peer).status, 'connected')
  assert.equal(core.transportDiagnostics().pendingRequests, 0); assert.equal(core.transportDiagnostics().deadlineTimers, 0)
})
