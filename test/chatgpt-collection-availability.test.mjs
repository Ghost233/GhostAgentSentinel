import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile, appendFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createLazyCore } from '../chatgpt/lazy-core.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { candidateHostProfile } from '../chatgpt/storage-compatibility.mjs'

test('an unavailable Core before source registration exposes its transport cause in the authenticated current-chat view', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-core-availability-')), token = 'c'.repeat(64)
  const server = await startServer({ dataDir: join(dir, 'core') })
  const client = await connectCore({ dataDir: join(dir, 'core') })
  await server.close()
  const core = createLazyCore({ dataDir: join(dir, 'core'), connect: async () => client })
  let materialReads = 0
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'qualified-app', hostStartedAt: Date.now() - 1000, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread() { materialReads++; throw new Error('material reads require an available Core') },
    async close() {},
  }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'core-availability', token, collectMaterials: true, pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'current-main' } } }) })
  assert.equal(response.status, 200)
  const view = (await response.json()).result.structuredContent
  assert.equal(view.identity.threadId, null)
  assert.equal(view.signal, 'gray')
  assert.equal(view.observation.gap, 'observation-unavailable')
  assert.equal(view.health.state, 'failed')
  assert.deepEqual(view.health.failures[0].error, { code: 'ECONNREFUSED', stage: 'core', category: 'transport' })
  assert.equal(view.observation.failureStage, 'core-connection')
  assert.equal(view.discovery.connected, 0)
  assert.equal(materialReads, 0)
})

test('the production lazy Core reconnects after a server generation change and keeps main-chat materials and first captures separate', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-core-reconnect-')), dataDir = join(dir, 'core'), started = Date.now() - 5000
  const row = (ordinal, type, payload) => JSON.stringify({ ordinal, timestamp: new Date(started + 2000).toISOString(), type, payload }) + '\n'
  const ids = ['main-a', 'main-b']
  for (const id of ids) await writeFile(join(dir, id + '.jsonl'), row(0, 'session_meta', { id, cwd: dir, originator: 'Codex Desktop', source: 'vscode', cli_version: candidateHostProfile.cliVersion, history_mode: 'paginated' })
    + row(1, 'event_msg', { type: 'task_started', turn_id: 'run-' + id, turn_attribution: { turn_id: 'run-' + id, turn_trigger: 'composer' } })
    + row(2, 'event_msg', { type: 'item_completed', thread_id: id, turn_id: 'run-' + id, item: { type: 'UserMessage', id: id + '-input', content: [{ type: 'text', text: 'Request for ' + id }] } }))
  let server = await startServer({ dataDir })
  const core = createLazyCore({ dataDir })
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'qualified-app', hostStartedAt: started, profile: candidateHostProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread(id) { return { thread: { id, path: join(dir, id + '.jsonl'), cwd: dir, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop' }, anchorTurnId: 'run-' + id } },
    async close() {},
  }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'core-reconnect', token: 'c'.repeat(64), collectMaterials: true, nativeOriginVersion: 2, pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const caughtUp = async id => { let view; for (let attempt = 0; attempt < 8; attempt++) { view = await adapter.read({ thread_id: id }); if (view.observation.coverage === 'caught-up') break }; assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view)); return view }
  for (const id of ids) assert.equal((await caughtUp(id)).execution.request, 'Request for ' + id)
  const before = await core.snapshot(), firstCaptures = new Map(before.tasks.map(task => [task.id, task.events.find(event => event.kind === 'input').capturedAt]))
  const previousInstance = (await core.health()).instanceId
  await server.close()
  server = await startServer({ dataDir })
  for (const id of ids) await appendFile(join(dir, id + '.jsonl'), row(3, 'event_msg', { type: 'item_completed', thread_id: id, turn_id: 'run-' + id, item: { type: 'AgentMessage', id: id + '-output', content: [{ type: 'Text', text: 'New activity for ' + id }] } }))
  for (const id of [...ids].reverse()) {
    const view = await caughtUp(id)
    assert.equal(view.identity.threadId, id)
    assert.equal(view.execution.request, 'Request for ' + id)
    assert.equal(view.observation.coverage, 'caught-up')
    assert.equal(view.observation.failureStage, undefined)
  }
  assert.notEqual((await core.health()).instanceId, previousInstance)
  const after = await core.snapshot()
  for (const id of ids) {
    const task = after.tasks.find(task => task.id === id)
    assert.equal(task.events.find(event => event.kind === 'input').capturedAt, firstCaptures.get(id))
    assert.deepEqual(task.events.filter(event => event.kind === 'output').map(event => event.text), ['New activity for ' + id])
  }
})
