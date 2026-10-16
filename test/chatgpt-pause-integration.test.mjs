import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

test('authenticated current-chat controls keep collecting and survive adapter/core replacement without touching another main chat', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-chat-pause-')), started = Date.now() - 10_000, token = 'c'.repeat(64)
  const line = (type, payload) => JSON.stringify({ type, timestamp: new Date(started + 2000).toISOString(), payload }) + '\n'
  for (const id of ['main-a', 'main-b']) await writeFile(join(dir, id + '.jsonl'), line('session_meta', { id })
    + line('event_msg', { type: 'task_started', turn_id: 'turn-' + id, turn_attribution: { turn_id: 'turn-' + id, turn_trigger: 'composer' } })
    + line('event_msg', { type: 'item_completed', thread_id: id, turn_id: 'turn-' + id, item: { type: 'UserMessage', id: 'message-' + id, content: [{ type: 'text', text: 'Observe this task only' }] } }))
  const native = () => ({ async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: started, profile: { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' } } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread(id) { return { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path: join(dir, id + '.jsonl'), cwd: dir }, anchorTurnId: 'turn-' + id } }, async close() {} })
  let server, core, adapter
  const open = async () => {
    server = await startServer({ dataDir: join(dir, 'core') }); core = await connectCore({ dataDir: join(dir, 'core') })
    adapter = await startChatGPTAdapter({ core, native: native(), sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'pause-install', token, collectMaterials: true, pollMs: 0 })
  }
  const close = async () => { await adapter?.close(); await core?.close(); await server?.close() }
  t.after(async () => { await close(); await rm(dir, { recursive: true, force: true }) })
  await open()
  const call = async (id, name = 'sentinel_current_chat', args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args, _meta: { thread_id: id } } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const a = (await call('main-a')).structuredContent
  await call('main-b')
  assert.equal(a.monitoring?.paused, false, 'the production adapter publishes the per-chat revision')
  const change = { expectedThreadId: 'main-a', paused: true, revision: a.monitoring.revision }
  const switched = await call('main-b', 'sentinel_set_chat_analysis', change)
  assert.equal(switched.isError, true, 'a late click cannot select a chat through its arguments')
  assert.equal((await call('main-a')).structuredContent.monitoring.paused, false)
  const paused = (await call('main-a', 'sentinel_set_chat_analysis', change)).structuredContent
  assert.equal(paused.monitoring.paused, true)
  assert.equal(paused.analysis.state, 'paused')
  await appendFile(join(dir, 'main-a.jsonl'), line('event_msg', { type: 'item_completed', thread_id: 'main-a', turn_id: 'turn-main-a', item: { type: 'AgentMessage', id: 'activity-during-pause', content: [{ type: 'Text', text: 'New activity during the analysis pause' }] } }))
  await call('main-a')
  const snapshot = await core.snapshot(), taskA = snapshot.tasks.find(task => task.id === 'main-a'), taskB = snapshot.tasks.find(task => task.id === 'main-b')
  assert.ok(taskA.events.some(event => event.text.includes('New activity during')))
  assert.equal(taskA.analysis.state, 'paused')
  assert.equal(taskB.conversationMonitoring.paused, false)
  assert.equal(snapshot.monitoring.paused, false, 'the global and DSH pause control is untouched')
  const revision = paused.monitoring.revision
  await close(); await open()
  const restored = (await call('main-a')).structuredContent
  assert.equal(restored.observation.coverage, 'caught-up', JSON.stringify(restored.observation))
  assert.equal(restored.monitoring.paused, true)
  assert.equal(restored.monitoring.revision, revision)
  const resumed = (await call('main-a', 'sentinel_set_chat_analysis', { expectedThreadId: 'main-a', paused: false, revision })).structuredContent
  assert.equal(resumed.monitoring.paused, false)
  assert.notEqual(resumed.monitoring.revision, revision)
})
