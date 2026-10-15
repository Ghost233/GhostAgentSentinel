import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

test('native main materials reach six checks and the authenticated panel without disguising missing model configuration', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-material-integration-')), path = join(dir, 'main.jsonl')
  const started = Date.now() - 10_000, at = started + 2000
  const line = (type, payload, time = at) => JSON.stringify({ type, timestamp: new Date(time).toISOString(), payload }) + '\n'
  await writeFile(path, line('session_meta', { id: 'main' }) + line('event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + line('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn', item: { type: 'UserMessage', id: 'message', content: [{ type: 'text', text: 'Read the current log only' }] } }, at + 1)
    + line('response_item', { type: 'message', role: 'assistant', internal_chat_message_metadata_passthrough: { turn_id: 'turn' }, content: [{ type: 'output_text', text: 'Reading the current log' }] }, at + 2)
    + line('response_item', { type: 'function_call', name: 'read_file', call_id: 'call', arguments: '{"path":"current.log"}', internal_chat_message_metadata_passthrough: { turn_id: 'turn' } }, at + 3)
    + line('response_item', { type: 'function_call_output', call_id: 'call', output: 'Observed current log', internal_chat_message_metadata_passthrough: { turn_id: 'turn' } }, at + 4))
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') })
  const thread = { id: 'main', parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', path, cwd: dir }
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt: started, profile: { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' } } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async readThread() { return { thread, anchorTurnId: 'turn' } }, async close() {} }
  const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'material-install', token: 'b'.repeat(64), collectMaterials: true, pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async () => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'b'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: 'main' } } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result.structuredContent
  }
  await read()
  let observed
  for (let i = 0; i < 100; i++) { observed = (await core.snapshot()).tasks[0]; if (observed.analysis.completedAt !== null) break; await delay(5) }
  assert.equal(observed.currentRequest, 'Read the current log only')
  assert.equal(observed.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.ok(observed.events.some(event => event.kind === 'tool-result' && event.data.callId === 'call'))
  assert.equal(observed.analysis.state, 'unavailable')
  assert.equal(observed.analysis.checks.length, 6)
  const view = await read()
  assert.equal(view.identity.threadId, 'main')
  assert.equal(view.analysis.connected, true)
  assert.equal(view.analysis.state, 'unavailable')
  assert.match(view.analysis.summary, /未配置/)
  assert.equal(view.signal, 'gray')
  assert.deepEqual(await core.claimNotifications((await core.snapshot()).sources[0].key), [])
  assert.equal(JSON.stringify(view).includes('b'.repeat(64)), false)
  native.readEnablement = async () => ({ user: 'disabled', effective: 'disabled' })
  const disabled = await read()
  assert.equal(disabled.observation.gap, 'plugin-disabled')
  assert.equal(disabled.analysis.state, 'unknown')
  assert.equal(disabled.analysis.connected, true, 'source loss does not falsely claim that the check module was never integrated')
})
