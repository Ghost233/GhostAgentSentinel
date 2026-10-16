import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, appendFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const profile = { appVersion: '26.1007.71326', appBuild: '20398', cliVersion: '0.162.0-alpha.17.2' }
const line = (type, payload, at) => JSON.stringify({ type, payload, timestamp: new Date(at).toISOString() }) + '\n'
async function fixture(t) {
  let clock = Date.now()
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-review-fixes-')), path = join(dir, 'main.jsonl')
  const born = clock - 10000
  await writeFile(path, line('session_meta', { id: 'main' }, born + 2000)
    + line('event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } }, born + 2000)
    + line('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn', item: { type: 'UserMessage', id: 'user', content: [{ type: 'text', text: 'Read the current log only' }] } }, born + 2001))
  const server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }), core = await connectCore({ dataDir: join(dir, 'core') })
  const native = { async sampleHost() { return { available: true, hostRunId: 'review-host', hostStartedAt: born, profile } }, async readThread(id) { return id === 'main' ? { thread: { id, parentThreadId: null, forkedFromId: null, source: 'vscode', originator: 'Codex Desktop', cwd: dir, path }, anchorTurnId: 'turn' } : null }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async close() {} }
  let adapter
  const start = async () => { await adapter?.close(); adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'review-fixes', token: '9'.repeat(64), now: () => clock, pollMs: 0, collectMaterials: true }) }
  await start()
  t.after(async () => { await adapter?.close(); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const read = async (id = 'main') => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + '9'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: id } } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result.structuredContent
  }
  return { dir, path, core, native, start, read, now: () => clock, advance: ms => { clock += ms } }
}
const agent = (id, content, at, turn = 'turn', thread = 'main') => line('event_msg', { type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'AgentMessage', id, content, phase: 'commentary' } }, at)
const response = (id, text, at, turn = 'turn') => line('response_item', { type: 'message', id, role: 'assistant', content: [{ type: 'output_text', text }], internal_chat_message_metadata_passthrough: { turn_id: turn } }, at)

test('the typed AgentMessage producer reaches the public core with literal output and its native evidence reference', async t => {
  const f = await fixture(t)
  await appendFile(f.path, agent('native-answer', [{ type: 'Text', text: 'Observed useful diagnosis' }], f.now()))
  const view = await f.read(), task = (await f.core.snapshot()).tasks[0], outputs = task.events.filter(event => event.kind === 'output')
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(outputs.length, 1)
  assert.equal(outputs[0].text, 'Observed useful diagnosis')
  assert.equal(outputs[0].data.nativeType, 'item_completed/AgentMessage')
  assert.equal(outputs[0].data.messageId, 'native-answer')
  assert.match(outputs[0].evidence, /^chatgpt-native-record:[a-f0-9]{64}$/)
  assert.equal(outputs[0].evidence, 'chatgpt-native-record:' + outputs[0].data.nativeRecordId)
  assert.equal(task.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.deepEqual(task.coverage, { complete: true, gaps: [] })
})

test('equal native message IDs deduplicate the two verified output producers across pumps and recovery without collapsing other messages', async t => {
  for (const first of ['agent', 'response']) await t.test(first, async t => {
    const f = await fixture(t), text = 'One native output with a stable identity'
    await appendFile(f.path, first === 'agent' ? agent('same-answer', [{ type: 'Text', text }], f.now()) : response('same-answer', text, f.now()))
    await f.read()
    const original = (await f.core.snapshot()).tasks[0].events.find(event => event.kind === 'output')
    assert.ok(original)
    f.advance(1000)
    await appendFile(f.path, first === 'agent' ? response('same-answer', text, f.now()) : agent('same-answer', [{ type: 'Text', text }], f.now()))
    await f.read(); await f.start(); const recovered = await f.read()
    assert.equal(recovered.observation.coverage, 'caught-up', JSON.stringify(recovered.observation))
    let outputs = (await f.core.snapshot()).tasks[0].events.filter(event => event.kind === 'output')
    assert.equal(outputs.length, 1)
    assert.equal(outputs[0].capturedAt, original.capturedAt)
    assert.equal(outputs[0].evidence, original.evidence)
    await appendFile(f.path, agent('different-answer', [{ type: 'Text', text }], f.now()))
    await f.read()
    outputs = (await f.core.snapshot()).tasks[0].events.filter(event => event.kind === 'output')
    assert.equal(outputs.length, 2, 'equal text without equal native identity is a separate observed output')
    assert.notEqual(outputs[0].data.messageId, outputs[1].data.messageId)
    assert.notEqual(outputs[0].data.nativeRecordId, outputs[1].data.nativeRecordId)
  })
})

test('an unrecognized AgentMessage content variant exposes a collection gap instead of silently dropping output', async t => {
  const f = await fixture(t)
  await appendFile(f.path, agent('unknown-answer', [{ type: 'FutureText', text: 'Unverified body shape' }], f.now()))
  const view = await f.read(), task = (await f.core.snapshot()).tasks[0]
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(task.events.filter(event => event.kind === 'output').length, 0)
  const omission = task.events.find(event => event.kind === 'gap')
  assert.ok(omission)
  assert.equal(omission.data.nativeType, 'item_completed/AgentMessage')
  assert.equal(omission.data.truncated, true)
  assert.equal(task.coverage.complete, false)
  assert.ok(task.coverage.gaps.length)
  assert.equal(JSON.stringify(task.events).includes('Unverified body shape'), false)
})

test('a conflicting body under the same native output identity remains a coverage gap instead of a second output', async t => {
  const f = await fixture(t)
  await appendFile(f.path, response('conflicting-answer', 'First observed body', f.now()))
  await f.read()
  f.advance(1000)
  await appendFile(f.path, agent('conflicting-answer', [{ type: 'Text', text: 'Different observed body' }], f.now()))
  await f.read()
  const task = (await f.core.snapshot()).tasks[0]
  assert.equal(task.events.filter(event => event.kind === 'output').length, 1)
  assert.ok(task.events.some(event => event.kind === 'gap' && /输出身份.*不一致/.test(event.text)))
  assert.equal(task.coverage.complete, false)
})
