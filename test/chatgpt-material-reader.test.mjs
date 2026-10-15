import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'

const at = 1_800_000_000_000
const row = (type, payload, timestamp = at) => JSON.stringify({ type, timestamp: new Date(timestamp).toISOString(), payload }) + '\n'
async function fixture(t, records) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-material-')), path = join(dir, 'main.jsonl')
  await writeFile(path, row('session_meta', { id: 'main' }) + records)
  const source = { path, allowedRoot: dir, threadId: 'main', parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'host:one', hostStartedAt: at - 10000, profile: supportedProfile, anchorTurnId: 'turn-0' }
  const reader = new BoundedReader(source, null, { mode: 'material' })
  t.after(async () => { await reader.close(); await rm(dir, { recursive: true, force: true }) })
  return { reader, source }
}

test('material batches stop at sixteen deliverable records and resume without losing execution starts', async t => {
  const f = await fixture(t, Array.from({ length: 20 }, (_, index) => row('event_msg', { type: 'task_started', turn_id: 'turn-' + index }, at + index)).join(''))
  const first = await f.reader.pump()
  assert.equal(first.recentLifecycle.length, 16)
  assert.equal(first.coverage, 'catching-up')
  assert.equal(first.pendingBytes, 0)
  assert.ok(first.readBytes <= 262144)
  const second = await f.reader.pump()
  assert.equal(second.recentLifecycle.length, 4)
  assert.equal(second.coverage, 'caught-up')
  assert.deepEqual([...first.recentLifecycle, ...second.recentLifecycle].map(event => event.turnId), Array.from({ length: 20 }, (_, index) => 'turn-' + index))
  assert.ok(Buffer.byteLength(JSON.stringify(f.reader.checkpoint())) < 16384)
})

test('the same bounded pass keeps typed output excerpts and excludes reasoning without parsing whole messages', async t => {
  const passthrough = { turn_id: 'turn-0' }
  const records = row('event_msg', { type: 'task_started', turn_id: 'turn-0' })
    + row('response_item', { type: 'reasoning', summary: [{ text: 'private-reasoning'.repeat(40000) }], encrypted_content: 'hidden'.repeat(40000) })
    + row('response_item', { type: 'message', id: 'assistant-one', role: 'assistant', content: [{ type: 'output_text', text: 'Observed answer' }], internal_chat_message_metadata_passthrough: passthrough })
  const f = await fixture(t, records)
  const materials = []
  let view
  do { view = await f.reader.pump(); materials.push(...(view.materials ?? [])); assert.ok(view.readBytes <= 262144); assert.ok(view.metrics.peakProjectionBytes <= 16384) } while (view.coverage === 'catching-up')
  assert.equal(materials.length, 1)
  assert.equal(materials[0].kind, 'output')
  assert.equal(materials[0].text, 'Observed answer')
  assert.equal(materials[0].turnId, 'turn-0')
  assert.equal(materials[0].data.messageId, 'assistant-one')
  assert.doesNotMatch(JSON.stringify(view) + JSON.stringify(f.reader.checkpoint()), /private-reasoning|encrypted_content/)
})

test('a typed UserMessage and explicit user trigger produce the request while role-only and goal replay do not', async t => {
  for (const trigger of ['edit_user_message', 'goal']) {
    const passthrough = { turn_id: 'turn-0' }
    const records = row('event_msg', { type: 'task_started', turn_id: 'turn-0', turn_attribution: { turn_id: 'turn-0', turn_trigger: trigger } })
      + row('response_item', { type: 'message', id: 'role-only', role: 'user', content: [{ type: 'input_text', text: 'Role alone is no permission' }], internal_chat_message_metadata_passthrough: passthrough })
      + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn-0', item: { type: 'UserMessage', id: 'producer-one', client_id: 'client-one', content: [{ type: 'text', text: 'Read only the log' }] } })
    const f = await fixture(t, records), view = await f.reader.pump()
    const requests = view.materials.filter(event => event.kind === 'input')
    assert.equal(requests.length, trigger === 'edit_user_message' ? 1 : 0)
    if (requests.length) {
      assert.equal(requests[0].text, 'Read only the log')
      assert.equal(requests[0].data.nativeType, 'item_completed/UserMessage')
      assert.equal(requests[0].data.inputOrigin, 'user-entry')
      assert.equal(requests[0].data.runId, 'turn-0')
      assert.equal(requests[0].data.messageId, 'producer-one')
    }
    assert.equal(view.recentLifecycle[0].turnTrigger, trigger)
  }
})

test('typed command results retain their real exit result without inventing a parent tool match', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0' })
    + row('response_item', { type: 'function_call', call_id: 'outer-call', name: 'exec', arguments: '{"cmd":"run checks"}', internal_chat_message_metadata_passthrough: { turn_id: 'turn-0' } })
    + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn-0', item: { type: 'CommandExecution', id: 'nested-command', command: ['node', '--test'], aggregated_output: 'Failed assertion', exit_code: 1, status: 'failed', source: 'unified_exec_startup' } }))
  const view = await f.reader.pump()
  assert.equal(view.materials[0].kind, 'tool-start')
  assert.equal(view.materials[0].data.command, 'run checks')
  const result = view.materials.find(event => event.kind === 'tool-result')
  assert.equal(result.text, 'Failed assertion')
  assert.equal(result.data.exitCode, 1)
  assert.equal(result.data.callId, 'native-item:nested-command')
  assert.notEqual(result.data.callId, view.materials[0].data.callId)
  assert.equal(result.data.sourceVersion, undefined)
  assert.equal(result.data.validationScope, undefined)
})

test('oversized text is excerpted, late authentication fields are omitted, and foreign execution evidence is excluded', async t => {
  const long = 'Visible prefix '.repeat(40000)
  const records = row('event_msg', { type: 'task_started', turn_id: 'turn-0', turn_attribution: { turn_trigger: 'edit_user_message' } })
    + row('response_item', { type: 'message', id: 'oversized', role: 'assistant', content: [{ type: 'output_text', text: long }], internal_chat_message_metadata_passthrough: { turn_id: 'turn-0' } })
    + row('response_item', { type: 'function_call_output', call_id: 'private', output: long + ' Authorization: Bearer NEVER-RETAIN-THIS', internal_chat_message_metadata_passthrough: { turn_id: 'turn-0' } })
    + row('event_msg', { type: 'item_completed', thread_id: 'foreign', turn_id: 'turn-0', item: { type: 'UserMessage', id: 'foreign', content: [{ type: 'text', text: 'foreign request' }] } })
    + row('event_msg', { type: 'item_completed', turn_id: 'turn-0', item: { type: 'UserMessage', id: 'unbound', content: [{ type: 'text', text: 'unbound request' }] } })
    + row('response_item', { type: 'message', id: 'foreign-run', role: 'assistant', content: [{ type: 'output_text', text: 'foreign output' }], internal_chat_message_metadata_passthrough: { turn_id: 'foreign' } })
  const f = await fixture(t, records), material = []
  let view
  do { view = await f.reader.pump(); material.push(...view.materials); assert.ok(view.readBytes <= 262144) } while (view.coverage === 'catching-up')
  assert.equal(view.coverage, 'caught-up')
  assert.equal(material.length, 2)
  assert.equal(material[0].text.length, 240)
  assert.equal(material[0].data.truncated, true)
  assert.equal(material[1].data.redacted, true)
  assert.equal(material[1].data.truncated, true)
  assert.doesNotMatch(JSON.stringify(material) + JSON.stringify(f.reader.checkpoint()), /NEVER-RETAIN|Bearer|foreign request|unbound request|foreign output/)
  assert.equal(view.excluded, 3)
  assert.ok(Buffer.byteLength(JSON.stringify(f.reader.checkpoint())) < 16384)
})

test('switching an already advanced metadata cursor to materials exposes the missing material baseline', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0' }))
  const metadata = new BoundedReader(f.source)
  await metadata.pump()
  const material = new BoundedReader(f.source, metadata.checkpoint(), { mode: 'material' })
  try {
    const view = await material.pump()
    assert.equal(view.gap, 'material-cursor-baseline-missing')
    assert.equal(view.effectivePhase, 'unknown')
    assert.equal(view.readBytes, 0)
  } finally { await metadata.close(); await material.close() }
})

test('a trigger attributed to another turn cannot prove a user request for this execution', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0', turn_attribution: { turn_id: 'foreign', turn_trigger: 'composer' } })
    + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'turn-0', item: { type: 'UserMessage', id: 'producer', content: [{ type: 'text', text: 'A mismatched native trigger' }] } }))
  const view = await f.reader.pump()
  assert.equal(view.materials.some(event => event.kind === 'input'), false)
  assert.equal(view.recentLifecycle[0].turnTrigger, null)
})

test('unknown native material kinds remain visible capture omissions while known reasoning stays excluded', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0' })
    + row('response_item', { type: 'future_tool_kind', contents: 'unrecognized raw payload' }))
  const view = await f.reader.pump()
  assert.equal(view.materials.length, 1)
  assert.equal(view.materials[0].kind, 'gap')
  assert.equal(view.materials[0].data.truncated, true)
  assert.match(view.materials[0].text, /future_tool_kind/)
  assert.doesNotMatch(JSON.stringify(view.materials), /unrecognized raw payload/)
})

test('a source-unavailable retry does not reissue the previous pump material batch', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0' })
    + row('response_item', { type: 'message', role: 'assistant', id: 'out', content: [{ type: 'output_text', text: 'Already observed' }], internal_chat_message_metadata_passthrough: { turn_id: 'turn-0' } }))
  assert.equal((await f.reader.pump()).materials.length, 1)
  const unavailable = await f.reader.pump({ available: false })
  assert.deepEqual(unavailable.materials, [])
  assert.deepEqual(unavailable.recentLifecycle, [])
  assert.equal(unavailable.readBytes, 0)
})

test('a typed MCP result with unverified body shape records an explicit omission rather than fabricated output or parent matching', async t => {
  const f = await fixture(t, row('event_msg', { type: 'task_started', turn_id: 'turn-0' })
    + JSON.stringify({ type: 'event_msg', payload: { type: 'item_completed', thread_id: 'main', turn_id: 'turn-0', item: { type: 'McpToolCall', id: 'mcp-item', server: 'controlled', tool: 'read', result: { opaque: 'not an approved text field' } } } }) + '\n')
  const view = await f.reader.pump()
  assert.equal(view.materials.length, 1)
  assert.equal(view.materials[0].kind, 'gap')
  assert.equal(view.materials[0].data.nativeType, 'item_completed/McpToolCall')
  assert.equal(view.materials[0].data.truncated, true)
  assert.equal(view.materials[0].at, null)
  assert.equal(view.materials[0].data.callId, undefined)
  assert.doesNotMatch(JSON.stringify(view.materials), /not an approved text field/)
})
