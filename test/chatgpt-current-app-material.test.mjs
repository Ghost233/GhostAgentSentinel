import { withModelSummary } from './helpers/model-summary.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createServer } from 'node:http'
import { zstdCompressSync } from 'node:zlib'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { candidateHostProfile, supportedProfile } from '../chatgpt/storage-compatibility.mjs'
import { decodeJevRequest } from './helpers/jev-wire.mjs'
import { MaterialProjection, materialRecord } from '../chatgpt/material-projection.mjs'

const at = 1_800_000_000_000, owner = '00000000-0000-4000-8000-000000000163', parent = '00000000-0000-4000-8000-000000000162'
const row = (ordinal, type, payload, time = at) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(time).toISOString() }) + '\n'
const header = (id, producer = candidateHostProfile.cliVersion) => ({ id, cwd: '/synthetic-project', originator: 'Codex Desktop', source: 'vscode', cli_version: producer, history_mode: 'paginated' })
const start = (id, trigger = 'composer') => ({ type: 'task_started', turn_id: id, turn_attribution: { turn_id: id, turn_trigger: trigger } })
const user = (thread, turn, text = 'Inspect only the synthetic log') => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'UserMessage', id: turn + '-user', content: [{ type: 'text', text }] } })
const agent = (thread, turn, text = 'The synthetic log has been inspected') => ({ type: 'item_completed', thread_id: thread, turn_id: turn, item: { type: 'AgentMessage', id: turn + '-agent', content: [{ type: 'Text', text }] } })

async function fixture(t, { profile = candidateHostProfile, suffix = '', records, fork = false, compressed = false, onQualificationLost } = {}) {
  let clock = at
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-current-app-')), sessionsRoot = join(dir, 'sessions'), archived = join(dir, 'archived_sessions'), dataDir = join(dir, 'core'), path = join(sessionsRoot, 'rollout-' + owner + '.jsonl' + (compressed ? '.zst' : '')), parentPath = join(archived, 'rollout-' + parent + '.jsonl')
  await Promise.all([fs.mkdir(sessionsRoot), fs.mkdir(archived), fs.mkdir(dataDir)])
  let server, core, adapter
  const wire = createServer(async (req, res) => {
    let body = ''; for await (const chunk of req) body += chunk
    res.setHeader('content-type', 'application/json')
    if (req.url === '/v1/chat/completions') res.end(JSON.stringify({ model: 'fixed-extractor', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(JSON.parse(body), { selectedOptionIds: [], gaps: [] })) } }] }))
    else { const request = decodeJevRequest(JSON.parse(body)); res.end(JSON.stringify({ model: 'fixed-judge', answers: Object.fromEntries(Object.keys(request.questions).map(id => [id, { type: 'choice', choice: 'clear' }])) })) }
  })
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); wire.closeAllConnections(); await new Promise(resolve => wire.close(resolve)); await fs.rm(dir, { recursive: true, force: true }) })
  await new Promise(resolve => wire.listen(0, '127.0.0.1', resolve))
  const baseUrl = 'http://127.0.0.1:' + wire.address().port
  const inherited = row(0, 'session_meta', header(parent), at - 100000) + row(1, 'event_msg', start('inherited'), at - 100000) + row(2, 'event_msg', user(parent, 'inherited', 'Keep the synthetic inherited limit'), at - 100000)
    + row(3, 'event_msg', { type: 'item_completed', thread_id: parent, turn_id: 'inherited', item: { type: 'CommandExecution', id: 'old-validation', aggregated_output: 'Historical validation only', exit_code: 0 } }, at - 100000) + row(4, 'event_msg', { type: 'task_complete', turn_id: 'inherited' }, at - 100000)
  await fs.writeFile(parentPath, inherited)
  const base = fork ? 5 : 0
  const bytes = row(base, 'session_meta', { ...header(owner), ...(fork ? { forked_from_id: parent, forked_from_ordinal_exclusive: 5, history_base: { thread_id: parent, end_ordinal_exclusive: 5, end_byte_offset: Buffer.byteLength(inherited) } } : {}) })
    + (records ?? row(base + 1, 'event_msg', start('turn')) + row(base + 2, 'event_msg', user(owner, 'turn')) + row(base + 3, 'event_msg', agent(owner, 'turn'))) + suffix
  await fs.writeFile(path, compressed ? zstdCompressSync(Buffer.from(bytes)) : bytes)
  server = await startServer({ dataDir, now: () => clock, cooldownMs: 0 }); core = await connectCore({ dataDir })
  await configureInstallationModels(core, 'chatgpt:current-app-material', { llm: { baseUrl, model: 'fixed-extractor' }, jev: { baseUrl, model: 'fixed-judge' } })
  const state = { profile, enabled: true, available: true, threadReads: 0, currentRecordReads: 0 }
  const native = { async sampleHost() { return { available: state.available, hostRunId: 'current-app-fixture', hostStartedAt: at - 10000, profile: state.profile } }, async readThread(id) { state.threadReads++; return { thread: { id, path: id === owner ? path : parentPath, cwd: '/synthetic-project', parentThreadId: null, forkedFromId: id === owner && fork ? parent : null, source: 'vscode', originator: 'Codex Desktop' }, anchorTurnId: id === owner ? 'turn' : null } }, async readEnablement() { return { user: state.enabled ? 'enabled' : 'disabled', effective: state.enabled ? 'enabled' : 'disabled' } }, async close() {} }
  const open = fs.open, watched = new Set([await fs.realpath(path), await fs.realpath(parentPath)])
  t.mock.method(fs, 'open', async (...args) => { const handle = await open(...args); if (watched.has(String(args[0]))) { const read = handle.read; handle.read = async function (...values) { const got = await read.apply(this, values); state.currentRecordReads += got.bytesRead; return got } }; return handle })
  const reopen = async () => { await adapter?.close(); adapter = await startChatGPTAdapter({ core, native, sessionsRoot, dataDir: join(dir, 'adapter'), installationId: 'current-app-material', token: 'c'.repeat(64), pollMs: 0, collectMaterials: true, nativeOriginVersion: 2, now: () => clock, ...(onQualificationLost ? { onQualificationLost } : {}) }) }
  await reopen()
  const read = async () => { const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + 'c'.repeat(64), 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: owner } } }) }); assert.equal(response.status, 200); const value = await response.json(); assert.equal(value.result.isError, undefined); return value.result.structuredContent }
  const caughtUp = async () => { let view; for (let i = 0; i < 30; i++) { view = await read(); if (view.observation.coverage === 'caught-up' || view.observation.coverage === 'retained-history' && ['completed', 'aborted'].includes(view.observation.phase) || view.observation.gap?.startsWith('unsupported-') || ['cursor-invalid', 'record-malformed-or-over-limit'].includes(view.observation.gap)) break }; return view }
  return { core, state, path, dir, read, caughtUp, reopen, advance() { clock += 1000 }, get adapter() { return adapter } }
}

test('the exact current App reaches the production Core HTTP and panel with a typed user request', async t => {
  const f = await fixture(t), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
  assert.equal(view.identity.threadId, owner)
  assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.events.find(event => event.kind === 'input').authorizationEligible, true)
  assert.equal(task.events.find(event => event.kind === 'output').text, 'The synthetic log has been inspected')
  assert.equal(task.coverage.complete, true)
})

test('verified custom tool input_text arrays become one bounded result without becoming input or verification proof', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'array-result', output: [{ type: 'input_text', text: 'Synthetic result excerpt' }, { text: 'A second fixed text segment', type: 'input_text' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'array-result')
  assert.equal(result.kind, 'tool-result'); assert.equal(result.text, 'Synthetic result excerpt\nA second fixed text segment')
  assert.equal(result.data.nativeType, 'response_item/custom_tool_call_output')
  assert.notEqual(result.authorizationEligible, true)
  for (const key of ['inputOrigin', 'inputPhase', 'exitCode', 'sourceVersion', 'validationScope', 'command']) assert.equal(result.data[key], undefined)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.coverage.complete, true)
  assert.equal(task.events.filter(event => event.kind === 'input').length, 1)
})

test('observed function tool input_text arrays reach Core as bounded results without input or verification authority', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'function_call_output', call_id: 'function-array', output: [{ type: 'input_text', text: 'Synthetic function result' }, { text: 'A second fixed result segment', type: 'input_text' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up')
  const task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'function-array')
  assert.equal(result.kind, 'tool-result'); assert.equal(result.text, 'Synthetic function result\nA second fixed result segment')
  assert.equal(result.data.nativeType, 'response_item/function_call_output')
  assert.notEqual(result.authorizationEligible, true)
  for (const key of ['inputOrigin', 'inputPhase', 'exitCode', 'sourceVersion', 'validationScope', 'command']) assert.equal(result.data[key], undefined)
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.coverage.complete, true)
  assert.equal(task.events.filter(event => event.kind === 'input').length, 1)
})

test('dotted property names cannot impersonate structural array elements or the payload output field', async t => {
  await t.test('sibling-with-dotted-name', async t => {
    const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'custom_tool_call_output', call_id: 'real-array', output: [{ type: 'input_text', text: 'Actual structural text' }], 'output.1': { type: 'input_text', text: 'PHANTOM-SIBLING-BODY' } }) })
    assert.equal((await f.caughtUp()).observation.coverage, 'caught-up')
    const result = (await f.core.snapshot()).tasks[0].events.find(event => event.data?.callId === 'real-array')
    assert.equal(result.text, 'Actual structural text')
    assert.doesNotMatch(JSON.stringify(result), /PHANTOM/)
  })
  await t.test('root-with-dotted-name', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'response_item', timestamp: new Date(at).toISOString(), payload: { type: 'custom_tool_call_output', call_id: 'phantom-array' }, 'payload.output': [{ type: 'input_text', text: 'PHANTOM-ROOT-BODY' }] }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-tool-result-shape')
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'phantom-array'), false)
  })
  await t.test('payload-kind-alias', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'response_item', timestamp: new Date(at).toISOString(), payload: { type: 'future_tool_call_output', call_id: 'phantom-kind', output: [{ type: 'input_text', text: 'UNOBSERVED-KIND-BODY' }] }, 'payload.type': 'custom_tool_call_output' }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.coverage, 'caught-up')
    const task = (await f.core.snapshot()).tasks[0], gap = task.events.find(event => event.data?.nativeType === 'response_item/future_tool_call_output')
    assert.equal(gap.kind, 'gap'); assert.equal(task.coverage.complete, false)
    assert.equal(task.events.some(event => event.data?.callId === 'phantom-kind'), false)
    assert.doesNotMatch(JSON.stringify(task.events), /UNOBSERVED-KIND-BODY/)
    const projection = new MaterialProjection(); projection.write(Buffer.from(suffix))
    assert.equal(projection.finish().material.fields['payload.output'], undefined)
  })
  await t.test('root-kind-alias', async t => {
    const suffix = JSON.stringify({ ordinal: 4, type: 'world_state', timestamp: new Date(at).toISOString(), payload: { type: 'custom_tool_call_output', call_id: 'phantom-root-kind', output: [{ type: 'input_text', text: 'UNOBSERVED-RECORD-BODY' }] }, '': { type: 'response_item' } }) + '\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.observation.coverage, 'caught-up'); assert.equal(view.observation.gap, null)
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'phantom-root-kind'), false)
  })
})

test('current-producer fork material reaches the production panel with independent user input and inherited evidence kept historical', async t => {
  const f = await fixture(t, { fork: true, suffix: row(9, 'response_item', { type: 'custom_tool_call_output', call_id: 'current-array', output: [{ type: 'input_text', text: 'Only current synthetic tool activity' }] }) })
  const view = await f.caughtUp()
  assert.equal(view.observation.coverage, 'caught-up', JSON.stringify(view.observation))
  assert.equal(view.forkProvenance.complete, true)
  const snapshot = await f.core.snapshot(), task = snapshot.tasks.find(value => value.lifecycleId === 'turn')
  assert.equal(task.currentRequest, 'Inspect only the synthetic log')
  assert.equal(task.instructionContext.userLimitRefs.length, 1)
  const inherited = task.events.filter(event => event.data?.history)
  assert.ok(inherited.some(event => event.text === 'Keep the synthetic inherited limit'))
  const oldResult = inherited.find(event => event.text === 'Historical validation only')
  assert.ok(oldResult); assert.equal(oldResult.data.inherited, true)
  assert.ok(!task.activity.evidenceIds.includes(oldResult.id))
  assert.equal(task.events.find(event => event.data?.callId === 'current-array').data.history, undefined)
  assert.ok(task.events.filter(event => event.kind === 'input' && !event.data?.history).every(event => event.text === task.currentRequest && event.authorizationEligible === true))
  assert.ok(snapshot.tasks.every(value => value.id === owner))
})

test('nearby or untyped Host profiles do no material IO and keep the qualified owner gray instead of declaring global loss', async t => {
  for (const profile of [{ ...candidateHostProfile, appBuild: '20587' }, { ...candidateHostProfile, cliVersion: '0.163.0-alpha.10.3' }, { ...candidateHostProfile, appBuild: 20586 }]) await t.test(JSON.stringify(profile), async t => {
    const lost = [], f = await fixture(t, { profile, onQualificationLost: reason => lost.push(reason) }), view = await f.read()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-host-version')
    assert.equal(f.state.currentRecordReads, 0); assert.equal(f.state.threadReads, 0)
    assert.deepEqual(lost, []); assert.equal(f.adapter.diagnostics().closed, false)
    assert.equal((await f.core.snapshot()).tasks.length, 0)
  })
})

test('a genuine global plugin disable still closes the adapter and preserves an independently connected DSH source', async t => {
  const lost = [], f = await fixture(t, { onQualificationLost: reason => lost.push(reason) })
  await f.caughtUp()
  const dsh = await f.core.registerSource({ registrationId: 'independent-dsh', hostRunId: 'dsh-host', name: 'Owned DSH fixture' })
  f.state.enabled = false
  await f.adapter.refresh(); await f.adapter.close()
  assert.deepEqual(lost, ['plugin-disabled'])
  assert.equal(f.adapter.diagnostics().closed, true); assert.equal(f.adapter.diagnostics().openReaders, 0)
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.sources.find(source => source.key === dsh).status, 'connected')
})

test('unverified and malformed tool arrays stay gray across later completion and adapter recovery without reserving body identities', async t => {
  const cases = [
    ['empty', []], ['scalar-element', ['Not a typed element']], ['missing-type', [{ text: 'unverified-body' }]], ['missing-text', [{ type: 'input_text' }]],
    ['null-text', [{ type: 'input_text', text: null }]], ['number-text', [{ type: 'input_text', text: 7 }]], ['object-text', [{ type: 'input_text', text: { nested: 'unverified-body' } }]],
    ['unverified-image', [{ type: 'input_image', image_url: 'unverified-body' }]], ['mixed', [{ type: 'input_text', text: 'unverified-body' }, { type: 'input_audio', audio_url: 'unverified-body' }]],
    ['extra-field', [{ type: 'input_text', text: 'unverified-body', extra: { authorization: 'Bearer PRIVATE-UNVERIFIED' } }]],
  ]
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const [name, output] of cases) await t.test(type + '/' + name, async t => {
    const f = await fixture(t); await f.caughtUp()
    const before = (await f.core.snapshot()).tasks[0], old = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), progress = (await f.core.diagnostics()).retained.nativeCaptureProgress.records
    f.advance(); await fs.appendFile(f.path, row(4, 'response_item', { type, call_id: 'bad-array', output }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'unsupported-tool-result-shape')
    assert.equal(after.coverage.complete, false); assert.equal(after.status, 'unknown'); assert.equal(after.lastActivity, before.lastActivity)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), old)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, progress)
    await f.reopen(); view = await f.read(); after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), old)
    assert.doesNotMatch(JSON.stringify(after.events), /unverified-body|PRIVATE-UNVERIFIED|task_complete/)
  })
})

test('observed function arrays coexist with scalar function and custom results without replacing earlier material', async t => {
  const f = await fixture(t, { suffix: row(4, 'response_item', { type: 'function_call_output', call_id: 'function-scalar', output: 'Function scalar result' }) + row(5, 'response_item', { type: 'custom_tool_call_output', call_id: 'custom-scalar', output: 'Custom scalar result' }) })
  assert.equal((await f.caughtUp()).observation.coverage, 'caught-up')
  const first = (await f.core.snapshot()).tasks[0], results = first.events.filter(event => event.kind === 'tool-result')
  assert.deepEqual(results.map(event => event.text), ['Function scalar result', 'Custom scalar result'])
  await fs.appendFile(f.path, row(6, 'response_item', { type: 'function_call_output', call_id: 'observed-array', output: [{ type: 'input_text', text: 'Function array result' }] }))
  const view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
  assert.equal(view.observation.gap, null); assert.equal(view.observation.coverage, 'caught-up')
  assert.equal(after.coverage.complete, true)
  assert.deepEqual(after.events.filter(event => event.kind === 'tool-result').map(event => event.text), [...results.map(event => event.text), 'Function array result'])
})

test('array excerpts share one hard text budget and late authentication fields redact the whole output with an explicit coverage gap', async t => {
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const mode of ['many-elements', 'late-authentication']) await t.test(type + '/' + mode, async t => {
    const output = mode === 'many-elements' ? Array.from({ length: 5000 }, () => ({ type: 'input_text', text: 'A bounded excerpt segment' })) : [{ type: 'input_text', text: 'Prefix before a later sensitive element' }, { type: 'input_text', text: 'x'.repeat(400000) + ' Authorization: Bearer PRIVATE-LATE-ARRAY-TOKEN' }]
    const f = await fixture(t, { suffix: row(4, 'response_item', { type, call_id: 'bounded-array', output }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0], result = task.events.find(event => event.data?.callId === 'bounded-array')
    assert.ok(['caught-up', 'retained-history'].includes(view.observation.coverage)); assert.equal(task.coverage.complete, false)
    assert.equal(result.kind, 'tool-result'); assert.ok(result.text.length <= 240); assert.equal(result.data.truncated, true)
    assert.notEqual(result.authorizationEligible, true)
    assert.equal(result.data.exitCode, undefined)
    if (mode === 'late-authentication') { assert.equal(result.data.redacted, true); assert.equal(result.text, '认证材料已省略，当前证据不完整'); assert.doesNotMatch(JSON.stringify(task.events), /PRIVATE-LATE|Prefix before|Bearer/) }
  })
})

test('duplicate array fields cannot fall back to an earlier scalar or manufacture a complete result', async t => {
  const outputs = [
    '"output":[{"type":"input_text","text":"first","text":"second"}]',
    '"output":[{"type":"input_text","type":"input_text","text":"duplicate-type"}]',
    '"output":"earlier-scalar","output":[{"type":"input_text"}]',
    '"output":[{"type":"input_text","text":"earlier-array"}],"output":"later-scalar"',
  ]
  for (const type of ['function_call_output', 'custom_tool_call_output']) for (const [index, output] of outputs.entries()) await t.test(type + '/' + index, async t => {
    const suffix = '{"ordinal":4,"type":"response_item","timestamp":"' + new Date(at).toISOString() + '","payload":{"type":"' + type + '","call_id":"duplicate-output",' + output + '}}\n'
    const f = await fixture(t, { suffix }), view = await f.caughtUp()
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, index < 2 ? 'unsupported-tool-result-shape' : 'record-malformed-or-over-limit')
    assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).some(event => event.data?.callId === 'duplicate-output'), false)
  })
})

test('one fresh projection cannot splice array text or identities across payload containers and aliases', async t => {
  const body = '"type":"custom_tool_call_output","output":[{"type":"input_text","text":"One structural body"}]'
  const cases = [
    ['single-normal', '"payload":{' + body + ',"call_id":"own"}', true, 'own', 'turn'],
    ['body-then-id', '"payload":{' + body + '},"payload":{"call_id":"borrowed"}', false],
    ['id-then-body', '"payload":{"call_id":"borrowed"},"payload":{' + body + '}', false],
    ['body-then-empty', '"payload":{' + body + '},"payload":{}', false],
    ['empty-then-body', '"payload":{},"payload":{' + body + '}', false],
    ['body-then-false', '"payload":{' + body + '},"payload":false', false],
    ['false-then-body', '"payload":false,"payload":{' + body + '}', false],
    ['body-then-null', '"payload":{' + body + '},"payload":null', false],
    ['null-then-body', '"payload":null,"payload":{' + body + '}', false],
    ['body-then-array', '"payload":{' + body + '},"payload":[]', false],
    ['root-call-alias', '"payload":{' + body + '},"payload.call_id":"borrowed"', true, undefined, 'turn'],
    ['root-call-alias-after-own', '"payload":{' + body + ',"call_id":"own"},"payload.call_id":"borrowed"', true, 'own', 'turn'],
    ['root-call-alias-before-own', '"payload.call_id":"borrowed","payload":{' + body + ',"call_id":"own"}', true, 'own', 'turn'],
    ['turn-alias', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough.turn_id":"borrowed-turn"}', true, undefined, 'turn'],
    ['duplicate-turn-container', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"stale-turn"},"internal_chat_message_metadata_passthrough":{}}', false],
    ['duplicate-call-id', '"payload":{' + body + ',"call_id":"first","call_id":"second"}', false],
  ]
  for (const [name, properties, accepted, callId, runId] of cases) await t.test(name, () => {
    const input = '{"type":"response_item",' + properties + '}', bytes = Buffer.from(input)
    for (const chunk of [bytes.length, 7]) {
      const projection = new MaterialProjection()
      for (let offset = 0; offset < bytes.length; offset += chunk) projection.write(bytes.subarray(offset, offset + chunk))
      let event
      try { event = materialRecord(projection.finish(), 'turn', 0, bytes.length) }
      catch (error) { assert.equal(accepted, false); assert.equal(error.message, 'ambiguous material structure'); continue }
      if (!accepted) assert.ok(!event?.text || event.kind === 'gap')
      else { assert.equal(event.text, 'One structural body'); assert.equal(event.data.callId, callId); assert.equal(event.data.runId, runId) }
    }
  })
})

test('ambiguous payload structures stay gray through the production chain after known completion and recovery', async t => {
  const body = '"type":"custom_tool_call_output","output":[{"type":"input_text","text":"Do not publish a spliced body"}]'
  const cases = [
    ['body-then-id', '"payload":{' + body + '},"payload":{"call_id":"borrowed"}'],
    ['id-then-body', '"payload":{"call_id":"borrowed"},"payload":{' + body + '}'],
    ['body-then-empty', '"payload":{' + body + '},"payload":{}'],
    ['body-then-false', '"payload":{' + body + '},"payload":false'],
    ['null-then-body', '"payload":null,"payload":{' + body + '}'],
    ['duplicate-call', '"payload":{' + body + ',"call_id":"first","call_id":"second"}'],
    ['duplicate-context', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"turn"},"internal_chat_message_metadata_passthrough":{}}'],
    ['foreign-context-then-empty', '"payload":{' + body + ',"internal_chat_message_metadata_passthrough":{"turn_id":"foreign-turn"},"internal_chat_message_metadata_passthrough":{}}'],
  ]
  for (const [name, properties] of cases) await t.test(name, async t => {
    const f = await fixture(t); await f.caughtUp()
    const before = (await f.core.snapshot()).tasks[0], original = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), retained = (await f.core.diagnostics()).retained.nativeCaptureProgress.records
    f.advance()
    await fs.appendFile(f.path, '{"ordinal":4,"type":"response_item","timestamp":"' + new Date(at).toISOString() + '",' + properties + '}\n' + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp(), after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'record-malformed-or-over-limit')
    assert.equal(after.status, 'unknown'); assert.equal(after.coverage.complete, false); assert.equal(after.lastActivity, before.lastActivity)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), original)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, retained)
    await f.reopen(); view = await f.read(); after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false)
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), original)
    assert.equal(after.events.some(event => event.data.nativeType === 'task_complete'), false)
  })
})

test('the current Host still stops unknown started and completed item types before a later known end or adapter recovery', async t => {
  for (const type of ['item_started', 'item_completed']) await t.test(type, async t => {
    const f = await fixture(t); await f.caughtUp()
    const first = (await f.core.snapshot()).tasks[0], captures = first.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text])
    await fs.appendFile(f.path, row(4, 'event_msg', { type, thread_id: owner, turn_id: 'turn', item: { type: 'SyntheticOpaqueItem', query: 'unknown-body' } }) + row(5, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
    let view = await f.caughtUp()
    assert.match(view.observation.gap, /^unsupported-item-type:[a-f0-9]{64}$/); assert.equal(view.signal, 'gray')
    await f.reopen(); view = await f.read()
    const after = (await f.core.snapshot()).tasks[0]
    assert.equal(view.signal, 'gray'); assert.equal(after.coverage.complete, false); assert.equal(after.status, 'unknown')
    assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text]), captures)
    assert.equal(after.events.some(event => event.data?.nativeType === 'task_complete'), false)
  })
})

test('current-profile completion and cancellation preserve their distinct outcomes in plain and synthetic zstd regression sources', async t => {
  for (const compressed of [false, true]) for (const end of ['task_complete', 'turn_aborted']) await t.test([compressed, end].join(':'), async t => {
    const f = await fixture(t, { compressed, suffix: row(4, 'event_msg', { type: end, turn_id: 'turn' }) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
    assert.equal(view.observation.phase, end === 'task_complete' ? 'completed' : 'aborted')
    assert.equal(task.status, 'idle'); assert.equal(task.currentRequest, 'Inspect only the synthetic log')
    assert.ok(task.events.some(event => event.data?.nativeType === end))
    assert.equal(task.events.some(event => event.data?.nativeType === (end === 'task_complete' ? 'turn_aborted' : 'task_complete')), false)
    await f.reopen(); await f.read()
    assert.equal((await f.core.snapshot()).tasks[0].events.filter(event => event.data?.nativeType === end).length, 1)
  })
})

test('current-profile composer and nonhuman triggers retain separate authorization semantics', async t => {
  for (const trigger of ['composer', 'goal']) await t.test(trigger, async t => {
    const f = await fixture(t, { records: row(1, 'event_msg', start('turn', trigger)) + row(2, 'event_msg', user(owner, 'turn')) + row(3, 'event_msg', agent(owner, 'turn')) }), view = await f.caughtUp(), task = (await f.core.snapshot()).tasks[0]
    assert.equal(view.observation.coverage, 'caught-up')
    if (trigger === 'composer') { assert.equal(task.currentRequest, 'Inspect only the synthetic log'); assert.equal(task.events.filter(event => event.kind === 'input' && event.authorizationEligible).length, 1) }
    else { assert.equal(task.currentRequest, ''); assert.equal(task.events.some(event => event.kind === 'input'), false); assert.equal(view.signal, 'gray') }
  })
})

test('admitting the new Host does not migrate an old profile checkpoint or refresh already captured evidence', async t => {
  const f = await fixture(t, { profile: supportedProfile }); await f.caughtUp()
  const before = (await f.core.snapshot()).tasks[0], captures = before.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence])
  f.state.profile = candidateHostProfile; f.state.currentRecordReads = 0
  await f.reopen()
  const view = await f.read(), after = (await f.core.snapshot()).tasks[0]
  assert.equal(view.signal, 'gray'); assert.equal(view.observation.gap, 'cursor-invalid')
  assert.equal(f.state.currentRecordReads, 0)
  assert.deepEqual(after.events.map(event => [event.data.nativeRecordId, event.capturedAt, event.text, event.evidence]), captures)
})
