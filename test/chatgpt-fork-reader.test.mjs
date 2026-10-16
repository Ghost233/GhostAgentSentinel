import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { NativeJsonlSource } from '../chatgpt/native-jsonl-source.mjs'

const at = 1_800_000_000_000
const row = (ordinal, type, payload, time = at - 100_000) => JSON.stringify({ ordinal, type, timestamp: new Date(time).toISOString(), payload: type === 'session_meta' ? { originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, cwd: '/history-project', ...payload } : payload }) + '\n'
export async function forkFixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-fork-reader-')), sessionsRoot = join(dir, 'sessions'), archiveRoot = join(dir, 'archived_sessions')
  const { mkdir } = await import('node:fs/promises'); await mkdir(sessionsRoot); await mkdir(archiveRoot)
  const parentPath = join(archiveRoot, 'parent.jsonl'), forkPath = join(sessionsRoot, 'fork.jsonl')
  const inherited = row(0, 'session_meta', { id: 'original', history_mode: 'paginated' })
    + row(1, 'event_msg', { type: 'task_started', turn_id: 'old-run', turn_attribution: { turn_id: 'old-run', turn_trigger: 'composer' } })
    + row(2, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'UserMessage', id: 'old-user', content: [{ type: 'text', text: 'Keep changes in the requested directory' }] } })
    + row(3, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'AgentMessage', id: 'promise', content: [{ type: 'Text', text: 'I will change all directories' }] } })
    + row(4, 'event_msg', { type: 'item_completed', thread_id: 'original', turn_id: 'old-run', item: { type: 'CommandExecution', id: 'old-test', aggregated_output: 'Old tests passed', exit_code: 0 } })
    + row(5, 'event_msg', { type: 'task_complete', turn_id: 'old-run' })
  const range = { endOrdinalExclusive: 6, endByteOffset: Buffer.byteLength(inherited) }
  await writeFile(parentPath, inherited + row(6, 'event_msg', { type: 'task_started', turn_id: 'source-later' }) + row(7, 'response_item', { type: 'message', role: 'user', content: [{ type: 'input_text', text: 'Source later unrelated request' }] }))
  await writeFile(forkPath, row(6, 'session_meta', { id: 'fork', history_mode: 'paginated', forked_from_id: 'original', forked_from_ordinal_exclusive: 6, history_base: { thread_id: 'original', end_ordinal_exclusive: 6, end_byte_offset: range.endByteOffset } }, at)
    + row(7, 'event_msg', { type: 'thread_settings_applied', thread_id: 'fork' }, at)
    + row(8, 'event_msg', { type: 'task_started', turn_id: 'new-run', turn_attribution: { turn_id: 'new-run', turn_trigger: 'composer' } }, at)
    + row(9, 'event_msg', { type: 'item_completed', thread_id: 'fork', turn_id: 'new-run', item: { type: 'UserMessage', id: 'new-user', content: [{ type: 'text', text: 'Inspect the new request' }] } }, at))
  const source = { path: forkPath, allowedRoot: sessionsRoot, threadId: 'fork', parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'host', hostStartedAt: at - 1000, profile: supportedProfile, forkedFromId: 'original', anchorTurnId: 'new-run' }
  t.after(() => rm(dir, { recursive: true, force: true }))
  return { dir, source, parentPath, sessionsRoot, archiveRoot, range }
}

async function oldAnchorFixture(t, completed) {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), f = await forkFixture(t), old = at - 10000
  const original = (await readFile(f.source.path, 'utf8')).replaceAll(new Date(at).toISOString(), new Date(old).toISOString())
  await writeFile(f.source.path, original + row(10, 'response_item', { type: 'message', id: 'large-output', role: 'assistant', content: [{ type: 'output_text', text: 'p'.repeat(600000) }], internal_chat_message_metadata_passthrough: { turn_id: 'new-run' } }, old)
    + (completed ? row(11, 'event_msg', { type: 'task_complete', turn_id: 'new-run' }, old) : ''))
  const parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader({ ...f.source, nativeOriginVersion: 2 }, null, { native, sessionsRoot: f.sessionsRoot })
  t.after(() => reader.close())
  return { ...f, reader }
}

test('a fork error return accounts for the actual decoded bytes spent before a protocol gap', async t => {
  const f = await oldAnchorFixture(t, false), pump = NativeJsonlSource.prototype.pump
  await writeFile(f.source.path, (await readFile(f.source.path, 'utf8')).replaceAll(new Date(at - 10000).toISOString(), new Date(at).toISOString()).replace('"type":"response_item"', '"type":"unsupported_owned_record"'))
  let physical = 0, decoded = 0
  t.mock.method(NativeJsonlSource.prototype, 'pump', async function (...args) {
    try { const value = await pump.apply(this, args); physical += value.physicalBytes; decoded += value.decodedBytes; return value }
    catch (error) { physical += error.physicalBytes ?? 0; decoded += error.decodedBytes ?? 0; throw error }
  })
  let view
  for (let attempt = 0; attempt < 10; attempt++) {
    physical = 0; decoded = 0; view = await f.reader.pump()
    assert.equal(view.readBytes, physical); assert.equal(view.decodedBytes, decoded)
    if (view.coverage === 'gap') break
  }
  assert.equal(view.gap, 'unsupported-record-type')
  assert.ok(decoded > 0); assert.ok(decoded <= 262144)
  assert.equal(view.readBytes, physical)
  assert.equal(view.decodedBytes, decoded)
})

test('an old anchor completed in a later bounded pump can finish while a genuinely unfinished old anchor stays unknown', async t => {
  for (const completed of [true, false]) await t.test(completed ? 'completed-after-first-pump' : 'still-unfinished', async t => {
    const f = await oldAnchorFixture(t, completed)
    let view
    for (let pump = 0; pump < 20; pump++) {
      view = await f.reader.pump()
      assert.ok(view.readBytes <= 262144); assert.ok(view.decodedBytes <= 262144)
      assert.notEqual(view.effectivePhase, 'started', 'a start before this Host never establishes current running')
      if (view.coverage === 'caught-up') break
    }
    if (completed) {
      assert.equal(view.gap, null, JSON.stringify({ gap: view.gap, coverage: view.coverage }))
      assert.equal(view.coverage, 'caught-up'); assert.equal(view.effectivePhase, 'completed')
      assert.equal(view.forkProvenance.complete, true)
    } else {
      assert.equal(view.effectivePhase, 'unknown'); assert.equal(view.coldRead.phase, 'scan')
      assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, [])
    }
  })
})

test('fixed history mode reads only the verified ordinal and byte prefix and never calls inherited execution current', async t => {
  const f = await forkFixture(t)
  const reader = new BoundedReader({ ...f.source, path: f.parentPath, allowedRoot: f.archiveRoot, threadId: 'original', forkedFromId: null, anchorTurnId: null, historyRange: f.range }, null, { mode: 'history' })
  t.after(() => reader.close())
  const view = await reader.pump()
  assert.equal(view.coverage, 'caught-up')
  assert.equal(view.gap, null)
  assert.equal(view.effectivePhase, 'unknown')
  assert.equal(view.offset, f.range.endByteOffset)
  assert.deepEqual(view.materials.map(event => event.kind), ['input', 'output', 'tool-result'])
  assert.ok(view.materials.every(event => event.data.history && event.data.inherited && event.data.originRunId === 'old-run'))
  assert.deepEqual(view.materials.map(event => event.captureOffset), [2, 3, 4])
  assert.ok(view.recentLifecycle.every(event => event.history === true))
  assert.doesNotMatch(JSON.stringify(view), /Source later unrelated request|source-later/)
  assert.ok(view.readBytes <= 262144)
})

test('history mode rejects ordinal discontinuities and a byte boundary that cuts a record', async t => {
  for (const mutation of ['ordinal', 'boundary']) await t.test(mutation, async t => {
    const f = await forkFixture(t)
    if (mutation === 'ordinal') { const { readFile } = await import('node:fs/promises'); await writeFile(f.parentPath, (await readFile(f.parentPath, 'utf8')).replace('"ordinal":2', '"ordinal":50')) }
    const reader = new BoundedReader({ ...f.source, path: f.parentPath, allowedRoot: f.archiveRoot, threadId: 'original', forkedFromId: null, anchorTurnId: null, historyRange: mutation === 'boundary' ? { ...f.range, endByteOffset: f.range.endByteOffset - 1 } : f.range }, null, { mode: 'history' })
    t.after(() => reader.close())
    const view = await reader.pump()
    assert.equal(view.coverage, 'gap')
    assert.equal(view.gap, mutation === 'ordinal' ? 'record-ordinal-unverified' : 'history-boundary-unverified')
    assert.equal(view.effectivePhase, 'unknown')
  })
})


test('a history cursor is bound to its exact prefix and resumes without replaying already delivered bodies', async t => {
  const f = await forkFixture(t), source = { ...f.source, path: f.parentPath, allowedRoot: f.archiveRoot, threadId: 'original', forkedFromId: null, anchorTurnId: null, historyRange: f.range }
  const reader = new BoundedReader(source, null, { mode: 'history' }); t.after(() => reader.close())
  await reader.pump()
  const cursor = reader.checkpoint(), resumed = new BoundedReader(source, cursor, { mode: 'history' }); t.after(() => resumed.close())
  const view = await resumed.pump()
  assert.equal(view.coverage, 'caught-up')
  assert.deepEqual(view.materials, [])
  assert.deepEqual(view.recentLifecycle, [])
  for (const [name, change] of Object.entries({ range: value => { value.historyRange = { ...f.range, endOrdinalExclusive: 5 } }, ordinal: () => {} })) {
    const changed = structuredClone(source); change(changed)
    const invalid = new BoundedReader(changed, name === 'ordinal' ? { ...cursor, nextOrdinal: 'invalid' } : cursor, { mode: 'history' }); t.after(() => invalid.close())
    const rejected = await invalid.pump()
    assert.equal(rejected.gap, 'cursor-invalid')
    assert.equal(rejected.readBytes, 0)
  }
})

test('an explicit fork suffix validates native header and ordinal before publishing only its independent current request', async t => {
  const f = await forkFixture(t), reader = new BoundedReader(f.source, null, { mode: 'material', forkSuffix: true }); t.after(() => reader.close())
  const view = await reader.pump()
  assert.equal(view.coverage, 'caught-up')
  assert.equal(view.gap, null)
  assert.equal(view.forkProvenance.historyBase.sourceThreadId, 'original')
  assert.equal(view.forkProvenance.historyBase.endOrdinalExclusive, 6)
  assert.deepEqual(view.materials.map(item => [item.text, item.captureOffset]), [['Inspect the new request', 9]])
  assert.equal(view.materials[0].data.inherited, false)
  assert.deepEqual(view.recentLifecycle.map(item => item.captureOffset), [8])
  // This reader proves the suffix only; final fork admission requires the
  // separate bounded ancestry wrapper that remains unfinished in ticket04.
})

test('an unavailable old-generation byte range is a provenance gap with no old body read', async t => {
  const f = await forkFixture(t)
  const reader = new BoundedReader({ ...f.source, path: f.parentPath, allowedRoot: f.archiveRoot, threadId: 'original', forkedFromId: null, historyRange: { ...f.range, endByteOffset: f.range.endByteOffset + 1_000_000 } }, null, { mode: 'history' }); t.after(() => reader.close())
  const view = await reader.pump()
  assert.equal(view.gap, 'history-range-unavailable')
  assert.equal(view.readBytes, 0)
  assert.deepEqual(view.materials, [])
})

test('an unacknowledged history start replays from zero with learned file identity and no invented ordinal', async t => {
  const { createHash } = await import('node:crypto')
  const f = await forkFixture(t), source = { ...f.source, path: f.parentPath, allowedRoot: f.archiveRoot, threadId: 'original', forkedFromId: null, anchorTurnId: null, historyRange: f.range }
  const reader = new BoundedReader(source, null, { mode: 'history' }); t.after(() => reader.close()); await reader.pump()
  const replay = { ...reader.checkpoint(), offset: 0, guardHash: createHash('sha256').update('').digest('hex'), nextOrdinal: null, forkProvenance: null, state: { header: false, anchored: false, phase: 'unknown', turnId: null, at: null, orphan: false } }
  const restored = new BoundedReader(source, replay, { mode: 'history' }); t.after(() => restored.close())
  const view = await restored.pump()
  assert.equal(view.gap, null)
  assert.equal(view.materials.find(item => item.kind === 'input').captureOffset, 2)
})

test('fork wrapper resolves only explicit main ancestry, combines physical budgets, and emits history before independent current material', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  const f = await forkFixture(t), calls = []
  const parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread(id) { calls.push(id); return { thread: id === 'original' ? parent : null } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot })
  t.after(() => reader.close())
  const delivered = []; let view
  for (let attempt = 0; attempt < 20; attempt++) {
    view = await reader.pump({ budgetBytes: 1024 })
    assert.ok(view.readBytes <= 1024)
    const items = [...view.historyMaterials, ...view.historyLifecycle, ...view.historyBoundaries, ...view.materials, ...view.recentLifecycle]
    assert.ok(items.length <= 16)
    delivered.push(...items.sort((a, b) => a.captureOffset - b.captureOffset))
    if (view.coverage === 'caught-up') break
  }
  assert.equal(view.gap, null)
  assert.equal(view.coverage, 'caught-up')
  assert.equal(view.effectivePhase, 'started')
  assert.equal(view.forkProvenance.complete, true)
  assert.ok(calls.every(id => id === 'original'))
  assert.equal(delivered.find(item => item.data?.nativeType === 'item_completed/UserMessage' && item.data.history).text, 'Keep changes in the requested directory')
  assert.equal(delivered.find(item => item.data?.nativeType === 'item_completed/UserMessage' && !item.data.history).text, 'Inspect the new request')
  const boundary = delivered.find(item => item.data?.evidenceKind === 'fork-boundary')
  assert.equal(boundary.data.nativeType, 'session_meta')
  assert.equal(boundary.data.sourceForm, 'session_meta/history_base')
  assert.equal(boundary.data.endOrdinalExclusive, 6)
  assert.equal(boundary.captureOffset, 6)
  assert.deepEqual(delivered.map(item => item.captureOffset), [1, 2, 3, 4, 5, 6, 8, 9])
  const checkpoint = reader.checkpoint()
  assert.ok(Buffer.byteLength(JSON.stringify(checkpoint)) < 16384)
  assert.doesNotMatch(JSON.stringify(checkpoint), /Keep changes|Inspect the new request|Old tests passed/)
  await reader.close()
  const restored = new ForkHistoryReader(f.source, checkpoint, { native, sessionsRoot: f.sessionsRoot }); t.after(() => restored.close())
  const after = await restored.pump()
  assert.equal(after.coverage, 'caught-up')
  assert.deepEqual(after.historyMaterials, [])
  assert.deepEqual(after.materials, [])
})


test('self history resolves an older exact-ID generation by native ordinal and byte range without following creation instead', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { readFile } = await import('node:fs/promises')
  const f = await forkFixture(t), older = await readFile(f.source.path), olderPath = join(f.archiveRoot, 'rollout-test-fork.jsonl')
  await writeFile(olderPath, older)
  await writeFile(f.source.path, row(10, 'session_meta', { id: 'fork', history_mode: 'paginated', forked_from_id: 'original', history_base: { thread_id: 'fork', end_ordinal_exclusive: 10, end_byte_offset: older.length } }, at)
    + row(11, 'event_msg', { type: 'task_started', turn_id: 'next-run', turn_attribution: { turn_id: 'next-run', turn_trigger: 'composer' } }, at)
    + row(12, 'event_msg', { type: 'item_completed', thread_id: 'fork', turn_id: 'next-run', item: { type: 'UserMessage', id: 'next-user', content: [{ type: 'text', text: 'New generation request' }] } }, at))
  const fork = { id: 'fork', path: f.source.path, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: 'original' }, parent = { ...fork, id: 'original', path: f.parentPath, forkedFromId: null }
  const native = { async readThread(id) { return { thread: id === 'fork' ? fork : id === 'original' ? parent : null } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader({ ...f.source, anchorTurnId: 'next-run' }, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const materials = [], boundaries = []; let view
  for (let i = 0; i < 80; i++) { view = await reader.pump({ budgetBytes: 1024 }); assert.ok(view.readBytes <= 1024); materials.push(...view.historyMaterials, ...view.materials); boundaries.push(...view.historyBoundaries); if (view.coverage === 'caught-up' || view.coverage === 'gap') break }
  assert.equal(view.gap, null)
  assert.equal(view.forkProvenance.complete, true)
  assert.deepEqual(materials.filter(item => item.kind === 'input').map(item => item.text), ['Keep changes in the requested directory', 'Inspect the new request', 'New generation request'])
  assert.deepEqual(boundaries.map(item => [item.data.historyOwnerThreadId, item.data.sourceThreadId, item.data.endOrdinalExclusive]), [['fork', 'original', 6], ['fork', 'fork', 10]])
  assert.ok(materials.filter(item => item.text !== 'New generation request').every(item => item.data.history === true))
})

test('completed ancestry remains subject to project eligibility before any newly appended current material is released', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { appendFile } = await import('node:fs/promises')
  const f = await forkFixture(t); let enabled = true
  const parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: enabled ? 'enabled' : 'disabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  let view; for (let i = 0; i < 20; i++) { view = await reader.pump(); if (view.coverage === 'caught-up') break }
  assert.equal(view.forkProvenance.complete, true)
  enabled = false
  await appendFile(f.source.path, row(10, 'response_item', { type: 'message', id: 'fresh', role: 'assistant', content: [{ type: 'output_text', text: 'New current output must stay fenced' }], internal_chat_message_metadata_passthrough: { turn_id: 'new-run' } }, at))
  const disabled = await reader.pump()
  assert.equal(disabled.gap, 'history-project-disabled')
  assert.deepEqual(disabled.materials, [])
  assert.equal(disabled.readBytes, 0)
})

test('save-before-ingest replay preserves stable logical deliveries after a lost HTTP receipt without storing bodies in cursor', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const before = reader.checkpoint(), first = await reader.pump(), replay = reader.replayCheckpoint(before)
  const ids = view => [...view.historyLifecycle, ...view.historyMaterials, ...view.historyBoundaries, ...view.recentLifecycle, ...view.materials].map(item => item.captureOffset).sort((a, b) => a - b)
  assert.deepEqual(ids(first), [1, 2, 3, 4, 5, 6, 8, 9])
  assert.doesNotMatch(JSON.stringify(replay), /Keep changes|Inspect the new request|Old tests passed/)
  assert.ok(Buffer.byteLength(JSON.stringify(replay)) < 15000)
  await reader.close()
  const recovered = new ForkHistoryReader(f.source, replay, { native, sessionsRoot: f.sessionsRoot }); t.after(() => recovered.close())
  const resent = await recovered.pump()
  assert.equal(resent.gap, null)
  assert.deepEqual(ids(resent), ids(first))
  assert.equal(resent.captureThroughOffset, 9)
})

test('temporary host loss releases no buffered fork data and a fresh valid host can resume the same bounded reader', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  let calls = 0
  const native = { async readThread() { calls++; return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const absent = await reader.pump({ available: false })
  assert.equal(absent.gap, 'source-unavailable')
  assert.equal(absent.readBytes, 0)
  assert.equal(calls, 0)
  assert.deepEqual(absent.materials, [])
  let resumed; for (let i = 0; i < 10; i++) { resumed = await reader.pump({ available: true }); if (resumed.coverage === 'caught-up') break }
  assert.equal(resumed.gap, null)
  assert.equal(resumed.forkProvenance.complete, true)
})

test('buffered suffix data cannot cross the host generation or profile fence while ancestry is catching up', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { stat } = await import('node:fs/promises')
  for (const kind of ['generation', 'profile']) await t.test(kind, async t => {
    const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
    const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
    const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
    const first = await reader.pump({ budgetBytes: (await stat(f.source.path)).size + 64 })
    assert.equal(first.coverage, 'catching-up')
    const changed = await reader.pump(kind === 'generation' ? { hostRunId: 'different-host' } : { profile: { ...supportedProfile, appBuild: 'different' } })
    assert.equal(changed.gap, kind === 'generation' ? 'source-generation-changed' : 'unsupported-host-version')
    assert.equal(changed.readBytes, 0)
    assert.deepEqual(changed.materials, [])
    assert.equal(changed.forkProvenance.complete, false)
  })
})

test('the historical header producer project must be enabled independently of the source current metadata project', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  for (const enabled of [false, true]) await t.test(enabled ? 'enabled-producer' : 'disabled-producer', async t => {
    const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
    const native = { async readThread() { return { thread: parent } }, async readEnablement(cwd) { return { user: 'enabled', effective: cwd === '/history-project' && !enabled ? 'disabled' : 'enabled' } } }
    const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
    let view; const history = []; for (let i = 0; i < 20; i++) { view = await reader.pump(); history.push(...view.historyMaterials); if (view.coverage === 'gap' || view.coverage === 'caught-up') break }
    if (enabled) { assert.equal(view.gap, null); assert.ok(history.some(item => item.text === 'Keep changes in the requested directory')) }
    else { assert.equal(view.gap, 'history-project-disabled'); assert.deepEqual(history, []); assert.deepEqual(view.materials, []) }
  })
})

test('a matching duplicate generation remains ambiguous rather than being chosen by mtime', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { readFile, utimes } = await import('node:fs/promises')
  const f = await forkFixture(t), copy = join(f.archiveRoot, 'rollout-copy-original.jsonl')
  await writeFile(copy, await readFile(f.parentPath)); await utimes(copy, new Date(0), new Date(0))
  const parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const view = await reader.pump()
  assert.equal(view.gap, 'history-generation-ambiguous')
  assert.deepEqual(view.historyMaterials, [])
  assert.deepEqual(view.materials, [])
})

test('closing during delayed ancestry qualification discards the late result and leaves no owned readers or buffers', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  let release, reached
  const waiting = new Promise(resolve => { reached = resolve }), gate = new Promise(resolve => { release = resolve })
  const native = { async readThread() { reached(); await gate; return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const pending = reader.pump(); await waiting
  const closing = reader.close(); release()
  const late = await pending; await closing
  assert.equal(late.gap, 'reader-closed')
  assert.deepEqual(late.materials, [])
  assert.deepEqual(late.historyMaterials, [])
  assert.deepEqual(reader.diagnostics(), { readers: 0, pendingEvents: 0, ancestryNodes: 0, metadataCalls: 1, maximumMetadataCalls: 1, busy: false })
})

test('a cached suffix is not published after its underlying record is truncated during ancestry catch-up', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { stat } = await import('node:fs/promises')
  const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const first = await reader.pump({ budgetBytes: (await stat(f.source.path)).size + 64 }); assert.equal(first.coverage, 'catching-up')
  await writeFile(f.source.path, '')
  const stale = await reader.pump()
  assert.equal(stale.gap, 'record-file-truncated')
  assert.deepEqual(stale.materials, [])
  assert.deepEqual(stale.historyMaterials, [])
})

test('four physical ancestry nodes including a mapped record keep cursor private and bounded while each pump delivers at most sixteen records', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { readFile } = await import('node:fs/promises')
  const f = await forkFixture(t), allParent = await readFile(f.parentPath), originalPrefix = allParent.subarray(0, f.range.endByteOffset)
  const threads = new Map([['original', { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }]])
  const mappedOwner = '00000000-0000-4000-8000-000000000001', recordKey = '00000000-0000-4000-8000-000000000002'
  let parentId = 'original', parentRecordId = 'original', endOrdinal = 6, parentBytes = originalPrefix.length
  for (const id of [mappedOwner, 'middle-two']) {
    const path = join(f.sessionsRoot, 'rollout-' + id + (id === mappedOwner ? '_' + recordKey : '') + '.jsonl')
    const contents = row(endOrdinal, 'session_meta', { id, history_mode: 'paginated', forked_from_id: parentId, history_base: { thread_id: parentRecordId, end_ordinal_exclusive: endOrdinal, end_byte_offset: parentBytes } })
      + row(endOrdinal + 1, 'event_msg', { type: 'task_started', turn_id: id + '-run', turn_attribution: { turn_id: id + '-run', turn_trigger: 'composer' } })
      + row(endOrdinal + 2, 'event_msg', { type: 'item_completed', thread_id: id, turn_id: id + '-run', item: { type: 'UserMessage', id: id + '-user', content: [{ type: 'text', text: id + ' restriction' }] } })
      + row(endOrdinal + 3, 'event_msg', { type: 'task_complete', turn_id: id + '-run' })
    await writeFile(path, contents); threads.set(id, { ...threads.get('original'), id, path, forkedFromId: parentId }); parentId = id; parentRecordId = id === mappedOwner ? recordKey : id; parentBytes = Buffer.byteLength(contents); endOrdinal += 4
  }
  let current = row(14, 'session_meta', { id: 'fork', history_mode: 'paginated', forked_from_id: parentId, history_base: { thread_id: parentId, end_ordinal_exclusive: 14, end_byte_offset: parentBytes } }, at)
    + row(15, 'event_msg', { type: 'task_started', turn_id: 'current', turn_attribution: { turn_id: 'current', turn_trigger: 'composer' } }, at)
    + row(16, 'event_msg', { type: 'item_completed', thread_id: 'fork', turn_id: 'current', item: { type: 'UserMessage', id: 'current-user', content: [{ type: 'text', text: 'Current independent task' }] } }, at)
  for (let ordinal = 17; ordinal < 22; ordinal++) current += row(ordinal, 'response_item', { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: 'New output ' + ordinal }], internal_chat_message_metadata_passthrough: { turn_id: 'current' } }, at)
  await writeFile(f.source.path, current)
  const native = { async readThread(id) { return { thread: threads.get(id === recordKey ? mappedOwner : id) } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader({ ...f.source, anchorTurnId: 'current', forkedFromId: parentId }, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const all = []; let view
  for (let i = 0; i < 100; i++) { view = await reader.pump({ budgetBytes: 4096 }); const delivered = [...view.materials, ...view.recentLifecycle, ...view.historyMaterials, ...view.historyLifecycle, ...view.historyBoundaries]; assert.ok(view.readBytes <= 4096); assert.ok(delivered.length <= 16); all.push(...delivered); if (view.coverage === 'caught-up' || view.coverage === 'gap') break }
  assert.equal(view.gap, null); assert.equal(view.forkProvenance.complete, true)
  assert.equal(reader.diagnostics().ancestryNodes, 4)
  assert.equal(all.length, 21)
  const checkpoint = JSON.stringify(reader.checkpoint())
  t.diagnostic('four-node reader checkpoint bytes: ' + Buffer.byteLength(checkpoint))
  assert.ok(Buffer.byteLength(checkpoint) < 15000, 'space remains for the outer sixteen first-capture identities')
  assert.doesNotMatch(checkpoint, /restriction|New output|Keep changes|Current independent/)
  await reader.close(); assert.equal(reader.diagnostics().readers, 0)
})

test('a malformed qualified ancestry record remains a definite safe collection failure instead of a missing generation guess', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs'), { readFile } = await import('node:fs/promises')
  const f = await forkFixture(t), original = await readFile(f.parentPath, 'utf8')
  await writeFile(f.parentPath, original.replace('"ordinal":2', '"ordinal":x'))
  const parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const view = await reader.pump()
  assert.equal(view.gap, 'record-malformed-or-over-limit')
  assert.equal(view.error.code, 'record-malformed-or-over-limit')
  assert.equal(view.error.stage, 'collection')
  assert.equal(Object.hasOwn(view.error, 'message'), false)
  assert.deepEqual(view.historyMaterials, [])
})

test('fork wrapper refuses ancestry outside roots, child identity or a disabled source project without reading its body', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  for (const variant of ['path', 'child', 'disabled']) await t.test(variant, async t => {
    const f = await forkFixture(t)
    const parent = { id: 'original', path: variant === 'path' ? join(f.dir, 'outside.jsonl') : f.parentPath, cwd: f.dir, parentThreadId: variant === 'child' ? 'another' : null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
    const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: variant === 'disabled' ? 'disabled' : 'enabled' } } }
    const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
    const view = await reader.pump()
    assert.equal(view.coverage, 'gap')
    assert.match(view.gap, variant === 'disabled' ? /history-project-disabled/ : /history-source-unverified/)
    assert.deepEqual(view.materials, [])
    assert.deepEqual(view.historyMaterials, [])
  })
})

test('fork IO diagnostics count ancestry verification and material reads together with the current suffix', async t => {
  const { ForkHistoryReader } = await import('../chatgpt/fork-history.mjs')
  const f = await forkFixture(t), parent = { id: 'original', path: f.parentPath, cwd: f.dir, parentThreadId: null, originator: 'Codex Desktop', source: 'vscode', forkedFromId: null }
  const native = { async readThread() { return { thread: parent } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } } }
  const reader = new ForkHistoryReader(f.source, null, { native, sessionsRoot: f.sessionsRoot }); t.after(() => reader.close())
  const first = await reader.pump()
  assert.equal(first.metrics.physicalBytes, first.readBytes)
  assert.equal(first.metrics.maximumReadBytes, first.readBytes)
  const second = await reader.pump()
  assert.equal(second.metrics.physicalBytes, first.readBytes + second.readBytes)
})
