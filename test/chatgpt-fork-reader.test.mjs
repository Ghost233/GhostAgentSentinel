import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'

const at = 1_800_000_000_000
const row = (ordinal, type, payload, time = at - 100_000) => JSON.stringify({ ordinal, type, timestamp: new Date(time).toISOString(), payload }) + '\n'
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
