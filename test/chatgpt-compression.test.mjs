import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { zstdCompressSync } from 'node:zlib'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'

async function fixture(t, padding = '') {
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-codec-'))
  const owner = '00000000-0000-4000-8000-000000000001'
  const row = (ordinal, type, payload) => JSON.stringify({ ordinal, type, payload, timestamp: '2026-10-16T00:00:05.000Z' }) + '\n'
  const bytes = Buffer.from(row(0, 'session_meta', { id: owner, cwd: '/project', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
    + row(1, 'event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + row(2, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'UserMessage', id: 'user', content: [{ type: 'text', text: 'Inspect my project' }] } })
    + row(3, 'response_item', { type: 'message', role: 'assistant', content: [{ type: 'output_text', text: padding || 'Checking the project' }] })
    + row(4, 'event_msg', { type: 'task_complete', turn_id: 'turn' }))
  const path = join(dir, 'rollout-' + owner + '.jsonl')
  await fs.writeFile(path, bytes); await fs.writeFile(path + '.zst', zstdCompressSync(bytes))
  const readers = []
  t.after(async () => { for (const reader of readers) await reader.close(); await fs.rm(dir, { recursive: true, force: true }) })
  const open = (compressed, cursor = null) => {
    const reader = new BoundedReader({ path: path + (compressed ? '.zst' : ''), allowedRoot: dir, threadId: owner, parentId: null, hostRunId: 'host', hostStartedAt: Date.parse('2026-10-16T00:00:00.000Z'), sourceKind: 'vscode', originator: 'Codex Desktop', profile: supportedProfile, anchorTurnId: 'turn', nativeOriginVersion: 2 }, cursor, { mode: 'material' })
    readers.push(reader); return reader
  }
  return { dir, bytes, path, open }
}

async function collect(reader) {
  const rows = []; let view
  for (let index = 0; index < 100; index++) {
    view = await reader.pump()
    assert.ok(view.readBytes <= 262144)
    assert.ok((view.decodedBytes ?? view.readBytes) <= 262144)
    rows.push(...view.materials, ...view.recentLifecycle)
    if (view.coverage === 'gap' || view.coverage === 'caught-up') break
  }
  assert.equal(view.coverage, 'caught-up', JSON.stringify({ coverage: view.coverage, gap: view.gap }))
  return rows
}

test('plain and zstd observations of one native storage generation preserve record identity and material', async t => {
  const f = await fixture(t), plain = f.open(false), compressed = f.open(true)
  const a = await collect(plain), b = await collect(compressed)
  assert.equal(a.length, 4); assert.equal(b.length, 4)
  assert.deepEqual(b.map(row => [row.data?.nativeRecordId ?? row.nativeRecordId, row.text]), a.map(row => [row.data?.nativeRecordId ?? row.nativeRecordId, row.text]))
  assert.equal(compressed.checkpoint().nativeOriginVersion, 2)
  await compressed.close(); assert.equal(compressed.handle, null)
})

test('a retained plaintext metadata path reads the same archived generation after zstd compaction', async t => {
  const f = await fixture(t), plain = f.open(false)
  const expected = await collect(plain)
  const checkpoint = JSON.parse(JSON.stringify(plain.checkpoint()))
  await plain.close(); await fs.rm(f.path)
  const reader = f.open(false), rows = []; let view
  for (let pump = 0; pump < 20; pump++) {
    view = await reader.pump(); rows.push(...view.materials, ...view.recentLifecycle)
    if (['caught-up', 'gap'].includes(view.coverage)) break
  }
  assert.equal(view.coverage, 'caught-up', JSON.stringify({ gap: view.gap, error: view.error }))
  assert.deepEqual(rows.map(row => [row.data?.nativeRecordId ?? row.nativeRecordId, row.text]), expected.map(row => [row.data?.nativeRecordId ?? row.nativeRecordId, row.text]))
  await reader.close()
  const restored = f.open(false, checkpoint)
  assert.deepEqual(await collect(restored), [], 'compaction does not replay acknowledged material')
  assert.equal(restored.checkpoint().offset, checkpoint.offset)
})

test('a missing plaintext and compressed record keeps its precise ENOENT failure', async t => {
  const f = await fixture(t)
  await fs.rm(f.path); await fs.rm(f.path + '.zst')
  const view = await f.open(false).pump()
  assert.equal(view.coverage, 'gap'); assert.equal(view.gap, 'record-source-unavailable')
  assert.deepEqual(view.error, { code: 'ENOENT' })
  assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, [])
})

test('an existing plaintext record stays authoritative over a compressed sibling', async t => {
  const f = await fixture(t)
  await fs.writeFile(f.path + '.zst', 'invalid compressed sibling')
  assert.equal((await collect(f.open(false))).length, 4)
})

test('plaintext permission failure does not select an available compressed sibling', async t => {
  const f = await fixture(t), lstat = fs.lstat
  t.mock.method(fs, 'lstat', async (path, ...args) => {
    if (String(path) === f.path) throw Object.assign(new Error('permission denied'), { code: 'EACCES' })
    return lstat(path, ...args)
  })
  const view = await f.open(false).pump()
  assert.equal(view.coverage, 'gap'); assert.deepEqual(view.error, { code: 'EACCES' })
  assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, [])
})

test('compressed sibling resolution rejects an escape and a dangling plaintext symlink', async t => {
  const f = await fixture(t), outside = await fs.mkdtemp(join(tmpdir(), 'sentinel-codec-outside-'))
  t.after(() => fs.rm(outside, { recursive: true, force: true }))
  const target = join(outside, 'record.jsonl.zst')
  await fs.rename(f.path + '.zst', target); await fs.rm(f.path)
  await fs.symlink(target, f.path + '.zst')
  const escaped = await f.open(false).pump()
  assert.equal(escaped.gap, 'record-path-unverified'); assert.deepEqual(escaped.materials, [])
  await fs.rm(f.path + '.zst'); await fs.copyFile(target, f.path + '.zst')
  await fs.symlink(join(f.dir, 'missing.jsonl'), f.path)
  const dangling = await f.open(false).pump()
  assert.equal(dangling.gap, 'record-source-unavailable'); assert.deepEqual(dangling.error, { code: 'ENOENT' })
  assert.deepEqual(dangling.materials, [])
})

test('a compressed sibling with a corrupt frame publishes no material', async t => {
  const f = await fixture(t)
  await fs.rm(f.path)
  const bytes = await fs.readFile(f.path + '.zst'); await fs.writeFile(f.path + '.zst', bytes.subarray(0, bytes.length - 2))
  const reader = f.open(false), rows = []; let view
  for (let pump = 0; pump < 20; pump++) { view = await reader.pump(); rows.push(...view.materials, ...view.recentLifecycle); if (view.coverage === 'gap') break }
  assert.equal(view.gap, 'compressed-data-invalid'); assert.deepEqual(rows, [])
})

test('high-ratio compressed input counts decoded production independently and releases a partial decoder on close', async t => {
  const f = await fixture(t, 'x'.repeat(3 * 1024 * 1024)), reader = f.open(true)
  let physical = 0, decoded = 0, view
  for (let pump = 0; pump < 100; pump++) {
    view = await reader.pump()
    physical += view.readBytes; decoded += view.decodedBytes
    assert.ok(view.readBytes <= 262144); assert.ok(view.decodedBytes <= 262144)
    assert.notEqual(view.coverage, 'gap', view.gap)
    if (pump === 0) { assert.equal(view.coverage, 'catching-up'); assert.equal(view.materials.length, 0); assert.equal(view.recentLifecycle.length, 0) }
    if (view.coverage === 'caught-up') break
  }
  assert.equal(view.coverage, 'caught-up')
  assert.ok(decoded >= f.bytes.length * 2, 'full-frame validation and material replay both spend decoded budget')
  assert.ok(physical < decoded / 100)
  const partial = f.open(true); await partial.pump(); await partial.close(); await partial.close()
  assert.equal(partial.handle, null)
})

test('JSON checkpoint restore replays the codec with bounded work and preserves native identity', async t => {
  const f = await fixture(t, 'p'.repeat(400000)), reader = f.open(true)
  const first = await collect(reader), checkpoint = JSON.parse(JSON.stringify(reader.checkpoint()))
  await reader.close()
  const restored = f.open(true, checkpoint), rows = await collect(restored)
  assert.deepEqual(rows, [], 'acknowledged output is not re-emitted after bounded replay')
  assert.equal(restored.checkpoint().offset, checkpoint.offset)
  assert.equal(first[0].data?.nativeRecordId ?? first[0].nativeRecordId, (await collect(f.open(false)))[0].data?.nativeRecordId ?? (await collect(f.open(false)))[0].nativeRecordId)
})

test('a bad compressed tail is rejected before any material can be captured', async t => {
  for (const corrupt of [bytes => Buffer.concat([bytes, Buffer.from('garbage')]), bytes => bytes.subarray(0, bytes.length - 2), bytes => Buffer.concat([bytes, bytes])]) await t.test(corrupt.toString(), async t => {
    const f = await fixture(t), path = f.path + '.zst'
    await fs.writeFile(path, corrupt(await fs.readFile(path)))
    const reader = f.open(true), rows = []; let view
    for (let pump = 0; pump < 20; pump++) { view = await reader.pump(); rows.push(...view.materials, ...view.recentLifecycle); if (view.coverage === 'gap') break }
    assert.equal(view.coverage, 'gap'); assert.equal(view.gap, 'compressed-data-invalid'); assert.deepEqual(rows, [])
  })
})

test('four live compressed leases bound decoder allocation and queued readers resume after close', async t => {
  const f = await fixture(t, 'x'.repeat(2 * 1024 * 1024)), readers = Array.from({ length: 20 }, () => f.open(true))
  for (const reader of readers.slice(0, 4)) assert.equal((await reader.pump()).coverage, 'catching-up')
  for (const reader of readers.slice(4)) { const queued = await reader.pump(); assert.equal(queued.readBytes, 0); assert.equal(queued.decodedBytes, 0); assert.equal(queued.coverage, 'catching-up'); assert.equal(reader.handle, null) }
  await readers[0].close()
  assert.equal((await readers[4].pump()).coverage, 'catching-up'); assert.ok(readers[4].handle)
})

test('compressed siblings keep the existing four-decoder limit and close queued readers', async t => {
  const f = await fixture(t, 'x'.repeat(2 * 1024 * 1024))
  await fs.rm(f.path)
  const readers = Array.from({ length: 8 }, () => f.open(false))
  for (const reader of readers.slice(0, 4)) assert.equal((await reader.pump()).coverage, 'catching-up')
  for (const reader of readers.slice(4)) { const queued = await reader.pump(); assert.equal(queued.readBytes, 0); assert.equal(queued.decodedBytes, 0); assert.equal(reader.handle, null) }
  await readers[0].close()
  assert.equal((await readers[4].pump()).coverage, 'catching-up'); assert.ok(readers[4].handle)
  await Promise.all(readers.map(reader => reader.close()))
  assert.ok(readers.every(reader => reader.handle === null))
})


test('an oversized zstd window is rejected before a decoder can produce material', async t => {
  const f = await fixture(t)
  await fs.writeFile(f.path + '.zst', Buffer.from([0x28, 0xb5, 0x2f, 0xfd, 0x80, 0xa0, 0, 0, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
  const reader = f.open(true), view = await reader.pump()
  assert.equal(view.gap, 'compressed-window-unverified'); assert.equal(view.decodedBytes, 0)
  assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, []); assert.equal(reader.handle, null)
})

test('verify-only restoration refuses an immutable compressed file changed after its checkpoint', async t => {
  const f = await fixture(t), reader = f.open(true)
  await collect(reader)
  const checkpoint = JSON.parse(JSON.stringify(reader.checkpoint())); await reader.close()
  const bytes = await fs.readFile(f.path + '.zst'); bytes[bytes.length - 1] ^= 1; await fs.writeFile(f.path + '.zst', bytes)
  const restored = f.open(true, checkpoint), view = await restored.pump({ verifyOnly: true })
  assert.equal(view.gap, 'record-rewrite-suspected'); assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, [])
})

test('closing during an owned physical read prevents late decoded material and releases that file', async t => {
  const f = await fixture(t), reader = f.open(true), open = fs.open, target = await fs.realpath(f.path + '.zst')
  let release, entered
  const gate = new Promise(resolve => { release = resolve }), started = new Promise(resolve => { entered = resolve })
  t.mock.method(fs, 'open', async (path, ...args) => {
    const handle = await open(path, ...args)
    if (String(path) === target) { const read = handle.read; handle.read = async function (...values) { entered(); await gate; return read.apply(this, values) } }
    return handle
  })
  const pending = reader.pump(); await started
  const closing = reader.close(); release()
  const view = await pending; await closing
  assert.equal(view.coverage, 'gap'); assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, []); assert.equal(reader.handle, null)
})
