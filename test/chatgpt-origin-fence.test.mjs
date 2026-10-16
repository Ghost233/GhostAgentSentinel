import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { startServer } from '../src/server.mjs'
import { DatabaseSync } from 'node:sqlite'
import { nativeStorageKey } from '../chatgpt/native-jsonl-source.mjs'
import { zstdCompressSync } from 'node:zlib'
import { BoundedReader, supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { connectCore } from '../src/core-client.mjs'

const hash = text => createHash('sha256').update(text).digest('hex')
async function fixture(t) {
  let clock = Date.now(); t.mock.method(Date, 'now', () => clock)
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-origin-')); let server, core
  const start = async () => { server = await startServer({ dataDir: dir, now: () => clock }); core = await connectCore({ dataDir: dir }) }
  t.after(async () => { await core?.close(); await server?.close(); await rm(dir, { recursive: true, force: true }) })
  await start()
  return { dir, advance(ms = 500) { clock += ms }, get now() { return clock }, get core() { return core }, register: (version, host = 'host') => core.registerSource({ registrationId: 'chatgpt:origin', hostRunId: host, name: 'Owned origin fixture', policy: 'chatgpt-main/1', ...(version === 2 ? { nativeOriginVersion: 2 } : {}) }), async restart() { await core.close(); await server.close(); await start() } }
}

test('a nonempty legacy registration cannot mint codec-neutral identities by changing host or source version', async t => {
  const f = await fixture(t), source = await f.register(1)
  await f.core.ingest(source, { tasks: [{ id: 'main', lifecycleId: 'run', title: 'Owned', status: 'idle' }], events: [{ id: 'old', taskId: 'main', taskLifecycleId: 'run', kind: 'output', capturedAt: Date.now(), occurredAt: null, text: 'Old retained body', data: { sourceKind: 'chatgpt-native', nativeRecordId: hash('opaque-v1'), nativeType: 'item_completed/AgentMessage' } }] })
  await assert.rejects(f.register(2, 'new-host'), error => error.code === 'native_origin_upgrade_unverified')
  await f.restart()
  await assert.rejects(f.register(2, 'after-restart'), error => error.code === 'native_origin_upgrade_unverified')
  assert.equal((await f.core.snapshot()).tasks[0].events[0].text, 'Old retained body')
})

async function nativeFile(t, f) {
  const owner = '00000000-0000-4000-8000-000000000004', path = join(f.dir, 'rollout-' + owner + '.jsonl')
  const row = (ordinal, type, payload) => JSON.stringify({ ordinal, type, payload, timestamp: new Date(f.now - 1000).toISOString() }) + '\n'
  const bytes = row(0, 'session_meta', { id: owner, cwd: '/owned', originator: 'Codex Desktop', source: 'vscode', cli_version: supportedProfile.cliVersion, history_mode: 'paginated' })
    + row(1, 'event_msg', { type: 'task_started', turn_id: 'turn', turn_attribution: { turn_id: 'turn', turn_trigger: 'composer' } })
    + row(2, 'event_msg', { type: 'item_completed', thread_id: owner, turn_id: 'turn', item: { type: 'AgentMessage', id: 'agent', content: [{ type: 'Text', text: 'Original owned body' }] } })
    + row(3, 'event_msg', { type: 'task_complete', turn_id: 'turn' })
  await writeFile(path, bytes); await writeFile(path + '.zst', zstdCompressSync(Buffer.from(bytes)))
  const readers = []
  t.after(async () => { for (const reader of readers) await reader.close() })
  const open = (source, compressed = false, range = null) => { const reader = new BoundedReader({ path: path + (compressed ? '.zst' : ''), allowedRoot: f.dir, threadId: owner, parentId: null, sourceKind: 'vscode', originator: 'Codex Desktop', hostRunId: 'host', hostStartedAt: f.now - 10000, profile: supportedProfile, anchorTurnId: 'turn', nativeOriginVersion: 2, ...(range ? { historyRange: range } : {}) }, null, { mode: range ? 'history' : 'material', origins: { read: key => f.core.nativeOriginFence(source, key) } }); readers.push(reader); return reader }
  async function send(source, reader, task) {
    let view
    for (let pump = 0; pump < 100; pump++) {
      view = await reader.pump(); assert.notEqual(view.coverage, 'gap', view.gap)
      const rows = [...view.materials.map(event => ({ ...event, data: event.data })), ...view.recentLifecycle.map(event => ({ ...event, kind: 'status', text: event.type, data: { sourceKind: 'chatgpt-native', ...(event.history ? { history: true, inherited: true, sourceThreadId: event.sourceThreadId, originRunId: event.originRunId } : {}), nativeType: event.type, nativeRecordId: event.nativeRecordId, nativeGenerationKey: event.nativeGenerationKey, nativeRecordStart: event.nativeRecordStart, nativeRecordEnd: event.nativeRecordEnd } }))]
      const accepted = await f.core.ingest(source, { nativeOrigins: view.nativeOrigins, tasks: [{ id: task, lifecycleId: 'turn', title: 'Owned current task', status: 'idle' }], events: rows.map(event => ({ id: event.data.nativeRecordId, taskId: task, taskLifecycleId: 'turn', kind: event.kind, text: event.text, evidence: 'owned:' + event.data.nativeRecordId, occurredAt: event.at, capturedAt: f.now, data: event.data })) })
      reader.acknowledgeOrigins(accepted.nativeOrigins)
      if (view.coverage === 'caught-up') return
    }
    assert.fail('bounded synthetic source did not catch up')
  }
  return { path, bytes, open, send }
}

test('the public core preserves body, evidence and first time across plain-to-zstd and restart without sharing task authority', async t => {
  for (const compressedFirst of [false, true]) await t.test(compressedFirst ? 'zstd then plain' : 'plain then zstd', async t => {
  const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f)
  await input.send(source, input.open(source, compressedFirst), 'original')
  const first = (await f.core.snapshot()).tasks[0].events.find(event => event.kind === 'output')
  f.advance(); await f.restart()
  const next = await f.register(2, 'host-two')
  await input.send(next, input.open(next, !compressedFirst), 'fork-observer')
  const task = (await f.core.snapshot()).tasks.find(task => task.id === 'fork-observer'), event = task.events.find(event => event.kind === 'output')
  assert.equal(event.data.nativeRecordId, first.data.nativeRecordId); assert.equal(event.text, first.text); assert.equal(event.evidence, first.evidence); assert.equal(event.capturedAt, first.capturedAt)
  assert.equal(task.currentRequest, '')
  await assert.rejects(f.register(1, 'downgrade'), error => error.code === 'native_origin_upgrade_unverified')
  const progress = (await f.core.diagnostics()).retained.nativeOriginFences
  assert.equal(progress.generations, 1); assert.equal(progress.limit, 10000)
  const independent = await f.core.registerSource({ registrationId: 'chatgpt:independent', hostRunId: 'separate-host', name: 'Separate installation', policy: 'chatgpt-main/1', nativeOriginVersion: 2 })
  await input.send(independent, input.open(independent, !compressedFirst), 'independent-observer')
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'independent-observer').events.find(event => event.kind === 'output').capturedAt, f.now)
  })
})

test('changing a stored generation header or prefix cannot mint new native clocks in a new source', async t => {
  for (const mutation of ['header', 'body']) await t.test(mutation, async t => {
    const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f)
    await input.send(source, input.open(source), 'original')
    const rewritten = input.bytes.replace(mutation === 'header' ? '/owned' : 'Original owned body', mutation === 'header' ? '/other' : 'Rewritten owned body')
    await writeFile(input.path + '.zst', zstdCompressSync(Buffer.from(rewritten)))
    const next = await f.register(2, 'next-host'), reader = input.open(next, true)
    let view
    for (let pump = 0; pump < 100; pump++) { view = await reader.pump(); assert.deepEqual(view.materials, []); assert.deepEqual(view.recentLifecycle, []); if (view.coverage === 'gap') break }
    assert.equal(view.gap, mutation === 'header' ? 'native_origin_header_changed' : 'native_origin_prefix_changed')
    assert.equal((await f.core.snapshot()).tasks.length, 1)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 3)
  })
})


test('native generation fences, capture progress and observation rows roll back together on either new SQL dependency', async t => {
  for (const collection of ['native-capture-progress', 'events']) await t.test(collection, async t => {
    const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f), key = nativeStorageKey(input.path)
    const db = new DatabaseSync(join(f.dir, 'observations.sqlite'))
    t.after(() => db.close())
    db.exec("CREATE TRIGGER owned_origin_atomic_failure BEFORE INSERT ON records WHEN NEW.collection='" + collection + "' BEGIN SELECT RAISE(ABORT, 'owned origin atomic failure'); END")
    await assert.rejects(input.send(source, input.open(source), 'rejected'))
    assert.equal(await f.core.nativeOriginFence(source, key), null)
    assert.equal((await f.core.snapshot()).tasks.length, 0)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
    await f.restart()
    const next = await f.register(2, 'restart-after-abort')
    assert.equal(await f.core.nativeOriginFence(next, key), null)
    assert.equal((await f.core.snapshot()).tasks.length, 0)
    db.exec('DROP TRIGGER owned_origin_atomic_failure')
    f.advance(); await input.send(next, input.open(next, true), 'accepted')
    assert.ok((await f.core.snapshot()).tasks[0].events.every(event => event.capturedAt === f.now))
    assert.equal((await f.core.diagnostics()).retained.nativeOriginFences.generations, 1)
  })
})

test('after content GC and restart a codec switch cannot revive old native bodies', async t => {
  const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f)
  await input.send(source, input.open(source), 'old-main')
  await f.core.unregisterSource(source); f.advance(86400001)
  assert.equal((await f.core.snapshot()).tasks.length, 0)
  await f.restart()
  const next = await f.register(2, 'next-host')
  await input.send(next, input.open(next, true), 'later-observer')
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0)
  const diagnostics = (await f.core.diagnostics()).retained
  assert.equal(diagnostics.nativeCaptureProgress.expired, 3); assert.equal(diagnostics.nativeCaptureProgress.expiredLocators, 0); assert.equal(diagnostics.nativeCaptureProgress.expiredTimestamps, 0)
  assert.equal(diagnostics.nativeOriginFences.generations, 1)
})


test('concurrent cuts reject a stale extension and a shorter fork verifies the longer ACK without truncating it', async t => {
  const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f), key = nativeStorageKey(input.path)
  const cut = Buffer.byteLength(input.bytes.slice(0, input.bytes.lastIndexOf('\n', input.bytes.length - 2) + 1))
  const short = input.open(source, false, { endOrdinalExclusive: 3, endByteOffset: cut }), long = input.open(source)
  const a = await short.pump(), b = await long.pump()
  assert.equal(a.coverage, 'caught-up'); assert.equal(b.coverage, 'caught-up')
  await f.core.ingest(source, { nativeOrigins: a.nativeOrigins })
  await assert.rejects(f.core.ingest(source, { nativeOrigins: b.nativeOrigins }), error => error.code === 'native_origin_revision_conflict')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  await input.send(source, input.open(source), 'long-observer')
  const acknowledged = await f.core.nativeOriginFence(source, key)
  assert.equal(acknowledged.prefix.offset, Buffer.byteLength(input.bytes))
  const next = await f.register(2, 'shorter-fork-host')
  await input.send(next, input.open(next, true, { endOrdinalExclusive: 3, endByteOffset: cut }), 'short-fork')
  assert.deepEqual(await f.core.nativeOriginFence(next, key), acknowledged)
  const task = (await f.core.snapshot()).tasks.find(task => task.id === 'short-fork')
  assert.equal(task.events.length, 2); assert.ok(task.events.every(event => event.data.history && event.data.inherited)); assert.equal(task.currentRequest, '')
  const changed = { key, headerHash: acknowledged.headerHash, revision: acknowledged.revision, base: acknowledged.prefix, verifiedBase: acknowledged.prefix, next: { ...acknowledged.prefix, tailHash: '0'.repeat(64) }, complete: true }
  await assert.rejects(f.core.ingest(next, { nativeOrigins: [changed] }), error => error.code === 'native_origin_prefix_changed')
  assert.deepEqual(await f.core.nativeOriginFence(next, key), acknowledged)
})

test('v2 material cannot silently lose its native identity or use an unfinished frame proof', async t => {
  for (const mutation of ['missing-id', 'missing-key', 'wrong-position', 'unfinished']) await t.test(mutation, async t => {
    const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f), reader = input.open(source)
    const view = await reader.pump(), item = view.materials[0]
    const data = { ...item.data }, nativeOrigins = structuredClone(view.nativeOrigins)
    if (mutation === 'missing-id') delete data.nativeRecordId
    if (mutation === 'missing-key') delete data.nativeGenerationKey
    if (mutation === 'wrong-position') data.nativeRecordStart++
    if (mutation === 'unfinished') nativeOrigins[0].complete = false
    await assert.rejects(f.core.ingest(source, { nativeOrigins, tasks: [{ id: mutation, lifecycleId: 'turn', title: 'Owned bad proof', status: 'idle' }], events: [{ id: mutation, taskId: mutation, taskLifecycleId: 'turn', kind: item.kind, occurredAt: null, capturedAt: f.now, text: item.text, data }] }), error => error.code === 'native_origin_invalid')
    assert.equal((await f.core.snapshot()).tasks.length, 0)
    assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  })
})


test('a real append extends the retained storage fence and receives a new capture clock', async t => {
  const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f), reader = input.open(source)
  await input.send(source, reader, 'persistent-main')
  const before = (await f.core.snapshot()).tasks[0].events.map(event => [event.data.nativeRecordId, event.capturedAt])
  f.advance()
  const next = JSON.stringify({ ordinal: 4, type: 'event_msg', timestamp: new Date(f.now).toISOString(), payload: { type: 'item_completed', thread_id: '00000000-0000-4000-8000-000000000004', turn_id: 'turn', item: { type: 'AgentMessage', id: 'genuine-new-output', content: [{ type: 'Text', text: 'Genuine new append' }] } } }) + '\n'
  await writeFile(input.path, input.bytes + next)
  await input.send(source, reader, 'persistent-main')
  const events = (await f.core.snapshot()).tasks[0].events
  assert.deepEqual(events.filter(event => event.text !== 'Genuine new append').map(event => [event.data.nativeRecordId, event.capturedAt]), before)
  assert.equal(events.find(event => event.text === 'Genuine new append').capturedAt, f.now)
  assert.equal((await f.core.nativeOriginFence(source, nativeStorageKey(input.path))).prefix.offset, Buffer.byteLength(input.bytes + next))
})

test('the finite origin fence quota refuses new generations without evicting old keys or blocking DSH', async t => {
  const f = await fixture(t), source = await f.register(2), input = await nativeFile(t, f), view = await input.open(source).pump(), template = view.nativeOrigins[0]
  for (let offset = 0; offset < 10000; offset += 16) await f.core.ingest(source, { nativeOrigins: Array.from({ length: Math.min(16, 10000 - offset) }, (_, index) => ({ ...template, key: hash('owned-generation-' + (offset + index)) })) })
  const first = await f.core.nativeOriginFence(source, hash('owned-generation-0'))
  await assert.rejects(f.core.ingest(source, { nativeOrigins: [{ ...template, key: hash('one-more-owned-generation') }] }), error => error.code === 'native_origin_capacity')
  assert.deepEqual(await f.core.nativeOriginFence(source, hash('owned-generation-0')), first)
  assert.equal((await f.core.diagnostics()).retained.nativeOriginFences.generations, 10000)
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  const dsh = await f.core.registerSource({ registrationId: 'dsh-independent', hostRunId: 'dsh-host', name: 'DSH unaffected' })
  await f.core.ingest(dsh, { tasks: [{ id: 'dsh-task', lifecycleId: 'run', title: 'Owned DSH task', status: 'idle' }], events: [{ id: 'dsh-output', taskId: 'dsh-task', taskLifecycleId: 'run', kind: 'output', occurredAt: null, capturedAt: f.now, text: 'DSH continues' }] })
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'dsh-task').events[0].text, 'DSH continues')
})

test('future-only legacy attempts across many registrations cannot accumulate mode-only progress', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 64; index++) {
    const source = await f.core.registerSource({ registrationId: 'future-only-' + index, hostRunId: 'host', name: 'Owned future-only source', policy: 'chatgpt-main/1' })
    await f.core.ingest(source, { tasks: [{ id: 'future', lifecycleId: 'run', title: 'Ignored future body', status: 'idle' }], events: [{ id: 'future-event', taskId: 'future', taskLifecycleId: 'run', kind: 'output', capturedAt: f.now + 1000, occurredAt: null, text: 'Not captured', data: { nativeRecordId: hash('future-' + index), sourceKind: 'chatgpt-native', nativeType: 'item_completed/AgentMessage' } }] })
  }
  let retained = (await f.core.diagnostics()).retained
  assert.equal(retained.nativeCaptureProgress.records, 0); assert.equal(retained.nativeOriginFences.generations, 0); assert.equal(retained.nativeOriginFences.registrations, 0)
  await f.restart()
  retained = (await f.core.diagnostics()).retained
  assert.equal(retained.nativeOriginFences.registrations, 0)
  const emptyUpgrade = await f.core.registerSource({ registrationId: 'future-only-0', hostRunId: 'new-host', name: 'Empty upgrade', policy: 'chatgpt-main/1', nativeOriginVersion: 2 })
  assert.ok(emptyUpgrade)
})

test('pre-fix mode-only state remains a conservative upgrade fence until real progress backs it', async t => {
  const f = await fixture(t), db = new DatabaseSync(join(f.dir, 'observations.sqlite'))
  db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?)').run('native-origin-versions', 'old-mode-only', '1'); db.close()
  await f.restart()
  await assert.rejects(f.core.registerSource({ registrationId: 'old-mode-only', hostRunId: 'upgrade', name: 'Rejected old prototype upgrade', policy: 'chatgpt-main/1', nativeOriginVersion: 2 }), error => error.code === 'native_origin_upgrade_unverified')
  assert.equal((await f.core.diagnostics()).retained.nativeOriginFences.legacyModeOnly, 1)
  const source = await f.core.registerSource({ registrationId: 'old-mode-only', hostRunId: 'compatible', name: 'Existing v1 mode', policy: 'chatgpt-main/1' })
  const event = { id: 'expired', taskId: 'main', taskLifecycleId: 'turn', kind: 'output', capturedAt: f.now - 86400001, occurredAt: null, text: 'Expired and never revived', data: { sourceKind: 'chatgpt-native', nativeRecordId: hash('expired-native'), nativeType: 'item_completed/AgentMessage' } }
  await f.core.ingest(source, { tasks: [{ id: 'main', lifecycleId: 'turn', title: 'Owned historical attempt', status: 'idle' }], events: [event] })
  const retained = (await f.core.diagnostics()).retained
  assert.equal(retained.nativeCaptureProgress.expired, 1); assert.equal(retained.nativeOriginFences.legacyModeOnly, 0)
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0)
  await f.restart()
  await assert.rejects(f.core.registerSource({ registrationId: 'old-mode-only', hostRunId: 'upgrade-after-expiry', name: 'Still rejected', policy: 'chatgpt-main/1', nativeOriginVersion: 2 }), error => error.code === 'native_origin_upgrade_unverified')
})
