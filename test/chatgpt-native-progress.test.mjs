import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'

const DAY = 86400000
const nativeId = name => createHash('sha256').update(name).digest('hex')

async function fixture(t) {
  let clock = Date.now(), server, core
  t.mock.method(Date, 'now', () => clock)
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-native-progress-'))
  const start = async () => { server = await startServer({ dataDir: dir, now: () => clock }); core = await connectCore({ dataDir: dir }) }
  await start()
  t.after(async () => { await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const register = (registrationId = 'chatgpt:install-one', hostRunId = 'host-one', policy = 'chatgpt-main/1') => core.registerSource({ registrationId, hostRunId, name: 'Controlled native origin', ...(policy ? { policy } : {}) })
  const batch = (task = 'own-task', record = 'record-one', capturedAt = clock, text = 'First native body') => ({ tasks: [{ id: task, lifecycleId: 'own-run', title: 'Controlled execution', status: 'idle' }], events: [{ id: record, taskId: task, taskLifecycleId: 'own-run', kind: 'output', capturedAt, occurredAt: capturedAt, text, evidence: 'native-evidence:' + record, data: { nativeRecordId: nativeId(record), sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', nativeType: 'item_completed/AgentMessage', runId: 'own-run', history: false, inherited: false } }] })
  const emit = (source, ...args) => core.ingest(source, batch(...args))
  return { dir, register, emit, batch, get core() { return core }, get now() { return clock }, advance(ms = 500) { clock += ms }, async restart() { await core.close(); await server.close(); await start() } }
}

test('registration-native progress survives source GC without reviving its old content, while a new append and other scopes still work', async t => {
  const f = await fixture(t), firstSource = await f.register(), firstAt = f.now
  await f.emit(firstSource, 'old-task')
  await f.core.unregisterSource(firstSource)
  f.advance(DAY + 1)
  const collected = await f.core.snapshot()
  assert.deepEqual(collected.sources, []); assert.deepEqual(collected.tasks, [])
  assert.equal((await f.core.diagnostics()).failureReceipts.receipts, 0, 'normal retention pruning does not create a collection failure')
  const db = new DatabaseSync(join(f.dir, 'observations.sqlite'), { readOnly: true })
  const expired = JSON.parse(db.prepare("SELECT value FROM records WHERE collection='native-capture-progress'").get().value)
  db.close()
  assert.deepEqual(Object.keys(expired).sort(), ['expired', 'key', 'nativeRecordId', 'registrationId'])
  assert.equal(expired.expired, true)
  assert.doesNotMatch(JSON.stringify(expired), /First native body|old-task|own-run|native-evidence/)
  await f.restart()
  const nextSource = await f.register('chatgpt:install-one', 'host-two')
  assert.notEqual(nextSource, firstSource)
  await f.emit(nextSource, 'new-observer')
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0, 'an expired native origin cannot receive a new time after its source was removed')
  f.advance(); await f.emit(nextSource, 'new-observer', 'genuinely-new-record', f.now, 'Genuinely new body')
  const newTask = (await f.core.snapshot()).tasks.find(task => task.id === 'new-observer')
  assert.deepEqual(newTask.events.map(event => [event.text, event.capturedAt]), [['Genuinely new body', f.now]])
  assert.equal(newTask.currentRequest, '')
  const separate = await f.register('chatgpt:install-two', 'host-two')
  await f.emit(separate, 'separate-observer')
  const legacy = await f.register('dsh:independent', 'host-two', null)
  await f.emit(legacy, 'legacy-observer')
  const snapshot = await f.core.snapshot()
  assert.ok(snapshot.tasks.find(task => task.id === 'separate-observer').events.some(event => event.data.nativeRecordId === nativeId('record-one') && event.capturedAt > firstAt))
  assert.ok(snapshot.tasks.find(task => task.id === 'legacy-observer').events.length)
  assert.equal(snapshot.tasks.some(task => task.id === 'old-task'), false)
  const progress = (await f.core.diagnostics()).retained.nativeCaptureProgress
  assert.equal(progress.records, 3); assert.equal(progress.limit, 10000)
  assert.equal(progress.expired, 1); assert.equal(progress.expiredLocators, 0); assert.equal(progress.expiredTimestamps, 0)
})

test('a genuinely missing first body is refused with its fixed classification and only an actual attempted settlement creates a receipt', async t => {
  const f = await fixture(t), source = await f.register()
  await f.emit(source)
  const db = new DatabaseSync(join(f.dir, 'observations.sqlite'))
  db.prepare("DELETE FROM records WHERE collection='events'").run(); db.close()
  await f.restart()
  const next = await f.register('chatgpt:install-one', 'host-two')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.bodyGaps, 1)
  assert.equal((await f.core.diagnostics()).failureReceipts.receipts, 0)
  for (const roundId of ['refused-one', 'refused-two']) {
    let rejected
    await assert.rejects(f.emit(next, 'another-observer'), error => { rejected = error; return error.code === 'native_capture_body_unavailable' })
    await f.core.recordCollectionOutcome({ sourceKey: next, mainId: null, lifecycleId: null, channel: 'main:' + nativeId('another-observer'), roundId, outcome: 'failed', settledAt: f.now, error: { code: rejected.code, stage: 'core' } })
  }
  const receipts = await f.core.failureReceipts({ sourceKey: next })
  assert.equal(receipts.receipts.length, 1)
  assert.equal(receipts.receipts[0].error.code, 'native_capture_body_unavailable')
  assert.equal(receipts.receipts[0].error.category, 'collection')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
})

test('the global native progress quota fails explicitly without forgetting old identities or affecting DSH', async t => {
  const f = await fixture(t), first = await f.register(), second = await f.register('chatgpt:install-one', 'host-two')
  for (let offset = 0; offset < 10000; offset += 1000) {
    const batch = f.batch('bulk-task', 'bulk-' + offset, f.now, 'Bulk body ' + offset)
    for (let index = offset + 1; index < offset + 1000; index++) batch.events.push(...f.batch('bulk-task', 'bulk-' + index, f.now, 'Bulk body ' + index).events)
    await f.core.ingest(offset < 5000 ? first : second, batch)
  }
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 10000)
  for (const roundId of ['capacity-one', 'capacity-two']) {
    let rejected
    await assert.rejects(f.emit(second, 'unseen', 'over-capacity'), error => { rejected = error; return error.code === 'native_capture_capacity' })
    await f.core.recordCollectionOutcome({ sourceKey: second, mainId: null, lifecycleId: null, channel: 'main:' + nativeId('unseen'), roundId, outcome: 'failed', settledAt: f.now, error: { code: rejected.code, stage: 'core' } })
  }
  const failures = await f.core.failureReceipts({ sourceKey: second })
  assert.equal(failures.receipts.length, 1); assert.equal(failures.receipts[0].error.code, 'native_capture_capacity'); assert.equal(failures.receipts[0].error.category, 'collection')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 10000)
  assert.ok((await f.core.snapshot()).sources.find(source => source.key === second).gaps.includes('native_capture_capacity'))
  await f.emit(second, 'same-registration-copy', 'bulk-0', f.now, 'Bulk body 0')
  const legacy = await f.register('dsh:quota-independent', 'host-three', null)
  await f.emit(legacy, 'legacy-after-quota', 'new-dsh-native-tag')
  assert.ok((await f.core.snapshot()).tasks.find(task => task.id === 'legacy-after-quota').events.length)
})

test('unexpired native progress preserves first body, evidence and clock across source and core replacement without sharing authorization', async t => {
  const f = await fixture(t), firstSource = await f.register(), firstAt = f.now
  await f.emit(firstSource, 'original-task')
  const first = (await f.core.snapshot()).tasks[0].events[0]
  await f.core.unregisterSource(firstSource); f.advance(); await f.restart()
  const second = await f.register('chatgpt:install-one', 'host-two')
  await f.emit(second, 'fork-observer')
  const snapshot = await f.core.snapshot(), next = snapshot.tasks.find(task => task.sourceKey === second)
  assert.equal(next.events[0].text, first.text); assert.equal(next.events[0].evidence, first.evidence); assert.equal(next.events[0].capturedAt, firstAt)
  assert.equal(next.currentRequest, ''); assert.equal(next.events[0].authorizationEligible, undefined)
  const conflict = f.batch('conflict', 'record-one', f.now, 'Conflicting native body')
  await assert.rejects(f.core.ingest(second, conflict), error => error.code === 'hierarchy_context_invalid')
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
})

test('a successful expired-cursor replay records only an expired fence and cannot gain a new clock in another host source', async t => {
  const f = await fixture(t), first = await f.register()
  const result = await f.emit(first, 'expired-observer', 'record-one', f.now - DAY - 1)
  assert.equal(result.accepted, 0)
  const second = await f.register('chatgpt:install-one', 'host-two')
  await f.emit(second, 'second-observer')
  assert.equal((await f.core.snapshot()).tasks.flatMap(task => task.events).length, 0)
  const progress = (await f.core.diagnostics()).retained.nativeCaptureProgress
  assert.equal(progress.records, 1); assert.equal(progress.expired, 1); assert.equal(progress.locators, 0); assert.equal(progress.expiredTimestamps, 0)
})

test('an accepted earlier native clock shortens every registration copy including two scopes in the same batch', async t => {
  const f = await fixture(t), firstSource = await f.register(), earliest = f.now - 500
  const batch = f.batch('scope-one'), other = f.batch('scope-two', 'record-one', earliest)
  batch.tasks.push(...other.tasks); batch.events.push(...other.events)
  await f.core.ingest(firstSource, batch)
  let copies = (await f.core.snapshot()).tasks.flatMap(task => task.events)
  assert.equal(copies.length, 2); assert.ok(copies.every(event => event.capturedAt === earliest))
  f.advance(); await f.restart()
  const next = await f.register('chatgpt:install-one', 'host-two')
  await f.emit(next, 'scope-three')
  copies = (await f.core.snapshot()).tasks.flatMap(task => task.events)
  assert.equal(copies.length, 3); assert.ok(copies.every(event => event.capturedAt === earliest))
  await f.emit(next, 'scope-four', 'record-one', earliest - 500)
  copies = (await f.core.snapshot()).tasks.flatMap(task => task.events)
  assert.equal(copies.length, 4); assert.ok(copies.every(event => event.capturedAt === earliest - 500))
})

test('failed native proof validation allocates no progress and cannot shorten an accepted clock', async t => {
  const f = await fixture(t), source = await f.register(), firstAt = f.now
  const invalid = f.batch('unaccepted')
  invalid.hierarchyContext = { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'unaccepted' }], activations: [{ taskId: 'unaccepted', lifecycleId: 'own-run', sessionId: 'unaccepted', runId: 'own-run', state: 'running', stateRefs: [{ taskId: 'unaccepted', lifecycleId: 'own-run', eventId: 'missing-start' }] }], instructionBindings: [], relations: [], coverage: { complete: true, branchGaps: [] } }
  await assert.rejects(f.core.ingest(source, invalid))
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  f.advance(); await f.emit(source, 'accepted')
  const acceptedAt = f.now
  invalid.events[0].capturedAt = firstAt - 500
  await assert.rejects(f.core.ingest(source, invalid))
  const other = await f.register('chatgpt:install-one', 'host-two')
  await f.emit(other, 'another-observer')
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'another-observer').events[0].capturedAt, acceptedAt)
})

test('native progress commits its whole metadata batch atomically when SQLite rejects one write', async t => {
  const f = await fixture(t), source = await f.register(), db = new DatabaseSync(join(f.dir, 'observations.sqlite'))
  const rejected = nativeId('record-two')
  db.exec("CREATE TRIGGER controlled_native_progress_failure BEFORE INSERT ON records WHEN NEW.collection='native-capture-progress' AND instr(NEW.value, '" + rejected + "') > 0 BEGIN SELECT RAISE(ABORT, 'controlled native metadata failure'); END")
  const batch = f.batch(), second = f.batch('own-task', 'record-two')
  batch.events.push(...second.events)
  await assert.rejects(f.core.ingest(source, batch))
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0)
  assert.equal(db.prepare("SELECT count(*) AS n FROM records WHERE collection='native-capture-progress'").get().n, 0)
  const rejectedState = await f.core.snapshot()
  assert.equal(rejectedState.tasks.length, 0, 'failed native metadata commit cannot partially publish a task')
  assert.equal(rejectedState.tasks.flatMap(task => task.events).length, 0, 'failed native metadata commit cannot partially publish a body')
  db.exec('DROP TRIGGER controlled_native_progress_failure'); db.close()
  await f.restart()
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 0, 'restart must not backfill a failed new-version ingest')
  assert.deepEqual((await f.core.snapshot()).tasks, [])
  await f.emit(await f.register())
  assert.equal((await f.core.diagnostics()).retained.nativeCaptureProgress.records, 1)
})

for (const collection of ['native-capture-progress', 'events']) test('a rejected ' + collection + ' write leaves existing first capture times and task content unchanged before and after core reopening', async t => {
  const f = await fixture(t), source = await f.register(), firstAt = f.now
  await f.emit(source, 'existing-task', 'existing-record')
  const content = snapshot => snapshot.tasks.map(task => ({ id: task.id, events: task.events.map(event => [event.id, event.text, event.evidence, event.capturedAt]) }))
  const before = content(await f.core.snapshot()), db = new DatabaseSync(join(f.dir, 'observations.sqlite')), rejected = nativeId('record-two')
  db.exec("CREATE TRIGGER controlled_native_earlier_failure BEFORE INSERT ON records WHEN NEW.collection='" + collection + "' AND instr(NEW.value, '" + rejected + "') > 0 BEGIN SELECT RAISE(ABORT, 'controlled native metadata failure'); END")
  const batch = f.batch('new-one', 'record-one'), second = f.batch('new-two', 'record-two'), earlier = f.batch('earlier-copy', 'existing-record', firstAt - 500)
  batch.tasks.push(...second.tasks, ...earlier.tasks); batch.events.push(...second.events, ...earlier.events)
  await assert.rejects(f.core.ingest(source, batch))
  assert.deepEqual(content(await f.core.snapshot()), before)
  const retained = db.prepare("SELECT value FROM records WHERE collection='native-capture-progress'").all().map(row => JSON.parse(row.value))
  assert.equal(retained.length, 1); assert.equal(retained[0].firstCapturedAt, firstAt)
  db.exec('DROP TRIGGER controlled_native_earlier_failure'); db.close(); await f.restart()
  assert.deepEqual(content(await f.core.snapshot()), before)
})
