import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { controlledModelService } from './helpers/controlled-model-service.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'

test('partial metadata replay cannot recover an unaccepted native material batch or create repeated capacity notices', async t => {
  let clock = Date.now(), server, core, adapter, rejected = 0, block = true
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-capture-recovery-')), path = join(dir, 'main.jsonl')
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  let ordinal = 0
  const row = (type, payload) => JSON.stringify({ ordinal: ordinal++, type, timestamp: new Date(clock).toISOString(), payload }) + '\n'
  await fs.writeFile(path, row('session_meta', { id: 'main', cwd: dir, source: 'vscode', originator: 'Codex Desktop', history_mode: 'paginated' }) + row('event_msg', { type: 'task_started', turn_id: 'run', turn_attribution: { turn_id: 'run', turn_trigger: 'composer' } }) + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'UserMessage', id: 'request', content: [{ type: 'Text', text: 'Implement the owned task' }] } }))
  const thread = { id: 'main', path, cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null }
  const hostStartedAt = clock - 10000
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [thread], nextCursor: null } }, async readThread() { return { thread, anchorTurnId: 'run' } }, async close() {} }
  server = await startServer({ dataDir: join(dir, 'core'), now: () => clock }); core = await connectCore({ dataDir: join(dir, 'core') })
  const ingest = core.ingest
  core.ingest = async (source, batch) => { if (block && batch.events?.some(event => event.text === 'New native output')) { rejected++; throw Object.assign(new Error('native_capture_capacity'), { code: 'native_capture_capacity' }) }; return ingest(source, batch) }
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'capture-recovery', token: 'a'.repeat(64), collectMaterials: true, nativeOriginVersion: 2, pollMs: 0, now: () => clock })
  await adapter.read({ thread_id: 'main' })
  await fs.appendFile(path, row('event_msg', { type: 'thread_goal_updated', goal: 'x'.repeat(600000) }) + row('event_msg', { type: 'item_completed', thread_id: 'main', turn_id: 'run', item: { type: 'AgentMessage', id: 'new-output', content: [{ type: 'Text', text: 'New native output' }] } }))
  const sourceKey = (await core.snapshot()).sources[0].key
  for (let cycle = 0; cycle < 100 && rejected < 3; cycle++) { clock += 1500; await adapter.read({ thread_id: 'main' }) }
  assert.ok(rejected >= 3, 'the same material was retried across production reader recovery')
  const receipts = (await core.failureReceipts({ sourceKey })).receipts.filter(value => value.error.code === 'native_capture_capacity')
  assert.equal(receipts.length, 1, 'metadata-only replay does not create new failure episodes')
  assert.equal(receipts[0].state, 'active', 'the material admission failure has not recovered')
  assert.equal((await core.nativeNotificationRecords({ sourceKey })).records.filter(value => value.receiptIds.includes(receipts[0].id)).length, 0)
  block = false
  for (let cycle = 0; cycle < 100; cycle++) { clock += 1500; await adapter.read({ thread_id: 'main' }); if ((await core.failureReceipts({ sourceKey })).receipts.find(value => value.id === receipts[0].id)?.state === 'recovered') break }
  assert.equal((await core.failureReceipts({ sourceKey })).receipts.find(value => value.id === receipts[0].id).state, 'recovered')
  assert.ok((await core.snapshot()).tasks.flatMap(task => task.events).some(event => event.text === 'New native output'))
})

test('a full collector evicts the oldest body for active turns and resumes deferred history after expiry', async t => {
  let clock = Date.now(), server, core, adapter
  t.mock.method(Date, 'now', () => clock)
  const dir = await fs.mkdtemp(join(tmpdir(), 'sentinel-priority-')), coreDir = join(dir, 'core'), hostStartedAt = clock - 10000
  t.after(async () => { await adapter?.close(); await core?.close(); await server?.close(); await fs.rm(dir, { recursive: true, force: true }) })
  server = await startServer({ dataDir: coreDir, now: () => clock }); core = await connectCore({ dataDir: coreDir })
  const seed = await core.registerSource({ registrationId: 'chatgpt:retained', hostRunId: 'prior-host', name: 'Retained history', policy: 'chatgpt-main/1' })
  for (let offset = 0; offset < 9999; offset += 1000) {
    const events = Array.from({ length: Math.min(1000, 9999 - offset) }, (_, index) => {
      const id = String(offset + index)
      const at = clock - 86400000 + (id === '0' ? 9000 : 10000)
      return { id, taskId: 'retained', taskLifecycleId: 'old', kind: 'output', capturedAt: at, occurredAt: at, text: id === '0' ? 'Oldest body' : 'Retained body', data: { nativeRecordId: createHash('sha256').update(id).digest('hex'), ...(id === '0' ? { sourceKind: 'chatgpt-native' } : {}) } }
    })
    await core.ingest(seed, { tasks: [{ id: 'retained', lifecycleId: 'old', title: 'Retained history', status: 'idle' }], events })
  }
  const baselineHierarchy = (await core.diagnostics()).retained.hierarchyEvidenceIdentities
  const thread = id => ({ id, path: join(dir, id + '.jsonl'), cwd: dir, source: 'vscode', originator: 'Codex Desktop', parentThreadId: null, forkedFromId: null })
  const history = thread('history'), live = thread('live')
  let liveAnchor = 'live-run'
  for (const [record, at] of [[history, hostStartedAt - 5000], [live, clock]]) {
    let ordinal = 0
    const row = (type, payload) => JSON.stringify({ ordinal: ordinal++, type, timestamp: new Date(at).toISOString(), payload }) + '\n'
    await fs.writeFile(record.path, row('session_meta', { id: record.id, cwd: dir, source: 'vscode', originator: 'Codex Desktop', history_mode: 'paginated' })
      + row('event_msg', { type: 'task_started', turn_id: record.id + '-run', turn_attribution: { turn_id: record.id + '-run', turn_trigger: 'composer' } })
      + row('event_msg', { type: 'item_completed', thread_id: record.id, turn_id: record.id + '-run', item: { type: 'UserMessage', id: 'request', content: [{ type: 'text', text: record.id + ' request' }] } })
      + (record === history ? row('event_msg', { type: 'token_count', padding: 'x'.repeat(600000) }) + row('event_msg', { type: 'task_complete', turn_id: record.id + '-run' }) : ''))
  }
  const native = { async sampleHost() { return { available: true, hostRunId: 'host', hostStartedAt, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads({ archived }) { return { data: archived ? [] : [history, live], nextCursor: null } }, async readThread(id) { return { thread: id === live.id ? live : history, anchorTurnId: id === live.id ? liveAnchor : 'history-run' } }, async close() {} }
  const models = await controlledModelService(t)
  await configureInstallationModels(core, 'chatgpt:capture-priority', models.config)
  adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, 'adapter'), installationId: 'capture-priority', token: 'b'.repeat(64), collectMaterials: true, nativeOriginVersion: 2, pollMs: 0, now: () => clock })
  for (let cycle = 0; cycle < 12; cycle++) { clock += 10; await adapter.refresh() }
  let snapshot, liveTask
  for (let cycle = 0; cycle < 20; cycle++) {
    snapshot = await core.snapshot()
    liveTask = snapshot.tasks.find(task => task.id === 'live')
    if (liveTask?.analysis.completedAt) break
    await delay(5)
  }
  assert.equal(liveTask?.currentRequest, 'live request')
  assert.ok(liveTask.analysis.completedAt, 'summary and judgment complete before historical capacity is released')
  assert.equal(liveTask.analysis.error, null)
  assert.equal(snapshot.tasks.filter(task => task.id === 'history').flatMap(task => task.events).length, 0)
  const diagnostics = (await core.diagnostics()).retained
  const retained = diagnostics.nativeCaptureProgress
  assert.equal(retained.residentRecords, 10000)
  assert.equal(retained.expired, 1)
  assert.equal(diagnostics.hierarchyEvidenceIdentities, baselineHierarchy + 1, 'old hierarchy locator is discarded along with the body')
  assert.equal(snapshot.tasks.find(task => task.id === 'retained').events.some(event => event.text === 'Oldest body'), false)
  await core.ingest(seed, { events: [{ id: 'oldest-replay', taskId: 'retained', taskLifecycleId: 'old', kind: 'output', capturedAt: clock, occurredAt: clock, text: 'Oldest body', data: { nativeRecordId: createHash('sha256').update('0').digest('hex') } }] })
  assert.equal((await core.snapshot()).tasks.find(task => task.id === 'retained').events.some(event => event.text === 'Oldest body'), false)
  const deferred = await adapter.read({ thread_id: 'history' })
  assert.equal(deferred.collectionRecovery?.state, 'history-deferred')
  liveAnchor = 'live-next'
  const liveRow = (ordinal, payload) => JSON.stringify({ ordinal, type: 'event_msg', timestamp: new Date(clock).toISOString(), payload }) + '\n'
  await fs.appendFile(live.path, liveRow(3, { type: 'task_complete', turn_id: 'live-run' })
    + liveRow(4, { type: 'task_started', turn_id: liveAnchor, turn_attribution: { turn_id: liveAnchor, turn_trigger: 'composer' } })
    + liveRow(5, { type: 'item_completed', thread_id: live.id, turn_id: liveAnchor, item: { type: 'UserMessage', id: 'next-request', content: [{ type: 'text', text: 'next live request' }] } }))
  const database = new DatabaseSync(join(coreDir, 'observations.sqlite'))
  try {
    database.exec(`CREATE TRIGGER block_capacity_delete BEFORE DELETE ON records
      WHEN OLD.collection = 'events' AND json_extract(OLD.value, '$.text') = 'Retained body'
      BEGIN SELECT RAISE(FAIL, 'controlled eviction failure'); END`)
    for (let cycle = 0; cycle < 5; cycle++) { clock += 10; await adapter.refresh() }
    assert.equal((await core.snapshot()).tasks.flatMap(task => task.events).some(event => event.text === 'next live request'), false)
    assert.deepEqual((await core.diagnostics()).retained.nativeCaptureProgress, retained, 'a failed eviction preserves bodies and progress')
  } finally { database.exec('DROP TRIGGER IF EXISTS block_capacity_delete'); database.close() }
  for (let cycle = 0; cycle < 5; cycle++) { clock += 10; await adapter.refresh() }
  snapshot = await core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.lifecycleId === liveAnchor)?.currentRequest, 'next live request')
  assert.equal(snapshot.tasks.find(task => task.lifecycleId === 'live-run')?.status, 'idle')
  assert.equal((await core.diagnostics()).retained.nativeCaptureProgress.residentRecords, 10000)
  clock += 10001
  for (let cycle = 0; cycle < 12; cycle++) { clock += 10; await adapter.refresh() }
  snapshot = await core.snapshot()
  assert.ok(snapshot.tasks.find(task => task.lifecycleId === 'history-run')?.events.some(event => event.text === 'history request'))
  assert.equal((await adapter.read({ thread_id: 'history' })).collectionRecovery, undefined)
})
