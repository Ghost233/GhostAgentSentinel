import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { DatabaseSync } from 'node:sqlite'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createLazyCore, retentionProfileSignature } from '../chatgpt/lazy-core.mjs'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'
import { openHealthOutbox } from '../chatgpt/health-outbox.mjs'
import { controlledModelService } from './helpers/controlled-model-service.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'
import { setTimeout as delay } from 'node:timers/promises'

const DAY = 86_400_000
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-retention-')), owned = []; let clock = 1_000_000, server, client
  t.after(async () => {
    const results = await Promise.allSettled(owned.map(value => Promise.resolve().then(() => value.close())))
    const failures = results.filter(value => value.status === 'rejected').map(value => value.reason)
    for (const cleanup of [() => client?.close(), () => server?.close(), () => rm(dir, { recursive: true, force: true })]) try { await cleanup() } catch (error) { failures.push(error) }
    if (failures.length) { t.diagnostic('Retention fixture cleanup: ' + failures.map(error => error.message).join('; ')); throw new AggregateError(failures, 'Retention fixture cleanup failed') }
  })
  server = await startServer({ dataDir: dir, now: () => clock, leaseMs: DAY * 3 }); client = await connectCore({ dataDir: dir })
  return { dir, get client() { return client }, owned, setClock(value) { clock = value }, get clock() { return clock }, async closeServer() { await client.close(); await server.close() }, async restartServer() { await client.close(); await server.close(); server = await startServer({ dataDir: dir, now: () => clock, leaseMs: DAY * 3 }); client = await connectCore({ dataDir: dir }) } }
}

test('public retention deadline tracks real evidence and failures without leaking bodies or expiring DSH early', async t => {
  const f = await fixture(t), at = f.clock
  const source = await f.client.registerSource({ registrationId: 'chatgpt:retention', hostRunId: 'chat-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await f.client.recordCollectionOutcome({ sourceKey: source, mainId: null, lifecycleId: null, roundId: 'capture', outcome: 'failed', settledAt: at, error: { code: 'EACCES', stage: 'collection' } })
  await f.client.setNativeNotificationEligibility({ sourceKey: source, hostRunId: 'chat-host', appRunning: true, pluginEnabled: true })
  const notice = (await f.client.claimNotifications(source))[0]
  const emit = fields => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.id, bundleId: 'com.ghostsentinel.notifications', observedAt: f.clock, ...fields })
  await emit({ event: 'authorization', authorization: 'authorized' }); await emit({ event: 'submitting' }); await emit({ event: 'accepted' }); await emit({ event: 'user-action', action: 'default' })
  assert.equal((await f.client.nativeNotificationRecords({ sourceKey: source })).records[0].native.userAction.action, 'default')
  f.setClock(at + 100)
  const dsh = await f.client.registerSource({ registrationId: 'retention-dsh', hostRunId: 'dsh-host', name: 'DSH' })
  const monitoring = await f.client.installationMonitoring('retention-dsh')
  await f.client.setInstallationMonitoring('retention-dsh', true, monitoring.revision)
  await f.client.ingest(dsh, { tasks: [{ id: 'peer', lifecycleId: 'run', title: 'Peer', status: 'running' }], events: [{ id: 'input', taskId: 'peer', taskLifecycleId: 'run', kind: 'input', capturedAt: f.clock, occurredAt: null, text: 'PRIVATE RETENTION REQUEST' }] })
  let diagnostics = await f.client.diagnostics()
  assert.equal(diagnostics.retention?.nextExpiryAt, at + DAY)
  assert.equal(JSON.stringify(diagnostics.retention).includes('PRIVATE'), false)
  f.setClock(at + DAY)
  diagnostics = await f.client.diagnostics()
  assert.equal(diagnostics.retention.nextExpiryAt, at + 100 + DAY)
  assert.equal(diagnostics.failureReceipts.receipts, 0)
  assert.equal(diagnostics.nativeNotifications.failureLinks, 0)
  assert.deepEqual((await f.client.nativeNotificationRecords({ sourceKey: source })).records, [])
  assert.equal((await f.client.snapshot()).tasks.find(task => task.sourceKey === dsh).events[0].capturedAt, at + 100)
  f.setClock(at + 100 + DAY)
  diagnostics = await f.client.diagnostics()
  assert.equal(diagnostics.retention.nextExpiryAt, null, 'expired bodies must not leave a metadata-only busy-loop deadline')
  assert.equal(diagnostics.runtime.activeAnalyses, 0)
  const state = await f.client.snapshot()
  assert.equal(state.sources.find(value => value.key === dsh).status, 'connected')
  assert.equal(state.tasks.find(task => task.sourceKey === dsh).currentRequest, '')
})

test('a standby helper prunes existing expired bodies on startup before any Native sampling and preserves fresh DSH', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const f = await fixture(t), at = f.clock
  const model = await controlledModelService(t)
  await configureInstallationModels(f.client, 'expired-owned-fixture', model.config)
  const old = await f.client.registerSource({ registrationId: 'expired-owned-fixture', hostRunId: 'old', name: 'Expired' })
  await f.client.ingest(old, { tasks: [{ id: 'old', lifecycleId: 'run', title: 'Old', status: 'running' }], events: [{ id: 'old-input', taskId: 'old', taskLifecycleId: 'run', kind: 'input', capturedAt: at, occurredAt: null, text: 'PRIVATE EXPIRED OWNED BODY' }] })
  let seeded
  const deadline = performance.now() + 3000
  do { seeded = (await f.client.snapshot()).tasks.find(task => task.sourceKey === old); if (seeded.analysis.completedAt) break; assert.ok(performance.now() < deadline, 'actual configured analysis completes'); await delay(5) } while (true)
  assert.equal(seeded.analysis.summary.currentTask, 'PRIVATE EXPIRED OWNED BODY', 'a real configured analysis result contains retained request text')
  await f.client.unregisterSource(old)
  f.setClock(at + DAY - 10000)
  const dsh = await f.client.registerSource({ registrationId: 'live-dsh-peer', hostRunId: 'dsh', name: 'DSH' })
  await f.client.ingest(dsh, { tasks: [{ id: 'peer', lifecycleId: 'run', title: 'Peer', status: 'idle' }], events: [{ id: 'peer-input', taskId: 'peer', taskLifecycleId: 'run', kind: 'input', capturedAt: f.clock, occurredAt: null, text: 'FRESH DSH BODY' }] })
  const identity = (await f.client.health()).instanceId
  f.setClock(at + DAY + 1)
  // Read-only, self-owned SQLite witness avoids a verification GET itself pruning old rows.
  const countOld = (collection = 'events') => { const db = new DatabaseSync(join(f.dir, 'observations.sqlite'), { readOnly: true }); try { return db.prepare("SELECT count(*) AS n FROM records WHERE collection=? AND value LIKE '%PRIVATE EXPIRED OWNED BODY%'").get(collection).n } finally { db.close() } }
  assert.equal(countOld(), 1)
  assert.equal(countOld('tasks'), 1)
  const prepared = await prepareInstallation({ dataDir: join(f.dir, 'helper'), coreDataDir: f.dir, sessionsRoot: join(f.dir, 'sessions'), metadataSocket: join(f.dir, 'missing.sock'), pluginId: 'sentinel@retention-test' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  await writeFile(join(config.dataDir, 'maintenance.json'), JSON.stringify({ format: 1, installationId: config.installationId, operationId: 'af7dd7a7-e6bc-4cfe-85c7-e5e6466f1806' }), { mode: 0o600 })
  let nativeCalls = 0
  const native = { async sampleHost() { nativeCalls++; throw new Error('standby maintenance must not sample Native') }, async disconnectMetadata() {}, async close() {} }
  const helper = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 }); f.owned.push(helper)
  assert.equal(countOld(), 0, 'helper startup must clear already expired content without a panel or qualification')
  assert.equal(countOld('tasks'), 0, 'expired request and actual summary must be removed from persistence before verification GETs')
  assert.equal(nativeCalls, 0)
  assert.equal((await f.client.health()).instanceId, identity)
  const snapshot = await f.client.snapshot()
  assert.equal(snapshot.sources.some(value => value.descriptor.registrationId === 'chatgpt:' + config.installationId), false)
  assert.equal(snapshot.sources.find(value => value.key === dsh).status, 'connected')
  assert.equal(snapshot.tasks.find(value => value.sourceKey === dsh).events[0].text, 'FRESH DSH BODY')
})

test('maintenance requires an existing trusted profile and explicit new capability, then closes only its own client', async t => {
  const f = await fixture(t); let connects = 0, closes = 0, diagnosticCalls = 0
  const missing = createLazyCore({ dataDir: join(f.dir, 'never-created'), connect: async () => { connects++; assert.fail('no existing material must not create Core') } })
  assert.equal(await missing.maintenance(), null); await missing.close(); assert.equal(connects, 0)
  const facade = createLazyCore({ dataDir: f.dir, connect: async () => { connects++; return { async health() { return { features: ['chatgpt-installation-purge/1'] } }, async diagnostics() { diagnosticCalls++; return {} }, async close() { closes++ } } } })
  await assert.rejects(facade.maintenance(), { code: 'retention_capability_unavailable' })
  assert.equal(closes, 1); assert.equal(diagnosticCalls, 0)
  await facade.close(); assert.equal((await f.client.health()).protocol, 1, 'maintenance never owns or stops the existing Core')
})

test('an overdue deadline is rejected without zero-delay retry and close waits for a late maintenance client', async t => {
  const f = await fixture(t); let closes = 0, release, entered
  const old = createLazyCore({ dataDir: f.dir, connect: async () => ({ async health() { return { features: ['chatgpt-retention-maintenance/1'] } }, async diagnostics() { return { generatedAt: 1000, retention: { retentionMs: DAY, nextExpiryAt: 999 } } }, async close() { closes++ } }) })
  await assert.rejects(old.maintenance(), { code: 'retention_deadline_unconfirmed' }); await old.close(); assert.equal(closes, 1)
  const started = new Promise(resolve => { entered = resolve }), gate = new Promise(resolve => { release = resolve })
  const facade = createLazyCore({ dataDir: f.dir, connect: async () => { entered(); await gate; return { async health() { assert.fail('a late client cannot be used after close') }, async close() { closes++ } } } })
  const pending = facade.maintenance().catch(error => error)
  await started; const closing = facade.close(); release(); await closing
  assert.ok(await pending instanceof Error); assert.equal(closes, 2); assert.equal(facade.transportDiagnostics().maintenancePending, false)
})

test('maintenance keeps an original error and cleanup error separate and cannot hide a failed client close', async t => {
  const f = await fixture(t), original = new Error('controlled-maintenance-health-failure'), cleanup = new Error('controlled-maintenance-close-failure')
  const facade = createLazyCore({ dataDir: f.dir, connect: async () => ({ async health() { throw original }, async close() { throw cleanup } }) })
  await assert.rejects(facade.maintenance(), error => error instanceof AggregateError && error.errors[0] === original && error.errors[1] === cleanup)
  await assert.rejects(facade.close(), error => error === cleanup)
})

test('null retention does not reconnect until WAL-only external material changes, then the actual deadline prunes it', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  let wallClock = Date.now(); t.mock.method(Date, 'now', () => wallClock)
  const f = await fixture(t)
  const prepared = await prepareInstallation({ dataDir: join(f.dir, 'helper'), coreDataDir: f.dir, sessionsRoot: join(f.dir, 'sessions'), metadataSocket: join(f.dir, 'missing.sock'), pluginId: 'sentinel@retention-null' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  await writeFile(join(config.dataDir, 'maintenance.json'), JSON.stringify({ format: 1, installationId: config.installationId, operationId: '9ba5d074-4dfd-4fbd-aed8-7f4ef0dc37b4' }), { mode: 0o600 })
  const helper = await startFromInstallation({ configPath: prepared.configPath, native: { async sampleHost() { assert.fail('maintenance cannot sample Native') }, async close() {} }, pollMs: 0 }); f.owned.push(helper)
  assert.equal(helper.diagnostics().retention.errorCode, null)
  assert.equal(helper.diagnostics().retention.nextAt, null)
  let before = await f.client.diagnostics()
  for (let i = 0; i < 4; i++) await helper.maintainRetention()
  let after = await f.client.diagnostics()
  assert.equal(after.http.totalRequests, before.http.totalRequests + 1, 'an unchanged null profile must only stat, not reconnect')
  const mainBefore = await stat(join(f.dir, 'observations.sqlite')), walBefore = await stat(join(f.dir, 'observations.sqlite-wal'))
  const peer = await f.client.registerSource({ registrationId: 'external-dsh', hostRunId: 'dsh', name: 'DSH' })
  await f.client.ingest(peer, { tasks: [{ id: 'new', lifecycleId: 'run', title: 'New', status: 'idle' }], events: [{ id: 'new-input', taskId: 'new', taskLifecycleId: 'run', kind: 'input', text: 'WAL ONLY NEW BODY', capturedAt: f.clock, occurredAt: null }] })
  assert.equal((await stat(join(f.dir, 'observations.sqlite'))).mtimeMs, mainBefore.mtimeMs)
  assert.ok((await stat(join(f.dir, 'observations.sqlite-wal'))).size > walBefore.size)
  await helper.maintainRetention()
  assert.equal(helper.diagnostics().retention.nextAt, wallClock + DAY)
  const count = () => { const db = new DatabaseSync(join(f.dir, 'observations.sqlite'), { readOnly: true }); try { return db.prepare("SELECT count(*) AS n FROM records WHERE collection='events'").get().n } finally { db.close() } }
  assert.equal(count(), 1)
  wallClock += DAY; f.setClock(f.clock + DAY)
  await helper.maintainRetention()
  assert.equal(count(), 0, 'the deadline must trigger public maintenance without a panel or external write')
  assert.equal(helper.diagnostics().retention.nextAt, null)
  await helper.close()
  assert.deepEqual({ timers: helper.diagnostics().retention.timers, pending: helper.diagnostics().retention.pending }, { timers: 0, pending: 0 })
  assert.equal((await f.client.snapshot()).sources.find(source => source.key === peer).status, 'connected')
})

test('an empty profile settles its WAL checkpoint and a later Core exit does not cause standby respawn', async t => {
  t.mock.timers.enable({ apis: ['setInterval'] })
  const f = await fixture(t), prepared = await prepareInstallation({ dataDir: join(f.dir, 'helper'), coreDataDir: f.dir, sessionsRoot: join(f.dir, 'sessions'), metadataSocket: join(f.dir, 'missing.sock'), pluginId: 'sentinel@retention-idle' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  await writeFile(join(config.dataDir, 'maintenance.json'), JSON.stringify({ format: 1, installationId: config.installationId, operationId: '1a1840aa-238b-4727-b762-4de2f92f92eb' }), { mode: 0o600 })
  const helper = await startFromInstallation({ configPath: prepared.configPath, native: { async sampleHost() { assert.fail('maintenance cannot sample Native') }, async close() {} }, pollMs: 0 }); f.owned.push(helper)
  assert.equal(helper.diagnostics().retention.nextAt, null)
  await f.restartServer()
  await helper.maintainRetention()
  const stable = await retentionProfileSignature(f.dir)
  await f.closeServer()
  assert.equal(await retentionProfileSignature(f.dir), stable, 'an empty WAL is metadata, not new retained content')
  for (let i = 0; i < 4; i++) await helper.maintainRetention()
  await assert.rejects(stat(join(f.dir, 'runtime.json')), { code: 'ENOENT' })
  assert.equal(helper.diagnostics().retention.errorCode, null)
  assert.equal(helper.diagnostics().retention.nextAt, null)
})

test('maintenance reopens the same dormant profile without restoring sources or analysis and Core exits naturally', async t => {
  let managedServer, inspection, facade, idleDone
  const idle = new Promise(resolve => { idleDone = resolve })
  const f = await fixture(t)
  // Keep late-bound resources in the fixture that aggregates errors and removes their directory.
  f.owned.push({ close: () => facade?.close() }, { close: () => inspection?.close() }, { close: () => managedServer?.close() })
  const old = await f.client.registerSource({ registrationId: 'dormant-own-source', hostRunId: 'old', name: 'Owned' })
  await f.client.ingest(old, { tasks: [{ id: 'retained', lifecycleId: 'run', title: 'Retained', status: 'running' }], events: [{ id: 'retained-input', taskId: 'retained', taskLifecycleId: 'run', kind: 'input', text: 'RETAINED OWNED MATERIAL', capturedAt: f.clock, occurredAt: null }] })
  await f.closeServer(); f.setClock(f.clock + 1000)
  let connects = 0
  facade = createLazyCore({ dataDir: f.dir, connect: async () => {
    connects++
    managedServer = await startServer({ dataDir: f.dir, now: () => f.clock, idleGraceMs: 500, onIdle: () => { void managedServer.close().then(idleDone) } })
    return connectCore({ dataDir: f.dir })
  } })
  const maintained = await facade.maintenance()
  assert.equal(connects, 1); assert.ok(maintained.nextExpiryAt > maintained.generatedAt)
  inspection = await connectCore({ dataDir: f.dir })
  const state = await inspection.snapshot(), diagnostics = await inspection.diagnostics()
  assert.equal(state.sources.length, 1); assert.equal(state.sources[0].status, 'disconnected')
  assert.equal(state.tasks[0].status, 'unknown'); assert.equal(diagnostics.runtime.activeAnalyses, 0)
  assert.equal(state.tasks[0].events[0].text, 'RETAINED OWNED MATERIAL')
  await inspection.close()
  let timer
  try { await Promise.race([idle, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('owned Core did not exit after idle')), 5000) })]) }
  finally { clearTimeout(timer) }
  await assert.rejects(stat(join(f.dir, 'runtime.json')), { code: 'ENOENT' })
  assert.equal(facade.transportDiagnostics().maintenancePending, false)
})

test('standby startup also expires its own safe failure outbox without sending it or reading Native', async t => {
  const f = await fixture(t), dataDir = join(f.dir, 'helper'), old = Date.now() - DAY - 1
  const outbox = await openHealthOutbox({ dataDir, now: () => old })
  outbox.push({ sourceKey: 'owned-old-source', mainId: null, lifecycleId: null, roundId: 'old-failure', outcome: 'failed', settledAt: old, error: { code: 'EIO', stage: 'collection' } })
  await outbox.close()
  const prepared = await prepareInstallation({ dataDir, coreDataDir: f.dir, sessionsRoot: join(f.dir, 'sessions'), metadataSocket: join(f.dir, 'missing.sock'), pluginId: 'sentinel@retention-outbox' })
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  await writeFile(join(dataDir, 'maintenance.json'), JSON.stringify({ format: 1, installationId: config.installationId, operationId: '25b6dd73-5883-4861-9bf4-4e24d1a4f4fa' }), { mode: 0o600 })
  const helper = await startFromInstallation({ configPath: prepared.configPath, native: { async sampleHost() { assert.fail('maintenance cannot sample Native') }, async close() {} }, pollMs: 0 }); f.owned.push(helper)
  assert.equal(helper.diagnostics().healthOutbox.pending, 0)
  assert.equal(helper.diagnostics().healthOutbox.expired, 1)
  assert.equal((await f.client.diagnostics()).failureReceipts.receipts, 0, 'expired outbox metadata cannot be replayed as a new failure')
})
