import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile, mkdir, writeFile, chmod, realpath } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { migrateCoreProfile } from '../src/core-profile-migration.mjs'

const hash = value => createHash('sha256').update(value).digest('hex')
test('a live shared Core migrates only one Codex installation, preserving settings and replay fences while DSH continues independently', async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-core-split-'))), from = join(root, 'shared'), to = join(root, 'codex')
  let time = Date.now(), splitServer, split
  t.mock.method(Date, 'now', () => time)
  const server = await startServer({ dataDir: from, now: () => time }), shared = await connectCore({ dataDir: from })
  t.after(async () => { await split?.close(); await splitServer?.close(); await shared.close(); await server.close(); await rm(root, { recursive: true, force: true }) })
  const registrationId = 'chatgpt:owned', own = await shared.registerSource({ registrationId, hostRunId: 'old-host', name: 'Owned Codex', policy: 'chatgpt-main/1' }), dsh = await shared.registerSource({ registrationId: 'dsh:owned', hostRunId: 'dsh-host', name: 'DSH' })
  const foreign = await shared.registerSource({ registrationId: 'chatgpt:other', hostRunId: 'other-host', name: 'Other Codex', policy: 'chatgpt-main/1' })
  const batch = name => ({ tasks: [{ id: name, lifecycleId: 'run', title: name, status: 'idle' }], events: [{ id: 'record', taskId: name, taskLifecycleId: 'run', kind: 'output', text: name, capturedAt: time, occurredAt: time, data: { nativeRecordId: hash(name), sourceKind: 'chatgpt-native', nativeType: 'item_completed/AgentMessage' } }] })
  await shared.ingest(own, batch('old-owned')); await shared.ingest(foreign, batch('foreign')); await shared.ingest(dsh, batch('dsh'))
  time += 86400001; await shared.snapshot()
  const live = await shared.registerSource({ registrationId, hostRunId: 'old-host', name: 'Owned Codex', policy: 'chatgpt-main/1' })
  await shared.ingest(live, batch('live-owned'))
  const firstCapture = time
  const setting = await shared.installationAnalysisConfig(registrationId), config = { llm: { baseUrl: 'http://127.0.0.1:9', model: 'owned-summary' }, jev: { baseUrl: 'http://127.0.0.1:9', model: 'owned-judge' } }
  await shared.configureInstallationAnalysis(registrationId, config, setting.revision)
  const monitoring = await shared.installationMonitoring(registrationId); await shared.setInstallationMonitoring(registrationId, true, monitoring.revision)
  await shared.setInstallationNotificationPreference(registrationId, 'monitoring-failure', false)
  const result = await migrateCoreProfile({ from, to, registrationId })
  assert.equal(result.migrated, true)
  splitServer = await startServer({ dataDir: to, now: () => time }); split = await connectCore({ dataDir: to })
  assert.notEqual((await split.health()).instanceId, (await shared.health()).instanceId)
  assert.deepEqual((await split.installationAnalysisConfig(registrationId)).config, config)
  assert.equal((await split.installationMonitoring(registrationId)).paused, true)
  assert.equal((await split.snapshot()).sources.some(value => value.descriptor.registrationId !== registrationId), false)
  assert.equal((await split.diagnostics()).retained.nativeCaptureProgress.expired, 1)
  assert.deepEqual(await split.installationNotificationPreferences(registrationId), await shared.installationNotificationPreferences(registrationId))
  const resumed = await split.registerSource({ registrationId, hostRunId: 'new-host', name: 'Owned Codex', policy: 'chatgpt-main/1' })
  time += 1000
  await split.ingest(resumed, batch('old-owned'))
  assert.equal((await split.snapshot()).tasks.flatMap(task => task.events).some(event => event.text === 'old-owned'), false, 'migration never revives expired content')
  await split.ingest(resumed, batch('live-owned'))
  assert.ok((await split.snapshot()).tasks.flatMap(task => task.events).filter(event => event.text === 'live-owned').every(event => event.capturedAt === firstCapture))
  await split.ingest(resumed, batch('new-owned'))
  const dshNow = await shared.registerSource({ registrationId: 'dsh:owned', hostRunId: 'dsh-host', name: 'DSH' })
  await shared.ingest(dshNow, batch('dsh-continues'))
  assert.ok((await shared.snapshot()).tasks.some(value => value.id === 'dsh-continues'))
  assert.equal((await split.snapshot()).tasks.some(value => value.id === 'dsh-continues'), false)
  assert.equal((await migrateCoreProfile({ from, to, registrationId })).migrated, false, 'retry never overwrites the active independent profile')
  assert.ok((await split.snapshot()).tasks.some(value => value.id === 'new-owned'))
  await assert.rejects(migrateCoreProfile({ from, to, registrationId: 'chatgpt:foreign' }), { code: 'core_profile_unconfirmed' })
})

test('malformed source data and foreign targets fail without publishing a partial migration or modifying the source', async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-core-split-refused-'))), from = join(root, 'old'), to = join(root, 'new')
  t.after(() => rm(root, { recursive: true, force: true })); await mkdir(from, { mode: 0o700 })
  const path = join(from, 'observations.sqlite'), db = new DatabaseSync(path)
  db.exec('CREATE TABLE records(collection TEXT,key TEXT,value TEXT,PRIMARY KEY(collection,key))'); db.prepare('INSERT INTO records VALUES(?,?,?)').run('native-capture-progress', 'broken', '{broken'); db.close(); await chmod(path, 0o600)
  const before = hash(await readFile(path))
  await assert.rejects(migrateCoreProfile({ from, to, registrationId: 'chatgpt:owned' }))
  await assert.rejects(readFile(join(to, 'core-profile.json')), { code: 'ENOENT' })
  assert.equal(hash(await readFile(path)), before)
  await mkdir(to, { mode: 0o700 }); await writeFile(join(to, 'foreign'), 'preserve')
  await assert.rejects(migrateCoreProfile({ from, to, registrationId: 'chatgpt:owned' }), { code: 'core_profile_unconfirmed' })
  assert.equal(await readFile(join(to, 'foreign'), 'utf8'), 'preserve')
})
