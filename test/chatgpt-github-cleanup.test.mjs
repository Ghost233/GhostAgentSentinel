import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm, realpath, writeFile, readFile, stat, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { createHash } from 'node:crypto'
import { cleanupGitHubInstallation, parseGitHubCleanupArguments } from '../chatgpt/github-cleanup.mjs'

async function fixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-github-cleanup-'))), stateRoot = join(root, 'github/test')
  await mkdir(stateRoot, { recursive: true, mode: 0o700 })
  t.after(() => rm(root, { recursive: true, force: true }))
  return { stateRoot }
}

test('a disabled but installed test plugin keeps its data and never reaches notification or Core cleanup', async t => {
  const f = await fixture(t), calls = []
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, {
    metadata: { async request(method) { calls.push(method); return { marketplaces: [{ name: 'ghostsentinel', plugins: [{ id: 'ghost-sentinel-test@ghostsentinel', name: 'ghost-sentinel-test', installed: true, enabled: false }] }], marketplaceLoadErrors: [] } }, async close() { calls.push('close') } },
    async connectCore() { assert.fail('disabled is not uninstalled') },
    notifications: { async purgeInstallation() { assert.fail('disabled is not uninstalled') } },
  })
  assert.equal(result.status, 'needs-user-action')
  assert.equal(result.errorCode, 'github-plugin-still-installed')
  assert.deepEqual(calls, ['plugin/list', 'close'])
})

async function ownedFixture(t) {
  const f = await fixture(t), archive = 'a'.repeat(64), releaseRoot = join(f.stateRoot, 'payloads', archive)
  await mkdir(join(releaseRoot, 'runtime'), { recursive: true, mode: 0o700 })
  const manifest = { runtimeVersion: '0.1.0-dev.1', notificationPath: null, files: { 'owned.mjs': {}, 'runtime/node': {} } }, manifestBytes = JSON.stringify(manifest)
  const releaseId = createHash('sha256').update(manifestBytes).digest('hex')
  await writeFile(join(releaseRoot, 'plugin-package.json'), manifestBytes, { mode: 0o600 })
  await writeFile(join(releaseRoot, 'owned.mjs'), '// private test fixture\n', { mode: 0o600 })
  await writeFile(join(releaseRoot, 'runtime/node'), '// fixture executable is not run\n', { mode: 0o700 })
  const inspectPackage = async (root, options) => { assert.equal(root, releaseRoot); assert.deepEqual(options, { digest: releaseId }); return { root, digest: releaseId, notificationApp: null, nodePath: join(root, 'runtime/node'), manifest } }
  const { prepareGitHubInstallation, inspectGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  await prepareGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', releaseRoot, archiveSHA256: archive, coreDataDir: join(dirname(f.stateRoot), '../shared-core'), sessionsRoot: join(dirname(f.stateRoot), '../sessions') }, { inspectPackage })
  const installation = await inspectGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test' }), { config, receipt } = installation
  const calls = [], io = {
    metadata: { async request(method) { calls.push(method); return { marketplaces: [{ name: 'ghostsentinel', plugins: [{ id: config.pluginId, name: 'ghost-sentinel-test', installed: false, enabled: false }, { id: 'ghost-sentinel@ghostsentinel', name: 'ghost-sentinel', installed: true, enabled: true }] }], marketplaceLoadErrors: [] } }, async close() { calls.push('metadata-close') } },
    inspectPackage,
    async processes(configPath) { assert.equal(configPath, installation.configPath); return [] },
  }
  return { ...f, config, receipt, io, calls, releaseRoot }
}

test('cleanup preview requires real uninstall evidence but leaves the channel data and shared Core untouched', async t => {
  const f = await ownedFixture(t)
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test' }, { ...f.io, async connectCore() { assert.fail('preview must not attach Core') } })
  assert.equal(result.status, 'preview')
  assert.equal(result.installationId, f.config.installationId)
  assert.ok(await stat(join(f.stateRoot, 'state/installation.json')))
  await assert.rejects(stat(join(f.stateRoot, '.github-prepare.lock')), { code: 'ENOENT' })
})

test('explicit cleanup removes one uninstalled channel through the public Core purge while preserving normal and DSH sources', async t => {
  const f = await ownedFixture(t), { startServer } = await import('../src/server.mjs'), { connectCore } = await import('../src/core-client.mjs')
  const server = await startServer({ dataDir: f.config.coreDataDir }), core = await connectCore({ dataDir: f.config.coreDataDir })
  t.after(async () => { await core.close(); await server.close() })
  const keys = {}
  for (const [name, installationId, policy] of [['test', f.config.installationId, 'chatgpt-main/1'], ['normal', 'normal-installation', 'chatgpt-main/1'], ['dsh', 'dsh-installation', null]]) {
    keys[name] = await core.registerSource({ registrationId: 'chatgpt:' + installationId, hostRunId: name + '-host', name, ...(policy ? { policy } : {}) })
    await core.ingest(keys[name], { tasks: [{ id: name + '-task', lifecycleId: name + '-run', title: name, status: 'running' }] })
  }
  await core.unregisterSource(keys.test, 'plugin uninstalled')
  const normalRoot = join(dirname(f.stateRoot), 'stable')
  await mkdir(normalRoot, { mode: 0o700 }); await writeFile(join(normalRoot, 'preserve'), 'normal data', { mode: 0o600 })
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, f.io)
  assert.equal(result.status, 'complete')
  assert.equal(result.stateRemoved, true)
  assert.equal(result.coreCleanup.status, 'complete')
  await assert.rejects(stat(f.stateRoot), { code: 'ENOENT' })
  assert.equal(await readFile(join(normalRoot, 'preserve'), 'utf8'), 'normal data')
  const snapshot = await core.snapshot()
  assert.equal(snapshot.sources.some(source => source.key === keys.test), false)
  for (const name of ['normal', 'dsh']) assert.equal(snapshot.tasks.find(task => task.sourceKey === keys[name]).status, 'running')
  assert.deepEqual(f.calls, ['plugin/list', 'plugin/list', 'metadata-close'])
})

for (const scenario of ['catalog-missing', 'catalog-error', 'duplicate-plugin', 'unknown-installed']) test(`cleanup refuses ambiguous uninstall evidence: ${scenario}`, async t => {
  const f = await fixture(t)
  const catalogs = {
    'catalog-missing': { marketplaces: [], marketplaceLoadErrors: [] },
    'catalog-error': { marketplaces: [], marketplaceLoadErrors: [{ message: 'unavailable' }] },
    'duplicate-plugin': { marketplaces: [{ name: 'ghostsentinel', plugins: [{ id: 'ghost-sentinel-test@ghostsentinel', name: 'ghost-sentinel-test', installed: false }, { id: 'ghost-sentinel-test@ghostsentinel', name: 'ghost-sentinel-test', installed: false }] }], marketplaceLoadErrors: [] },
    'unknown-installed': { marketplaces: [{ name: 'ghostsentinel', plugins: [{ id: 'ghost-sentinel-test@ghostsentinel', name: 'ghost-sentinel-test', enabled: false }] }], marketplaceLoadErrors: [] },
  }
  let closed = false
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, { metadata: { async request() { return catalogs[scenario] }, async close() { closed = true } }, async inspectInstallation() { assert.fail('catalog evidence must come first') } })
  assert.equal(result.status, 'needs-user-action')
  assert.equal(result.errorCode, 'github-plugin-uninstall-unconfirmed')
  assert.equal(closed, true)
  assert.ok(await stat(f.stateRoot))
})

for (const scenario of ['prepare', 'operation', 'maintenance', 'runtime', 'process']) test(`cleanup preserves unresolved lifecycle state: ${scenario}`, async t => {
  const f = await ownedFixture(t)
  const files = { prepare: '.github-prepare.lock', operation: 'github-operation.json', maintenance: 'state/maintenance.json', runtime: 'state/runtime.json' }
  if (files[scenario]) await writeFile(join(f.stateRoot, files[scenario]), 'retained pending state', { mode: 0o600 })
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, { ...f.io, ...(scenario === 'process' ? { async processes() { return [{ pid: 12345 }] } } : {}), async inspectInstallation() { assert.fail('pending work must block before ownership inspection') } })
  assert.equal(result.status, 'needs-user-action')
  assert.equal(result.errorCode, scenario === 'prepare' ? 'github-cleanup-prepare-in-progress' : 'github-cleanup-observation-not-settled')
  assert.ok(await stat(join(f.stateRoot, 'state/installation.json')))
})

test('unknown files and symlinks are preserved before any purge', async t => {
  const f = await ownedFixture(t)
  await writeFile(join(f.stateRoot, 'keep-user-note'), 'must remain', { mode: 0o600 })
  await assert.rejects(cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, f.io), { code: 'github-cleanup-unknown-files-preserved' })
  assert.equal(await readFile(join(f.stateRoot, 'keep-user-note'), 'utf8'), 'must remain')
  await rm(join(f.stateRoot, 'keep-user-note'))
  await symlink(join(dirname(f.stateRoot), 'stable'), join(f.stateRoot, 'foreign-link'))
  await assert.rejects(cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, f.io), { code: 'github-cleanup-entry-untrusted' })
  await assert.rejects(stat(join(f.stateRoot, '.github-prepare.lock')), { code: 'ENOENT' })
})

test('a concurrent file change during cleanup keeps installation data instead of deleting it', async t => {
  const f = await ownedFixture(t), coreDataDir = f.config.coreDataDir
  await mkdir(coreDataDir, { mode: 0o700 })
  await writeFile(join(coreDataDir, 'observations.sqlite'), '', { mode: 0o600 })
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, { ...f.io, async connectCore() { return { async health() { return { features: ['chatgpt-installation-purge/1'] } }, async purgeChatGPTInstallation(scope) { assert.deepEqual(scope, { installationId: f.config.installationId }); await writeFile(join(f.releaseRoot, 'owned.mjs'), '// concurrent change\n'); return { installationId: scope.installationId, registrationId: 'chatgpt:' + scope.installationId, status: 'complete', logical: { status: 'purged' }, physical: { status: 'checkpointed', secureDelete: true } } }, async close() {} } } })
  assert.equal(result.status, 'needs-user-action')
  assert.equal(result.errorCode, 'github-cleanup-files-changed')
  assert.ok(await stat(join(f.stateRoot, 'state/installation.json')))
})

test('cleanup CLI defaults to preview and accepts only an explicit matching channel root', async t => {
  assert.deepEqual(parseGitHubCleanupArguments(['--state-root', '/private/example/github/test', '--channel', 'test']), { apply: false, stateRoot: '/private/example/github/test', channel: 'test' })
  assert.deepEqual(parseGitHubCleanupArguments(['--state-root', '/private/example/github/stable', '--channel', 'stable', '--apply']), { apply: true, stateRoot: '/private/example/github/stable', channel: 'stable' })
  for (const args of [[], ['--channel', 'test'], ['--state-root', '/private/a', '--channel', 'normal'], ['--state-root', '/private/a', '--channel', 'test', '--apply', '--apply']]) assert.throws(() => parseGitHubCleanupArguments(args), /arguments-invalid/)
  const f = await fixture(t)
  await assert.rejects(cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'stable' }), { code: 'github-cleanup-root-invalid' })
})

for (const complete of [true, false]) test(`native cleanup is installation-scoped and bounded when completion is ${complete}`, async t => {
  const f = await ownedFixture(t), bundlePath = join(f.releaseRoot, 'notification/GhostSentinelNotifications.app')
  await mkdir(bundlePath, { recursive: true, mode: 0o700 })
  await writeFile(join(bundlePath, 'fixture'), 'Native boundary fixture', { mode: 0o600 })
  f.config.notificationBundlePath = bundlePath
  f.receipt.releases[0].notificationBundlePath = bundlePath
  await writeFile(join(f.stateRoot, 'state/installation.json'), JSON.stringify(f.config), { mode: 0o600 })
  await writeFile(join(f.stateRoot, 'github-installation.json'), JSON.stringify(f.receipt), { mode: 0o600 })
  const { createNativeNotificationIO, notificationPrefix } = await import('../chatgpt/native-notifications.mjs')
  const requests = [], native = createNativeNotificationIO({ bundlePath, installationId: f.config.installationId, testMode: true, run: async (file, args, input) => {
    const request = JSON.parse(input); requests.push(request)
    assert.deepEqual(request, { schemaVersion: 1, operation: 'purge-installation', ownerPrefix: notificationPrefix(f.config.installationId) })
    const done = complete && requests.length === 2
    return { stdout: JSON.stringify({ schemaVersion: 1, operation: 'purge-installation', bundleId: 'com.ghostsentinel.notifications', ok: true, result: { complete: done, requestedCancellations: 1, remainingPending: done ? 0 : 1, clearedActions: 0, deliveredPreserved: true, observedAt: 1000 } }) }
  } })
  const result = await cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, { ...f.io, notifications: native, async inspectPackage(...args) { const value = await f.io.inspectPackage(...args); return { ...value, notificationApp: bundlePath, manifest: { files: { ...value.manifest.files, 'notification/GhostSentinelNotifications.app/fixture': {} } } } } })
  assert.equal(requests.length, complete ? 2 : 3)
  assert.equal(result.status, complete ? 'complete' : 'needs-user-action')
  if (complete) assert.equal(result.notifications.deliveredPreserved, true)
  else { assert.equal(result.errorCode, 'github-cleanup-notifications-incomplete'); assert.ok(await stat(join(f.stateRoot, 'state/installation.json'))) }
})

test('a cleanup failure releases its lock and metadata while preserving both original and close errors', async t => {
  const f = await ownedFixture(t), original = Object.assign(new Error('owned package read failed'), { code: 'owned-read-failed' }), closing = Object.assign(new Error('owned metadata close failed'), { code: 'owned-close-failed' })
  let closed = false
  await assert.rejects(cleanupGitHubInstallation({ stateRoot: f.stateRoot, channel: 'test', apply: true }, { ...f.io, async inspectPackage() { throw original }, metadata: { ...f.io.metadata, async close() { closed = true; throw closing } } }), error => error instanceof AggregateError && error.cause === original && error.errors.includes(original) && error.errors.includes(closing))
  assert.equal(closed, true)
  await assert.rejects(stat(join(f.stateRoot, '.github-prepare.lock')), { code: 'ENOENT' })
  assert.ok(await stat(join(f.stateRoot, 'state/installation.json')))
})
