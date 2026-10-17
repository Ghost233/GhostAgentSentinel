import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, realpath, mkdir, writeFile, readFile, rm, stat, symlink } from 'node:fs/promises'
import { join, dirname } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { previewPluginChange, applyPluginChange, pluginManagementDefaults } from '../chatgpt/plugin-installation.mjs'
import { installationWorld, unlaunchedWorld } from './fixtures/chatgpt-plugin-installation/world.mjs'
import { execFile, spawn } from 'node:child_process'
import { promisify } from 'node:util'
import { fileURLToPath } from 'node:url'
import { once } from 'node:events'
import fs from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import { resolvePluginOwner } from '../chatgpt/runtime.mjs'
import { setTimeout as delay } from 'node:timers/promises'

async function enableInTestApp(world, initialize, configPath) {
  await world.host.setEnabled(true, world.host.state.revision)
  await initialize(world.host.state.revision)
  const deadline = Date.now() + 5000
  while (!await resolvePluginOwner({ configPath })) {
    assert.ok(Date.now() < deadline, 'the simulated App must finish observation startup before it reports activation')
    await delay(25)
  }
}

async function fixture(t) {
  const dir = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-plugin-management-'))), installRoot = join(dir, 'install'), packageRoot = join(dir, 'package')
  await mkdir(packageRoot, { mode: 0o700 })
  const digest = createHash('sha256').update('owned-preview-package').digest('hex')
  const inspected = { root: packageRoot, digest, manifest: { version: '0.2.0-dev.1', runtimeVersion: '0.1.0-dev.1' }, nodePath: join(packageRoot, 'runtime/node'), bridgePath: join(packageRoot, 'chatgpt/plugin-bridge.mjs'), ownerPath: join(packageRoot, 'chatgpt/plugin-main.mjs'), notificationApp: null }
  let mutations = 0
  const host = { async inspect() { return { pluginId: 'ghost-sentinel@owned-test', installed: false, enabled: 'disabled', revision: 'r0', pluginPath: null, marketplaceRegistered: false } }, async register() { mutations++; assert.fail('preview cannot register a plugin') }, async setEnabled() { mutations++; assert.fail('preview cannot change plugin settings') }, async unregister() { mutations++; assert.fail('preview cannot unregister a plugin') } }
  const options = { operation: 'install', installRoot, packageRoot, pluginName: 'ghost-sentinel', marketplaceName: 'owned-test', pluginId: 'ghost-sentinel@owned-test', coreDataDir: join(dir, 'core'), sessionsRoot: join(dir, 'sessions'), metadataSocket: join(dir, 'metadata.sock') }
  return { dir, installRoot, packageRoot, options, io: { host, inspectPackage: async () => inspected }, mutations: () => mutations }
}

test('plugin installation preview is read-only and describes explicit stdio activation rather than an independent service', async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  const plan = await previewPluginChange(f.options, f.io)
  assert.equal(plan.canApply, true)
  assert.equal(plan.activationExpectation, 'enabled-with-authenticated-release-owner')
  assert.equal(plan.paths.pluginDir, join(f.installRoot, 'state/plugin'))
  assert.equal(plan.package.digest.length, 64)
  assert.equal(plan.host.installed, false)
  assert.equal(f.mutations(), 0)
  await assert.rejects(stat(f.installRoot), { code: 'ENOENT' })
  assert.equal(JSON.stringify(plan).includes('SMAppService'), false)
  assert.equal(JSON.stringify(plan).includes('activation.pending'), false)
})

test('an installation root cannot contain the shared Core or the observed sessions directory', async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  for (const key of ['coreDataDir', 'sessionsRoot']) await assert.rejects(previewPluginChange({ ...f.options, [key]: join(f.installRoot, key) }, f.io), error => error.code === 'installation_scope_overlaps_observation')
  await assert.rejects(stat(f.installRoot), { code: 'ENOENT' })
})

test('legacy operations and unknown installation files are listed as blockers and never adopted', async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  await mkdir(f.installRoot, { mode: 0o700 })
  const path = join(f.installRoot, 'helper-operation.json'); await writeFile(path, '{"status":"failed"}', { mode: 0o600 })
  const plan = await previewPluginChange(f.options, f.io)
  assert.equal(plan.canApply, false)
  assert.ok(plan.blockers.some(value => value.code === 'unmanaged_installation_files'))
  await assert.rejects(applyPluginChange(plan, f.io), error => error.code === 'installation_plan_blocked')
  assert.equal(await readFile(path, 'utf8'), '{"status":"failed"}')
  assert.equal(f.mutations(), 0)
})

test('install enables an authenticated owner from the installed release and full uninstall preserves a real DSH Core source', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  const plan = await previewPluginChange({ ...f.options, packageRoot }, f.io)
  const installed = await applyPluginChange(plan, f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  assert.equal(installed.activation.enabled, true)
  assert.equal(installed.activation.ownerVerified, true)
  assert.equal(installed.activation.ownerEntry, join(f.installRoot, 'releases', plan.package.digest, 'chatgpt/plugin-main.mjs'))
  const config = JSON.parse(await readFile(join(f.installRoot, 'state/installation.json'), 'utf8'))
  const snapshot = await f.core.snapshot()
  assert.equal(snapshot.tasks.find(task => task.id === 'resource-main').currentRequest, 'Read this synthetic resource fixture only')
  const launch = JSON.parse(await readFile(join(f.installRoot, 'state/plugin/.mcp.json'), 'utf8')).mcpServers.sentinel
  assert.equal(launch.command, join(f.installRoot, 'releases', plan.package.digest, 'runtime/node'))
  assert.equal(launch.url, undefined)
  const uninstall = await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io)
  const removed = await applyPluginChange(uninstall, f.io)
  assert.equal(removed.status, 'complete')
  assert.equal(removed.processes.remaining, 0)
  assert.equal(removed.coreCleanup.installationId, config.installationId)
  assert.equal(removed.coreCleanup.status, 'complete')
  assert.equal((await f.host.inspect()).installed, false)
  assert.equal((await f.host.inspect()).marketplaceRegistered, false)
  assert.equal((await f.core.snapshot()).sources[0].key, f.peer)
  await f.core.heartbeat(f.peer)
  await assert.rejects(stat(join(f.installRoot, 'state/installation.json')), { code: 'ENOENT' })
})

test('a real release upgrade and failed activation rollback preserve identity, claimed enrollment, pause and first capture', { timeout: 40000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2'), third = await f.packageFixture('0.2.0-dev.3')
  const install = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(install.status, 'complete', JSON.stringify(install))
  const path = join(f.installRoot, 'state/installation.json'), original = JSON.parse(await readFile(path, 'utf8')), task = (await f.core.snapshot()).tasks.find(task => task.id === 'resource-main'), firstCapture = task.events.find(event => event.kind === 'input').capturedAt
  assert.equal(original.initialEnrollmentBaseline.claimable, false)
  const scope = { registrationId: 'chatgpt:' + original.installationId, taskId: task.id }, control = await f.core.conversationMonitoring(scope)
  await f.core.setConversationMonitoring({ ...scope, paused: true, revision: control.revision })
  const upgrade = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: second }, f.io), f.io)
  assert.equal(upgrade.status, 'complete', JSON.stringify(upgrade))
  assert.notEqual(upgrade.activation.runId, install.activation.runId)
  assert.equal(upgrade.activation.ownerEntry, join(f.installRoot, 'releases', upgrade.digest, 'chatgpt/plugin-main.mjs'))
  f.host.failNextEnable = 'owned_activation_failed'
  const failed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: third }, f.io), f.io)
  assert.equal(failed.status, 'recovery-required')
  assert.equal(failed.errorCode, 'owned_activation_failed')
  assert.equal(failed.rollback.status, 'complete', JSON.stringify(failed))
  assert.equal(failed.rollback.restoredDigest, upgrade.digest)
  assert.notEqual(failed.rollback.activation.runId, upgrade.activation.runId)
  const saved = JSON.parse(await readFile(path, 'utf8'))
  for (const key of ['installationId', 'token', 'initialEnrollmentBaseline']) assert.deepEqual(saved[key], original[key])
  assert.equal((await f.core.conversationMonitoring(scope)).paused, true)
  assert.equal((await f.core.snapshot()).tasks.find(task => task.id === 'resource-main').events.find(event => event.kind === 'input').capturedAt, firstCapture)
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('metadata enabled without a spawned owner stays resumable and cannot be reported as installed', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  f.io.timeoutMs = 300; f.host.autoStart = false
  const result = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(result.status, 'needs-user-action', JSON.stringify(result))
  assert.equal(result.errorCode, 'plugin_activation_not_observed')
  assert.equal((await f.host.inspect()).enabled, 'disabled')
  assert.ok((await stat(join(f.installRoot, 'state/maintenance.json'))).isFile())
  const blocked = await previewPluginChange({ ...f.options, packageRoot }, f.io)
  assert.equal(blocked.canApply, false)
  assert.ok(blocked.blockers.some(value => value.code === 'unfinished_plugin_operation'))
  f.io.timeoutMs = 15000; f.host.autoStart = true
  const resumed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot, resumeOperationId: result.operationId }, f.io), f.io)
  assert.equal(resumed.status, 'complete', JSON.stringify(resumed))
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('connection initialization failure after a confirmed enable preserves the revision needed for natural failure cleanup', async t => {
  const f = await unlaunchedWorld(t)
  let initialized = 0
  f.host.initializeConnection = async revision => {
    initialized++
    assert.equal(revision, f.host.state.revision)
    assert.equal(f.host.state.enabled, 'enabled')
    throw Object.assign(new Error('Owned connection initialization failure'), { code: 'owned_initialization_failed' })
  }
  const result = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io)
  assert.equal(result.errorCode, 'owned_initialization_failed')
  assert.equal(initialized, 1)
  assert.equal(result.failureCleanup?.naturalExit, true, JSON.stringify(result))
  assert.equal(result.failureCleanup.remaining, 0)
  assert.equal(f.host.state.enabled, 'disabled')
})

test('a configuration-only host leaves a recoverable App activation step without warming or undoing the installation', async t => {
  const f = await unlaunchedWorld(t)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const result = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io)
  assert.equal(result.status, 'needs-user-action', JSON.stringify(result))
  assert.equal(result.errorCode, 'plugin_app_activation_required')
  assert.equal(result.action, 'enable-in-app')
  assert.equal(result.configurationRetained, true)
  assert.equal(result.failureCleanup, undefined)
  assert.equal(f.host.state.installed, true)
  assert.equal(f.host.state.enabled, 'disabled')
  const transaction = JSON.parse(await readFile(join(f.installRoot, 'operation.json'), 'utf8'))
  assert.equal(transaction.phase, 'awaiting-app-activation')
  assert.equal(transaction.appActivation.digest, JSON.parse(await readFile(join(f.installRoot, 'installation-receipt.json'), 'utf8')).activeDigest)
  await assert.rejects(stat(join(f.installRoot, 'state/maintenance.json')), { code: 'ENOENT' })
  const revision = f.host.state.revision
  const resumed = await applyPluginChange(await previewPluginChange({ ...f.options, resumeOperationId: result.operationId }, f.io), f.io)
  assert.equal(resumed.status, 'needs-user-action')
  assert.equal(resumed.operationId, result.operationId)
  assert.equal(f.host.state.revision, revision, 'resume cannot toggle or reinstall while waiting for the native App')
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: result.operationId }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('a pending App activation survives final lock cleanup failure without registering the plugin again', async t => {
  const f = await unlaunchedWorld(t), originalRm = fs.rm, register = f.host.register.bind(f.host)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  let registrations = 0, injected = false, result
  f.host.register = async () => { registrations++; return register() }
  fs.rm = async (path, ...args) => {
    if (!injected && path === join(f.installRoot, '.operation.lock/owner.json')) { injected = true; throw Object.assign(new Error('Owned final lock failure'), { code: 'EROFS' }) }
    return originalRm(path, ...args)
  }
  syncBuiltinESMExports()
  try { result = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io) }
  finally { fs.rm = originalRm; syncBuiltinESMExports() }
  assert.equal(result.status, 'recovery-required')
  assert.equal(result.lockCleanupErrorCode, 'EROFS')
  const revision = f.host.state.revision, resume = { ...f.options, resumeOperationId: result.operationId }
  const retried = await applyPluginChange(await previewPluginChange(resume, f.io), f.io)
  assert.equal(retried.errorCode, 'plugin_app_activation_required')
  assert.equal(registrations, 1)
  assert.equal(f.host.state.revision, revision)
  const removed = await applyPluginChange(await previewPluginChange({ ...resume, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('an unconfirmed App activation transaction is rejected without changing the plugin or its saved evidence', async t => {
  const f = await unlaunchedWorld(t)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const pending = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io)
  const path = join(f.installRoot, 'operation.json'), transaction = JSON.parse(await readFile(path, 'utf8'))
  transaction.configIdentity = '0'.repeat(64)
  const evidence = JSON.stringify(transaction, null, 2) + '\n'
  await writeFile(path, evidence, { mode: 0o600 })
  await writeFile(join(f.installRoot, 'transactions', pending.operationId + '.json'), evidence, { mode: 0o600 })
  const before = await f.host.inspect(), plan = await previewPluginChange({ ...f.options, resumeOperationId: pending.operationId }, f.io)
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'plugin_transaction_identity_unconfirmed' })
  assert.deepEqual(await f.host.inspect(), before)
  assert.equal(await readFile(path, 'utf8'), evidence)
  await assert.rejects(stat(join(f.installRoot, 'state/maintenance.json')), { code: 'ENOENT' })
})

test('native App activation resumes the staged release without repeating registration or overwriting the user toggle', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  const initialize = f.host.initializeConnection.bind(f.host)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const staged = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(staged.errorCode, 'plugin_app_activation_required')
  const configPath = join(f.installRoot, 'state/installation.json'), before = JSON.parse(await readFile(configPath, 'utf8'))
  await enableInTestApp(f, initialize, configPath)
  const calls = f.host.calls.slice(), revision = f.host.state.revision
  const resumeOptions = { ...f.options, packageRoot, resumeOperationId: staged.operationId }
  const ownerOnly = await applyPluginChange(await previewPluginChange(resumeOptions, f.io), f.io)
  assert.equal(ownerOnly.errorCode, 'plugin_app_activation_required', 'a running observation alone cannot prove an App connection')
  const witness = bridgePath => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: f.children.at(-1).pid, bridgeEntry: bridgePath })
  let witnessed = 0
  f.host.appActivationWitness = async ({ bridgePath }) => ++witnessed === 1 ? witness(bridgePath) : null
  const disappeared = await applyPluginChange(await previewPluginChange(resumeOptions, f.io), f.io)
  assert.equal(disappeared.errorCode, 'plugin_app_activation_required', 'a late-lost App connection cannot complete the receipt')
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'installation-receipt.json'), 'utf8')).status, 'staged')
  f.host.appActivationWitness = async ({ bridgePath }) => witness(bridgePath)
  const resumed = await applyPluginChange(await previewPluginChange(resumeOptions, f.io), f.io)
  assert.equal(resumed.status, 'complete', JSON.stringify(resumed))
  assert.equal(resumed.activation.appConnectionVerified, true)
  assert.deepEqual(f.host.calls, calls)
  assert.equal(f.host.state.revision, revision)
  const after = JSON.parse(await readFile(configPath, 'utf8'))
  for (const key of ['installationId', 'token']) assert.equal(after[key], before[key])
  assert.equal(after.initialEnrollmentBaseline.id, before.initialEnrollmentBaseline.id)
  assert.equal(after.initialEnrollmentBaseline.claimable, false, 'the first real observation consumes the original enrollment claim once')
  await assert.rejects(stat(join(f.installRoot, 'operation.json')), { code: 'ENOENT' })
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
  await f.core.heartbeat(f.peer)
})

test('a failed upgrade can restore an old release pending native App activation without retrying the failed candidate', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2')
  const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  const configPath = join(f.installRoot, 'state/installation.json'), original = JSON.parse(await readFile(configPath, 'utf8'))
  const task = (await f.core.snapshot()).tasks.find(row => row.id === 'resource-main'), firstCapture = task.events.find(row => row.kind === 'input').capturedAt
  const scope = { registrationId: 'chatgpt:' + original.installationId, taskId: task.id }, monitoring = await f.core.conversationMonitoring(scope)
  await f.core.setConversationMonitoring({ ...scope, paused: true, revision: monitoring.revision })
  const initialize = f.host.initializeConnection.bind(f.host)
  let firstAttempt = true
  f.host.initializeConnection = async () => {
    if (firstAttempt) { firstAttempt = false; throw Object.assign(new Error('Owned candidate failure'), { code: 'owned_candidate_failed' }) }
    return { appActivationRequired: true, connectionScope: 'configuration-only' }
  }
  const options = { ...f.options, operation: 'upgrade', packageRoot: second }
  const waiting = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(waiting.status, 'needs-user-action', JSON.stringify(waiting))
  assert.equal(waiting.errorCode, 'plugin_app_activation_required')
  assert.equal(waiting.originalErrorCode, 'owned_candidate_failed')
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'installation-receipt.json'), 'utf8')).activeDigest, installed.digest)
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'operation.json'), 'utf8')).phase, 'awaiting-app-activation')
  await f.host.setEnabled(true, f.host.state.revision)
  f.host.appActivationWitness = async ({ bridgePath }) => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: process.pid, bridgeEntry: bridgePath })
  f.io.timeoutMs = 1
  const resumeOptions = { ...options, resumeOperationId: waiting.operationId }, failedCheck = await applyPluginChange(await previewPluginChange(resumeOptions, f.io), f.io)
  assert.equal(failedCheck.errorCode, 'plugin_activation_not_observed')
  assert.equal(failedCheck.originalErrorCode, 'owned_candidate_failed')
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'operation.json'), 'utf8')).errorCode, 'owned_candidate_failed')
  f.io.timeoutMs = 15000
  await enableInTestApp(f, initialize, configPath)
  f.host.appActivationWitness = async ({ bridgePath }) => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: f.children.at(-1).pid, bridgeEntry: bridgePath })
  const calls = f.host.calls.slice()
  const resumed = await applyPluginChange(await previewPluginChange(resumeOptions, f.io), f.io)
  assert.equal(resumed.status, 'rolled-back', JSON.stringify(resumed))
  assert.equal(resumed.errorCode, 'owned_candidate_failed')
  assert.equal(resumed.restoredDigest, installed.digest)
  assert.equal(resumed.rollback.activation.appConnectionVerified, true)
  assert.deepEqual(f.host.calls, calls)
  const saved = JSON.parse(await readFile(configPath, 'utf8'))
  for (const key of ['installationId', 'token']) assert.equal(saved[key], original[key])
  assert.equal((await f.core.conversationMonitoring(scope)).paused, true)
  assert.equal((await f.core.snapshot()).tasks.find(row => row.id === task.id).events.find(row => row.kind === 'input').capturedAt, firstCapture)
  await assert.rejects(stat(join(f.installRoot, 'operation.json')), { code: 'ENOENT' })
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
  await f.core.heartbeat(f.peer)
})

test('an upgrade awaiting native App activation preserves enrollment, pause and first capture until its exact release is resumed', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2')
  const initial = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(initial.status, 'complete', JSON.stringify(initial))
  const configPath = join(f.installRoot, 'state/installation.json'), before = JSON.parse(await readFile(configPath, 'utf8'))
  const task = (await f.core.snapshot()).tasks.find(row => row.id === 'resource-main'), firstCapture = task.events.find(row => row.kind === 'input').capturedAt
  const scope = { registrationId: 'chatgpt:' + before.installationId, taskId: task.id }, monitoring = await f.core.conversationMonitoring(scope)
  await f.core.setConversationMonitoring({ ...scope, paused: true, revision: monitoring.revision })
  const initialize = f.host.initializeConnection.bind(f.host)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const options = { ...f.options, operation: 'upgrade', packageRoot: second }
  const staged = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(staged.errorCode, 'plugin_app_activation_required')
  const receipt = JSON.parse(await readFile(join(f.installRoot, 'installation-receipt.json'), 'utf8'))
  assert.notEqual(receipt.activeDigest, initial.digest)
  assert.equal((await f.core.conversationMonitoring(scope)).paused, true)
  await enableInTestApp(f, initialize, configPath)
  f.host.appActivationWitness = async ({ bridgePath }) => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: f.children.at(-1).pid, bridgeEntry: bridgePath })
  const calls = f.host.calls.slice()
  const resumed = await applyPluginChange(await previewPluginChange({ ...options, resumeOperationId: staged.operationId }, f.io), f.io)
  assert.equal(resumed.status, 'complete', JSON.stringify(resumed))
  assert.equal(resumed.digest, receipt.activeDigest)
  assert.equal(resumed.activation.appConnectionVerified, true)
  assert.deepEqual(f.host.calls, calls)
  const after = JSON.parse(await readFile(configPath, 'utf8'))
  for (const key of ['installationId', 'token', 'initialEnrollmentBaseline']) assert.deepEqual(after[key], before[key])
  assert.equal((await f.core.conversationMonitoring(scope)).paused, true)
  assert.equal((await f.core.snapshot()).tasks.find(row => row.id === task.id).events.find(row => row.kind === 'input').capturedAt, firstCapture)
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
  await f.core.heartbeat(f.peer)
})

test('an App-verified receipt recovers its final lock without restarting or registering the installation again', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1'), initialize = f.host.initializeConnection.bind(f.host)
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const staged = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(staged.errorCode, 'plugin_app_activation_required')
  const configPath = join(f.installRoot, 'state/installation.json')
  await enableInTestApp(f, initialize, configPath)
  f.host.appActivationWitness = async ({ bridgePath }) => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: f.children.at(-1).pid, bridgeEntry: bridgePath })
  const calls = f.host.calls.slice(), owner = await resolvePluginOwner({ configPath }), options = { ...f.options, packageRoot, resumeOperationId: staged.operationId }, originalRm = fs.rm
  let injected = false, failed
  fs.rm = async (path, ...args) => {
    if (!injected && path === join(f.installRoot, '.operation.lock/owner.json')) { injected = true; throw Object.assign(new Error('Owned final lock failure'), { code: 'EROFS' }) }
    return originalRm(path, ...args)
  }
  syncBuiltinESMExports()
  try { failed = await applyPluginChange(await previewPluginChange(options, f.io), f.io) }
  finally { fs.rm = originalRm; syncBuiltinESMExports() }
  assert.equal(failed.status, 'recovery-required')
  assert.equal(failed.lockCleanupErrorCode, 'EROFS')
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'installation-receipt.json'), 'utf8')).status, 'installed')
  const recovered = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(recovered.status, 'complete', JSON.stringify(recovered))
  assert.equal(recovered.activation.appConnectionVerified, true)
  assert.deepEqual(f.host.calls, calls)
  assert.equal((await resolvePluginOwner({ configPath })).runId, owner.runId)
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

for (const operation of ['install', 'upgrade']) test(`resuming native App ${operation} without an authenticated owner never repeats host mutations`, { timeout: 30000 }, async t => {
  const f = operation === 'install' ? await unlaunchedWorld(t) : await installationWorld(t)
  let packageRoot = f.options.packageRoot
  if (operation === 'upgrade') {
    const first = await f.packageFixture('0.2.0-dev.1')
    const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
    assert.equal(installed.status, 'complete', JSON.stringify(installed))
    packageRoot = await f.packageFixture('0.2.0-dev.2')
  }
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const options = { ...f.options, operation, packageRoot }, pending = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(pending.errorCode, 'plugin_app_activation_required')
  await f.host.setEnabled(true, f.host.state.revision)
  f.host.appActivationWitness = async ({ bridgePath }) => ({ appHostRunId: 'owned-app-instance', appHostStartedAt: 1, bridgePid: process.pid, bridgeEntry: bridgePath })
  f.io.timeoutMs = 1
  const revision = f.host.state.revision, calls = f.host.calls?.slice()
  const resumed = await applyPluginChange(await previewPluginChange({ ...options, resumeOperationId: pending.operationId }, f.io), f.io)
  assert.equal(resumed.errorCode, 'plugin_activation_not_observed', JSON.stringify(resumed))
  assert.equal(f.host.state.revision, revision)
  assert.equal(f.host.state.enabled, 'enabled')
  if (calls) assert.deepEqual(f.host.calls, calls)
  await assert.rejects(stat(join(f.installRoot, 'state/maintenance.json')), { code: 'ENOENT' })
  assert.equal(JSON.parse(await readFile(join(f.installRoot, 'operation.json'), 'utf8')).phase, 'awaiting-app-activation')
  const removed = await applyPluginChange(await previewPluginChange({ ...options, operation: 'uninstall', resumeOperationId: pending.operationId }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('resuming a staged upgrade preserves a native App uninstall instead of registering the old release again', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2')
  const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  f.host.initializeConnection = async () => ({ appActivationRequired: true, connectionScope: 'configuration-only' })
  const options = { ...f.options, operation: 'upgrade', packageRoot: second }, staged = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(staged.errorCode, 'plugin_app_activation_required')
  await f.host.unregister()
  const before = await f.host.inspect(), calls = f.host.calls.slice()
  const result = await applyPluginChange(await previewPluginChange({ ...options, resumeOperationId: staged.operationId }, f.io), f.io)
  assert.equal(result.errorCode, 'plugin_app_activation_qualification_changed')
  assert.equal(result.rollback, null)
  assert.deepEqual(await f.host.inspect(), before)
  assert.deepEqual(f.host.calls, calls)
  await assert.rejects(stat(join(f.installRoot, 'state/maintenance.json')), { code: 'ENOENT' })
  const removed = await applyPluginChange(await previewPluginChange({ ...options, operation: 'uninstall', resumeOperationId: staged.operationId }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
  await f.core.heartbeat(f.peer)
})

test('a failed upgrade connection initialization restores the old release without losing pause or first capture', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2')
  const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  const configPath = join(f.installRoot, 'state/installation.json'), before = JSON.parse(await readFile(configPath, 'utf8'))
  const task = (await f.core.snapshot()).tasks.find(task => task.id === 'resource-main'), capturedAt = task.events.find(event => event.kind === 'input').capturedAt
  const scope = { registrationId: 'chatgpt:' + before.installationId, taskId: task.id }, monitoring = await f.core.conversationMonitoring(scope)
  await f.core.setConversationMonitoring({ ...scope, paused: true, revision: monitoring.revision })
  f.host.failNextInitialize = 'owned_initialization_failed'
  const failed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: second }, f.io), f.io)
  assert.equal(failed.errorCode, 'owned_initialization_failed')
  assert.equal(failed.rollback?.status, 'complete', JSON.stringify(failed))
  assert.equal(failed.rollback.restoredDigest, installed.digest)
  assert.equal(f.host.state.enabled, 'enabled')
  const after = JSON.parse(await readFile(configPath, 'utf8'))
  for (const key of ['installationId', 'token']) assert.equal(after[key], before[key])
  assert.equal((await f.core.conversationMonitoring(scope)).paused, true)
  assert.equal((await f.core.snapshot()).tasks.find(row => row.id === task.id).events.find(event => event.kind === 'input').capturedAt, capturedAt)
  await f.core.heartbeat(f.peer)
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
})

test('resuming a staged installation preserves user-edited descriptors and blocks unproved partial writes', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  f.io.timeoutMs = 100; f.host.autoStart = false
  const first = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(first.status, 'needs-user-action')
  const path = join(f.installRoot, 'state/plugin/.mcp.json'), original = await readFile(path, 'utf8'), userText = '{"userChoice":"preserve this edit"}\n'
  await writeFile(path, userText, { mode: 0o600 })
  const options = { ...f.options, packageRoot, resumeOperationId: first.operationId }, changed = await previewPluginChange(options, f.io)
  assert.equal(changed.canApply, false)
  assert.ok(changed.blockers.some(value => value.code === 'plugin_descriptor_concurrently_changed'))
  await assert.rejects(applyPluginChange(changed, f.io), { code: 'installation_plan_blocked' })
  assert.equal(await readFile(path, 'utf8'), userText)
  await writeFile(path, original, { mode: 0o600 })
  const disable = f.host.setEnabled.bind(f.host); let concurrentEdit = false
  f.host.setEnabled = async (enabled, revision) => { const result = await disable(enabled, revision); if (!enabled && !concurrentEdit) { concurrentEdit = true; await writeFile(path, userText, { mode: 0o600 }) }; return result }
  const raced = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(raced.errorCode, 'plugin_descriptor_concurrently_changed')
  assert.equal(await readFile(path, 'utf8'), userText, 'stop/register waits must not create an overwrite window after a valid preview')
  await writeFile(path, original, { mode: 0o600 })
  const operation = join(f.installRoot, 'operation.json'), journal = JSON.parse(await readFile(operation, 'utf8'))
  delete journal.writtenFiles
  await writeFile(operation, JSON.stringify(journal), { mode: 0o600 })
  const unproved = await previewPluginChange(options, f.io)
  assert.equal(unproved.canApply, false, 'a crash before write provenance was journaled cannot authorize rewriting descriptor files')
  assert.ok(unproved.blockers.some(value => value.code === 'plugin_descriptor_concurrently_changed'))
})

test('an explicitly fresh uninstall resume can clean a partial marketplace registration after its revision conflict', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  f.host.register = async () => { f.host.calls.push('partial-register'); f.host.state.marketplaceRegistered = true; f.host.state.revision = 'fresh-partial-add-revision'; throw Object.assign(new Error('Revision changed after add'), { code: 'plugin_host_revision_conflict' }) }
  const first = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(first.status, 'recovery-required')
  assert.equal(first.errorCode, 'plugin_host_revision_conflict')
  assert.equal(first.failureCleanup.errorCode, 'plugin_host_concurrently_changed')
  assert.equal(f.host.state.revision, 'fresh-partial-add-revision')
  const retry = await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: first.operationId }, f.io)
  assert.equal(retry.canApply, true, JSON.stringify(retry.blockers))
  assert.equal(retry.host.revision, 'fresh-partial-add-revision')
  const removed = await applyPluginChange(retry, f.io)
  assert.equal(removed.status, 'complete', JSON.stringify(removed))
  assert.equal(f.host.state.marketplaceRegistered, false)
  assert.equal(f.host.calls.includes('setEnabled:true'), false)
  await f.core.heartbeat(f.peer)
})

for (const name of ['state/maintenance.json', 'releases/unknown-release', 'transactions/user-backup.json']) test(`preview explicitly lists unowned ${name} before mutating the host`, async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  await mkdir(name.endsWith('.json') ? dirname(join(f.installRoot, name)) : join(f.installRoot, name), { recursive: true, mode: 0o700 })
  if (name.endsWith('.json')) await writeFile(join(f.installRoot, name), '{"owner":"foreign"}', { mode: 0o600 })
  const plan = await previewPluginChange(f.options, f.io)
  assert.equal(plan.canApply, false)
  assert.ok(plan.blockers.some(value => value.entries?.includes(name)), JSON.stringify(plan.blockers))
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'installation_plan_blocked' })
  assert.equal(f.mutations(), 0)
  assert.ok(await stat(join(f.installRoot, name)))
})

test('a foreign malformed maintenance record is listed without adopting or parsing its private contents', async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  await mkdir(join(f.installRoot, 'state'), { recursive: true, mode: 0o700 })
  await writeFile(join(f.installRoot, 'state/maintenance.json'), 'foreign private incomplete JSON', { mode: 0o600 })
  const plan = await previewPluginChange(f.options, f.io)
  assert.equal(plan.canApply, false)
  assert.ok(plan.blockers.some(value => value.entries?.includes('state/maintenance.json')))
  assert.equal(JSON.stringify(plan).includes('foreign private'), false)
})

for (const stage of ['release', 'maintenance', 'configuration', 'operation']) test(`uninstall ${stage} unlink failure retains truthful recovery state and same-operation retry completes`, { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1'), installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(installed.status, 'complete')
  const target = stage === 'release' ? join(f.installRoot, 'releases', installed.digest, 'chatgpt/plugin-bridge.mjs') : join(f.installRoot, stage === 'maintenance' ? 'state/maintenance.json' : stage === 'configuration' ? 'state/installation.json' : 'operation.json')
  const original = fs.rm; let injected = false, removed
  fs.rm = async (path, ...args) => { if (path === target && !injected) { injected = true; throw Object.assign(new Error('Synthetic unlink denial'), { code: 'EACCES' }) }; return original(path, ...args) }
  syncBuiltinESMExports()
  try { removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io) }
  finally { fs.rm = original; syncBuiltinESMExports() }
  assert.equal(injected, true)
  assert.equal(removed.status, 'recovery-required')
  assert.equal(removed.errorCode, 'EACCES')
  const configPresent = await stat(join(f.installRoot, 'state/installation.json')).then(() => true, error => { if (error.code === 'ENOENT') return false; throw error })
  assert.equal(removed.configurationRetained, configPresent)
  const retry = await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: removed.operationId }, f.io)
  assert.equal(retry.canApply, true, JSON.stringify(retry.blockers))
  const result = await applyPluginChange(retry, f.io)
  assert.equal(result.status, 'complete', JSON.stringify(result))
  assert.equal((await f.core.snapshot()).sources[0].key, f.peer)
  await f.core.heartbeat(f.peer)
})

test('uninstall keeps the primary unlink failure separate when recovery journal writing also fails', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)).status, 'complete')
  const operation = join(f.installRoot, 'operation.json'), originalRm = fs.rm, originalWrite = fs.writeFile
  let unlinkFailed = false, journalFailed = false, result
  fs.rm = async (path, ...args) => { if (path === operation && !unlinkFailed) { unlinkFailed = true; throw Object.assign(new Error('Synthetic unlink'), { code: 'EACCES' }) }; return originalRm(path, ...args) }
  fs.writeFile = async (path, ...args) => { if (unlinkFailed && String(path).startsWith(operation + '.') && !journalFailed) { journalFailed = true; throw Object.assign(new Error('Synthetic journal write'), { code: 'EROFS' }) }; return originalWrite(path, ...args) }
  syncBuiltinESMExports()
  try { result = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io) }
  finally { fs.rm = originalRm; fs.writeFile = originalWrite; syncBuiltinESMExports() }
  assert.equal(unlinkFailed, true); assert.equal(journalFailed, true)
  assert.equal(result.status, 'recovery-required')
  assert.equal(result.errorCode, 'EACCES')
  assert.equal(result.recoveryJournalErrorCode, 'EROFS')
  assert.equal(result.configurationRetained, false)
  const retry = await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: result.operationId }, f.io)
  assert.equal(retry.canApply, true, JSON.stringify(retry.blockers))
  assert.equal((await applyPluginChange(retry, f.io)).status, 'complete')
})

test('an authenticated preview cannot overwrite a concurrently changed host revision', async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1'), plan = await previewPluginChange({ ...f.options, packageRoot }, f.io)
  f.host.state.revision = 'changed-by-user'
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'installation_preview_changed' })
  assert.deepEqual(f.host.calls, [])
  await assert.rejects(stat(f.installRoot), { code: 'ENOENT' })
})

test('an upgrade failure cannot roll back over a concurrent user choice to disable the plugin', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), first = await f.packageFixture('0.2.0-dev.1'), second = await f.packageFixture('0.2.0-dev.2')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)).status, 'complete')
  const change = f.host.setEnabled.bind(f.host)
  f.host.setEnabled = async (enabled, revision) => {
    if (!enabled) return change(enabled, revision)
    f.host.state.revision = 'user-disabled-concurrently'; f.host.state.enabled = 'disabled'
    throw Object.assign(new Error('User chose disabled'), { code: 'host_config_conflict' })
  }
  const result = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: second }, f.io), f.io)
  assert.equal(result.status, 'recovery-required')
  assert.equal(result.rollback.status, 'incomplete')
  assert.equal(result.rollback.errorCode, 'plugin_host_concurrently_changed')
  assert.equal(f.host.state.revision, 'user-disabled-concurrently')
  assert.equal(f.host.state.enabled, 'disabled')
  assert.ok((await stat(join(f.installRoot, 'state/maintenance.json'))).isFile())
})

test('unknown own state files block uninstall preview before cleanup and remain preserved', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)).status, 'complete')
  const foreign = join(f.installRoot, 'state/user-note.txt'); await writeFile(foreign, 'keep owned user material', { mode: 0o600 })
  const plan = await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io)
  assert.equal(plan.canApply, false)
  assert.ok(plan.blockers.some(value => value.entries?.includes('state/user-note.txt')))
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'installation_plan_blocked' })
  assert.equal(await readFile(foreign, 'utf8'), 'keep owned user material')
  assert.ok((await stat(join(f.installRoot, 'state/installation.json'))).isFile())
  assert.equal((await f.core.snapshot()).sources[0].key, f.peer)
})

test('notification purge failure retains private recovery data instead of declaring an uninstall complete', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)).status, 'complete')
  const path = join(f.installRoot, 'state/installation.json'), config = JSON.parse(await readFile(path, 'utf8'))
  await writeFile(path, JSON.stringify({ ...config, notificationBundlePath: join(f.dir, 'Synthetic.app') }), { mode: 0o600 })
  let closes = 0
  f.io.notifications = { async purgeInstallation() { return { complete: false, remainingPending: 1 } }, async close() { closes++ } }
  const result = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(result.status, 'recovery-required')
  assert.equal(result.errorCode, 'notification_cleanup_incomplete')
  assert.equal(closes, 1)
  assert.equal((await f.host.inspect()).enabled, 'disabled')
  assert.ok((await stat(path)).isFile())
  assert.equal((await f.core.snapshot()).sources.some(source => source.key === f.peer), true)
})

test('uninstall waits for its release-scoped short stdio notification child before deleting executable files', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), executable = join(f.dir, 'synthetic-notifier')
  await writeFile(join(f.dir, 'synthetic-notifier.c'), '#include <unistd.h>\nint main(void) { char c; while (read(0,&c,1)>0) {} return 0; }\n', { mode: 0o600 })
  await promisify(execFile)('/usr/bin/clang', [join(f.dir, 'synthetic-notifier.c'), '-o', executable])
  const packageRoot = await f.packageFixture('0.2.0-dev.1', { notificationExecutable: executable })
  f.io.notifications = { async snapshot() { return { pending: [] } }, async purgeInstallation() { return { complete: true, remainingPending: 0, deliveredPreserved: true } }, async close() {} }
  const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  const app = JSON.parse(await readFile(join(f.installRoot, 'state/installation.json'), 'utf8')).notificationBundlePath
  const child = spawn(join(app, 'Contents/MacOS/GhostSentinelNotifications'), ['--stdio'], { stdio: ['pipe', 'ignore', 'ignore'] }), closed = once(child, 'close')
  child.stdin.on('error', () => {})
  await once(child, 'spawn')
  t.after(async () => { child.stdin.end(); await closed })
  const plan = await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io)
  let settled = false
  const removing = applyPluginChange(plan, f.io).then(value => { settled = true; return value })
  await new Promise(resolve => setTimeout(resolve, 2300))
  assert.equal(settled, false, 'an in-flight notification process must prevent complete deletion even after owner/bridge exit')
  assert.ok((await stat(join(app, 'Contents/MacOS/GhostSentinelNotifications'))).isFile())
  child.stdin.end(); assert.deepEqual(await closed, [0, null])
  assert.equal((await removing).status, 'complete')
})

test('release readiness permits its own short notification process but rejects another release and unknown notification arguments', { timeout: 45000 }, async t => {
  const f = await installationWorld(t), executable = join(f.dir, 'synthetic-ready-notifier')
  await writeFile(join(f.dir, 'synthetic-ready-notifier.c'), '#include <unistd.h>\nint main(void) { char c; while (read(0,&c,1)>0) {} return 0; }\n', { mode: 0o600 })
  await promisify(execFile)('/usr/bin/clang', [join(f.dir, 'synthetic-ready-notifier.c'), '-o', executable])
  const first = await f.packageFixture('0.2.0-dev.1', { notificationExecutable: executable }), second = await f.packageFixture('0.2.0-dev.2', { notificationExecutable: executable })
  f.io.notifications = { async snapshot() { return { pending: [] } }, async purgeInstallation() { return { complete: true, remainingPending: 0, deliveredPreserved: true } }, async close() {} }
  let child = null, closed = null, inject = 'current', oldApp = null
  const change = f.host.setEnabled.bind(f.host)
  f.host.setEnabled = async (enabled, revision) => {
    if (!enabled && child) { child.stdin.end(); await closed; child = null }
    const result = await change(enabled, revision)
    if (enabled && inject) {
      const mode = inject; inject = null
      const config = JSON.parse(await readFile(join(f.installRoot, 'state/installation.json'), 'utf8')), app = mode === 'old' ? oldApp : config.notificationBundlePath
      child = spawn(join(app, 'Contents/MacOS/GhostSentinelNotifications'), [mode === 'unknown' ? '--unexpected' : '--stdio'], { stdio: ['pipe', 'ignore', 'ignore'] }); closed = once(child, 'close'); child.stdin.on('error', () => {})
      await once(child, 'spawn')
    }
    return result
  }
  t.after(async () => { if (child) { child.stdin.end(); await closed } })
  const installed = await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot: first }, f.io), f.io)
  assert.equal(installed.status, 'complete', JSON.stringify(installed))
  assert.equal(child.exitCode, null, 'the legal current-release component was alive during authenticated readiness')
  oldApp = JSON.parse(await readFile(join(f.installRoot, 'state/installation.json'), 'utf8')).notificationBundlePath
  child.stdin.end(); assert.deepEqual(await closed, [0, null]); child = null
  inject = 'old'
  const rejected = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: second }, f.io), f.io)
  assert.equal(rejected.errorCode, 'plugin_release_process_mismatch', JSON.stringify(rejected))
  assert.equal(rejected.rollback.status, 'complete', JSON.stringify(rejected))
  inject = 'unknown'
  const unknown = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'upgrade', packageRoot: second }, f.io), f.io)
  assert.equal(unknown.errorCode, 'unrecognized_installation_process', JSON.stringify(unknown))
  assert.equal(unknown.rollback.status, 'complete', JSON.stringify(unknown))
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)).status, 'complete')
})

for (const shape of ['malformed', 'oversize', 'symlink']) test(`management CLI rejects ${shape} private defaults without revealing their contents`, async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  await mkdir(join(f.installRoot, 'state'), { recursive: true, mode: 0o700 })
  const path = join(f.installRoot, 'state/installation.json'), secret = 'private-value-must-never-be-printed'
  if (shape === 'symlink') { await writeFile(join(f.dir, 'outside.json'), JSON.stringify({ private: secret }), { mode: 0o600 }); await symlink(join(f.dir, 'outside.json'), path) }
  else await writeFile(path, shape === 'oversize' ? secret.repeat(40000) : '{"private":"' + secret + '" broken', { mode: 0o600 })
  await assert.rejects(promisify(execFile)(process.execPath, [fileURLToPath(new URL('../scripts/manage-chatgpt-plugin.mjs', import.meta.url)), 'uninstall', '--root', f.installRoot], { cwd: f.dir }), error => {
    assert.equal(error.code, 1)
    assert.equal(error.stdout, '')
    assert.equal(error.stderr.includes(secret), false)
    assert.deepEqual(Object.keys(JSON.parse(error.stderr)).sort(), ['code', 'status'])
    return true
  })
})

test('private management defaults cannot be read through a replaced state directory symlink', async t => {
  const f = await fixture(t); t.after(() => rm(f.dir, { recursive: true, force: true }))
  const outside = join(f.dir, 'private-outside'); await mkdir(outside, { mode: 0o700 }); await mkdir(f.installRoot, { mode: 0o700 })
  await writeFile(join(outside, 'installation.json'), JSON.stringify({ pluginId: 'outside-owned-installation' }), { mode: 0o600 })
  await symlink(outside, join(f.installRoot, 'state'))
  await assert.rejects(pluginManagementDefaults(f.installRoot), { code: 'installation_symlink_path_rejected' })
})

test('incomplete public Core purge keeps recovery ownership and can be retried without removing DSH', { timeout: 30000 }, async t => {
  const f = await installationWorld(t), packageRoot = await f.packageFixture('0.2.0-dev.1')
  assert.equal((await applyPluginChange(await previewPluginChange({ ...f.options, packageRoot }, f.io), f.io)).status, 'complete')
  f.io.connectCore = async () => ({ health: () => f.core.health(), async purgeChatGPTInstallation(scope) { const result = await f.core.purgeChatGPTInstallation(scope); return { ...result, status: 'incomplete', physical: { status: 'busy', secureDelete: true } } }, async close() {} })
  const removed = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall' }, f.io), f.io)
  assert.equal(removed.status, 'recovery-required')
  assert.equal(removed.errorCode, 'core_purge_incomplete')
  assert.equal(f.host.state.installed, true)
  assert.equal(f.host.state.enabled, 'disabled')
  assert.ok((await stat(join(f.installRoot, 'state/installation.json'))).isFile())
  assert.equal((await f.core.snapshot()).sources[0].key, f.peer)
  delete f.io.connectCore
  const retried = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: removed.operationId }, f.io), f.io)
  assert.equal(retried.status, 'complete', JSON.stringify(retried))
  await f.core.heartbeat(f.peer)
})

async function pendingRemoval(t) {
  const f = await unlaunchedWorld(t), first = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io), original = fs.rm
  assert.equal(first.status, 'needs-user-action')
  let injected = false
  fs.rm = async (path, ...args) => { if (path === f.marker && !injected) { injected = true; throw Object.assign(new Error('Synthetic retained marker'), { code: 'EACCES' }) }; return original(path, ...args) }
  syncBuiltinESMExports()
  try { const result = await applyPluginChange(await previewPluginChange({ ...f.options, operation: 'uninstall', resumeOperationId: first.operationId }, f.io), f.io); assert.equal(result.errorCode, 'EACCES'); f.operationId = result.operationId }
  finally { fs.rm = original; syncBuiltinESMExports() }
  f.resume = { ...f.options, operation: 'uninstall', resumeOperationId: f.operationId }
  return f
}

for (const branch of ['release', 'state']) test(`cleanup fault loop rejects a replaced ${branch} ancestor before deleting any outside leaf`, async t => {
  const f = await pendingRemoval(t), plan = await previewPluginChange(f.resume, f.io), original = branch === 'release' ? dirname(f.marker) : join(f.installRoot, 'state'), outside = join(f.dir, 'moved-' + branch), leaf = branch === 'release' ? 'marker' : 'installation.json', before = await readFile(join(original, leaf)), inspect = f.host.inspect.bind(f.host)
  let reads = 0, swapped = false
  f.host.inspect = async () => { if (++reads === 3) { await fs.rename(original, outside); await symlink(outside, original); swapped = true }; return inspect() }
  const result = await applyPluginChange(plan, f.io)
  assert.equal(swapped, true)
  assert.equal(result.status, 'recovery-required')
  assert.equal(createHash('sha256').update(await readFile(join(outside, leaf))).digest('hex'), createHash('sha256').update(before).digest('hex'), 'same leaf inode behind a symlink must remain untouched')
})

test('cleanup fault loop rejects an unknown final lock ancestor without replacing the primary failure', async t => {
  const f = await pendingRemoval(t), original = fs.rm, lock = join(f.installRoot, '.operation.lock'), outside = join(f.dir, 'moved-lock')
  let swapped = false, result
  fs.rm = async (path, ...args) => { if (path === f.marker && !swapped) { await fs.rename(lock, outside); await symlink(outside, lock); swapped = true; throw Object.assign(new Error('Synthetic primary unlink'), { code: 'EACCES' }) }; return original(path, ...args) }
  syncBuiltinESMExports()
  try { result = await applyPluginChange(await previewPluginChange(f.resume, f.io), f.io) }
  finally { fs.rm = original; syncBuiltinESMExports() }
  assert.equal(result.errorCode, 'EACCES')
  assert.equal(typeof result.lockCleanupErrorCode, 'string')
  assert.ok((await stat(join(outside, 'owner.json'))).isFile())
})

test('cleanup fault loop preserves control files when their known directory is replaced by another regular directory', async t => {
  const f = await pendingRemoval(t), plan = await previewPluginChange(f.resume, f.io), directory = join(f.installRoot, 'transactions'), outside = join(f.dir, 'old-transactions'), leaf = join(directory, f.operationId + '.json'), inspect = f.host.inspect.bind(f.host)
  let reads = 0, expected
  f.host.inspect = async () => { if (++reads === 3) { expected = await readFile(leaf); await fs.rename(directory, outside); await mkdir(directory, { mode: 0o700 }); await fs.rename(join(outside, f.operationId + '.json'), leaf) }; return inspect() }
  const result = await applyPluginChange(plan, f.io)
  assert.equal(result.status, 'recovery-required')
  assert.equal(createHash('sha256').update(await readFile(leaf)).digest('hex'), createHash('sha256').update(expected).digest('hex'), 'a valid leaf inode must not authorize deleting or rewriting its replaced control directory')
})

test('cleanup fault loop refuses an actually oversized journal before replacing the still-readable recovery record', async t => {
  const f = await pendingRemoval(t), path = join(f.installRoot, 'operation.json'), transaction = JSON.parse(await readFile(path, 'utf8'))
  transaction.removal.files = Array.from({ length: 35000 }, (_, index) => ({ key: 'releases/' + 'a'.repeat(64) + '/virtual-' + index + '-' + 'x'.repeat(22), identity: '[' + '1,'.repeat(35) + '1]' }))
  const compact = JSON.stringify(transaction), pretty = JSON.stringify(transaction, null, 2) + '\n'
  assert.ok(Buffer.byteLength(compact) < 8388608)
  assert.ok(Buffer.byteLength(pretty) > 8388608)
  await writeFile(path, compact, { mode: 0o600 })
  const result = await applyPluginChange(await previewPluginChange(f.resume, f.io), f.io)
  assert.equal(result.errorCode, 'installation_record_over_budget')
  assert.ok((await stat(path)).size <= 8388608, 'a failed write must retain a record its own reader can consume')
  assert.equal((await pluginManagementDefaults(f.installRoot)).pluginId, f.options.pluginId)
  assert.equal(await readFile(f.marker, 'utf8'), 'owned package marker')
})

test('cleanup fault loop applies the ordinary record budget to the bytes of a generated descriptor', async t => {
  const f = await unlaunchedWorld(t)
  const result = await applyPluginChange(await previewPluginChange({ ...f.options, pluginName: 'x'.repeat(1048576) }, f.io), f.io)
  assert.equal(result.errorCode, 'installation_record_over_budget')
  await assert.rejects(stat(join(f.installRoot, 'state/plugin/.codex-plugin/plugin.json')), { code: 'ENOENT' })
})

for (const fault of ['primary', 'journal-and-primary', 'lock-only']) test(`cleanup fault loop classifies ${fault} and final lock errors without losing recovery`, async t => {
  const f = await pendingRemoval(t), originalRm = fs.rm, originalWrite = fs.writeFile, operation = join(f.installRoot, 'operation.json'), lockOwner = join(f.installRoot, '.operation.lock/owner.json')
  let primary = false, journal = false, lock = false, result
  fs.rm = async (path, ...args) => {
    if (fault !== 'lock-only' && path === f.marker && !primary) { primary = true; throw Object.assign(new Error('Synthetic primary unlink'), { code: 'EACCES' }) }
    if (path === lockOwner && !lock) { lock = true; throw Object.assign(new Error('Synthetic final lock cleanup'), { code: 'EROFS' }) }
    return originalRm(path, ...args)
  }
  fs.writeFile = async (path, ...args) => { if (fault === 'journal-and-primary' && primary && !journal && String(path).startsWith(operation + '.')) { journal = true; throw Object.assign(new Error('Synthetic recovery journal'), { code: 'ENOSPC' }) }; return originalWrite(path, ...args) }
  syncBuiltinESMExports()
  try { result = await applyPluginChange(await previewPluginChange(f.resume, f.io), f.io) }
  finally { fs.rm = originalRm; fs.writeFile = originalWrite; syncBuiltinESMExports() }
  assert.equal(result.status, 'recovery-required')
  if (fault !== 'lock-only') assert.equal(result.errorCode, 'EACCES')
  if (fault === 'journal-and-primary') assert.equal(result.recoveryJournalErrorCode, 'ENOSPC')
  assert.equal(result.lockCleanupErrorCode, 'EROFS')
  assert.equal(result.recoveryConfigurationRetained, true)
  const retried = await applyPluginChange(await previewPluginChange(f.resume, f.io), f.io)
  assert.equal(retried.status, 'complete', JSON.stringify(retried))
})

test('cleanup fault loop releases its verified empty lock when creating the lock-owner record fails', async t => {
  const f = await unlaunchedWorld(t), original = fs.writeFile, owner = join(f.installRoot, '.operation.lock/owner.json'); let injected = false
  fs.writeFile = async (path, ...args) => { if (!injected && String(path).startsWith(owner + '.')) { injected = true; throw Object.assign(new Error('Synthetic owner write'), { code: 'EACCES' }) }; return original(path, ...args) }
  syncBuiltinESMExports()
  try { await assert.rejects(applyPluginChange(await previewPluginChange(f.options, f.io), f.io), { code: 'EACCES' }) }
  finally { fs.writeFile = original; syncBuiltinESMExports() }
  assert.equal(injected, true)
  await assert.rejects(stat(join(f.installRoot, '.operation.lock')), { code: 'ENOENT' })
})

test('cleanup fault loop refuses a replaced plugin parent before creating descriptor directories outside the root', async t => {
  const f = await unlaunchedWorld(t), first = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io), resume = { ...f.options, resumeOperationId: first.operationId }, plan = await previewPluginChange(resume, f.io), plugin = join(f.installRoot, 'state/plugin'), outside = join(f.dir, 'outside-plugin'), inspect = f.host.inspect.bind(f.host)
  let reads = 0, swapped = false
  f.host.inspect = async () => { if (++reads === 3) { await fs.rename(plugin, outside); await fs.rename(join(outside, '.codex-plugin'), join(f.dir, 'saved-descriptor-dir')); await symlink(outside, plugin); swapped = true }; return inspect() }
  const result = await applyPluginChange(plan, f.io)
  assert.equal(swapped, true)
  assert.equal(result.status, 'recovery-required')
  await assert.rejects(stat(join(outside, '.codex-plugin')), { code: 'ENOENT' })
})

test('cleanup fault loop refuses a replaced releases parent before copying a stage outside the root', async t => {
  const f = await unlaunchedWorld(t), inspect = f.io.inspectPackage, outside = join(f.dir, 'outside-releases'); let reads = 0, swapped = false
  f.io.inspectPackage = async (...args) => { if (++reads === 4) { await mkdir(outside, { mode: 0o700 }); await symlink(outside, join(f.installRoot, 'releases')); swapped = true }; return inspect(...args) }
  const result = await applyPluginChange(await previewPluginChange(f.options, f.io), f.io)
  assert.equal(swapped, true)
  assert.equal(result.status, 'recovery-required')
  assert.deepEqual(await fs.readdir(outside), [], 'no stage, release, marker or cleanup artifact may be created outside the installation')
})

for (const exists of [false, true]) for (const replacement of ['directory', 'symlink']) test(`approval binding loop refuses ${replacement} replacement when target initially ${exists ? 'exists' : 'does not exist'}`, async t => {
  const f = await unlaunchedWorld(t)
  if (exists) await mkdir(f.installRoot, { mode: 0o700 })
  const plan = await previewPluginChange(f.options, f.io), original = exists ? f.installRoot : f.dir, moved = f.dir + '-moved', outside = f.dir + '-outside'
  t.after(async () => { await rm(moved, { recursive: true, force: true }); await rm(outside, { recursive: true, force: true }) })
  await fs.rename(original, moved)
  if (replacement === 'symlink') { await mkdir(outside, { mode: 0o700 }); await symlink(outside, original) }
  else await mkdir(original, { mode: 0o700 })
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'installation_preview_changed' })
  assert.equal(f.host.state.installed, false)
  const target = replacement === 'symlink' ? (exists ? outside : join(outside, 'install')) : f.installRoot
  await assert.rejects(stat(join(target, 'state/installation.json')), { code: 'ENOENT' })
})

for (const exists of [false, true]) for (const alias of [false, true]) test(`approval binding loop permits ${alias ? '/var alias' : 'canonical'} target initially ${exists ? 'present' : 'absent'}`, async t => {
  const f = await unlaunchedWorld(t)
  if (exists) await mkdir(f.installRoot, { mode: 0o700 })
  const options = { ...f.options }
  if (alias) for (const key of ['installRoot', 'packageRoot', 'coreDataDir', 'sessionsRoot', 'metadataSocket']) options[key] = options[key].replace(/^\/private\/var\//, '/var/')
  const first = await applyPluginChange(await previewPluginChange(options, f.io), f.io)
  assert.equal(first.status, 'needs-user-action', JSON.stringify(first))
  const result = await applyPluginChange(await previewPluginChange({ ...options, operation: 'uninstall', resumeOperationId: first.operationId }, f.io), f.io)
  assert.equal(result.status, 'complete', JSON.stringify(result))
})

for (const edit of ['same-ids-extra-field', 'same-ids-new-inode', 'different-ids']) test(`approval binding loop preserves an unknown ${edit} mutex leaf and refuses fresh adoption`, async t => {
  const f = await pendingRemoval(t), owner = join(f.installRoot, '.operation.lock/owner.json'), original = fs.rm
  let changed = false, expectedBytes, expectedInode, result
  fs.rm = async (path, ...args) => {
    if (path === f.marker && !changed) {
      changed = true
      const current = JSON.parse(await readFile(owner, 'utf8'))
      if (edit === 'same-ids-new-inode') { const temporary = join(f.installRoot, '.operation.lock/replacement'); await writeFile(temporary, await readFile(owner), { mode: 0o600 }); await fs.rename(temporary, owner) }
      else await writeFile(owner, JSON.stringify({ ...current, ...(edit === 'different-ids' ? { operationId: '00000000-0000-4000-8000-000000000000' } : {}), foreignRecord: 'preserve this edit' }), { mode: 0o600 })
      expectedBytes = createHash('sha256').update(await readFile(owner)).digest('hex'); expectedInode = (await stat(owner)).ino
      throw Object.assign(new Error('Synthetic primary unlink'), { code: 'EACCES' })
    }
    return original(path, ...args)
  }
  syncBuiltinESMExports()
  try { result = await applyPluginChange(await previewPluginChange(f.resume, f.io), f.io) }
  finally { fs.rm = original; syncBuiltinESMExports() }
  assert.equal(changed, true)
  assert.equal(result.errorCode, 'EACCES')
  assert.equal(createHash('sha256').update(await readFile(owner)).digest('hex'), expectedBytes)
  assert.equal((await stat(owner)).ino, expectedInode)
  const plan = await previewPluginChange(f.resume, f.io)
  await assert.rejects(applyPluginChange(plan, f.io), { code: 'plugin_operation_locked' })
  assert.equal(createHash('sha256').update(await readFile(owner)).digest('hex'), expectedBytes)
})
