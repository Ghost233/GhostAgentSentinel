import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm, mkdir, realpath, chmod } from 'node:fs/promises'
import fs from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import { join } from 'node:path'
import { tmpdir, homedir } from 'node:os'
import { createServer } from 'node:http'
import { createHash } from 'node:crypto'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { PassThrough } from 'node:stream'
import { setTimeout as delay } from 'node:timers/promises'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { resourceMaterial } from './fixtures/chatgpt-plugin-lazy/resource-material.mjs'
import * as runtime from '../chatgpt/runtime.mjs'

const firstRelease = 'a'.repeat(64), nextRelease = 'b'.repeat(64)
const hash = bytes => createHash('sha256').update(bytes).digest('hex')

async function githubFixture(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-github-prepare-')))
  const cleanups = []
  t.after(async () => { try { for (const cleanup of cleanups.reverse()) await cleanup() } finally { await rm(root, { recursive: true, force: true }) } })
  let inspections = 0
  const packet = async (channel, archiveSHA256, label = 'first') => {
    const stateRoot = join(root, channel), releaseRoot = join(stateRoot, 'payloads', archiveSHA256)
    await mkdir(join(releaseRoot, 'runtime'), { recursive: true, mode: 0o700 })
    await mkdir(join(releaseRoot, 'chatgpt'), { mode: 0o700 })
    await writeFile(join(releaseRoot, 'runtime/node'), 'fixture-node', { mode: 0o700 })
    const manifest = { version: '0.1.2-preview.1', runtimeVersion: runtime.HELPER_VERSION, node: { version: 'v24.21.0', sha256: hash('fixture-node') }, notificationPath: null, fixtureLabel: label }
    await writeFile(join(releaseRoot, 'plugin-package.json'), JSON.stringify(manifest), { mode: 0o600 })
    return { channel, stateRoot, releaseRoot, archiveSHA256, coreDataDir: join(root, 'core'), sessionsRoot: join(root, 'sessions') }
  }
  const io = { async inspectPackage(releaseRoot, { digest }) {
    inspections++
    const bytes = await readFile(join(releaseRoot, 'plugin-package.json')), manifest = JSON.parse(bytes)
    assert.equal(hash(bytes), digest); assert.equal(hash(await readFile(join(releaseRoot, 'runtime/node'))), manifest.node.sha256)
    return { root: releaseRoot, digest, manifest, nodePath: join(releaseRoot, 'runtime/node'), notificationApp: null }
  } }
  return { root, packet, io, cleanup: value => cleanups.push(value), get inspections() { return inspections } }
}

test('concurrent GitHub preparation preserves one private installation and reuses only an intact release cache', async t => {
  const f = await githubFixture(t), options = await f.packet('stable', 'c'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const values = await Promise.all(Array.from({ length: 20 }, () => prepareGitHubInstallation(options, f.io)))
  const initial = await runtime.loadInstallationConfig(values[0].configPath)
  const expectedRelease = hash(await readFile(join(options.releaseRoot, 'plugin-package.json')))
  assert.ok(values.every(value => value.configPath === values[0].configPath && value.releaseId === expectedRelease))
  assert.equal(initial.pluginId, 'ghost-sentinel@ghostsentinel'); assert.equal(initial.releaseId, expectedRelease)
  assert.equal(initial.coreDataDir, options.coreDataDir); assert.notEqual(initial.coreDataDir, initial.dataDir)
  assert.equal(f.inspections, 1)
  const again = await prepareGitHubInstallation(options, f.io)
  assert.deepEqual(await runtime.loadInstallationConfig(again.configPath), initial)
  await writeFile(join(options.releaseRoot, 'runtime/node'), 'tampered-node')
  await assert.rejects(prepareGitHubInstallation(options, f.io))
  assert.equal(f.inspections, 2)
  assert.deepEqual(await runtime.loadInstallationConfig(again.configPath), initial)
})

test('real owners hand off naturally, retain failed upgrade recovery and isolate stable from test', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', '1'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const prepared = await prepareGitHubInstallation(first, f.io), coreServer = await startServer({ dataDir: first.coreDataDir }), core = await connectCore({ dataDir: first.coreDataDir }), children = []
  async function finished(child) {
    if (child.exitCode !== null || child.signalCode !== null) return
    const timeout = AbortSignal.timeout(2500)
    await once(child, 'close', { signal: timeout })
  }
  f.cleanup(async () => {
    try {
      for (const child of children) {
        if (child.connected) { child.send('disable'); child.send('release-close') }
        try { await finished(child) }
        catch (error) { child.kill('SIGKILL'); throw error }
        assert.equal(child.exitCode, 0); assert.equal(child.signalCode, null)
      }
    } finally { await core.close(); await coreServer.close() }
  })
  async function owner(configPath, hold = '') {
    await mkdir(first.sessionsRoot, { recursive: true, mode: 0o700 })
    const child = spawn(process.execPath, [new URL('./fixtures/chatgpt-github-owner.mjs', import.meta.url).pathname, configPath, hold], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'] })
    children.push(child)
    let stderr = ''; child.stderr.on('data', bytes => { stderr += bytes })
    const [ready] = await Promise.race([once(child, 'message', { signal: AbortSignal.timeout(5000) }), once(child, 'close').then(() => { throw new Error('fixture owner ended before ready: ' + stderr) })])
    return { child, ...ready }
  }
  const dshSource = await core.registerSource({ registrationId: 'github-upgrade-dsh', hostRunId: 'dsh-own', name: 'DSH' }), coreId = (await core.health()).instanceId
  const preference = { registrationId: 'chatgpt:' + (await runtime.loadInstallationConfig(prepared.configPath)).installationId, taskId: 'synthetic-main' }
  const pauseSource = await core.registerSource({ registrationId: preference.registrationId, hostRunId: 'seed-pause', name: 'Synthetic pause', policy: 'chatgpt-main/1' })
  await core.ingest(pauseSource, { tasks: [{ id: preference.taskId, lifecycleId: 'seed', title: 'Synthetic pause', status: 'idle' }] })
  await core.setConversationMonitoring({ ...preference, paused: true, revision: (await core.conversationMonitoring(preference)).revision })
  const oldOwner = await owner(prepared.configPath)
  assert.equal((await runtime.resolvePluginOwner(prepared)).runId, oldOwner.runId)
  const second = await f.packet('stable', '2'.repeat(64), 'second'), upgraded = await prepareGitHubInstallation(second, f.io)
  await finished(oldOwner.child); assert.equal(oldOwner.child.exitCode, 0)
  const secondOwner = await owner(upgraded.configPath, 'hold-close')
  assert.notEqual(secondOwner.runId, oldOwner.runId)
  const third = await f.packet('stable', '3'.repeat(64), 'third')
  await assert.rejects(prepareGitHubInstallation(third, { ...f.io, stopTimeoutMs: 100 }), { code: 'github_owner_stop_unconfirmed' })
  assert.equal((await runtime.loadInstallationConfig(prepared.configPath)).releaseId, upgraded.releaseId)
  assert.equal(JSON.parse(await readFile(join(third.stateRoot, 'github-operation.json'))).phase, 'upgrade')
  secondOwner.child.send('release-close'); await finished(secondOwner.child)
  const recovered = await prepareGitHubInstallation(third, f.io), stableOwner = await owner(recovered.configPath)
  assert.equal(stableOwner.releaseId, recovered.releaseId)
  const other = await f.packet('test', '4'.repeat(64)), testPrepared = await prepareGitHubInstallation(other, f.io), testOwner = await owner(testPrepared.configPath)
  const stableConfig = await runtime.loadInstallationConfig(recovered.configPath), testConfig = await runtime.loadInstallationConfig(testPrepared.configPath)
  assert.notEqual(stableConfig.installationId, testConfig.installationId); assert.notEqual(stableConfig.token, testConfig.token)
  testOwner.child.send('disable'); await finished(testOwner.child)
  assert.equal((await runtime.resolvePluginOwner(recovered)).runId, stableOwner.runId)
  assert.equal((await core.conversationMonitoring(preference)).paused, true)
  assert.equal((await core.health()).instanceId, coreId); await core.heartbeat(dshSource)
})

for (const point of ['lock-realpath', 'owner-realpath', 'owner-open', 'publication-realpath', 'publication-open', 'lock-readdir']) {
  test('an owner snapshot retries a disappearing ' + point + ' through the public upgrade', async t => {
    const f = await githubFixture(t), first = await f.packet('stable', 'a'.repeat(64))
    const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
    const initial = await prepareGitHubInstallation(first, f.io), config = await runtime.loadInstallationConfig(initial.configPath)
    const lock = join(config.dataDir, 'runtime.lock'), owner = join(lock, 'owner.json'), publication = join(config.dataDir, 'runtime.json')
    const identity = { protocol: runtime.HELPER_PROTOCOL, version: runtime.HELPER_VERSION, installationId: config.installationId, releaseId: config.releaseId, ownership: 'plugin', runId: '3412cf54-6353-4d67-95af-0e43873fcaad', pid: process.pid }
    await mkdir(lock, { mode: 0o700 }); await writeFile(owner, JSON.stringify(identity), { mode: 0o600 }); await writeFile(publication, JSON.stringify(identity), { mode: 0o600 })
    const next = await f.packet('stable', 'b'.repeat(64), 'next')
    const [which, method] = point.split('-'), path = { lock, owner, publication }[which], original = fs[method]
    let hits = 0
    fs[method] = async (target, ...args) => {
      if (target === path && hits++ === 0) { await rm(publication); await rm(lock, { recursive: true }) }
      return original(target, ...args)
    }
    syncBuiltinESMExports()
    let upgraded
    try { upgraded = await prepareGitHubInstallation(next, f.io) }
    finally { fs[method] = original; syncBuiltinESMExports() }
    assert.ok(hits > 0)
    assert.notEqual(upgraded.releaseId, initial.releaseId)
    assert.equal((await runtime.loadInstallationConfig(initial.configPath)).releaseId, upgraded.releaseId)
    await assert.rejects(readFile(join(config.dataDir, 'maintenance.json')), { code: 'ENOENT' })
  })
}

test('an owner snapshot bounds repeated ENOENT instead of inventing an absent owner', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', 'c'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const initial = await prepareGitHubInstallation(first, f.io), config = await runtime.loadInstallationConfig(initial.configPath)
  const lock = join(config.dataDir, 'runtime.lock'), next = await f.packet('stable', 'd'.repeat(64), 'next'), original = fs.realpath
  await mkdir(lock, { mode: 0o700 })
  let hits = 0
  fs.realpath = async (target, ...args) => { if (target === lock) { hits++; throw Object.assign(new Error('fixture missing owner directory'), { code: 'ENOENT' }) }; return original(target, ...args) }
  syncBuiltinESMExports()
  try { await assert.rejects(prepareGitHubInstallation(next, f.io), { code: 'ENOENT' }) }
  finally { fs.realpath = original; syncBuiltinESMExports() }
  assert.equal(hits, 3)
  assert.equal((await runtime.loadInstallationConfig(initial.configPath)).releaseId, initial.releaseId)
  await assert.rejects(readFile(join(first.stateRoot, 'github-operation.json')), { code: 'ENOENT' })
})

for (const kind of ['permissions', 'symlink', 'foreign-owner']) {
  test('an owner snapshot still refuses ' + kind + ' without retrying a security failure', async t => {
    const f = await githubFixture(t), first = await f.packet('stable', 'e'.repeat(64))
    const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
    const initial = await prepareGitHubInstallation(first, f.io), config = await runtime.loadInstallationConfig(initial.configPath)
    const lock = join(config.dataDir, 'runtime.lock'), owner = join(lock, 'owner.json'), next = await f.packet('stable', 'f'.repeat(64), 'next'), original = fs.realpath
    await mkdir(lock, { mode: 0o700 })
    let hits = 0
    fs.realpath = async (target, ...args) => {
      if (target === lock && hits++ === 0) {
        if (kind === 'permissions') await chmod(lock, 0o755)
        else if (kind === 'symlink') { const other = join(config.dataDir, 'other'); await mkdir(other, { mode: 0o700 }); await rm(lock, { recursive: true }); await fs.symlink(other, lock) }
        else await writeFile(owner, JSON.stringify({ protocol: runtime.HELPER_PROTOCOL, version: runtime.HELPER_VERSION, ownership: 'plugin', installationId: 'foreign', releaseId: config.releaseId, runId: '3412cf54-6353-4d67-95af-0e43873fcaad', pid: process.pid }), { mode: 0o600 })
      }
      return original(target, ...args)
    }
    syncBuiltinESMExports()
    try { await assert.rejects(prepareGitHubInstallation(next, f.io), { code: { permissions: 'github_private_directory_required', symlink: 'github_symlink_path_rejected', 'foreign-owner': 'github_owner_unconfirmed' }[kind] }) }
    finally { fs.realpath = original; syncBuiltinESMExports() }
    assert.equal(hits, kind === 'foreign-owner' ? 2 : 1)
    assert.equal((await runtime.loadInstallationConfig(initial.configPath)).releaseId, initial.releaseId)
  })
}

test('a channel upgrade keeps credentials, enrollment, cursors and the shared Core path while switching release', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', 'd'.repeat(64))
  const { prepareGitHubInstallation, inspectGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const prepared = await prepareGitHubInstallation(first, f.io), config = await runtime.loadInstallationConfig(prepared.configPath)
  await mkdir(join(config.dataDir, 'cursors'), { mode: 0o700 })
  const cursor = join(config.dataDir, 'cursors', 'e'.repeat(64) + '.json')
  await writeFile(cursor, 'already verified cursor', { mode: 0o600 })
  const changed = { ...config, initialEnrollmentBaseline: { ...config.initialEnrollmentBaseline, claimable: false }, port: 39897 }
  await writeFile(prepared.configPath, JSON.stringify(changed))
  const next = await f.packet('stable', 'e'.repeat(64), 'next')
  const upgraded = await prepareGitHubInstallation(next, f.io), current = await runtime.loadInstallationConfig(upgraded.configPath)
  assert.notEqual(upgraded.releaseId, prepared.releaseId)
  for (const key of ['token', 'installationId', 'coreDataDir', 'sessionsRoot', 'initialEnrollmentBaseline', 'port']) assert.deepEqual(current[key], changed[key])
  assert.equal(await readFile(cursor, 'utf8'), 'already verified cursor')
  const checked = await inspectGitHubInstallation(next)
  assert.equal(checked.receipt.releases.length, 2); assert.equal(checked.receipt.activeReleaseId, upgraded.releaseId)
  await assert.rejects(readFile(join(next.stateRoot, 'github-operation.json')), { code: 'ENOENT' })
  await assert.rejects(readFile(join(current.dataDir, 'maintenance.json')), { code: 'ENOENT' })
})

test('unknown operations, foreign configuration and a cross-channel state root are preserved rather than adopted', async t => {
  const f = await githubFixture(t), options = await f.packet('stable', '5'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const prepared = await prepareGitHubInstallation(options, f.io), config = await runtime.loadInstallationConfig(prepared.configPath)
  await assert.rejects(prepareGitHubInstallation({ ...options, channel: 'test' }, f.io), { code: 'github_channel_path_invalid' })
  const path = join(options.stateRoot, 'github-operation.json'), foreign = { phase: 'initial', kind: 'foreign', releaseId: prepared.releaseId, archiveSHA256: options.archiveSHA256, planned: config }
  await writeFile(path, JSON.stringify(foreign), { mode: 0o600 })
  await assert.rejects(prepareGitHubInstallation(options, f.io), { code: 'github_operation_requires_review' })
  assert.deepEqual(JSON.parse(await readFile(path)), foreign)
  assert.deepEqual(await runtime.loadInstallationConfig(prepared.configPath), config)
  const other = await f.packet('test', '6'.repeat(64))
  const legacy = await runtime.prepareInstallation({ dataDir: join(other.stateRoot, 'state'), coreDataDir: other.coreDataDir, sessionsRoot: other.sessionsRoot, metadataSocket: join(other.stateRoot, 'unused.sock'), pluginId: 'ghost-sentinel-test@ghostsentinel', releaseId: hash(await readFile(join(other.releaseRoot, 'plugin-package.json'))) })
  const bytes = await readFile(legacy.configPath)
  await assert.rejects(prepareGitHubInstallation(other, f.io), { code: 'github_foreign_configuration' })
  assert.deepEqual(await readFile(legacy.configPath), bytes)
})

test('a late old bridge cannot turn a known nonactive release into an implicit rollback', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', '7'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const old = await prepareGitHubInstallation(first, f.io), next = await f.packet('stable', '8'.repeat(64), 'newer')
  const active = await prepareGitHubInstallation(next, f.io)
  await assert.rejects(prepareGitHubInstallation(first, f.io), { code: 'github_release_superseded' })
  assert.equal((await runtime.loadInstallationConfig(active.configPath)).releaseId, active.releaseId)
  assert.notEqual(old.releaseId, active.releaseId)
})

test('old and new Codex tabs can read the panel during a held release handoff', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', '7'.repeat(64))
  const { startGitHubBridge } = await import('../chatgpt/github-plugin-bridge.mjs')
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const initial = await prepareGitHubInstallation(first, f.io)
  const next = await f.packet('stable', '8'.repeat(64), 'replacement')
  const { promise: hold, resolve: resume } = Promise.withResolvers()
  const { promise: held, resolve: markHeld } = Promise.withResolvers()
  const { promise: completed, resolve: markComplete } = Promise.withResolvers()
  const uri = 'ui://ghost-sentinel/current-chat.html'
  let upgraded, launches = 0
  f.cleanup(async () => { resume(); await upgraded?.catch(() => {}) })
  async function tab(options, io = f.io) {
    const input = new PassThrough(), output = new PassThrough()
    const bridge = await startGitHubBridge({ ...options, input, output }, { async prepareInstallation(options) {
      const preparation = prepareGitHubInstallation(options, io)
      if (options.archiveSHA256 === next.archiveSHA256) {
        upgraded = preparation
        void preparation.then(markComplete, () => {})
      }
      return preparation
    }, async launchOwner() { launches++ } })
    f.cleanup(() => bridge.close())
    let id = 0
    async function request(method, params = {}) {
      const reply = once(output, 'data', { signal: AbortSignal.timeout(1000) })
      input.write(JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) + '\n')
      const message = JSON.parse((await reply)[0])
      assert.equal(message.id, id)
      return message.result
    }
    await request('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'restored-tab', version: '1' } })
    return { request, input, bridge }
  }
  const old = await tab(first)
  const resource = await old.request('resources/read', { uri })
  const io = { ...f.io, async inspectPackage(...args) { markHeld(); await hold; return f.io.inspectPackage(...args) } }
  const fresh = await tab(next, io)
  await held
  const before = launches
  assert.deepEqual(await fresh.request('resources/read', { uri }), resource)
  assert.equal((await fresh.request('tools/call', { name: 'sentinel_current_chat', arguments: {} })).structuredContent.signal, 'gray')
  assert.equal(launches, before)
  assert.equal((await runtime.loadInstallationConfig(initial.configPath)).releaseId, initial.releaseId)
  resume()
  assert.notEqual((await completed).releaseId, initial.releaseId)
  assert.equal((await old.request('tools/call', { name: 'sentinel_current_chat', arguments: {} })).structuredContent.signal, 'gray')
  assert.deepEqual(await old.request('resources/read', { uri }), resource)
  await fresh.request('tools/call', { name: 'sentinel_current_chat', arguments: {} })
  assert.ok(launches > before, 'the existing connection can start observation after preparation')
  old.input.end()
  assert.deepEqual(await old.bridge.closed, { reason: 'stdio-eof' })
})

test('existing and reopened MCP connections to a superseded package read the active observer without restarting it', async t => {
  const f = await githubFixture(t), first = await f.packet('stable', '7'.repeat(64))
  const next = await f.packet('stable', '8'.repeat(64), 'replacement')
  const { startGitHubBridge } = await import('../chatgpt/github-plugin-bridge.mjs')
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  await prepareGitHubInstallation(first, f.io)
  const core = await startServer({ dataDir: first.coreDataDir })
  f.cleanup(() => core.close())
  const thread = { id: 'resource-main', originator: 'Codex Desktop', source: 'vscode', parentThreadId: null,
    path: join(first.sessionsRoot, 'resource-main.jsonl'), cwd: first.sessionsRoot, status: { type: 'active' } }
  await mkdir(first.sessionsRoot, { mode: 0o700 })
  await writeFile(thread.path, resourceMaterial({ at: Date.now(), cwd: thread.cwd, producerVersion: supportedProfile.cliVersion }))
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'active-observer', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async listThreads() { return { data: [thread], nextCursor: null } },
    async readThread(id) { return id === thread.id ? { thread, anchorTurnId: 'resource-turn' } : null },
    async close() {},
  }
  let launches = 0
  async function tab() {
    const input = new PassThrough(), output = new PassThrough()
    const bridge = await startGitHubBridge({ ...first, input, output }, {
      prepareInstallation: options => prepareGitHubInstallation(options, f.io),
      async launchOwner() { launches++ },
    })
    f.cleanup(() => bridge.close())
    let id = 0
    const request = async (method, params) => {
      const reply = once(output, 'data', { signal: AbortSignal.timeout(3000) })
      input.write(JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) + '\n')
      const message = JSON.parse((await reply)[0])
      assert.equal(message.id, id)
      return message.result?.structuredContent
    }
    await request('initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'restored-tab', version: '1' } })
    return { read: () => request('tools/call', { name: 'sentinel_current_chat', arguments: {}, _meta: { thread_id: thread.id } }), bridge }
  }
  const existing = await tab()
  const deadline = Date.now() + 2000
  while (!launches && Date.now() < deadline) { await existing.read(); await delay(20) }
  assert.equal(launches, 1)
  const active = await prepareGitHubInstallation(next, f.io)
  const owner = await runtime.startFromInstallation({ ...active, ownership: 'plugin', native, pollMs: 0, expectedReleaseId: active.releaseId })
  f.cleanup(() => owner.close())
  const reopened = await tab()
  for (const connection of [existing, reopened]) {
    let view
    const readyBy = Date.now() + 2000
    do {
      view = await connection.read()
      if (view?.identity?.threadId) break
      await delay(25)
    } while (Date.now() < readyBy)
    assert.equal(view?.identity?.threadId, thread.id)
    assert.equal(connection.bridge.runId, owner.runId)
  }
  assert.equal(launches, 1)
  assert.equal((await runtime.loadInstallationConfig(active.configPath)).releaseId, active.releaseId)
})

test('runtime refuses an expected release mismatch and stops a late old owner after configuration advances', async t => {
  const f = await githubFixture(t), options = await f.packet('stable', '9'.repeat(64))
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const prepared = await prepareGitHubInstallation(options, f.io), core = await startServer({ dataDir: options.coreDataDir })
  let owner, nativeSamples = 0
  f.cleanup(async () => { await owner?.close(); await core.close() })
  const native = { async sampleHost() { nativeSamples++; return { available: true, hostRunId: 'expected-release', hostStartedAt: 1, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [], nextCursor: null } }, async close() {} }
  await assert.rejects(async () => {
    owner = await runtime.startFromInstallation({ ...prepared, ownership: 'plugin', native, pollMs: 0, expectedReleaseId: firstRelease })
    await owner.close(); owner = null
  }, { code: 'plugin_release_mismatch' })
  assert.equal(nativeSamples, 0)
  owner = await runtime.startFromInstallation({ ...prepared, ownership: 'plugin', native, pollMs: 0, expectedReleaseId: prepared.releaseId })
  const before = nativeSamples, config = await runtime.loadInstallationConfig(prepared.configPath)
  await writeFile(prepared.configPath, JSON.stringify({ ...config, releaseId: nextRelease }))
  await owner.refresh()
  assert.deepEqual(await owner.closed, { reason: 'plugin-release-superseded' })
  assert.equal(nativeSamples, before)
})

test('installation persists an optional release identity and refuses a different release through the legacy prepare API', async t => {
  const root = await mkdtemp(join(tmpdir(), 'sentinel-github-release-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const options = { dataDir: join(root, 'state'), coreDataDir: join(root, 'core'), sessionsRoot: join(root, 'sessions'), metadataSocket: join(root, 'unused.sock'), pluginId: 'ghost-sentinel@ghostsentinel', releaseId: firstRelease }
  const value = await runtime.prepareInstallation(options)
  assert.equal(JSON.parse(await readFile(value.configPath)).releaseId, firstRelease)
  await assert.rejects(runtime.prepareInstallation({ ...options, releaseId: nextRelease }), /different settings/)
  assert.equal((await runtime.loadInstallationConfig(value.configPath)).releaseId, firstRelease)
  const config = JSON.parse(await readFile(value.configPath)); config.releaseId = 'not-a-digest'
  await writeFile(value.configPath, JSON.stringify(config), { mode: 0o600 })
  await assert.rejects(runtime.loadInstallationConfig(value.configPath), /release/)
})

test('owner resolution authenticates release identity in the owner, publication and HTTP health', async t => {
  const root = await mkdtemp(join(tmpdir(), 'sentinel-github-owner-'))
  const options = { dataDir: join(root, 'state'), coreDataDir: join(root, 'core'), sessionsRoot: root, metadataSocket: join(root, 'unused.sock'), pluginId: 'ghost-sentinel@ghostsentinel', releaseId: firstRelease }
  const prepared = await runtime.prepareInstallation(options)
  const config = await runtime.loadInstallationConfig(prepared.configPath)
  const identity = { protocol: runtime.HELPER_PROTOCOL, version: runtime.HELPER_VERSION, installationId: config.installationId, runId: '3412cf54-6353-4d67-95af-0e43873fcaad', pid: process.pid, ownership: 'plugin', releaseId: firstRelease }
  let health = identity
  const server = createServer((request, response) => { assert.equal(request.headers.authorization, 'Bearer ' + config.token); response.end(JSON.stringify(health)) })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { await new Promise(resolve => server.close(resolve)); await rm(root, { recursive: true, force: true }) })
  const { mkdir } = await import('node:fs/promises')
  await mkdir(join(config.dataDir, 'runtime.lock'), { mode: 0o700 })
  const ownerPath = join(config.dataDir, 'runtime.lock/owner.json'), publicationPath = join(config.dataDir, 'runtime.json')
  const publication = { ...identity, url: 'http://127.0.0.1:' + server.address().port }
  await writeFile(ownerPath, JSON.stringify(identity), { mode: 0o600 })
  await writeFile(publicationPath, JSON.stringify(publication), { mode: 0o600 })
  assert.equal((await runtime.resolvePluginOwner(prepared)).releaseId, firstRelease)
  assert.equal(await runtime.resolvePluginOwner({ ...prepared, expectedReleaseId: nextRelease }), null)
  health = { ...identity, releaseId: nextRelease }
  await assert.rejects(runtime.resolvePluginOwner(prepared), /identity mismatch/)
  health = identity
  await writeFile(publicationPath, JSON.stringify({ ...publication, releaseId: nextRelease }))
  await assert.rejects(runtime.resolvePluginOwner(prepared), /untrusted helper endpoint/)
  await writeFile(publicationPath, JSON.stringify(publication))
  await writeFile(ownerPath, JSON.stringify({ ...identity, releaseId: nextRelease }))
  await assert.rejects(runtime.resolvePluginOwner(prepared), /untrusted helper owner/)
})

for (const failedWrite of [false, true]) test(`a managed legacy Core path migrates once to an independent Codex profile (${failedWrite ? 'retry after config write' : 'normal upgrade'})`, async t => {
  const f = await githubFixture(t), first = await f.packet('stable', '1'.repeat(64)), calls = []
  first.coreDataDir = join(homedir(), '.ghost-agent-sentinel')
  const { prepareGitHubInstallation, inspectGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const initial = await prepareGitHubInstallation(first, f.io), previous = await runtime.loadInstallationConfig(initial.configPath)
  const next = await f.packet('stable', '2'.repeat(64), 'independent')
  const io = { ...f.io, async migrateCoreProfile(value) { calls.push(value); return { migrated: true } } }
  if (failedWrite) {
    const write = fs.writeFile; let rejected = false
    t.mock.method(fs, 'writeFile', async (...args) => { if (!rejected && String(args[0]).includes('github-installation.json.') && String(args[1]).includes(next.archiveSHA256)) { rejected = true; throw Object.assign(new Error('owned receipt write failure'), { code: 'EIO' }) }; return write(...args) }); syncBuiltinESMExports()
    try { await assert.rejects(prepareGitHubInstallation(next, io), { code: 'EIO' }) } finally { t.mock.restoreAll(); syncBuiltinESMExports() }
  }
  await prepareGitHubInstallation(next, io)
  const current = await runtime.loadInstallationConfig(initial.configPath)
  assert.equal(current.coreDataDir, join(homedir(), '.ghost-agent-sentinel-codex', previous.installationId))
  for (const key of ['installationId', 'token', 'initialEnrollmentBaseline', 'dataDir']) assert.deepEqual(current[key], previous[key])
  assert.ok(calls.every(value => value.from === previous.coreDataDir && value.to === current.coreDataDir && value.registrationId === 'chatgpt:' + previous.installationId))
  assert.equal((await inspectGitHubInstallation(next)).config.coreDataDir, current.coreDataDir)
})

test('a new Codex installation defaults to its own Core rather than the DSH directory', async t => {
  const f = await githubFixture(t), options = await f.packet('stable', '3'.repeat(64))
  delete options.coreDataDir
  const { prepareGitHubInstallation } = await import('../chatgpt/github-installation.mjs')
  const result = await prepareGitHubInstallation(options, f.io), config = await runtime.loadInstallationConfig(result.configPath)
  assert.equal(config.coreDataDir, join(homedir(), '.ghost-agent-sentinel-codex', config.installationId))
  assert.notEqual(config.coreDataDir, join(homedir(), '.ghost-agent-sentinel'))
})
