import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdir, writeFile, readFile, rm, realpath, lstat } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { randomUUID, createHash } from 'node:crypto'
import { previewHelperChange, applyHelperChange } from '../chatgpt/helper-installation.mjs'
import { parseHelperManagementArguments } from '../scripts/manage-chatgpt-helper.mjs'
import { collectHelperPayload } from '../scripts/build-chatgpt-helper.mjs'

const hash = value => createHash('sha256').update(value).digest('hex')
async function fixture(t) {
  const dir = resolve('.scratch/chatgpt-sentinel-delivery/execution/stage05/ticket10/installer/test-' + randomUUID())
  await mkdir(dir, { recursive: true, mode: 0o700 }); t.after(() => rm(dir, { recursive: true, force: true }))
  const dataDir = join(dir, 'data'); await mkdir(dataDir, { mode: 0o700 })
  const configPath = join(dataDir, 'installation.json'), destinationBundle = join(dir, 'GhostSentinel.app')
  const config = { format: 1, installationId: randomUUID(), pluginId: 'sentinel@isolated', token: 'a'.repeat(64), dataDir, coreDataDir: join(dir, 'private-core'), sessionsRoot: join(dir, 'synthetic-sessions'), metadataSocket: join(dir, 'unused.sock'), port: 52184 }
  await writeFile(configPath, JSON.stringify(config), { mode: 0o600 })
  const candidateBundle = join(dir, 'candidate/GhostSentinel.app')
  const info = { CFBundleIdentifier: 'com.ghostsentinel.helper', CFBundleExecutable: 'GhostSentinelHelper', LSUIElement: true, GhostSentinelRuntimeVersion: '0.1.0-dev.1' }
  const files = { 'Contents/Info.plist': JSON.stringify(info), 'Contents/MacOS/GhostSentinelHelper': 'isolated non-executed fixture', 'Contents/Resources/node/bin/node': 'isolated Node payload', 'Contents/Resources/helper-config.json': JSON.stringify({ installationConfigPath: await realpath(configPath) }), 'Contents/Library/LaunchAgents/com.ghostsentinel.helper.agent.plist': JSON.stringify({ Label: 'com.ghostsentinel.helper.agent', BundleProgram: 'Contents/MacOS/GhostSentinelHelper', ProgramArguments: ['GhostSentinelHelper', '--agent'], RunAtLoad: true, KeepAlive: { SuccessfulExit: false }, ThrottleInterval: 30, ProcessType: 'Background', ExitTimeOut: 15 }), 'Contents/Resources/observer/chatgpt/helper-main.mjs': 'fixture entry', 'Contents/Resources/observer/src/daemon.mjs': 'fixture daemon', 'Contents/Resources/observer/chatgpt/panel.html': 'fixture panel' }
  for (const [name, bytes] of Object.entries(files)) { const path = join(candidateBundle, name); await mkdir(resolve(path, '..'), { recursive: true, mode: 0o700 }); await writeFile(path, bytes, { mode: name.includes('/MacOS/') || name.endsWith('/node') ? 0o755 : 0o600 }) }
  for (const key of ['chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'src/server.mjs', 'src/core-client.mjs', 'package.json', 'package-lock.json']) { const path = join(candidateBundle, 'Contents/Resources/observer', key); await mkdir(resolve(path, '..'), { recursive: true, mode: 0o700 }); await writeFile(path, 'fixture ' + key, { mode: 0o600 }) }
  const sources = { 'chatgpt/helper-main.mjs': hash('fixture entry'), 'src/daemon.mjs': hash('fixture daemon'), 'chatgpt/panel.html': hash('fixture panel') }
  for (const key of ['chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'src/server.mjs', 'src/core-client.mjs', 'package.json', 'package-lock.json']) sources[key] = hash('fixture ' + key)
  const manifest = { nodeInputSHA256: '738852ca4f83d6a0485e4ae25e04b84243ae1d331e5f003b7cd0291cdf4b6821', format: 1, bundleId: info.CFBundleIdentifier, runtimeVersion: info.GhostSentinelRuntimeVersion, installationConfigPath: await realpath(configPath), sources, sourceSetSHA256: hash(JSON.stringify(sources)), codeSignedExecutable: 'Contents/MacOS/GhostSentinelHelper', packagedFiles: await collectHelperPayload(candidateBundle) }
  await writeFile(join(candidateBundle, 'Contents/Resources/helper-inputs.json'), JSON.stringify(manifest), { mode: 0o600 })
  const calls = []
  const io = { async execute(file, args) { calls.push({ file, args }); if (file === '/usr/bin/codesign') { assert.ok(args[0] === '--verify' || args[0] === '--display'); return { stdout: '', stderr: 'Identifier=com.ghostsentinel.helper\n' } } if (file === '/usr/bin/plutil') return { stdout: await readFile(args.at(-1), 'utf8'), stderr: '' }; throw new Error('unexpected command in preview') }, async request() { throw new Error('preview must not connect') }, signal() { throw new Error('preview must not signal') } }
  return { dir, configPath, config, dataDir, destinationBundle, candidateBundle, manifest, io, calls }
}

test('preview verifies a real temporary payload without activation, writes or private identity disclosure', async t => {
  const f = await fixture(t), before = await readFile(f.configPath)
  const result = await previewHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, f.io)
  assert.equal(result.mode, 'preview'); assert.equal(result.operation, 'install'); assert.equal(result.activation, 'not-requested')
  assert.equal(result.pluginRegistration, 'pending-first-party-tool')
  assert.equal(JSON.stringify(result).includes(f.config.token), false); assert.equal(JSON.stringify(result).includes(f.config.installationId), false); assert.equal(JSON.stringify(result).includes(f.config.coreDataDir), false)
  assert.deepEqual(await readFile(f.configPath), before)
  await assert.rejects(lstat(f.destinationBundle), { code: 'ENOENT' })
  assert.ok(f.calls.every(call => ['/usr/bin/codesign', '/usr/bin/plutil'].includes(call.file)))
})

test('install preview refuses an existing foreign directory even when helper files are missing', async t => {
  const f = await fixture(t)
  await mkdir(f.destinationBundle, { mode: 0o700 }); await writeFile(join(f.destinationBundle, 'foreign.txt'), 'preserve')
  await assert.rejects(previewHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, f.io))
  assert.equal(await readFile(join(f.destinationBundle, 'foreign.txt'), 'utf8'), 'preserve')
})

test('install apply stages a verified bundle and preserves maintenance while approval is pending', async t => {
  const f = await fixture(t), io = { ...f.io, async request() { throw Object.assign(new Error('closed fixture endpoint'), { code: 'ECONNREFUSED' }) }, async execute(file, args) { if (file.endsWith('/GhostSentinelHelper')) { assert.ok(['--register', '--status'].includes(args[0])); if (args[0] === '--register') { const marker = JSON.parse(await readFile(join(f.dataDir, 'maintenance.json'))); assert.equal(marker.installationId, f.config.installationId) }; return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: args[0], serviceStatus: args[0] === '--status' ? 'notRegistered' : 'requiresApproval', unregisterCompleted: false }) } } return f.io.execute(file, args) } }
  const result = await applyHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'pending-approval'); assert.equal(result.collectionResumed, false)
  assert.equal((await lstat(join(f.dataDir, 'maintenance.json'))).mode & 0o777, 0o600)
  assert.equal(JSON.parse(await readFile(join(f.dataDir, 'helper-installation.json'))).manifestSHA256, hash(await readFile(join(f.destinationBundle, 'Contents/Resources/helper-inputs.json'))))
  assert.equal(JSON.stringify(result).includes(f.config.token), false)
})

async function pendingInstall(f) {
  const io = { ...f.io, async request() { throw Object.assign(new Error('closed fixture endpoint'), { code: 'ECONNREFUSED' }) }, async execute(file, args) { if (file.endsWith('/GhostSentinelHelper')) return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: args[0], serviceStatus: args[0] === '--status' ? 'notRegistered' : 'requiresApproval', unregisterCompleted: false }) }; return f.io.execute(file, args) } }
  return applyHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
}
async function readyRuntime(f) {
  const identity = { protocol: 'ghost-sentinel-chatgpt-helper/1', installationId: f.config.installationId, runId: randomUUID(), pid: 12345, version: '0.1.0-dev.1' }, wrapper = 12344
  const url = 'http://127.0.0.1:' + f.config.port
  await mkdir(join(f.dataDir, 'runtime.lock'), { mode: 0o700 }); await mkdir(join(f.dataDir, 'plugin/.codex-plugin'), { recursive: true, mode: 0o700 })
  await writeFile(join(f.dataDir, 'runtime.lock/owner.json'), JSON.stringify(identity), { mode: 0o600 })
  await writeFile(join(f.dataDir, 'runtime.json'), JSON.stringify({ format: 1, ...identity, url, startedAt: Date.now() }), { mode: 0o600 })
  await writeFile(join(f.dataDir, 'plugin/.mcp.json'), JSON.stringify({ mcpServers: { sentinel: { url: url + '/mcp', http_headers: { Authorization: 'Bearer ' + f.config.token } } } }), { mode: 0o600 })
  await writeFile(join(f.dataDir, 'plugin/.codex-plugin/plugin.json'), JSON.stringify({ name: 'ghost-sentinel', version: '0.1.0-dev.1', mcpServers: './.mcp.json' }), { mode: 0o600 })
  const node = join(f.destinationBundle, 'Contents/Resources/node/bin/node'), entry = join(f.destinationBundle, 'Contents/Resources/observer/chatgpt/helper-main.mjs'), main = join(f.destinationBundle, 'Contents/MacOS/GhostSentinelHelper')
  const io = { ...f.io, async execute(file, args) {
    if (file.endsWith('/GhostSentinelHelper')) { assert.deepEqual(args, ['--status']); return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: '--status', serviceStatus: 'enabled', unregisterCompleted: false }) } }
    if (file === '/bin/ps') {
      if (args[0] === '-axo') return { stdout: process.getuid() + ' ' + identity.pid + ' ' + node + ' ' + entry + ' --config ' + f.configPath + '\n' }
      const pid = Number(args[1]), field = args[3]
      if (field === 'uid=,ppid=,lstart=') return { stdout: process.getuid() + ' ' + (pid === identity.pid ? wrapper : 1) + ' Sat Oct 17 12:00:00 2026\n' }
      return { stdout: (field === 'comm=' ? pid === identity.pid ? node : main : pid === identity.pid ? node + ' ' + entry + ' --config ' + f.configPath : main + ' --agent') + '\n' }
    }
    return f.io.execute(file, args)
  }, async request(target, path, body) { assert.equal(target.url, url); assert.equal(target.token, f.config.token); assert.equal(path, '/health'); assert.equal(body, undefined); return identity } }
  return { io, identity, wrapper, node, entry, main }
}

test('approval resume adopts only the first saved port after exact helper identity verification', async t => {
  const f = await fixture(t)
  const withoutPort = { ...f.config }; delete withoutPort.port; await writeFile(f.configPath, JSON.stringify(withoutPort))
  const pending = await pendingInstall(f)
  assert.equal(pending.status, 'pending-approval')
  await writeFile(f.configPath, JSON.stringify(f.config))
  const live = await readyRuntime(f)
  const result = await applyHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath, resume: pending.operationId }, live.io)
  assert.equal(result.status, 'complete'); assert.equal(result.collectionResumed, true)
  await assert.rejects(lstat(join(f.dataDir, 'maintenance.json')), { code: 'ENOENT' })
  assert.equal(JSON.parse(await readFile(join(f.dataDir, 'helper-installation.json'))).knownPort, f.config.port)
  assert.equal(JSON.stringify(result).includes(f.config.installationId), false)
})

async function managed(f) {
  const pending = await pendingInstall(f), live = await readyRuntime(f)
  await applyHelperChange({ operation: 'install', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: pending.operationId }, live.io)
  return live
}
async function lifecycleIO(f, live) {
  const core = { protocol: 1, url: 'http://127.0.0.1:56111', token: 'b'.repeat(64), instanceId: randomUUID(), pid: 55555 }
  await mkdir(f.config.coreDataDir, { mode: 0o700 }); await writeFile(join(f.config.coreDataDir, 'runtime.json'), JSON.stringify(core), { mode: 0o600 })
  const steps = []; let nodeAlive = true, wrapperAlive = true
  const io = { ...live.io, async signal(pid, signal) { assert.equal(pid, live.identity.pid); assert.equal(signal, 'SIGTERM'); assert.ok(await lstat(join(f.dataDir, 'maintenance.json'))); steps.push('TERM'); nodeAlive = false; wrapperAlive = false; await rm(join(f.dataDir, 'runtime.json')); await rm(join(f.dataDir, 'runtime.lock'), { recursive: true }) }, async request(target, path, body) {
    if (target.url === core.url) {
      assert.equal(target.token, core.token)
      if (path === '/v1/health') return { protocol: 1, instanceId: core.instanceId, features: ['chatgpt-installation-purge/1'] }
      if (path === '/v1/sources') return { entries: [{ registrationId: 'chatgpt:' + f.config.installationId, status: nodeAlive ? 'connected' : 'disconnected' }] }
      if (path === '/v1/chatgpt/installation/purge') { assert.deepEqual(body, { installationId: f.config.installationId }); steps.push('core-purge'); return { installationId: f.config.installationId, registrationId: 'chatgpt:' + f.config.installationId, status: 'complete', logical: { status: 'purged' }, physical: { status: 'checkpointed', secureDelete: true } } }
    }
    if (!nodeAlive) throw Object.assign(new Error('isolated endpoint closed'), { code: 'ECONNREFUSED' })
    return live.io.request(target, path, body)
  }, async execute(file, args, input) {
    if (file.endsWith('/GhostSentinelHelper') && args[0] === '--unregister') { assert.equal(nodeAlive, false); steps.push('unregister'); wrapperAlive = false; return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: '--unregister', serviceStatus: 'notRegistered', unregisterCompleted: true }) } }
    if (file.endsWith('/GhostSentinelHelper') && args[0] === '--register') { assert.equal(wrapperAlive, false); steps.push('register'); return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: args[0], serviceStatus: args[0] === '--status' ? 'notRegistered' : 'requiresApproval', unregisterCompleted: false }) } }
    if (file === '/bin/ps' && args[0] === '-axo' && !nodeAlive) return { stdout: '' }
    if (file === '/bin/ps' && args[0] === '-p' && (Number(args[1]) === live.identity.pid && !nodeAlive || Number(args[1]) === live.wrapper && !wrapperAlive)) throw Object.assign(new Error('no such process'), { code: 1 })
    return live.io.execute(file, args, input)
  } }
  return { io, steps, core }
}

test('upgrade stops only the verified managed Node, unregisters and retains a recoverable old bundle', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live)
  const result = await applyHelperChange({ operation: 'upgrade', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, controlled.io)
  assert.equal(result.status, 'pending-approval', JSON.stringify(result))
  assert.deepEqual(controlled.steps, ['TERM', 'unregister', 'register'])
  assert.ok(await lstat(result.recovery.backupBundle))
  assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
  assert.ok(result.completed.includes('owned-processes-stopped'))
  assert.equal(result.processExitCode, null)
  const successor = await readyRuntime(f)
  const completed = await applyHelperChange({ operation: 'upgrade', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: result.operationId }, successor.io)
  assert.equal(completed.status, 'complete', JSON.stringify(completed))
  await assert.rejects(lstat(result.recovery.backupBundle), { code: 'ENOENT' })
})

test('uninstall clears only this installation through notification and Core seams and preserves foreign files', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live)
  await writeFile(join(f.dataDir, 'plugin/foreign.txt'), 'leave-user-file')
  await mkdir(join(f.dataDir, 'cursors'), { mode: 0o700 })
  await writeFile(join(f.dataDir, 'cursors', 'a'.repeat(64) + '.json'), JSON.stringify({ format: 2, reader: {}, captures: [], captureThroughOffset: -1 }), { mode: 0o600 })
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, controlled.io)
  assert.equal(result.status, 'incomplete', JSON.stringify(result))
  assert.deepEqual(controlled.steps, ['TERM', 'core-purge', 'unregister'])
  assert.equal(result.notifications.status, 'not-configured'); assert.equal(result.core.status, 'complete')
  assert.equal(result.configPreserved, false); assert.equal(result.configRevoked, true); assert.equal(result.residual.unknownPrivateEntries, 1)
  assert.equal(await readFile(join(f.dataDir, 'plugin/foreign.txt'), 'utf8'), 'leave-user-file')
  await assert.rejects(lstat(f.configPath), { code: 'ENOENT' }); await assert.rejects(lstat(f.destinationBundle), { code: 'ENOENT' })
  await assert.rejects(lstat(join(f.dataDir, 'plugin/.mcp.json')), { code: 'ENOENT' })
  await assert.rejects(lstat(join(f.dataDir, 'cursors', 'a'.repeat(64) + '.json')), { code: 'ENOENT' })
})

test('payload additions and private config substitutions are rejected before any activation', async t => {
  const f = await fixture(t), options = { operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }
  const extra = join(f.candidateBundle, 'Contents/Resources/unlisted.mjs'); await writeFile(extra, 'foreign payload')
  await assert.rejects(previewHelperChange(options, f.io), { code: 'packaged-file-set-mismatch' }); await rm(extra)
  const node = join(f.candidateBundle, 'Contents/Resources/node/bin/node'); await writeFile(node, 'tampered Node')
  await assert.rejects(previewHelperChange(options, f.io), { code: 'packaged-file-hash-mismatch' })
  await writeFile(node, 'isolated Node payload')
  await writeFile(f.configPath, JSON.stringify({ ...f.config, port: 0 }))
  await assert.rejects(previewHelperChange(options, f.io), { code: 'installation-config-invalid' })
  assert.ok(f.calls.every(call => ['/usr/bin/codesign', '/usr/bin/plutil'].includes(call.file)))
})

test('resume cannot accept changed immutable identity or a different previously fixed port', async t => {
  const f = await fixture(t), pending = await pendingInstall(f), options = { operation: 'install', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: pending.operationId }
  await writeFile(f.configPath, JSON.stringify({ ...f.config, metadataSocket: join(f.dir, 'changed.sock') }))
  await assert.rejects(applyHelperChange(options, f.io), { code: 'unknown-managed-installation' })
  await writeFile(f.configPath, JSON.stringify({ ...f.config, port: 52185 }))
  await assert.rejects(applyHelperChange(options, f.io), { code: 'unknown-managed-installation' })
  assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('an unknown older owner is never signalled or unregistered', async t => {
  const f = await fixture(t), live = await managed(f), ownerPath = join(f.dataDir, 'runtime.lock/owner.json')
  await writeFile(ownerPath, JSON.stringify({ ...live.identity, version: '0.1.0-dev.0' }))
  let signals = 0, controls = 0
  const io = { ...live.io, signal() { signals++; throw new Error('must not signal unknown process') }, async execute(file, args) { if (file.endsWith('/GhostSentinelHelper')) controls++; return live.io.execute(file, args) } }
  const result = await applyHelperChange({ operation: 'upgrade', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'helper-runtime-identity-unconfirmed')
  assert.equal(signals, 0); assert.equal(controls, 0); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('Core connected rejection preserves the bundle, plugin and maintenance without claiming a purge', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live)
  const io = { ...controlled.io, async request(target, path, body) { if (path === '/v1/chatgpt/installation/purge') throw Object.assign(new Error('target is connected'), { code: 'chatgpt_installation_connected', status: 409 }); return controlled.io.request(target, path, body) } }
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'chatgpt_installation_connected'); assert.equal(result.error.status, 409)
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(join(f.dataDir, 'plugin/.mcp.json'))); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('management CLI defaults to preview and exposes no alternate launcher or ordinary toggle', () => {
  const args = ['install', '--bundle', '/candidate/GhostSentinel.app', '--config', '/private/installation.json', '--destination', '/owned/GhostSentinel.app']
  assert.equal(parseHelperManagementArguments(args).apply, false)
  assert.equal(parseHelperManagementArguments([...args, '--apply']).apply, true)
  assert.equal(parseHelperManagementArguments(['upgrade', '--config', '/private/installation.json', '--destination', '/owned/GhostSentinel.app', '--resume', 'b75e441a-d847-4f3f-9599-a2b4e7ece3da']).options.resume, 'b75e441a-d847-4f3f-9599-a2b4e7ece3da')
  assert.equal(parseHelperManagementArguments(['uninstall', '--config', '/private/installation.json', '--destination', '/owned/GhostSentinel.app', '--resume', 'b75e441a-d847-4f3f-9599-a2b4e7ece3da']).options.operation, 'uninstall')
  for (const more of [['--node', '/bin/sh'], ['--entry', '/foreign'], ['--apply', '--apply'], ['--config', '/other']]) assert.throws(() => parseHelperManagementArguments([...args, ...more]))
  assert.throws(() => parseHelperManagementArguments(['enable']))
})

async function notificationFixture(f) {
  const bundle = join(f.dir, 'GhostSentinelNotifications.app')
  for (const [name, body] of Object.entries({ 'Contents/Info.plist': JSON.stringify({ CFBundleIdentifier: 'com.ghostsentinel.notifications' }), 'Contents/MacOS/GhostSentinelNotifications': 'isolated never executed' })) { const path = join(bundle, name); await mkdir(resolve(path, '..'), { recursive: true, mode: 0o700 }); await writeFile(path, body, { mode: name.includes('MacOS') ? 0o755 : 0o600 }) }
  f.config.notificationBundlePath = bundle; await writeFile(f.configPath, JSON.stringify(f.config))
}

test('uninstall repeats only bounded own notification purge and preserves delivered OS history', async t => {
  const f = await fixture(t); await notificationFixture(f)
  const live = await managed(f), controlled = await lifecycleIO(f, live); let calls = 0
  const io = { ...controlled.io, async execute(file, args, input) {
    if (file === '/usr/bin/codesign' && args[0] === '--display' && args.at(-1) === f.config.notificationBundlePath) return { stdout: '', stderr: 'Identifier=com.ghostsentinel.notifications\n' }
    if (file.endsWith('/GhostSentinelNotifications')) {
      const packet = JSON.parse(input); assert.deepEqual(args, ['--stdio']); assert.equal(packet.operation, 'purge-installation'); assert.match(packet.ownerPrefix, /^ghostsentinel:[a-f0-9]{24}:$/); assert.equal(input.includes(f.config.installationId), false)
      controlled.steps.push('notification-purge'); calls++
      return { stdout: JSON.stringify({ schemaVersion: 1, operation: 'purge-installation', bundleId: 'com.ghostsentinel.notifications', ok: true, result: { complete: calls === 2, requestedCancellations: calls === 1 ? 100 : 50, remainingPending: calls === 1 ? 50 : 0, clearedActions: 1, deliveredPreserved: true, observedAt: Date.now() } }) }
    }
    return controlled.io.execute(file, args, input)
  } }
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'complete', JSON.stringify(result)); assert.equal(result.notifications.operations, 2); assert.equal(result.notifications.requestedCancellations, 150); assert.equal(result.notifications.remainingPending, 0); assert.equal(result.notifications.deliveredPreserved, true)
  assert.deepEqual(controlled.steps, ['TERM', 'notification-purge', 'notification-purge', 'core-purge', 'unregister'])
})

test('WAL busy stays incomplete after three scoped retries and does not remove registered files', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let purges = 0
  const io = { ...controlled.io, async request(target, path, body) {
    if (path === '/v1/chatgpt/installation/purge') { purges++; return { installationId: f.config.installationId, registrationId: 'chatgpt:' + f.config.installationId, status: 'incomplete', logical: { status: 'purged', removed: { sources: 0 }, remaining: { sources: 0 } }, physical: { status: 'busy', secureDelete: true, wal: { busy: 1, logFrames: 9, checkpointedFrames: 0 } }, retryable: true, errors: [{ stage: 'checkpoint', code: 'SQLITE_BUSY' }] } }
    return controlled.io.request(target, path, body)
  } }
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'core-installation-purge-incomplete'); assert.equal(result.core.status, 'incomplete'); assert.equal(result.core.logical.status, 'purged'); assert.equal(purges, 3)
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(join(f.dataDir, 'plugin/.mcp.json'))); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('same-operation uninstall cancels an unapproved installation and is idempotent', async t => {
  const f = await fixture(t), pending = await pendingInstall(f); let unregistered = false, controls = 0
  const io = { ...f.io, async request() { throw Object.assign(new Error('not listening'), { code: 'ECONNREFUSED' }) }, async execute(file, args) {
    if (file === '/bin/ps') { assert.equal(args[0], '-axo'); return { stdout: '' } }
    if (file.endsWith('/GhostSentinelHelper')) { controls++; if (args[0] === '--unregister') unregistered = true; return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: args[0], serviceStatus: unregistered ? 'notRegistered' : 'requiresApproval', unregisterCompleted: args[0] === '--unregister' }) } }
    return f.io.execute(file, args)
  } }
  const options = { operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: pending.operationId }
  const preview = await previewHelperChange(options, f.io); assert.equal(preview.mode, 'preview'); assert.equal(preview.activation, 'not-requested'); assert.equal(controls, 0)
  const result = await applyHelperChange(options, io)
  assert.equal(result.status, 'cancelled', JSON.stringify(result)); assert.equal(result.cancelledOperation, 'install'); assert.equal(result.configPreserved, true); assert.equal(result.collectionResumed, false)
  assert.equal(result.core.status, 'not-required-before-first-qualification')
  await assert.rejects(lstat(f.destinationBundle), { code: 'ENOENT' }); assert.ok(await lstat(f.configPath)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
  const before = controls; assert.deepEqual(await applyHelperChange(options, io), result); assert.equal(controls, before)
})

test('same-operation uninstall recovers a partially failed first registration without accepting a different marker', async t => {
  const f = await fixture(t)
  const failing = { ...f.io, async request() { throw Object.assign(new Error('closed'), { code: 'ECONNREFUSED' }) }, async execute(file, args) { if (file.endsWith('/GhostSentinelHelper')) { if (args[0] === '--status') return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: '--status', serviceStatus: 'notRegistered', unregisterCompleted: false }) }; throw Object.assign(new Error('isolated register failure'), { code: 'fixture_register_failure' }) }; return f.io.execute(file, args) } }
  const failed = await applyHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, failing)
  assert.equal(failed.status, 'failed'); assert.ok(await lstat(f.destinationBundle))
  const markerPath = join(f.dataDir, 'maintenance.json'), marker = await readFile(markerPath)
  await writeFile(markerPath, JSON.stringify({ format: 1, installationId: f.config.installationId, operationId: randomUUID() }))
  const options = { operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: failed.operationId }
  await assert.rejects(applyHelperChange(options, failing)); assert.ok(await lstat(f.destinationBundle))
  await writeFile(markerPath, marker)
  const cleanup = { ...failing, async request() { throw Object.assign(new Error('closed'), { code: 'ECONNREFUSED' }) }, async execute(file, args) { if (file === '/bin/ps') return { stdout: '' }; return failing.execute(file, args) } }
  const result = await applyHelperChange(options, cleanup)
  assert.equal(result.status, 'cancelled', JSON.stringify(result)); assert.equal(result.originalFailure.code, 'fixture_register_failure')
  await assert.rejects(lstat(f.destinationBundle), { code: 'ENOENT' }); assert.ok(await lstat(f.configPath))
})

test('complete ready uninstall revokes its exact private configuration and token', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live)
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, controlled.io)
  assert.equal(result.status, 'complete', JSON.stringify(result)); assert.equal(result.configRevoked, true); assert.equal(result.configPreserved, false); assert.equal(result.maintenancePreserved, false)
  await assert.rejects(lstat(f.configPath), { code: 'ENOENT' }); await assert.rejects(lstat(join(f.dataDir, 'maintenance.json')), { code: 'ENOENT' })
  const terminal = await readFile(join(f.dataDir, 'helper-operation.json'), 'utf8'); assert.equal(terminal.includes(f.config.token), false)
})

test('a concurrent config edit after process stop is preserved and blocks complete uninstall', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live), changed = JSON.stringify({ ...f.config, port: 52185 })
  const io = { ...controlled.io, async execute(file, args, input) { const value = await controlled.io.execute(file, args, input); if (file.endsWith('/GhostSentinelHelper') && args[0] === '--unregister') await writeFile(f.configPath, changed); return value } }
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'failed', JSON.stringify(result)); assert.equal(result.error.code, 'managed-config-changed-before-removal')
  assert.equal(await readFile(f.configPath, 'utf8'), changed); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('an internally consistent manifest cannot omit the packaged shared server endpoint', async t => {
  const f = await fixture(t)
  await rm(join(f.candidateBundle, 'Contents/Resources/observer/src/server.mjs')); delete f.manifest.sources['src/server.mjs']; f.manifest.sourceSetSHA256 = hash(JSON.stringify(f.manifest.sources)); f.manifest.packagedFiles = await collectHelperPayload(f.candidateBundle)
  await writeFile(join(f.candidateBundle, 'Contents/Resources/helper-inputs.json'), JSON.stringify(f.manifest))
  await assert.rejects(previewHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, f.io), { code: 'packaged-endpoint-prerequisite-missing' })
})

test('notification residue exhausts four operations without purging Core or unregistering', async t => {
  const f = await fixture(t); await notificationFixture(f)
  const live = await managed(f), controlled = await lifecycleIO(f, live); let calls = 0
  const io = { ...controlled.io, async execute(file, args, input) {
    if (file === '/usr/bin/codesign' && args[0] === '--display' && args.at(-1) === f.config.notificationBundlePath) return { stderr: 'Identifier=com.ghostsentinel.notifications\n' }
    if (file.endsWith('/GhostSentinelNotifications')) { calls++; return { stdout: JSON.stringify({ schemaVersion: 1, operation: 'purge-installation', bundleId: 'com.ghostsentinel.notifications', ok: true, result: { complete: false, requestedCancellations: 100, remainingPending: 1, clearedActions: 0, deliveredPreserved: true, observedAt: Date.now() } }) } }
    return controlled.io.execute(file, args, input)
  } }
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'native-notification-purge-incomplete'); assert.equal(result.notifications.remainingPending, 1); assert.equal(result.notifications.requestedCancellations, 400); assert.equal(calls, 4)
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(f.configPath)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('a live unowned endpoint rejects a first install before creating maintenance or invoking SM', async t => {
  const f = await fixture(t), io = { ...f.io, async request(target, path) { assert.equal(target.url, 'http://127.0.0.1:' + f.config.port); assert.equal(path, '/health'); return { unknown: true } } }
  await assert.rejects(applyHelperChange({ operation: 'install', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, io), { code: 'unmanaged-endpoint-requires-migration' })
  await assert.rejects(lstat(join(f.dataDir, 'maintenance.json')), { code: 'ENOENT' }); await assert.rejects(lstat(f.destinationBundle), { code: 'ENOENT' })
})

test('explicit same-operation upgrade resume retries a failed registration once and reaches verified ready', async t => {
  const f = await fixture(t), old = await managed(f), controlled = await lifecycleIO(f, old)
  const failing = { ...controlled.io, async execute(file, args, input) { if (file.endsWith('/GhostSentinelHelper') && args[0] === '--register') throw Object.assign(new Error('isolated initial registration failure'), { code: 'fixture_register_failure' }); return controlled.io.execute(file, args, input) } }
  const failed = await applyHelperChange({ operation: 'upgrade', candidateBundle: f.candidateBundle, destinationBundle: f.destinationBundle, configPath: f.configPath }, failing)
  assert.equal(failed.status, 'failed'); assert.equal(failed.error.code, 'fixture_register_failure'); assert.ok(await lstat(failed.recovery.backupBundle))
  let live, retries = 0
  const resumeIO = { ...f.io, async execute(file, args, input) {
    if (file.endsWith('/GhostSentinelHelper') && args[0] === '--status') return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: '--status', serviceStatus: 'notRegistered', unregisterCompleted: false }) }
    if (file.endsWith('/GhostSentinelHelper') && args[0] === '--register') { retries++; assert.ok(await lstat(failed.recovery.backupBundle)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json'))); live = await readyRuntime(f); return { stdout: JSON.stringify({ ok: true, bundleId: 'com.ghostsentinel.helper', command: '--register', serviceStatus: 'enabled', unregisterCompleted: false }) } }
    return (live?.io ?? f.io).execute(file, args, input)
  }, async request(...args) { return live.io.request(...args) } }
  const recovered = await applyHelperChange({ operation: 'upgrade', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: failed.operationId }, resumeIO)
  assert.equal(recovered.status, 'complete', JSON.stringify(recovered)); assert.equal(recovered.collectionResumed, true); assert.equal(recovered.recoveredFailure.code, 'fixture_register_failure'); assert.equal(retries, 1)
  await assert.rejects(lstat(failed.recovery.backupBundle), { code: 'ENOENT' }); await assert.rejects(lstat(join(f.dataDir, 'maintenance.json')), { code: 'ENOENT' })
})

test('same-operation ready uninstall resumes after scoped Core physical cleanup was busy', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let busy = true, purgeCalls = 0
  const io = { ...controlled.io, async request(target, path, body) {
    if (path === '/v1/chatgpt/installation/purge') { purgeCalls++; if (busy) return { installationId: f.config.installationId, registrationId: 'chatgpt:' + f.config.installationId, status: 'incomplete', logical: { status: 'purged' }, physical: { status: 'busy', secureDelete: true }, retryable: true, errors: [{ stage: 'checkpoint', code: 'SQLITE_BUSY' }] } }
    return controlled.io.request(target, path, body)
  } }
  const failed = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(failed.status, 'failed'); assert.equal(failed.error.code, 'core-installation-purge-incomplete'); assert.equal(purgeCalls, 3)
  busy = false
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: failed.operationId }, { ...io, signal() { throw new Error('resume must never resignal the old PID') } })
  assert.equal(result.status, 'complete', JSON.stringify(result)); assert.equal(purgeCalls, 4); assert.equal(result.configRevoked, true)
  await assert.rejects(lstat(f.configPath), { code: 'ENOENT' }); await assert.rejects(lstat(join(f.dataDir, 'maintenance.json')), { code: 'ENOENT' })
})

test('explicit uninstall reconnects dormant same-profile Core and closes only its management client', async t => {
  for (const mode of ['idle-runtime-absent', 'core-never-created']) await t.test(mode, async t => {
    const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let connected = 0, closed = 0
    if (mode === 'idle-runtime-absent') await rm(join(f.config.coreDataDir, 'runtime.json'))
    else await rm(f.config.coreDataDir, { recursive: true })
    const io = { ...controlled.io, async connectCore({ dataDir, signal }) {
      assert.equal(dataDir, f.config.coreDataDir); assert.ok(signal instanceof AbortSignal); connected++
      controlled.core.instanceId = randomUUID(); controlled.core.token = 'c'.repeat(64)
      await mkdir(dataDir, { recursive: true, mode: 0o700 }); await writeFile(join(dataDir, 'runtime.json'), JSON.stringify(controlled.core), { mode: 0o600 })
      return { health: () => controlled.io.request(controlled.core, '/v1/health'), async close() { closed++ } }
    } }
    const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
    assert.equal(result.status, 'complete', JSON.stringify(result)); assert.equal(connected, 1); assert.equal(closed, 1); assert.equal(result.configRevoked, true)
    assert.ok(await lstat(join(f.config.coreDataDir, 'runtime.json')))
    assert.deepEqual(controlled.steps, ['TERM', 'core-purge', 'unregister'])
  })
})

test('same-operation ready uninstall reconnects after offline Core without signalling an old PID', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let offline = true, connects = 0, closed = 0
  await rm(join(f.config.coreDataDir, 'runtime.json'))
  const io = { ...controlled.io, async connectCore({ dataDir }) {
    connects++; if (offline) throw Object.assign(new Error('isolated Core offline'), { code: 'ECONNREFUSED' })
    assert.equal(dataDir, f.config.coreDataDir); await writeFile(join(dataDir, 'runtime.json'), JSON.stringify(controlled.core), { mode: 0o600 })
    return { health: () => controlled.io.request(controlled.core, '/v1/health'), async close() { closed++ } }
  } }
  const failed = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  assert.equal(failed.status, 'failed'); assert.equal(failed.error.code, 'ECONNREFUSED')
  const recorded = JSON.parse(await readFile(join(f.dataDir, 'helper-operation.json')))
  assert.equal(recorded.ownedStop.nodeGone, true); assert.equal(recorded.ownedStop.wrapperGone, true); assert.equal(recorded.phase, 'owned-stop-confirmed')
  offline = false
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: failed.operationId }, { ...io, signal() { throw new Error('must not signal recorded PID') } })
  assert.equal(result.status, 'complete', JSON.stringify(result)); assert.equal(result.recoveredFailure.code, 'ECONNREFUSED'); assert.equal(connects, 2); assert.equal(closed, 1)
  assert.deepEqual(controlled.steps, ['TERM', 'core-purge', 'unregister'])
})

test('missing Core runtime never authorizes reconnect over an unknown live profile owner', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let connects = 0
  await rm(join(f.config.coreDataDir, 'runtime.json')); await mkdir(join(f.config.coreDataDir, 'core.lock'), { mode: 0o700 })
  const ownerPath = join(f.config.coreDataDir, 'core.lock/owner.json'), owner = JSON.stringify({ pid: 66666, instanceId: randomUUID() }); await writeFile(ownerPath, owner, { mode: 0o600 })
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, { ...controlled.io, async connectCore() { connects++; throw new Error('must not attach unknown live profile') } })
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'core-profile-owner-unconfirmed'); assert.equal(connects, 0); assert.equal(await readFile(ownerPath, 'utf8'), owner)
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(f.configPath))
})

test('ready-uninstall resume rejects missing stop proof, a new runtime and concurrent immutable config', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live)
  const io = { ...controlled.io, async request(target, path, body) { if (path === '/v1/chatgpt/installation/purge') return { installationId: f.config.installationId, registrationId: 'chatgpt:' + f.config.installationId, status: 'incomplete', logical: { status: 'purged' }, physical: { status: 'busy', secureDelete: true }, retryable: false }; return controlled.io.request(target, path, body) } }
  const failed = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, io)
  const operationPath = join(f.dataDir, 'helper-operation.json'), recorded = await readFile(operationPath), invalid = JSON.parse(recorded); invalid.ownedStop.wrapperGone = false; await writeFile(operationPath, JSON.stringify(invalid))
  const options = { operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath, resume: failed.operationId }, guarded = { ...io, signal() { throw new Error('must never signal recorded or new PID on resume') } }
  await assert.rejects(applyHelperChange(options, guarded), { code: 'uninstall-owned-stop-witness-unconfirmed' })
  await writeFile(operationPath, recorded); await writeFile(join(f.dataDir, 'runtime.json'), '{}', { mode: 0o600 })
  const blocked = await applyHelperChange(options, guarded)
  assert.equal(blocked.status, 'failed'); assert.equal(blocked.error.code, 'uninstall-owned-stop-no-longer-confirmed')
  await rm(join(f.dataDir, 'runtime.json')); await writeFile(f.configPath, JSON.stringify({ ...f.config, metadataSocket: join(f.dir, 'changed.sock') }))
  await assert.rejects(applyHelperChange(options, guarded), { code: 'unknown-managed-installation' })
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})

test('a reconnected Core identity mismatch closes the client and preserves the installation', async t => {
  const f = await fixture(t), live = await managed(f), controlled = await lifecycleIO(f, live); let closed = 0
  await rm(join(f.config.coreDataDir, 'runtime.json'))
  const result = await applyHelperChange({ operation: 'uninstall', destinationBundle: f.destinationBundle, configPath: f.configPath }, { ...controlled.io, async connectCore({ dataDir }) {
    await writeFile(join(dataDir, 'runtime.json'), JSON.stringify(controlled.core), { mode: 0o600 })
    return { health: async () => ({ protocol: 1, instanceId: randomUUID() }), async close() { closed++ } }
  } })
  assert.equal(result.status, 'failed'); assert.equal(result.error.code, 'reconnected-core-identity-unconfirmed'); assert.equal(closed, 1)
  assert.deepEqual(controlled.steps, ['TERM']); assert.ok(await lstat(f.destinationBundle)); assert.ok(await lstat(f.configPath)); assert.ok(await lstat(join(f.dataDir, 'maintenance.json')))
})
