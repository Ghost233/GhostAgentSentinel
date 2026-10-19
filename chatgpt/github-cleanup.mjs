import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { lstat, open, readdir, realpath, rename, rm, unlink } from 'node:fs/promises'
import { constants } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { basename, dirname, isAbsolute, join, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createStdioMetadataClient } from './stdio-metadata.mjs'
import { createNativeNotificationIO } from './native-notifications.mjs'
import { inspectPluginPackage } from './plugin-package.mjs'
import { connectCore } from '../src/core-client.mjs'

const pluginIds = { stable: 'ghost-sentinel@ghostsentinel', test: 'ghost-sentinel-test@ghostsentinel' }
const execute = promisify(execFile), uid = () => process.getuid(), digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
const identity = info => JSON.stringify([info.dev, info.ino, info.mode, info.uid, info.size, info.mtimeMs, info.ctimeMs])
const failure = code => Object.assign(new Error(code), { code })
const absent = async path => { try { await lstat(path); return false } catch (error) { if (error.code === 'ENOENT') return true; throw error } }
const action = (channel, errorCode) => ({ status: 'needs-user-action', channel, errorCode })

async function privateRoot(stateRoot, channel) {
  if (typeof stateRoot !== 'string' || !isAbsolute(stateRoot) || resolve(stateRoot) !== stateRoot || basename(stateRoot) !== channel || basename(dirname(stateRoot)) !== 'github' || /[\x00-\x1f\x7f]/.test(stateRoot)) throw failure('github-cleanup-root-invalid')
  let path = sep
  for (const part of stateRoot.split(sep).filter(Boolean)) { path = join(path, part); if ((await lstat(path)).isSymbolicLink()) throw failure('github-cleanup-symlink-rejected') }
  const parent = await lstat(dirname(stateRoot))
  if (!parent.isDirectory() || parent.uid !== uid() || parent.mode & 0o077) throw failure('github-cleanup-private-parent-required')
  const info = await lstat(stateRoot)
  if (!info.isDirectory() || info.uid !== uid() || info.mode & 0o077 || await realpath(stateRoot) !== stateRoot) throw failure('github-cleanup-private-root-required')
  return info
}
async function pluginAbsent(metadata, channel) {
  let catalog
  try { catalog = await metadata.request('plugin/list', { forceRefetch: true }) } catch { return 'github-plugin-uninstall-unconfirmed' }
  if (!Array.isArray(catalog?.marketplaces) || !Array.isArray(catalog.marketplaceLoadErrors) || catalog.marketplaceLoadErrors.length) return 'github-plugin-uninstall-unconfirmed'
  const markets = catalog.marketplaces.filter(market => market.name === 'ghostsentinel')
  const plugins = markets.length === 1 && Array.isArray(markets[0].plugins) ? markets[0].plugins.filter(plugin => plugin.id === pluginIds[channel]) : []
  if (plugins.length !== 1 || plugins[0].name !== pluginIds[channel].split('@')[0] || typeof plugins[0].installed !== 'boolean') return 'github-plugin-uninstall-unconfirmed'
  return plugins[0].installed ? 'github-plugin-still-installed' : null
}
async function ownProcesses(configPath) {
  const { stdout } = await execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'], { timeout: 5000, maxBuffer: 4194304 })
  return stdout.split('\n').flatMap(line => { const row = line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/); return row && Number(row[1]) === uid() && Number(row[2]) !== process.pid && row[3].includes(configPath) ? [{ pid: Number(row[2]) }] : [] })
}
async function settled(stateRoot, configPath, processes) {
  for (const name of ['github-operation.json', 'state/maintenance.json', 'state/runtime.lock', 'state/runtime.json']) if (!await absent(join(stateRoot, name))) return false
  const values = await processes(configPath)
  if (!Array.isArray(values)) throw failure('github-cleanup-process-state-unconfirmed')
  return values.length === 0
}
async function snapshotTree(root, allowed, prefix = '', records = {}, budget = { count: 0 }) {
  const before = await lstat(root)
  if (!before.isDirectory() || before.isSymbolicLink() || before.uid !== uid() || before.mode & 0o077) throw failure('github-cleanup-entry-untrusted')
  records[prefix] = identity(before)
  for (const name of (await readdir(root)).sort()) {
    if (++budget.count > 40000) throw failure('github-cleanup-file-budget')
    const key = prefix ? prefix + '/' + name : name, path = join(root, name), info = await lstat(path)
    if (info.isSymbolicLink() || info.uid !== uid() || info.mode & 0o077) throw failure('github-cleanup-entry-untrusted')
    if (info.isDirectory()) {
      if (key !== 'state/cursors' && ![...allowed].some(value => value.startsWith(key + '/'))) throw failure('github-cleanup-unknown-files-preserved')
      await snapshotTree(path, allowed, key, records, budget)
    } else if (info.isFile() && (allowed.has(key) || /^state\/cursors\/[a-f0-9]{64}\.json$/.test(key))) records[key] = identity(info)
    else throw failure('github-cleanup-unknown-files-preserved')
  }
  if (identity(await lstat(root)) !== identity(before)) throw failure('github-cleanup-directory-changed')
  return records
}
async function ownedFiles(installation, inspectPackage) {
  const { stateRoot, receipt, config, configPath, channel } = installation
  if (configPath !== join(stateRoot, 'state/installation.json') || config.dataDir !== join(stateRoot, 'state') || config.pluginId !== pluginIds[channel] || receipt.installationId !== config.installationId || receipt.stateRoot !== stateRoot || receipt.channel !== channel || !Array.isArray(receipt.releases) || !receipt.releases.length) throw failure('github-cleanup-ownership-unconfirmed')
  if (config.coreDataDir === dirname(stateRoot) || config.coreDataDir.startsWith(dirname(stateRoot) + sep) || config.sessionsRoot === stateRoot || config.sessionsRoot.startsWith(stateRoot + sep)) throw failure('github-cleanup-shared-path-overlap')
  const allowed = new Set(['.github-prepare.lock', 'github-installation.json', 'state/installation.json', 'state/plugin/.codex-plugin/plugin.json', 'state/plugin/.mcp.json', 'state/health-outbox.sqlite', 'state/health-outbox.sqlite-wal', 'state/health-outbox.sqlite-shm'])
  for (const release of receipt.releases) {
    if (!digest(release.archiveSHA256) || !digest(release.releaseId) || release.releaseRoot !== join(stateRoot, 'payloads', release.archiveSHA256)) throw failure('github-cleanup-release-unconfirmed')
    const inspected = await inspectPackage(release.releaseRoot, { digest: release.releaseId })
    if (inspected.root !== release.releaseRoot || inspected.digest !== release.releaseId || inspected.notificationApp !== release.notificationBundlePath) throw failure('github-cleanup-release-unconfirmed')
    allowed.add('ready/' + release.archiveSHA256 + '.json')
    const prefix = 'payloads/' + release.archiveSHA256 + '/'
    allowed.add(prefix + 'plugin-package.json')
    for (const name of Object.keys(inspected.manifest.files)) allowed.add(prefix + name)
  }
  return allowed
}

// Explicit cleanup after App uninstall; this is never an uninstall hook.
export async function cleanupGitHubInstallation({ stateRoot, channel, apply = false }, io = {}) {
  if (!Object.hasOwn(pluginIds, channel) || typeof apply !== 'boolean') throw failure('github-cleanup-channel-invalid')
  const rootInfo = await privateRoot(stateRoot, channel), metadata = io.metadata ?? createStdioMetadataClient()
  const lockPath = join(stateRoot, '.github-prepare.lock'), nonce = randomUUID()
  let lock = null, lockInfo = null, tombstone = null, primary = null
  try {
    const missing = await pluginAbsent(metadata, channel)
    if (missing) return action(channel, missing)
    if (!await absent(lockPath)) return action(channel, 'github-cleanup-prepare-in-progress')
    if (apply) {
      try { lock = await open(lockPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600) }
      catch (error) { if (error.code === 'EEXIST') return action(channel, 'github-cleanup-prepare-in-progress'); throw error }
      await lock.writeFile(JSON.stringify({ format: 1, kind: 'ghost-sentinel-github-prepare-lock/1', channel, stateRoot, pid: process.pid, nonce }) + '\n')
      lockInfo = await lock.stat()
    }
    if (!await settled(stateRoot, join(stateRoot, 'state/installation.json'), io.processes ?? ownProcesses)) return action(channel, 'github-cleanup-observation-not-settled')
    const inspectInstallation = io.inspectInstallation ?? (await import('./github-installation.mjs')).inspectGitHubInstallation
    const installation = await inspectInstallation({ stateRoot, channel })
    if (!installation || installation.stateRoot !== stateRoot || installation.channel !== channel) throw failure('github-cleanup-ownership-unconfirmed')
    const allowed = await ownedFiles(installation, io.inspectPackage ?? inspectPluginPackage)
    const before = await snapshotTree(stateRoot, allowed)
    if (!apply) return { status: 'preview', channel, installationId: installation.config.installationId, action: 'purge-installation-data', deliveredNotifications: 'preserved' }
    const { config, configPath } = installation
    let notifications = { status: 'not-configured' }, coreCleanup = { status: 'not-created' }
    if (config.notificationBundlePath) {
      const native = io.notifications ?? createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId, testMode: channel === 'test' })
      try {
        let result
        for (let attempts = 0; attempts < 3; attempts++) { result = await native.purgeInstallation(); if (result.complete === true && result.remainingPending === 0) break }
        if (result.complete !== true || result.remainingPending !== 0 || result.deliveredPreserved !== true) return action(channel, 'github-cleanup-notifications-incomplete')
        notifications = { status: 'complete', deliveredPreserved: true }
      } finally { await native.close?.() }
    }
    if (!await absent(join(config.coreDataDir, 'observations.sqlite'))) {
      const core = await (io.connectCore ?? connectCore)({ dataDir: config.coreDataDir })
      try {
        if (!(await core.health()).features?.includes('chatgpt-installation-purge/1')) return action(channel, 'github-cleanup-core-purge-unsupported')
        const result = await core.purgeChatGPTInstallation({ installationId: config.installationId })
        if (result.installationId !== config.installationId || result.registrationId !== 'chatgpt:' + config.installationId || result.status !== 'complete' || result.logical?.status !== 'purged' || result.physical?.status !== 'checkpointed' || result.physical?.secureDelete !== true) return action(channel, 'github-cleanup-core-purge-incomplete')
        coreCleanup = { status: 'complete' }
      } finally { await core.close() }
    }
    const changed = await pluginAbsent(metadata, channel)
    if (changed) return action(channel, changed)
    if (!await settled(stateRoot, configPath, io.processes ?? ownProcesses)) return action(channel, 'github-cleanup-observation-not-settled')
    await inspectInstallation({ stateRoot, channel })
    if (JSON.stringify(await snapshotTree(stateRoot, allowed)) !== JSON.stringify(before)) return action(channel, 'github-cleanup-files-changed')
    const current = await lstat(stateRoot)
    if (current.dev !== rootInfo.dev || current.ino !== rootInfo.ino) throw failure('github-cleanup-root-changed')
    tombstone = join(dirname(stateRoot), '.' + channel + '-cleanup-' + nonce)
    if (!await absent(tombstone)) throw failure('github-cleanup-quarantine-conflict')
    await rename(stateRoot, tombstone)
    await rm(tombstone, { recursive: true }); tombstone = null
    return { status: 'complete', channel, installationId: config.installationId, notifications, coreCleanup, stateRemoved: true }
  } catch (error) {
    primary = error
    if (tombstone) error.recoveryPath = tombstone
    throw error
  } finally {
    const errors = []
    try { await lock?.close() } catch (error) { errors.push(error) }
    try { if (lockInfo && !tombstone && !await absent(lockPath)) { const current = await lstat(lockPath); if (current.dev !== lockInfo.dev || current.ino !== lockInfo.ino) throw failure('github-cleanup-lock-changed'); await unlink(lockPath) } } catch (error) { errors.push(error) }
    try { await metadata.close() } catch (error) { errors.push(error) }
    if (errors.length) throw Object.assign(new AggregateError([...(primary ? [primary] : []), ...errors], 'GitHub cleanup resource release failed', { cause: primary ?? errors[0] }), tombstone ? { recoveryPath: tombstone } : {})
  }
}

export function parseGitHubCleanupArguments(args) {
  const options = { apply: false }
  for (let index = 0; index < args.length; index++) {
    const name = args[index]
    if (name === '--help') return { help: 'cleanup --state-root <absolute github/stable|test> --channel <stable|test> [--apply]' }
    if (name === '--apply' && !options.apply) options.apply = true
    else if (['--state-root', '--channel'].includes(name) && args[index + 1] && !args[index + 1].startsWith('--') && !Object.hasOwn(options, name === '--state-root' ? 'stateRoot' : 'channel')) options[name === '--state-root' ? 'stateRoot' : 'channel'] = args[++index]
    else throw failure('github-cleanup-arguments-invalid')
  }
  if (!options.stateRoot || !Object.hasOwn(pluginIds, options.channel)) throw failure('github-cleanup-arguments-invalid')
  return options
}
if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try { const options = parseGitHubCleanupArguments(process.argv.slice(2)); const result = options.help ? options : await cleanupGitHubInstallation(options); console.log(JSON.stringify(result)); process.exitCode = result.status === 'needs-user-action' ? 2 : 0 }
  catch (error) { console.error(JSON.stringify({ status: 'failed', errorCode: error.code ?? 'github-cleanup-failed', ...(error.recoveryPath ? { recoveryPath: error.recoveryPath } : {}) })); process.exitCode = 1 }
}
