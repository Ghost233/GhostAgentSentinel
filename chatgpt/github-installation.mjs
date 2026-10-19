import { open, lstat, realpath, mkdir, writeFile, rename, rm, link, readdir, rmdir } from 'node:fs/promises'
import { constants } from 'node:fs'
import { join, resolve, dirname, basename, isAbsolute } from 'node:path'
import { homedir } from 'node:os'
import { randomBytes, randomUUID, createHash } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import { inspectPluginPackage } from './plugin-package.mjs'
import { prepareInstallation, loadInstallationConfig, resolvePluginOwner, HELPER_VERSION, HELPER_PROTOCOL } from './runtime.mjs'

export const GITHUB_CHANNELS = Object.freeze({
  stable: Object.freeze({ pluginId: 'ghost-sentinel@ghostsentinel', displayName: 'GhostSentinel' }),
  test: Object.freeze({ pluginId: 'ghost-sentinel-test@ghostsentinel', displayName: 'GhostSentinel test' }),
})
const hash = bytes => createHash('sha256').update(bytes).digest('hex')
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
const uuid = value => typeof value === 'string' && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value)
const fail = code => { throw Object.assign(new Error(code), { code }) }
const identity = info => JSON.stringify([info.dev, info.ino, info.size, info.mtimeMs, info.ctimeMs, info.uid, info.mode])
const optional = async path => { try { return await lstat(path) } catch (error) { if (error.code === 'ENOENT') return null; throw error } }
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)
const alive = pid => { try { process.kill(pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }

function pathsFor({ stateRoot, channel }) {
  if (!Object.hasOwn(GITHUB_CHANNELS, channel) || typeof stateRoot !== 'string' || !isAbsolute(stateRoot) || resolve(stateRoot) !== stateRoot || /[\x00-\x1f\x7f]/.test(stateRoot) || basename(stateRoot) !== channel) fail('github_channel_path_invalid')
  const dataDir = join(stateRoot, 'state')
  return { stateRoot, channel, ...GITHUB_CHANNELS[channel], dataDir, configPath: join(dataDir, 'installation.json'), receiptPath: join(stateRoot, 'github-installation.json'), operationPath: join(stateRoot, 'github-operation.json'), lockPath: join(stateRoot, '.github-prepare.lock'), maintenancePath: join(dataDir, 'maintenance.json') }
}
async function directory(path, create = false) {
  let parent = path
  while (!await optional(parent)) parent = dirname(parent)
  if (await realpath(parent) !== parent) fail('github_symlink_path_rejected')
  if (create) await mkdir(path, { recursive: true, mode: 0o700 })
  const info = await optional(path)
  if (!info) return null
  if (!info.isDirectory() || info.isSymbolicLink() || info.uid !== process.getuid() || (info.mode & 0o7777) !== 0o700 || await realpath(path) !== path) fail('github_private_directory_required')
  return info
}
async function record(path, limit = 65536) {
  const before = await optional(path)
  if (!before) return null
  if (!before.isFile() || before.uid !== process.getuid() || (before.mode & 0o7777) !== 0o600 || before.size > limit || await realpath(path) !== path) fail('github_private_record_required')
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const bytes = Buffer.alloc(before.size + 1), { bytesRead } = await file.read(bytes, 0, bytes.length, 0)
    if (bytesRead !== before.size || identity(await file.stat()) !== identity(before) || identity(await lstat(path)) !== identity(before)) fail('github_record_changed')
    const kept = bytes.subarray(0, bytesRead)
    return { value: JSON.parse(kept), bytes: kept, identity: identity(before), digest: hash(kept) }
  } finally { await file.close() }
}
async function save(path, value, exclusive = false) {
  const temporary = path + '.' + randomUUID() + '.tmp'; let primary
  try {
    await writeFile(temporary, JSON.stringify(value, null, 2) + '\n', { mode: 0o600, flag: 'wx' })
    if (exclusive) await link(temporary, path)
    else await rename(temporary, path)
  } catch (error) { primary = error; throw error }
  finally {
    try { await rm(temporary, { force: true }) }
    catch (cleanup) { if (primary) throw new AggregateError([primary, cleanup], 'GitHub record cleanup failed', { cause: primary }); throw cleanup }
  }
}
async function removeRecord(path, expected) {
  const current = await record(path)
  if (!current || !same(current.value, expected)) fail('github_record_ownership_changed')
  await rm(path)
}
async function locked(paths, io, action) {
  const owner = { format: 1, kind: 'ghost-sentinel-github-prepare-lock/1', channel: paths.channel, stateRoot: paths.stateRoot, pid: process.pid, nonce: randomUUID() }, deadline = Date.now() + (io.lockTimeoutMs ?? 10000)
  while (true) {
    try { await save(paths.lockPath, owner, true); break }
    catch (error) { if (error.code !== 'EEXIST') throw error }
    let current
    try { current = await record(paths.lockPath) }
    catch (error) { if (!['ENOENT', 'github_record_changed'].includes(error.code)) throw error }
    const value = current?.value
    if (value && (Object.keys(value).length !== 6 || value.format !== 1 || value.kind !== owner.kind || value.channel !== paths.channel || value.stateRoot !== paths.stateRoot || !Number.isSafeInteger(value.pid) || value.pid < 1 || !uuid(value.nonce))) fail('github_prepare_lock_unconfirmed')
    if (!value) { if (Date.now() >= deadline) fail('github_prepare_busy'); await delay(25); continue }
    if (!alive(value.pid)) { await removeRecord(paths.lockPath, value); continue }
    if (Date.now() >= deadline) fail('github_prepare_busy')
    await delay(25)
  }
  let primary
  try { return await action() }
  catch (error) { primary = error; throw error }
  finally {
    try { await removeRecord(paths.lockPath, owner) }
    catch (cleanup) { if (primary) throw new AggregateError([primary, cleanup], 'GitHub preparation cleanup failed', { cause: primary }); throw cleanup }
  }
}
const configIdentity = config => hash(JSON.stringify(['installationId', 'pluginId', 'token', 'dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket'].map(key => config[key]).concat(config.initialEnrollmentBaseline?.id ?? null)))
function checkedInitialOperation(operation, paths, packet) {
  if (Object.keys(operation).length !== 9 || operation.format !== 1 || operation.kind !== 'ghost-sentinel-github-operation/1' || operation.phase !== 'initial' || !uuid(operation.operationId) || operation.channel !== paths.channel || operation.stateRoot !== paths.stateRoot || operation.archiveSHA256 !== packet.archiveSHA256 || operation.releaseId !== packet.releaseId || operation.planned?.pluginId !== paths.pluginId || operation.planned?.dataDir !== paths.dataDir || operation.planned?.releaseId !== packet.releaseId) fail('github_operation_requires_review')
}
function checkedReceipt(value, config, paths) {
  if (!value || value.format !== 1 || value.kind !== 'ghost-sentinel-github-installation/1' || value.channel !== paths.channel || value.stateRoot !== paths.stateRoot || value.pluginId !== paths.pluginId || value.installationId !== config.installationId || value.configIdentity !== configIdentity(config) || value.activeReleaseId !== config.releaseId || !digest(value.activeArchiveSHA256) || !Array.isArray(value.releases) || value.releases.length > 128) fail('github_installation_unconfirmed')
  for (const release of value.releases) if (!digest(release.archiveSHA256) || !digest(release.releaseId) || release.releaseRoot !== join(paths.stateRoot, 'payloads', release.archiveSHA256) || release.notificationBundlePath !== null && release.notificationBundlePath !== join(release.releaseRoot, 'notification/GhostSentinelNotifications.app')) fail('github_release_ownership_unconfirmed')
  const current = value.releases.filter(item => item.archiveSHA256 === value.activeArchiveSHA256)
  if (current.length !== 1 || current[0].releaseId !== value.activeReleaseId || (config.notificationBundlePath ?? null) !== current[0].notificationBundlePath) fail('github_active_release_unconfirmed')
  return value
}
async function checkedConfig(paths) {
  const stored = await record(paths.configPath, 16384)
  if (!stored) return null
  const config = await loadInstallationConfig(paths.configPath)
  if (!same(stored.value, config) || config.dataDir !== paths.dataDir || config.pluginId !== paths.pluginId || !digest(config.releaseId) || config.metadataSocket !== join(paths.stateRoot, 'unused.sock')) fail('github_configuration_unconfirmed')
  return config
}
const ownerFields = ['protocol', 'installationId', 'version', 'pid', 'runId', 'ownership', 'releaseId']
async function ownerRecords(paths, releaseId, installationId) {
  for (let attempt = 0; ; attempt++) {
    try {
      const lockPath = join(paths.dataDir, 'runtime.lock'), lock = await directory(lockPath), owner = (await record(join(lockPath, 'owner.json')))?.value, published = (await record(join(paths.dataDir, 'runtime.json')))?.value
      if (lock && (await readdir(lockPath)).some(name => name !== 'owner.json')) fail('github_owner_unconfirmed')
      if (owner && (owner.protocol !== HELPER_PROTOCOL || owner.version !== HELPER_VERSION || owner.ownership !== 'plugin' || owner.releaseId !== releaseId || owner.installationId !== installationId || !uuid(owner.runId) || !Number.isSafeInteger(owner.pid) || owner.pid < 1)) fail('github_owner_unconfirmed')
      if (published && (!owner || ownerFields.some(key => published[key] !== owner[key]))) fail('github_owner_unconfirmed')
      return { lockPath, lock, owner, published }
    } catch (error) { if (error.code !== 'ENOENT' || attempt === 2) throw error }
  }
}
function nextReceipt(paths, config, packet, previous) {
  const entry = { archiveSHA256: packet.archiveSHA256, releaseId: packet.releaseId, releaseRoot: packet.releaseRoot, notificationBundlePath: packet.notificationBundlePath }
  const known = previous?.releases.find(value => value.archiveSHA256 === packet.archiveSHA256)
  if (known && !same(known, entry)) fail('github_archive_identity_changed')
  const releases = known ? previous.releases : [...(previous?.releases ?? []), entry]
  if (releases.length > 128) fail('github_release_history_full')
  return { format: 1, kind: 'ghost-sentinel-github-installation/1', channel: paths.channel, stateRoot: paths.stateRoot, pluginId: paths.pluginId, installationId: config.installationId, configIdentity: configIdentity(config), activeReleaseId: packet.releaseId, activeArchiveSHA256: packet.archiveSHA256, releases }
}
async function upgrade(paths, config, receipt, packet, pending, io) {
  let operation = pending
  if (!operation) {
    if (await record(paths.maintenancePath)) fail('github_maintenance_requires_review')
    const current = await ownerRecords(paths, config.releaseId, config.installationId)
    if (current.owner && alive(current.owner.pid) && current.published) {
      const authenticated = await (io.resolveOwner ?? resolvePluginOwner)({ configPath: paths.configPath })
      if (!authenticated || ownerFields.some(key => authenticated[key] !== current.owner[key])) fail('github_owner_unconfirmed')
    }
    operation = { format: 1, kind: 'ghost-sentinel-github-operation/1', phase: 'upgrade', operationId: randomUUID(), channel: paths.channel, stateRoot: paths.stateRoot, archiveSHA256: packet.archiveSHA256, releaseId: packet.releaseId, previousReceipt: receipt, owner: current.owner ?? null }
    await save(paths.operationPath, operation, true)
  }
  if (Object.keys(operation).length !== 10 || operation.format !== 1 || operation.kind !== 'ghost-sentinel-github-operation/1' || operation.phase !== 'upgrade' || !uuid(operation.operationId) || operation.channel !== paths.channel || operation.stateRoot !== paths.stateRoot || operation.archiveSHA256 !== packet.archiveSHA256 || operation.releaseId !== packet.releaseId) fail('github_operation_requires_review')
  const previous = operation.previousReceipt
  const oldEntry = previous?.releases?.find(value => value.archiveSHA256 === previous.activeArchiveSHA256)
  const oldConfig = { ...config, releaseId: previous?.activeReleaseId }
  if (oldEntry?.notificationBundlePath) oldConfig.notificationBundlePath = oldEntry.notificationBundlePath
  else delete oldConfig.notificationBundlePath
  checkedReceipt(previous, oldConfig, paths)
  if (![previous.activeReleaseId, packet.releaseId].includes(config.releaseId) || !same(receipt, previous) && !same(receipt, nextReceipt(paths, config, packet, previous))) fail('github_upgrade_configuration_changed')
  const marker = { format: 1, installationId: config.installationId, operationId: operation.operationId }, maintenance = await record(paths.maintenancePath)
  if (maintenance && !same(maintenance.value, marker)) fail('github_maintenance_requires_review')
  if (!maintenance) await save(paths.maintenancePath, marker, true)
  const deadline = Date.now() + (io.stopTimeoutMs ?? 10000)
  while (true) {
    const current = await ownerRecords(paths, previous.activeReleaseId, config.installationId)
    if (current.owner) {
      if (operation.owner && ownerFields.some(key => operation.owner[key] !== current.owner[key])) fail('github_owner_changed_during_upgrade')
      if (!operation.owner) { operation.owner = current.owner; await save(paths.operationPath, operation) }
      if (!alive(current.owner.pid)) {
        if (current.published) await removeRecord(join(paths.dataDir, 'runtime.json'), current.published)
        await removeRecord(join(current.lockPath, 'owner.json'), current.owner)
        await rmdir(current.lockPath)
        continue
      }
    }
    if (!current.lock && !current.published && (!operation.owner || !alive(operation.owner.pid))) break
    if (Date.now() >= deadline) fail('github_owner_stop_unconfirmed')
    await delay(25)
  }
  config = await checkedConfig(paths)
  if (configIdentity(config) !== previous.configIdentity || ![previous.activeReleaseId, packet.releaseId].includes(config.releaseId)) fail('github_upgrade_configuration_changed')
  const updated = { ...config, releaseId: packet.releaseId }
  if (packet.notificationBundlePath) updated.notificationBundlePath = packet.notificationBundlePath
  else delete updated.notificationBundlePath
  await save(paths.configPath, updated)
  await save(paths.receiptPath, nextReceipt(paths, updated, packet, previous))
  await removeRecord(paths.maintenancePath, marker)
  await removeRecord(paths.operationPath, operation)
}
async function releaseAt(paths, options, io) {
  const { releaseRoot, archiveSHA256 } = options
  if (!digest(archiveSHA256) || releaseRoot !== join(paths.stateRoot, 'payloads', archiveSHA256)) fail('github_release_path_invalid')
  const rootInfo = await directory(releaseRoot), manifest = await record(join(releaseRoot, 'plugin-package.json'), 8388608), node = await lstat(join(releaseRoot, 'runtime/node'))
  if (!rootInfo || !manifest || !node.isFile() || node.isSymbolicLink() || node.uid !== process.getuid() || (node.mode & 0o7777) !== 0o700 || await realpath(join(releaseRoot, 'runtime/node')) !== join(releaseRoot, 'runtime/node')) fail('github_release_identity_unconfirmed')
  const readyPath = join(paths.stateRoot, 'ready', archiveSHA256 + '.json')
  const proof = { format: 1, kind: 'ghost-sentinel-github-release-ready/1', archiveSHA256, releaseRoot, releaseId: manifest.digest, rootIdentity: identity(rootInfo), manifestIdentity: manifest.identity, nodeIdentity: identity(node) }
  const saved = await record(readyPath)
  if (saved && (saved.value.kind !== proof.kind || saved.value.archiveSHA256 !== archiveSHA256 || saved.value.releaseRoot !== releaseRoot)) fail('github_release_cache_unconfirmed')
  if (!saved || !same(saved.value, proof)) {
    const inspected = await (io.inspectPackage ?? inspectPluginPackage)(releaseRoot, { digest: manifest.digest })
    if (inspected.root !== releaseRoot || inspected.digest !== manifest.digest || !same(inspected.manifest, manifest.value) || inspected.nodePath !== join(releaseRoot, 'runtime/node') || identity(await lstat(releaseRoot)) !== proof.rootIdentity || identity(await lstat(join(releaseRoot, 'plugin-package.json'))) !== proof.manifestIdentity || identity(await lstat(inspected.nodePath)) !== proof.nodeIdentity) fail('github_release_changed')
    await directory(join(paths.stateRoot, 'ready'), true); await save(readyPath, proof)
  }
  if (manifest.value.runtimeVersion !== HELPER_VERSION || ![null, 'notification/GhostSentinelNotifications.app'].includes(manifest.value.notificationPath)) fail('github_runtime_unsupported')
  return { archiveSHA256, releaseId: manifest.digest, releaseRoot, notificationBundlePath: manifest.value.notificationPath ? join(releaseRoot, manifest.value.notificationPath) : null, nodePath: join(releaseRoot, 'runtime/node'), ownerPath: join(releaseRoot, 'chatgpt/github-plugin-main.mjs') }
}

export async function inspectGitHubInstallation(options) {
  const paths = pathsFor(options)
  if (!await directory(paths.stateRoot)) return null
  await directory(paths.dataDir)
  const config = await checkedConfig(paths), receipt = await record(paths.receiptPath)
  if (!config && !receipt) return null
  checkedReceipt(receipt?.value, config ?? {}, paths)
  return { configPath: paths.configPath, config, receipt: receipt.value, stateRoot: paths.stateRoot, channel: paths.channel, pluginId: paths.pluginId }
}

export async function prepareGitHubInstallation(options, io = {}) {
  const paths = pathsFor(options)
  await directory(paths.stateRoot, true)
  return locked(paths, io, async () => {
    const packet = await releaseAt(paths, options, io)
    await directory(paths.dataDir, true)
    let config = await checkedConfig(paths), receipt = (await record(paths.receiptPath))?.value
    const pending = (await record(paths.operationPath))?.value
    if (receipt) {
      if (pending?.phase !== 'upgrade') checkedReceipt(receipt, config ?? {}, paths)
      if (receipt.activeArchiveSHA256 !== packet.archiveSHA256 && receipt.releases.some(value => value.archiveSHA256 === packet.archiveSHA256)) fail('github_release_superseded')
      if (pending?.phase === 'upgrade' || receipt.activeReleaseId !== packet.releaseId || receipt.activeArchiveSHA256 !== packet.archiveSHA256) await upgrade(paths, config, receipt, packet, pending, io)
      else {
        if (await record(paths.maintenancePath)) fail('github_maintenance_requires_review')
        if (pending) {
          checkedInitialOperation(pending, paths, packet)
          if (configIdentity(pending.planned) !== receipt.configIdentity) fail('github_operation_requires_review')
          await removeRecord(paths.operationPath, pending)
        }
      }
    } else {
      if (await record(paths.maintenancePath)) fail('github_maintenance_requires_review')
      if (config && !pending) fail('github_foreign_configuration')
      let operation = pending
      if (!operation) {
        const planned = { format: 1, installationId: randomUUID(), initialEnrollmentBaseline: { id: randomUUID(), claimable: true }, dataDir: paths.dataDir, coreDataDir: resolve(options.coreDataDir ?? join(homedir(), '.ghost-agent-sentinel')), sessionsRoot: resolve(options.sessionsRoot ?? join(homedir(), '.codex/sessions')), metadataSocket: join(paths.stateRoot, 'unused.sock'), pluginId: paths.pluginId, token: randomBytes(32).toString('hex'), releaseId: packet.releaseId, ...(packet.notificationBundlePath ? { notificationBundlePath: packet.notificationBundlePath } : {}) }
        operation = { format: 1, kind: 'ghost-sentinel-github-operation/1', phase: 'initial', operationId: randomUUID(), channel: paths.channel, stateRoot: paths.stateRoot, archiveSHA256: packet.archiveSHA256, releaseId: packet.releaseId, planned }
        await save(paths.operationPath, operation, true)
      }
      checkedInitialOperation(operation, paths, packet)
      if (!config) { await save(paths.configPath, operation.planned, true); config = await checkedConfig(paths) }
      if (configIdentity(config) !== configIdentity(operation.planned) || config.releaseId !== packet.releaseId) fail('github_configuration_unconfirmed')
      await prepareInstallation({ dataDir: paths.dataDir, coreDataDir: config.coreDataDir, sessionsRoot: config.sessionsRoot, metadataSocket: config.metadataSocket, pluginId: paths.pluginId, releaseId: packet.releaseId, ...(packet.notificationBundlePath ? { notificationBundlePath: packet.notificationBundlePath } : {}) })
      receipt = nextReceipt(paths, config, packet)
      await save(paths.receiptPath, receipt); await removeRecord(paths.operationPath, operation)
    }
    return { configPath: paths.configPath, ...packet, channel: paths.channel, pluginId: paths.pluginId, displayName: paths.displayName }
  })
}
