import { open, lstat, realpath, readdir, mkdir, writeFile, rename, rm, cp, rmdir } from 'node:fs/promises'
import { constants } from 'node:fs'
import { join, resolve, dirname, basename, isAbsolute, sep } from 'node:path'
import { createHash, randomUUID } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { setTimeout as delay } from 'node:timers/promises'
import { prepareInstallation, resolvePluginOwner, HELPER_PROTOCOL, HELPER_VERSION } from './runtime.mjs'
import { connectCore } from '../src/core-client.mjs'

const kind = 'ghost-sentinel-plugin-installation/1', hash = value => createHash('sha256').update(value).digest('hex')
const uuid = value => typeof value === 'string' && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value)
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
const fail = code => { throw Object.assign(new Error(code), { code }) }
const inside = (path, root) => path === root || path.startsWith(root + sep)
const temporaryCursor = key => /^cursors\/[a-f0-9]{64}\.json\.[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}\.tmp$/.test(key)
const identity = info => JSON.stringify([info.dev, info.ino, info.size, info.mtimeMs, info.ctimeMs, info.uid, info.mode])
const directoryIdentity = info => JSON.stringify([info.dev, info.ino, info.uid, info.mode])
const optional = async path => { try { return await lstat(path) } catch (error) { if (error.code === 'ENOENT') return null; throw error } }
async function directoryChain(path) {
  const entries = { [sep]: directoryIdentity(await lstat(sep)) }; let current = sep
  for (const part of path.split(sep).filter(Boolean)) {
    current = join(current, part)
    const info = await lstat(current)
    if (!info.isDirectory() || info.isSymbolicLink()) fail('installation_directory_changed_during_removal')
    entries[current] = directoryIdentity(info)
  }
  return entries
}
async function verifyDirectoryChain(entries, path, missingWithin = null) {
  let current = sep
  for (const part of path.split(sep).filter(Boolean)) {
    current = join(current, part)
    const info = await optional(current)
    if (!info && missingWithin && current !== missingWithin && inside(current, missingWithin)) return false
    if (!info?.isDirectory() || info.isSymbolicLink() || entries[current] !== directoryIdentity(info)) fail('installation_directory_changed_during_removal')
  }
  return true
}
async function managedDirectory(paths, path) {
  if (!inside(path, paths.installRoot)) fail('installation_directory_changed_during_removal')
  await verifyDirectoryChain(paths.directoryAnchors, paths.installRoot)
  let current = paths.installRoot
  for (const part of path.slice(paths.installRoot.length).split(sep).filter(Boolean)) {
    const parent = current; current = join(current, part)
    let info = await optional(current)
    if (!info) {
      if (paths.directoryAnchors[current]) fail('installation_directory_changed_during_removal')
      await verifyDirectoryChain(paths.directoryAnchors, parent); await mkdir(current, { mode: 0o700 }); info = await lstat(current)
      paths.directoryAnchors[current] = directoryIdentity(info)
    }
    if (!info.isDirectory() || info.isSymbolicLink() || info.uid !== process.getuid() || info.mode & 0o077 || paths.directoryAnchors[current] !== directoryIdentity(info)) fail('installation_directory_changed_during_removal')
  }
}
async function canonical(path) {
  if (typeof path !== 'string' || !isAbsolute(path) || /[\x00-\x1f\x7f]/.test(path) || resolve(path) === sep) fail('explicit_installation_path_required')
  const missing = []; let parent = resolve(path)
  while (!await optional(parent)) { missing.push(basename(parent)); parent = dirname(parent) }
  return join(await realpath(parent), ...missing.reverse())
}
async function rootAuthority(target) {
  let anchor = target
  while (!await optional(anchor)) anchor = dirname(anchor)
  return { target, anchor, directories: await directoryChain(anchor) }
}
async function createApprovedRoot(paths, authority) {
  if (authority.target !== paths.installRoot) fail('installation_preview_changed')
  paths.directoryAnchors = { ...authority.directories }
  let current = authority.anchor
  await verifyDirectoryChain(paths.directoryAnchors, current)
  for (const part of paths.installRoot.slice(current.length).split(sep).filter(Boolean)) {
    await verifyDirectoryChain(paths.directoryAnchors, current); current = join(current, part)
    await mkdir(current, { mode: 0o700 }); paths.directoryAnchors[current] = directoryIdentity(await lstat(current))
  }
  await verifyDirectoryChain(paths.directoryAnchors, paths.installRoot)
}
async function record(path, limit = 1048576) {
  let parent = sep
  const parents = []
  for (const part of dirname(path).split(sep).filter(Boolean)) {
    parent = join(parent, part)
    const info = await optional(parent)
    if (!info) return null
    if (info.isSymbolicLink() || !info.isDirectory()) fail('installation_symlink_path_rejected')
    parents.push({ path: parent, dev: info.dev, ino: info.ino })
  }
  const before = await optional(path)
  if (!before) return null
  if (!before.isFile() || before.size > limit || before.mode & 0o077 || typeof process.getuid === 'function' && before.uid !== process.getuid()) fail('untrusted_installation_record')
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const buffer = Buffer.alloc(before.size + 1), { bytesRead } = await file.read(buffer, 0, buffer.length, 0), bytes = buffer.subarray(0, bytesRead)
    if (bytes.length !== before.size || identity(await file.stat()) !== identity(before) || identity(await lstat(path)) !== identity(before)) fail('installation_record_changed')
    for (const parent of parents) { const current = await lstat(parent.path); if (!current.isDirectory() || current.dev !== parent.dev || current.ino !== parent.ino) fail('installation_symlink_path_rejected') }
    let value
    try { value = JSON.parse(bytes) } catch { fail('invalid_installation_record') }
    return { value, digest: hash(bytes) }
  } finally { await file.close() }
}
function encodeRecord(value, limit = 1048576) {
  const bytes = Buffer.from(JSON.stringify(value, null, 2) + '\n')
  if (bytes.length > limit) fail('installation_record_over_budget')
  return bytes
}
async function saveBytes(path, bytes) {
  const temporary = path + '.' + randomUUID() + '.tmp'
  const chain = await directoryChain(dirname(path)); let primary = null
  try {
    await verifyDirectoryChain(chain, dirname(path)); await writeFile(temporary, bytes, { mode: 0o600, flag: 'wx' }); const written = await lstat(temporary)
    await verifyDirectoryChain(chain, dirname(path)); await rename(temporary, path); const published = await lstat(path)
    if (['dev', 'ino', 'size', 'mtimeMs', 'uid', 'mode'].some(key => written[key] !== published[key])) fail('installation_record_changed')
    return { identity: identity(published), digest: hash(bytes) }
  }
  catch (error) { primary = error; throw error }
  finally { try { await verifyDirectoryChain(chain, dirname(path)); await rm(temporary, { force: true }) } catch (error) { if (primary) primary.temporaryCleanupErrorCode = safeCode(error); else throw error } }
}
const save = (path, value) => saveBytes(path, encodeRecord(value))
const matchesLock = (proof, owner, info, directory) => Boolean(proof && owner && info && proof.directory === directory && proof.digest === owner.digest && proof.identity === identity(info))
const configIdentity = value => hash(JSON.stringify(['installationId', 'token', 'dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket', 'pluginId'].map(key => value[key]).concat(value.initialEnrollmentBaseline?.id ?? null)))
async function packageAt(path, io, expected) {
  const inspect = io.inspectPackage ?? (await import('./plugin-package.mjs')).inspectPluginPackage
  return inspect(path, expected ? { digest: expected } : {})
}
function checkedHost(value, pluginId) {
  if (!value || value.pluginId !== pluginId || typeof value.installed !== 'boolean' || !['enabled', 'disabled', 'unknown'].includes(value.enabled) || typeof value.marketplaceRegistered !== 'boolean' || value.revision !== null && typeof value.revision !== 'string') fail('plugin_host_identity_unconfirmed')
  return { pluginId, installed: value.installed, enabled: value.enabled, revision: value.revision, pluginPath: value.pluginPath ?? null, marketplaceRegistered: value.marketplaceRegistered, ...(value.version ? { version: value.version } : {}) }
}

export async function pluginManagementDefaults(installRoot) {
  installRoot = await canonical(installRoot)
  const pending = (await record(join(installRoot, 'operation.json'), 8388608))?.value
  const config = (await record(join(installRoot, 'state/installation.json')))?.value ?? recoveryConfig(pending), receipt = (await record(join(installRoot, 'installation-receipt.json')))?.value ?? pending?.removal?.receipt
  return { installRoot, ...Object.fromEntries(['pluginId', 'coreDataDir', 'sessionsRoot', 'metadataSocket'].filter(key => config?.[key] !== undefined).map(key => [key, config[key]])), ...Object.fromEntries(['pluginName', 'marketplaceName'].filter(key => receipt?.[key] !== undefined).map(key => [key, receipt[key]])) }
}

export async function previewPluginChange(options, io = {}) {
  if (!io.host?.inspect || !['install', 'upgrade', 'uninstall'].includes(options?.operation)) fail('explicit_plugin_operation_and_host_required')
  const installRoot = await canonical(options.installRoot), stateDir = join(installRoot, 'state')
  const paths = { installRoot, stateDir, pluginDir: join(stateDir, 'plugin'), configPath: join(stateDir, 'installation.json'), receipt: join(installRoot, 'installation-receipt.json'), operation: join(installRoot, 'operation.json'), maintenance: join(stateDir, 'maintenance.json'), releases: join(installRoot, 'releases'), marketplacePath: join(stateDir, '.agents/plugins/marketplace.json') }
  const [configuration, saved, pending] = await Promise.all([record(paths.configPath), record(paths.receipt), record(paths.operation, 8388608)])
  const transaction = pending?.value, recoveringRemoval = options.operation === 'uninstall' && transaction?.operationId === options.resumeOperationId && Boolean(transaction?.removal)
  const config = configuration?.value ?? (recoveringRemoval ? recoveryConfig(transaction) : null), receipt = saved?.value ?? (recoveringRemoval ? transaction.removal.receipt : null)
  const pluginId = config?.pluginId ?? options.pluginId
  if (typeof pluginId !== 'string' || !pluginId || typeof options.pluginName !== 'string' || !options.pluginName || typeof options.marketplaceName !== 'string' || !options.marketplaceName) fail('explicit_plugin_identity_required')
  const request = { operation: options.operation, installRoot, pluginId, pluginName: options.pluginName, marketplaceName: options.marketplaceName, coreDataDir: await canonical(config?.coreDataDir ?? options.coreDataDir), sessionsRoot: await canonical(config?.sessionsRoot ?? options.sessionsRoot), metadataSocket: await canonical(config?.metadataSocket ?? options.metadataSocket), ...(options.resumeOperationId ? { resumeOperationId: options.resumeOperationId } : {}) }
  if ([request.coreDataDir, request.sessionsRoot].some(path => inside(path, installRoot))) fail('installation_scope_overlaps_observation')
  if (config && (config.format !== 1 || !uuid(config.installationId) || !digest(config.token) || config.dataDir !== stateDir || config.pluginId !== pluginId)) fail('installation_configuration_unconfirmed')
  const host = checkedHost(await io.host.inspect(), pluginId), blockers = []
  const rootInfo = await optional(installRoot)
  if (rootInfo && (!rootInfo.isDirectory() || rootInfo.mode & 0o077 || typeof process.getuid === 'function' && rootInfo.uid !== process.getuid())) fail('private_installation_directory_required')
  const authority = await rootAuthority(installRoot)
  const known = new Set(['state', 'releases', 'installation-receipt.json', 'operation.json', 'transactions', '.operation.lock'])
  const unknown = rootInfo ? (await readdir(installRoot)).filter(name => !known.has(name)) : []
  if (unknown.length || config && !receipt && !transaction) blockers.push({ code: 'unmanaged_installation_files', entries: unknown })
  if (receipt && (receipt.format !== 1 || receipt.kind !== kind || receipt.installationId !== config?.installationId || receipt.pluginId !== pluginId || receipt.installRoot !== installRoot || !digest(receipt.activeDigest) || receipt.configIdentity !== configIdentity(config) || receipt.pluginName !== request.pluginName || receipt.marketplaceName !== request.marketplaceName)) fail('installation_receipt_unconfirmed')
  if (host.installed && host.pluginPath !== paths.pluginDir) blockers.push({ code: 'plugin_source_path_unconfirmed' })
  if (options.operation === 'install' && receipt && !options.resumeOperationId) blockers.push({ code: 'plugin_installation_already_exists' })
  if (options.operation !== 'install' && !receipt && !(options.operation === 'uninstall' && transaction?.operationId === options.resumeOperationId)) blockers.push({ code: 'managed_plugin_installation_required' })
  if (host.installed && host.enabled === 'unknown') blockers.push({ code: 'plugin_enablement_unconfirmed' })
  if (transaction && transaction.operationId !== options.resumeOperationId) blockers.push({ code: 'unfinished_plugin_operation', operationId: transaction.operationId, phase: transaction.phase })
  if (options.resumeOperationId && (!transaction || !uuid(options.resumeOperationId) || transaction.operation !== options.operation && options.operation !== 'uninstall' || transaction.installationId !== config?.installationId)) fail('plugin_resume_identity_unconfirmed')
  const owner = await record(join(stateDir, 'runtime.lock/owner.json'))
  if (owner && owner.value.ownership !== 'plugin') blockers.push({ code: 'non_plugin_owner_requires_review' })
  const candidate = options.operation === 'uninstall' ? null : await packageAt(await canonical(options.packageRoot ?? transaction?.packageRoot), io, transaction?.candidateDigest)
  if (candidate && candidate.manifest.runtimeVersion !== HELPER_VERSION) fail('plugin_runtime_protocol_unsupported')
  if (candidate) request.packageRoot = candidate.root
  const descriptors = await Promise.all(descriptorNames.map(path => record(join(stateDir, path))))
  if (descriptorNames.some((name, index) => !knownDescriptor(descriptors[index], name, receipt, transaction))) blockers.push({ code: 'plugin_descriptor_concurrently_changed' })
  const inventory = await previewInventory(paths, config, receipt, transaction)
  blockers.push(...inventory.blockers)
  const fingerprints = { request, paths, config: config ? configIdentity(config) : null, receipt: saved?.digest ?? null, operation: pending?.digest ?? null, descriptors: descriptors.map(value => value?.digest ?? null), host, candidateDigest: candidate?.digest ?? null, inventory: inventory.fingerprint }, stateDigest = hash(JSON.stringify(fingerprints))
  return { format: 1, request, paths, rootAuthority: authority, stateDigest, directoryClaims: inventory.directories, package: candidate ? { root: candidate.root, digest: candidate.digest, version: candidate.manifest.version, runtimeVersion: candidate.manifest.runtimeVersion } : null, host, installationId: config?.installationId ?? null, activeDigest: receipt?.activeDigest ?? null, blockers, canApply: blockers.length === 0, readSetDigest: hash(JSON.stringify({ stateDigest, authority })), activationExpectation: options.operation === 'uninstall' ? 'uninstalled-with-owned-processes-gone' : 'enabled-with-authenticated-release-owner' }
}

export async function applyPluginChange(plan, io = {}) {
  if (!plan?.canApply) fail('installation_plan_blocked')
  const fresh = await previewPluginChange(plan.request, io)
  if (!fresh.canApply || fresh.readSetDigest !== plan.readSetDigest) fail('installation_preview_changed')
  const { paths, request } = fresh, operationId = request.resumeOperationId ?? randomUUID(), lock = join(paths.installRoot, '.operation.lock')
  await createApprovedRoot(paths, fresh.rootAuthority)
  Object.assign(paths.directoryAnchors, fresh.directoryClaims)
  try { await mkdir(lock, { mode: 0o700 }) }
  catch (error) {
    if (error.code !== 'EEXIST') throw error
    const lockInfo = await lstat(lock)
    if (!lockInfo.isDirectory() || lockInfo.isSymbolicLink() || lockInfo.mode & 0o077 || lockInfo.uid !== process.getuid()) fail('plugin_operation_locked')
    const chain = { ...paths.directoryAnchors, [lock]: directoryIdentity(lockInfo) }
    await verifyDirectoryChain(chain, lock)
    const owner = await record(join(lock, 'owner.json'))
    if (!request.resumeOperationId || owner?.value.operationId !== operationId || !Number.isSafeInteger(owner.value.pid)) fail('plugin_operation_locked')
    if (Object.keys(owner.value).some(key => !['operationId', 'pid', 'released'].includes(key)) || owner.value.released !== undefined && owner.value.released !== true) fail('plugin_operation_locked')
    const pending = (await record(paths.operation, 8388608))?.value, proof = owner.value.released ? pending?.releasedLock : pending?.lockProof
    if (pending?.kind !== kind || pending.operationId !== operationId || !matchesLock(proof, owner, await lstat(join(lock, 'owner.json')), directoryIdentity(lockInfo)) || (await readdir(lock)).some(name => name !== 'owner.json')) fail('plugin_operation_locked')
    if (!(owner.value.pid === process.pid && owner.value.released === true)) { try { process.kill(owner.value.pid, 0); fail('plugin_operation_locked') } catch (error) { if (error.code !== 'ESRCH') throw error } }
    await verifyDirectoryChain(chain, lock); await rm(join(lock, 'owner.json')); await verifyDirectoryChain(chain, lock); await rmdir(lock); await mkdir(lock, { mode: 0o700 })
  }
  paths.lockDirectoryIdentity = directoryIdentity(await lstat(lock))
  await verifyDirectoryChain({ ...paths.directoryAnchors, [lock]: paths.lockDirectoryIdentity }, lock)
  let transaction, config, result, primaryError = null
  try {
    const owner = await save(join(lock, 'owner.json'), { operationId, pid: process.pid })
    paths.lockOwnerIdentity = owner.identity; paths.lockOwnerDigest = owner.digest
    const rechecked = await previewPluginChange(plan.request, io)
    if (!rechecked.canApply || rechecked.stateDigest !== fresh.stateDigest) fail('installation_preview_changed')
    await verifyDirectoryChain(paths.directoryAnchors, paths.installRoot)
    await managedDirectory(paths, join(paths.installRoot, 'transactions'))
    if (request.operation === 'install' && !fresh.installationId) { await managedDirectory(paths, join(paths.pluginDir, '.codex-plugin')); await prepareInstallation({ dataDir: paths.stateDir, coreDataDir: request.coreDataDir, sessionsRoot: request.sessionsRoot, metadataSocket: request.metadataSocket, pluginId: request.pluginId }) }
    config = (await record(paths.configPath))?.value ?? recoveryConfig((await record(paths.operation, 8388608))?.value)
    const prior = await record(paths.receipt)
    transaction = request.resumeOperationId ? (await record(paths.operation, 8388608)).value : { format: 1, kind, operationId, operation: request.operation, installationId: config.installationId, configIdentity: configIdentity(config), packageRoot: fresh.package?.root ?? null, candidateDigest: fresh.package?.digest ?? null, phase: 'prepared', previous: { receipt: prior?.value ?? null, files: await descriptorRecords(paths), notificationBundlePath: config.notificationBundlePath ?? null }, startedAt: Date.now() }
    if (transaction.kind !== kind || transaction.configIdentity !== configIdentity(config)) fail('plugin_transaction_identity_unconfirmed')
    transaction.lockProof = { identity: paths.lockOwnerIdentity, digest: paths.lockOwnerDigest, directory: paths.lockDirectoryIdentity }
    delete transaction.releasedLock
    transaction.hostRevision = fresh.host.revision
    if (request.operation === 'uninstall') transaction.operation = 'uninstall'
    await journal(paths, transaction, 'prepared')
    if (request.operation === 'uninstall') result = await removeInstallation(fresh, config, transaction, io)
    else result = await installRelease(fresh, config, transaction, io)
    if (result.status === 'complete' && request.operation !== 'uninstall') {
      await journal(paths, transaction, 'complete'); await rm(paths.operation)
    }
    return result
  } catch (error) {
    primaryError = error
    if (!transaction) throw error
    transaction.errorCode = safeCode(error)
    let rollback = null
    if (request.operation === 'upgrade' && transaction.previous.receipt && transaction.phase !== 'complete') {
      try { rollback = await restoreRelease(fresh, config, transaction, io) }
      catch (error) { rollback = { status: 'incomplete', errorCode: safeCode(error) } }
    } else if (!transaction.removal) {
      try { transaction.failureCleanup = await stopInstallation(fresh, (await record(paths.configPath)).value, transaction, io) }
      catch (error) { transaction.failureCleanup = { status: 'incomplete', errorCode: safeCode(error) } }
    }
    let recoveryJournalErrorCode = null
    try {
      await journal(paths, transaction, rollback?.status === 'complete' ? 'rolled-back' : 'recovery-required')
      if (rollback?.status === 'complete') await rm(paths.operation)
    } catch (error) { recoveryJournalErrorCode = safeCode(error) }
    let recoveryConfigurationRetained = false
    try { const persisted = (await record(paths.operation, 8388608))?.value; recoveryConfigurationRetained = persisted?.operationId === operationId && Boolean(recoveryConfig(persisted)) } catch {}
    result = { status: error.code === 'plugin_trust_required' || error.code === 'plugin_activation_not_observed' ? 'needs-user-action' : 'recovery-required', operationId, errorCode: transaction.errorCode, rollback, ...(transaction.failureCleanup ? { failureCleanup: transaction.failureCleanup } : {}), ...(recoveryJournalErrorCode ? { recoveryJournalErrorCode } : {}), ...(error.temporaryCleanupErrorCode ? { temporaryCleanupErrorCode: error.temporaryCleanupErrorCode } : {}), ...(error.stageCleanupErrorCode ? { stageCleanupErrorCode: error.stageCleanupErrorCode } : {}), configurationRetained: Boolean(await optional(paths.configPath)), recoveryConfigurationRetained }
    return result
  } finally {
    try {
      const chain = { ...paths.directoryAnchors, [lock]: paths.lockDirectoryIdentity }, path = join(lock, 'owner.json')
      await verifyDirectoryChain(chain, lock)
      const owner = await record(path)
      if (!owner && paths.lockOwnerIdentity === undefined) { if ((await readdir(lock)).length) fail('plugin_operation_lock_ownership_changed') }
      else {
        if (!matchesLock({ identity: paths.lockOwnerIdentity, digest: paths.lockOwnerDigest, directory: paths.lockDirectoryIdentity }, owner, await lstat(path), paths.lockDirectoryIdentity)) fail('plugin_operation_lock_ownership_changed')
        await verifyDirectoryChain(chain, lock); await rm(path); paths.lockOwnerRemoved = true
      }
      await verifyDirectoryChain(chain, lock); await rmdir(lock)
      if (result?.status === 'complete' && request.operation === 'uninstall') { await verifyDirectoryChain(paths.directoryAnchors, paths.installRoot); await rmdir(paths.installRoot) }
    } catch (error) {
      if (!result) { if (primaryError) primaryError.lockCleanupErrorCode = safeCode(error); else throw error }
      else {
        result.status = 'recovery-required'; result.errorCode ??= 'plugin_lock_cleanup_failed'; result.lockCleanupErrorCode = safeCode(error)
        if (transaction) {
          try { await journal(paths, transaction, 'recovery-required') } catch (error) { result.lockRecoveryJournalErrorCode = safeCode(error) }
          try {
            if (await optional(lock)) {
              await verifyDirectoryChain({ ...paths.directoryAnchors, [lock]: paths.lockDirectoryIdentity }, lock)
              const names = await readdir(lock), owner = await record(join(lock, 'owner.json'))
              const stillOwned = matchesLock({ identity: paths.lockOwnerIdentity, digest: paths.lockOwnerDigest, directory: paths.lockDirectoryIdentity }, owner, await optional(join(lock, 'owner.json')), paths.lockDirectoryIdentity)
              if (names.some(name => name !== 'owner.json') || !stillOwned && !(paths.lockOwnerRemoved === true && names.length === 0)) fail('plugin_operation_lock_ownership_changed')
              const released = await save(join(lock, 'owner.json'), { operationId, pid: process.pid, released: true })
              transaction.releasedLock = { ...released, directory: paths.lockDirectoryIdentity }
              await journal(paths, transaction, 'recovery-required')
            }
          } catch (error) { result.lockReleaseErrorCode = safeCode(error) }
          result.configurationRetained = Boolean(await optional(paths.configPath))
          result.recoveryConfigurationRetained = false
          try { const saved = (await record(paths.operation, 8388608))?.value; result.recoveryConfigurationRetained = saved?.operationId === operationId && Boolean(recoveryConfig(saved)) } catch {}
        }
      }
    }
  }
}

const safeCode = error => /^[a-z][a-z0-9_-]{0,80}$/i.test(error?.code ?? '') ? error.code : 'plugin_operation_failed'
const descriptorNames = ['plugin/.codex-plugin/plugin.json', 'plugin/.mcp.json', '.agents/plugins/marketplace.json']
function recoveryConfig(transaction) {
  const value = transaction?.removal?.configuration
  if (!value) return null
  if (transaction.kind !== kind || transaction.operation !== 'uninstall' || !uuid(transaction.operationId) || !uuid(value.installationId) || transaction.installationId !== value.installationId || transaction.configIdentity !== configIdentity(value)) fail('plugin_resume_identity_unconfirmed')
  return value
}
function knownDescriptor(current, name, receipt, transaction) {
  if (transaction) {
    const previous = transaction.previous?.files?.[name], written = transaction.writtenFiles?.[name]
    if (!current) return Boolean(transaction.removal) || previous === null && !written
    return [previous?.digest, written?.digest].some(value => digest(value) && value === current.digest)
  }
  return receipt ? current?.digest === receipt.files?.[name] : current === null
}
async function previewInventory(paths, config, receipt, transaction) {
  const blockers = [], entries = [], releaseNames = [], journalDigests = [], directories = {}, maintenanceInfo = await optional(paths.maintenance)
  let maintenance = null
  if (maintenanceInfo) {
    if (!transaction) entries.push('state/maintenance.json')
    else {
      try { maintenance = await record(paths.maintenance) } catch { entries.push('state/maintenance.json') }
      if (maintenance && (maintenance.value.format !== 1 || Object.keys(maintenance.value).length !== 3 || maintenance.value.installationId !== config?.installationId || maintenance.value.operationId !== transaction.operationId)) entries.push('state/maintenance.json')
    }
  }
  const knownReleases = new Set([...(receipt?.knownDigests ?? []), receipt?.activeDigest, transaction?.candidateDigest].filter(Boolean))
  const releaseInfo = await optional(paths.releases)
  if (releaseInfo) {
    if (!releaseInfo.isDirectory() || releaseInfo.isSymbolicLink() || releaseInfo.mode & 0o077) entries.push('releases')
    else {
      directories[paths.releases] = directoryIdentity(releaseInfo)
      const names = await readdir(paths.releases)
      if (names.length > 1024) blockers.push({ code: 'installation_inventory_budget_exceeded', entries: ['releases'] })
      else for (const name of names.sort()) { const path = join(paths.releases, name), info = await lstat(path); releaseNames.push(name); if (!digest(name) || !knownReleases.has(name) || !info.isDirectory() || info.isSymbolicLink() || info.mode & 0o077 || info.uid !== process.getuid()) entries.push('releases/' + name); else directories[path] = directoryIdentity(info) }
    }
  }
  const journalDir = join(paths.installRoot, 'transactions'), journalInfo = await optional(journalDir)
  if (journalInfo) {
    if (!journalInfo.isDirectory() || journalInfo.isSymbolicLink() || journalInfo.mode & 0o077 || journalInfo.uid !== process.getuid()) entries.push('transactions')
    else {
      directories[journalDir] = directoryIdentity(journalInfo)
      const names = await readdir(journalDir); let bytesRemaining = 33554432
      if (names.length > 1024) blockers.push({ code: 'installation_inventory_budget_exceeded', entries: ['transactions'] })
      else for (const name of names.sort()) {
        const path = join(journalDir, name), info = await lstat(path)
        if (!uuid(name.replace(/\.json$/, '')) || !name.endsWith('.json') || !info.isFile() || info.mode & 0o077 || info.uid !== process.getuid()) { entries.push('transactions/' + name); continue }
        if (info.size > Math.min(bytesRemaining, 8388608)) { blockers.push({ code: 'installation_inventory_budget_exceeded', entries: ['transactions'] }); break }
        bytesRemaining -= info.size
        let saved
        try { saved = await record(path, 8388608) } catch { entries.push('transactions/' + name); continue }
        if (saved.value.kind !== kind || saved.value.installationId !== config?.installationId || saved.value.operationId + '.json' !== name) entries.push('transactions/' + name)
        else journalDigests.push([name, saved.digest])
      }
    }
  }
  const exact = new Set(['installation.json', 'runtime.json', 'runtime.lock/owner.json', 'maintenance.json', 'health-outbox.sqlite', 'health-outbox.sqlite-wal', 'health-outbox.sqlite-shm', ...descriptorNames])
  const dirs = new Set(['plugin', 'plugin/.codex-plugin', '.agents', '.agents/plugins', 'cursors', 'runtime.lock'])
  let count = 0, liveOwner
  async function liveCursorWriter() {
    if (liveOwner === undefined) {
      try { liveOwner = Boolean(config && await resolvePluginOwner({ configPath: paths.configPath })) } catch { liveOwner = false }
    }
    return liveOwner
  }
  async function scan(path, prefix = '') {
    const parent = await optional(path)
    if (!parent) return
    if (!parent.isDirectory() || parent.isSymbolicLink() || parent.mode & 0o077 || parent.uid !== process.getuid()) { entries.push('state/' + prefix.replace(/\/$/, '')); return }
    directories[path] = directoryIdentity(parent)
    for (const name of await readdir(path)) {
      if (++count > 4096) { blockers.push({ code: 'installation_inventory_budget_exceeded', entries: ['state'] }); return }
      const key = prefix + name, cursor = /^cursors\/[a-f0-9]{64}\.json$/.test(key), temporary = temporaryCursor(key)
      let info
      try { info = await lstat(join(path, name)) } catch (error) { if (error.code === 'ENOENT' && temporary && await liveCursorWriter()) continue; throw error }
      const activeWrite = temporary && info.isFile() && (info.mode & 0o7777) === 0o600 && info.uid === process.getuid() && await liveCursorWriter()
      if (info.isDirectory() && dirs.has(key) && !info.isSymbolicLink()) { await scan(join(path, name), key + '/'); if (count > 4096) return }
      else if (!info.isFile() || info.mode & 0o077 || info.uid !== process.getuid() || !exact.has(key) && !cursor && !activeWrite) entries.push('state/' + key)
    }
  }
  await scan(paths.stateDir)
  if (entries.length) blockers.push({ code: 'unmanaged_installation_files', entries: [...new Set(entries)].sort() })
  const claims = Object.fromEntries(Object.entries(directories).sort(([a], [b]) => a.localeCompare(b)))
  return { blockers, directories: claims, fingerprint: { releases: releaseNames, maintenance: maintenance?.digest ?? (maintenanceInfo ? hash(identity(maintenanceInfo)) : null), journals: journalDigests, directories: claims } }
}
async function descriptorRecords(paths) {
  return Object.fromEntries(await Promise.all(descriptorNames.map(async name => [name, await record(join(paths.stateDir, name))])))
}
async function requireWrittenDescriptors(paths, files) {
  const current = await descriptorRecords(paths)
  if (descriptorNames.some(name => current[name]?.digest !== files[name]?.digest)) fail('plugin_descriptor_concurrently_changed')
}
async function journal(paths, transaction, phase, addition = {}) {
  const next = { ...transaction, ...addition, phase }, bytes = encodeRecord(next, 8388608)
  const directory = join(paths.installRoot, 'transactions')
  await managedDirectory(paths, directory)
  paths.transactionDirectoryIdentity = paths.directoryAnchors[directory]
  Object.assign(transaction, next)
  const copy = join(paths.installRoot, 'transactions', transaction.operationId + '.json')
  await saveBytes(paths.operation, bytes); await saveBytes(copy, bytes)
  if ((await record(paths.operation, 8388608))?.digest !== hash(bytes) || (await record(copy, 8388608))?.digest !== hash(bytes)) fail('installation_record_changed')
}
async function gate(paths, config, transaction) {
  await managedDirectory(paths, paths.stateDir)
  const previous = await record(paths.maintenance)
  if (previous && (previous.value.installationId !== config.installationId || previous.value.operationId !== transaction.operationId)) fail('foreign_maintenance_requires_review')
  await save(paths.maintenance, { format: 1, installationId: config.installationId, operationId: transaction.operationId })
}
async function ungate(paths, config, transaction) {
  const current = await record(paths.maintenance)
  if (!current || current.value.installationId !== config.installationId || current.value.operationId !== transaction.operationId) fail('maintenance_ownership_changed')
  await verifyDirectoryChain(paths.directoryAnchors, paths.stateDir); await rm(paths.maintenance)
}
async function ownedProcesses(paths, releases) {
  const { stdout } = await promisify(execFile)('/bin/ps', ['-ww', '-axo', 'uid=,pid=,args='], { timeout: 10000, maxBuffer: 8388608, env: { ...process.env, LC_ALL: 'C' } })
  const expected = releases.flatMap(release => [release.ownerPath, release.bridgePath].map(entry => ({ entry, command: release.nodePath + ' ' + entry + ' --config ' + paths.configPath })))
  const notifications = releases.filter(release => release.notificationApp).map(release => join(release.notificationApp, 'Contents/MacOS/GhostSentinelNotifications'))
  expected.push(...notifications.map(entry => ({ entry, command: entry + ' --stdio' })))
  const result = []
  for (const line of stdout.split('\n')) {
    const match = line.match(/^\s*(\d+)\s+(\d+)\s+(.+)$/)
    if (!match || Number(match[1]) !== process.getuid() || !match[3].includes('--config ' + paths.configPath) && !notifications.some(entry => match[3].startsWith(entry + ' '))) continue
    const target = expected.find(value => value.command === match[3])
    if (!target) fail('unrecognized_installation_process')
    result.push({ pid: Number(match[2]), entry: target.entry })
  }
  return result
}
async function releasesFor(plan, io) {
  const receipt = (await record(plan.paths.receipt))?.value
  const ids = new Set([...(receipt?.knownDigests ?? []), ...(receipt?.activeDigest ? [receipt.activeDigest] : [])])
  const pending = (await record(plan.paths.operation, 8388608))?.value
  if (digest(pending?.candidateDigest) && await optional(join(plan.paths.releases, pending.candidateDigest))) ids.add(pending.candidateDigest)
  if (plan.package?.digest && await optional(join(plan.paths.releases, plan.package.digest))) ids.add(plan.package.digest)
  return Promise.all([...ids].map(id => { if (!digest(id)) fail('release_identity_unconfirmed'); return packageAt(join(plan.paths.releases, id), io, id) }))
}
async function stopInstallation(plan, config, transaction, io) {
  await gate(plan.paths, config, transaction)
  const host = checkedHost(await io.host.inspect(), config.pluginId)
  if (host.revision !== transaction.hostRevision) fail('plugin_host_concurrently_changed')
  if (host.installed) {
    if (host.pluginPath !== plan.paths.pluginDir || host.enabled === 'unknown') fail('plugin_enablement_unconfirmed')
    const disabled = checkedHost(await io.host.setEnabled(false, host.revision), config.pluginId)
    if (disabled.enabled !== 'disabled') fail('plugin_disable_unconfirmed')
    transaction.hostRevision = disabled.revision
  }
  const releases = await releasesFor(plan, io), deadline = Date.now() + (io.timeoutMs ?? 15000)
  let remaining
  do {
    remaining = await ownedProcesses(plan.paths, releases)
    if (!remaining.length && !await optional(join(plan.paths.stateDir, 'runtime.lock')) && !await optional(join(plan.paths.stateDir, 'runtime.json'))) {
      const inventory = await previewInventory(plan.paths, config, (await record(plan.paths.receipt))?.value, transaction)
      if (inventory.blockers.length) fail('installation_files_unconfirmed_after_stop')
      return { remaining: 0, naturalExit: true }
    }
    await delay(100)
  } while (Date.now() < deadline)
  fail('plugin_process_exit_not_observed')
}
async function patchNotifier(paths, expected, next, immutable) {
  const current = (await record(paths.configPath)).value
  if (configIdentity(current) !== immutable || (current.notificationBundlePath ?? null) !== expected) fail('notification_configuration_changed')
  if (next === null) delete current.notificationBundlePath
  else current.notificationBundlePath = next
  await verifyDirectoryChain(paths.directoryAnchors, paths.stateDir); await save(paths.configPath, current)
}
async function installedOwner(plan, release, io) {
  const deadline = Date.now() + (io.timeoutMs ?? 15000)
  do {
    const host = checkedHost(await io.host.inspect(), plan.request.pluginId)
    if (host.enabled !== 'enabled' || !host.installed || host.pluginPath !== plan.paths.pluginDir) fail('plugin_activation_qualification_changed')
    // A lock can precede runtime.json. Other identity/authentication errors are not retried as absence.
    const owner = await resolvePluginOwner({ configPath: plan.paths.configPath })
    if (owner) {
      const processes = await ownedProcesses(plan.paths, await releasesFor(plan, io))
      const entries = [release.ownerPath, release.bridgePath, ...(release.notificationApp ? [join(release.notificationApp, 'Contents/MacOS/GhostSentinelNotifications')] : [])]
      if (!processes.some(item => item.pid === owner.pid && item.entry === release.ownerPath) || processes.some(item => !entries.includes(item.entry))) fail('plugin_release_process_mismatch')
      if (owner.protocol !== HELPER_PROTOCOL || owner.version !== release.manifest.runtimeVersion || owner.ownership !== 'plugin') fail('plugin_owner_identity_unconfirmed')
      await packageAt(release.root, io, release.digest)
      return { enabled: true, ownerVerified: true, ownerEntry: release.ownerPath, pid: owner.pid, runId: owner.runId, runtimeVersion: owner.version }
    }
    await delay(100)
  } while (Date.now() < deadline)
  fail('plugin_activation_not_observed')
}
async function notificationCleanup(config, io, uninstall) {
  if (!config.notificationBundlePath) return { status: 'not-configured' }
  const native = io.notifications ?? (await import('./native-notifications.mjs')).createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId })
  try {
    if (uninstall) {
      const result = await native.purgeInstallation()
      if (result.complete !== true || result.remainingPending !== 0) fail('notification_cleanup_incomplete')
      return { status: 'complete', ...result }
    }
    const first = await native.snapshot(), ids = first.pending.map(value => value.requestId)
    if (ids.length) await native.cancel(ids)
    if ((await native.snapshot()).pending.length) fail('notification_cleanup_incomplete')
    return { status: 'complete', deliveredPreserved: true }
  } finally { await native.close?.() }
}
async function installRelease(plan, config, transaction, io) {
  const { paths, request } = plan, source = await packageAt(plan.package.root, io, plan.package.digest)
  await managedDirectory(paths, paths.releases)
  const releasePath = join(paths.releases, source.digest)
  if (!await optional(releasePath)) {
    const stage = join(paths.releases, '.stage-' + transaction.operationId)
    if (await optional(stage)) fail('unfinished_package_copy_requires_review')
    await managedDirectory(paths, stage)
    const stageIdentity = paths.directoryAnchors[stage]; let primary = null
    try {
      for (const name of await readdir(source.root)) { await verifyDirectoryChain(paths.directoryAnchors, stage); await cp(join(source.root, name), join(stage, name), { recursive: true, errorOnExist: true, force: false, preserveTimestamps: false }) }
      const inspected = await packageAt(stage, io, source.digest)
      if (inspected.root !== stage) fail('release_identity_unconfirmed')
      await verifyDirectoryChain(paths.directoryAnchors, stage); await rename(stage, releasePath)
      delete paths.directoryAnchors[stage]; paths.directoryAnchors[releasePath] = stageIdentity
    } catch (error) { primary = error; throw error }
    finally {
      try {
        await verifyDirectoryChain(paths.directoryAnchors, paths.releases)
        const remaining = await optional(stage)
        if (remaining) { if (!remaining.isDirectory() || remaining.isSymbolicLink() || directoryIdentity(remaining) !== stageIdentity) fail('release_identity_unconfirmed'); await rm(stage, { recursive: true }); delete paths.directoryAnchors[stage] }
      } catch (error) { if (primary) primary.stageCleanupErrorCode = safeCode(error); else throw error }
    }
  }
  await verifyDirectoryChain(paths.directoryAnchors, releasePath)
  const release = await packageAt(releasePath, io, source.digest)
  if (release.root !== releasePath) fail('release_identity_unconfirmed')
  await verifyDirectoryChain(paths.directoryAnchors, releasePath)
  transaction.processes = await stopInstallation(plan, config, transaction, io)
  await journal(paths, transaction, 'stopped')
  transaction.notifications = await notificationCleanup(config, io, false)
  const nextNotifier = release.notificationApp
  if (config.notificationBundlePath && !nextNotifier) fail('notification_component_missing_from_upgrade')
  await patchNotifier(paths, config.notificationBundlePath ?? null, nextNotifier, transaction.configIdentity)
  transaction.nextNotificationBundlePath = nextNotifier
  const manifest = { name: request.pluginName, version: release.manifest.version, description: 'GhostSentinel 本机会话只读检查', requires_local_executor: true, mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查', shortDescription: '本机主会话只读检查' } }
  await managedDirectory(paths, join(paths.pluginDir, '.codex-plugin')); await managedDirectory(paths, dirname(paths.marketplacePath))
  const values = { 'plugin/.codex-plugin/plugin.json': manifest, 'plugin/.mcp.json': { mcpServers: { sentinel: { command: release.nodePath, args: [release.bridgePath, '--config', paths.configPath], cwd: release.root } } }, '.agents/plugins/marketplace.json': { name: request.marketplaceName, interface: { displayName: 'GhostSentinel' }, plugins: [{ name: request.pluginName, source: { source: 'local', path: './plugin' }, policy: { installation: 'AVAILABLE', authentication: 'ON_INSTALL' }, category: 'Productivity' }] } }
  for (const name of descriptorNames) {
    await verifyDirectoryChain(paths.directoryAnchors, dirname(join(paths.stateDir, name)))
    if (!knownDescriptor(await record(join(paths.stateDir, name)), name, transaction.previous.receipt, transaction)) fail('plugin_descriptor_concurrently_changed')
    await verifyDirectoryChain(paths.directoryAnchors, dirname(join(paths.stateDir, name)))
    await save(join(paths.stateDir, name), values[name])
  }
  const written = Object.fromEntries(Object.entries(values).map(([name, value]) => [name, { value, digest: hash(JSON.stringify(value, null, 2) + '\n') }]))
  await requireWrittenDescriptors(paths, written)
  transaction.writtenFiles = written
  const receipt = { format: 1, kind, installationId: config.installationId, pluginId: config.pluginId, installRoot: paths.installRoot, pluginName: request.pluginName, marketplaceName: request.marketplaceName, configIdentity: transaction.configIdentity, activeDigest: source.digest, knownDigests: [...new Set([...(transaction.previous.receipt?.knownDigests ?? []), source.digest])], files: Object.fromEntries(Object.entries(transaction.writtenFiles).map(([name, value]) => [name, value.digest])), status: 'staged' }
  await save(paths.receipt, receipt); await journal(paths, transaction, 'staged')
  transaction.hostRevision = checkedHost(await io.host.register(), config.pluginId).revision
  await stopInstallation(plan, (await record(paths.configPath)).value, transaction, io)
  await journal(paths, transaction, 'registered')
  await requireWrittenDescriptors(paths, written)
  await ungate(paths, config, transaction)
  const before = checkedHost(await io.host.inspect(), config.pluginId)
  if (before.revision !== transaction.hostRevision) fail('plugin_host_concurrently_changed')
  if (!before.installed || before.enabled !== 'disabled' || before.pluginPath !== paths.pluginDir) fail('plugin_enablement_unconfirmed')
  transaction.hostRevision = checkedHost(await io.host.setEnabled(true, before.revision), config.pluginId).revision
  await journal(paths, transaction, 'awaiting-activation')
  const activation = await installedOwner(plan, release, io)
  await requireWrittenDescriptors(paths, written)
  await save(paths.receipt, { ...receipt, status: 'installed' })
  return { status: 'complete', operationId: transaction.operationId, operation: request.operation, installationId: config.installationId, digest: source.digest, version: source.manifest.version, activation }
}
async function restoreRelease(plan, config, transaction, io) {
  await stopInstallation(plan, (await record(plan.paths.configPath)).value, transaction, io)
  await notificationCleanup((await record(plan.paths.configPath)).value, io, false)
  const currentFiles = await descriptorRecords(plan.paths)
  for (const [name, value] of Object.entries(transaction.previous.files)) {
    if (currentFiles[name]?.digest !== transaction.writtenFiles?.[name]?.digest && currentFiles[name]?.digest !== value?.digest) fail('plugin_descriptor_concurrently_changed')
    if (value) await save(join(plan.paths.stateDir, name), value.value)
    else await rm(join(plan.paths.stateDir, name), { force: true })
  }
  await patchNotifier(plan.paths, transaction.nextNotificationBundlePath ?? config.notificationBundlePath ?? null, transaction.previous.notificationBundlePath, transaction.configIdentity)
  // Retain both verified release directories in the receipt; never rewind observation configuration or Core.
  const receipt = { ...transaction.previous.receipt, knownDigests: [...new Set([...(transaction.previous.receipt.knownDigests ?? []), ...(plan.package ? [plan.package.digest] : [])])] }
  await save(plan.paths.receipt, receipt)
  transaction.hostRevision = checkedHost(await io.host.register(), config.pluginId).revision
  await stopInstallation(plan, (await record(plan.paths.configPath)).value, transaction, io)
  await ungate(plan.paths, config, transaction)
  const host = checkedHost(await io.host.inspect(), config.pluginId)
  if (host.revision !== transaction.hostRevision) fail('plugin_host_concurrently_changed')
  transaction.hostRevision = checkedHost(await io.host.setEnabled(true, host.revision), config.pluginId).revision
  const release = await packageAt(join(plan.paths.releases, receipt.activeDigest), io, receipt.activeDigest)
  return { status: 'complete', activation: await installedOwner(plan, release, io), restoredDigest: receipt.activeDigest }
}
async function removeInstallation(plan, config, transaction, io) {
  const { paths } = plan
  if (transaction.removal) {
    const removal = checkedRemoval(transaction, paths), host = checkedHost(await io.host.inspect(), config.pluginId)
    if (host.installed || host.marketplaceRegistered || (await ownedProcesses(paths, removal.releases)).length) fail('uninstall_recovery_qualification_changed')
    await removeOwnedFiles(plan, config, transaction, io)
    return { status: 'complete', operation: 'uninstall', operationId: transaction.operationId, installationId: config.installationId, ...removal.outcome }
  }
  const processes = await stopInstallation(plan, config, transaction, io)
  await journal(paths, transaction, 'stopped')
  const notifications = await notificationCleanup(config, io, true)
  let coreCleanup = { status: 'not-created', installationId: config.installationId }
  if (await optional(join(config.coreDataDir, 'observations.sqlite'))) {
    const core = await (io.connectCore ?? connectCore)({ dataDir: config.coreDataDir })
    try {
      if (!(await core.health()).features?.includes('chatgpt-installation-purge/1')) fail('core_purge_unsupported')
      coreCleanup = await core.purgeChatGPTInstallation({ installationId: config.installationId })
      if (coreCleanup.installationId !== config.installationId || coreCleanup.status !== 'complete' || coreCleanup.logical?.status !== 'purged' || coreCleanup.physical?.status !== 'checkpointed' || coreCleanup.physical?.secureDelete !== true) fail('core_purge_incomplete')
    } finally { await core.close() }
  }
  await journal(paths, transaction, 'data-purged')
  const host = checkedHost(await io.host.unregister(), config.pluginId)
  if (host.installed || host.marketplaceRegistered) fail('plugin_unregister_unconfirmed')
  transaction.removalOutcome = { processes, notifications, coreCleanup }
  await removeOwnedFiles(plan, config, transaction, io)
  return { status: 'complete', operation: 'uninstall', operationId: transaction.operationId, installationId: config.installationId, processes, notifications, coreCleanup }
}
async function removeOwnedFiles(plan, config, transaction, io) {
  if (transaction.removal) return finishRemoval(plan.paths, transaction)
  const { paths } = plan, receipt = (await record(paths.receipt))?.value
  if (configIdentity((await record(paths.configPath)).value) !== transaction.configIdentity) fail('installation_configuration_changed')
  const descriptors = await descriptorRecords(paths)
  for (const name of descriptorNames) if (descriptors[name]?.digest !== (receipt?.files?.[name] ?? transaction.writtenFiles?.[name]?.digest)) fail('plugin_descriptor_concurrently_changed')
  const allowed = new Set(['installation-receipt.json', 'operation.json', '.operation.lock/owner.json', 'state/installation.json', 'state/maintenance.json', ...descriptorNames.map(name => 'state/' + name)])
  const releases = await releasesFor(plan, io)
  for (const release of releases) for (const name of ['plugin-package.json', ...Object.keys(release.manifest.files)]) allowed.add('releases/' + release.digest + '/' + name)
  for (const name of await readdir(join(paths.installRoot, 'transactions'))) {
    if (!/^[a-f0-9-]{36}\.json$/.test(name)) fail('unknown_installation_files_preserved')
    const saved = (await record(join(paths.installRoot, 'transactions', name), 8388608)).value
    if (saved.kind !== kind || saved.installationId !== config.installationId || saved.operationId + '.json' !== name) fail('unknown_installation_files_preserved')
    allowed.add('transactions/' + name)
  }
  const cursors = join(paths.stateDir, 'cursors')
  if (await optional(cursors)) for (const name of await readdir(cursors)) {
    if (!/^[a-f0-9]{64}\.json$/.test(name)) fail('unknown_installation_files_preserved')
    const path = join(cursors, name), value = (await record(path)).value
    await (await import('./cursor-store.mjs')).loadCursor(path, { fork: value.format === 3 }); allowed.add('state/cursors/' + name)
  }
  const database = join(paths.stateDir, 'health-outbox.sqlite')
  if (await optional(database)) {
    const file = await open(database, constants.O_RDONLY | constants.O_NOFOLLOW)
    try { const bytes = Buffer.alloc(16); if ((await file.read(bytes, 0, 16, 0)).bytesRead !== 16 || !bytes.equals(Buffer.from('SQLite format 3\0'))) fail('private_outbox_unconfirmed') } finally { await file.close() }
    for (const suffix of ['', '-wal', '-shm']) allowed.add('state/health-outbox.sqlite' + suffix)
  }
  const files = [], directories = []
  const allowedDirectories = new Set(['state/cursors', 'transactions', '.operation.lock'])
  for (const file of allowed) { let parent = dirname(file); while (parent !== '.') { allowedDirectories.add(parent); parent = dirname(parent) } }
  async function scan(path, prefix = '') {
    for (const name of await readdir(path)) {
      const child = join(path, name), key = prefix + name, info = await lstat(child)
      if (info.uid !== process.getuid() || info.mode & 0o077 || info.isSymbolicLink()) fail('untrusted_installation_entry')
      if (info.isDirectory() && allowedDirectories.has(key)) { await scan(child, key + '/'); directories.push({ key, identity: JSON.stringify([info.dev, info.ino, info.uid, info.mode]) }) }
      else if (info.isFile() && allowed.has(key)) files.push({ key, identity: identity(info) })
      else fail('unknown_installation_files_preserved')
    }
  }
  await scan(paths.installRoot)
  const removal = { format: 1, configuration: (await record(paths.configPath)).value, receipt: receipt ?? null, files, directories, releases: releases.map(({ digest, nodePath, bridgePath, ownerPath, notificationApp }) => ({ digest, nodePath, bridgePath, ownerPath, notificationApp })), outcome: transaction.removalOutcome }
  // This private journal is the uninstall-only recovery authority, never an input for activation or rollback.
  await journal(paths, transaction, 'removing-owned-files', { removal })
  await finishRemoval(paths, transaction)
}
function checkedRemoval(transaction, paths) {
  const value = transaction.removal
  recoveryConfig(transaction)
  const relative = key => typeof key === 'string' && key.length < 2048 && !isAbsolute(key) && !/[\x00-\x1f\x7f\\]/.test(key) && key.split('/').every(part => part && part !== '.' && part !== '..')
  if (value?.format !== 1 || value.configuration.dataDir !== paths.stateDir || !Array.isArray(value.files) || value.files.length > 40000 || !Array.isArray(value.directories) || value.directories.length > 40000 || !Array.isArray(value.releases) || value.releases.length > 1024 || !value.outcome) fail('uninstall_recovery_manifest_unconfirmed')
  for (const entry of [...value.files, ...value.directories]) if (!relative(entry.key) || typeof entry.identity !== 'string' || entry.identity.length > 512) fail('uninstall_recovery_manifest_unconfirmed')
  for (const release of value.releases) {
    const root = join(paths.releases, release.digest ?? '')
    if (!digest(release.digest) || release.nodePath !== join(root, 'runtime/node') || release.bridgePath !== join(root, 'chatgpt/plugin-bridge.mjs') || release.ownerPath !== join(root, 'chatgpt/plugin-main.mjs') || release.notificationApp !== null && release.notificationApp !== join(root, 'notification/GhostSentinelNotifications.app')) fail('uninstall_recovery_manifest_unconfirmed')
  }
  return value
}
async function finishRemoval(paths, transaction) {
  const removal = checkedRemoval(transaction, paths), controls = ['state/maintenance.json', 'state/installation.json', 'installation-receipt.json', 'transactions/' + transaction.operationId + '.json', 'operation.json'], dynamic = new Set(controls.slice(3))
  const directories = { ...paths.directoryAnchors, ...Object.fromEntries(removal.directories.map(entry => [join(paths.installRoot, entry.key), entry.identity])) }
  if (paths.transactionDirectoryIdentity) directories[join(paths.installRoot, 'transactions')] = paths.transactionDirectoryIdentity
  const files = removal.files.filter(file => !inside(join(paths.installRoot, file.key), join(paths.installRoot, '.operation.lock'))).sort((a, b) => (controls.indexOf(a.key) + 1 || -1) - (controls.indexOf(b.key) + 1 || -1))
  for (const file of files) {
    const path = join(paths.installRoot, file.key)
    if (!await verifyDirectoryChain(directories, dirname(path), paths.installRoot)) continue
    const info = await optional(path)
    if (!info) continue
    if (dynamic.has(file.key)) {
      const current = (await record(path, 8388608)).value
      if (current.kind !== kind || current.operationId !== transaction.operationId || current.installationId !== transaction.installationId || hash(JSON.stringify(current.removal)) !== hash(JSON.stringify(removal))) fail('uninstall_recovery_manifest_unconfirmed')
    } else if (identity(info) !== file.identity) fail('installation_file_changed_during_removal')
    await verifyDirectoryChain(directories, dirname(path)); await rm(path)
  }
  for (const directory of removal.directories) {
    if (directory.key === '.operation.lock') continue
    const path = join(paths.installRoot, directory.key), info = await optional(path)
    if (!info) continue
    await verifyDirectoryChain(directories, path)
    if (!info.isDirectory() || info.isSymbolicLink() || directoryIdentity(info) !== directories[path] || info.uid !== process.getuid() || info.mode & 0o077) fail('installation_directory_changed_during_removal')
    await rmdir(path)
    delete paths.directoryAnchors[path]
  }
}
