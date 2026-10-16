import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { open, lstat, realpath, readdir, rename, cp, rm, rmdir } from 'node:fs/promises'
import { constants } from 'node:fs'
import { isAbsolute, join, resolve, dirname, basename, sep } from 'node:path'
import { createHash, randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import { createNativeNotificationIO } from './native-notifications.mjs'
import { loadCursor } from './cursor-store.mjs'
import { connectCore } from '../src/core-client.mjs'

const execute = promisify(execFile)
const bundleId = 'com.ghostsentinel.helper', version = '0.1.0-dev.1'
const main = 'Contents/MacOS/GhostSentinelHelper', manifestPath = 'Contents/Resources/helper-inputs.json'
const endpointFiles = ['chatgpt/helper-main.mjs', 'chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/panel.html', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'src/daemon.mjs', 'src/server.mjs', 'src/core-client.mjs', 'package.json', 'package-lock.json']
const hash = value => createHash('sha256').update(value).digest('hex')
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value)
const fail = code => { throw Object.assign(new Error(code), { code }) }
const absolute = value => typeof value === 'string' && isAbsolute(value) && value === resolve(value) && !/[\x00-\x1f\x7f]/.test(value)
const uid = () => process.getuid()
const uuid = value => typeof value === 'string' && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value)
const configIdentity = config => hash(JSON.stringify(['installationId', 'dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket', 'pluginId', 'notificationBundlePath', 'token'].map(key => config[key] ?? null)))
const sameFile = stat => JSON.stringify([stat.dev, stat.ino, stat.size, stat.mtimeMs, stat.ctimeMs, stat.mode, stat.uid])
const defaultIO = {
  connectCore,
  execute: (file, args, input) => input === undefined ? execute(file, args, { shell: false, timeout: 20000, maxBuffer: 1048576 }) : new Promise((resolve, reject) => {
    const child = execFile(file, args, { shell: false, timeout: 5000, maxBuffer: 262144 }, (error, stdout, stderr) => error ? reject(error) : resolve({ stdout, stderr }))
    child.stdin.once('error', reject); child.stdin.end(input)
  }),
  signal: (pid, signal) => process.kill(pid, signal),
  async request(target, path, body) {
    const response = await fetch(target.url + path, { method: body === undefined ? 'GET' : 'POST', redirect: 'error', signal: AbortSignal.timeout(2000), headers: { authorization: 'Bearer ' + target.token, 'content-type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) })
    const chunks = []; let size = 0
    for await (const chunk of response.body) { size += chunk.length; if (size > 1048576) fail('management-response-limit'); chunks.push(chunk) }
    const value = JSON.parse(Buffer.concat(chunks).toString())
    if (!response.ok) throw Object.assign(new Error(typeof value.error === 'string' ? value.error : 'management-http-error'), { status: response.status, code: value.code })
    return value
  },
}

async function noLinks(path) {
  if (!absolute(path)) fail('absolute-canonical-path-required')
  let current = sep
  for (const part of path.split(sep).filter(Boolean)) {
    current = join(current, part)
    const info = await lstat(current)
    if (info.isSymbolicLink()) fail('symlink-path-rejected')
  }
  return path
}
async function privateDirectory(path) {
  await noLinks(path)
  const info = await lstat(path)
  if (!info.isDirectory() || info.uid !== uid() || info.mode & 0o077) fail('private-directory-required')
}
async function bytes(path, limit = 1048576, privateFile = false) {
  await noLinks(path)
  const before = await lstat(path)
  if (!before.isFile() || before.uid !== uid() || before.size > limit || privateFile && (before.mode & 0o7777) !== 0o600) fail('untrusted-file')
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const opened = await file.stat(), buffer = Buffer.alloc(before.size + 1)
    let length = 0
    while (length < buffer.length) { const read = await file.read(buffer, length, buffer.length - length, length); if (!read.bytesRead) break; length += read.bytesRead }
    const [after, current] = await Promise.all([file.stat(), lstat(path)])
    if (length !== before.size || [opened, after, current].some(info => sameFile(info) !== sameFile(before))) fail('file-changed-during-read')
    return buffer.subarray(0, length)
  } finally { await file.close() }
}
async function json(path, limit = 16384, privateFile = true) {
  const value = await bytes(path, limit, privateFile)
  try { return JSON.parse(value) } catch { fail('invalid-private-json') }
}
async function optional(path, read) {
  try { return await read(path) } catch (error) { if (error.code === 'ENOENT') return null; throw error }
}
async function filesUnder(directory, prefix = '') {
  const values = []
  for (const entry of (await readdir(directory, { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) {
    const path = join(directory, entry.name), key = prefix + entry.name, info = await lstat(path)
    if (info.isSymbolicLink() || info.uid !== uid() || info.mode & 0o022) fail('untrusted-bundle-entry')
    if (info.isDirectory()) values.push(...await filesUnder(path, key + '/'))
    else if (info.isFile()) values.push(key)
    else fail('nonregular-bundle-entry')
    if (values.length > 20000) fail('bundle-file-count-limit')
  }
  return values
}
async function configuration(path) {
  if (!absolute(path)) fail('absolute-config-required')
  const config = await json(path)
  if (config.format !== 1 || !digest(config.token) || typeof config.installationId !== 'string' || !config.installationId.trim() || typeof config.pluginId !== 'string' || !config.pluginId.trim() || !['dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket'].every(key => absolute(config[key])) || config.port !== undefined && (!Number.isSafeInteger(config.port) || config.port < 1 || config.port > 65535) || config.notificationBundlePath !== undefined && (!absolute(config.notificationBundlePath) || !config.notificationBundlePath.endsWith('.app'))) fail('installation-config-invalid')
  await privateDirectory(config.dataDir)
  if (path !== join(config.dataDir, 'installation.json') || await realpath(path) !== path) fail('installation-config-location-mismatch')
  return config
}
async function bundle(path, configPath, io) {
  if (!absolute(path) || !path.endsWith('.app')) fail('absolute-app-bundle-required')
  await noLinks(path)
  const info = await lstat(path)
  if (!info.isDirectory() || info.uid !== uid() || info.mode & 0o022) fail('untrusted-bundle-directory')
  await io.execute('/usr/bin/codesign', ['--verify', '--deep', '--strict', path])
  const identity = await io.execute('/usr/bin/codesign', ['--display', '--verbose=4', path])
  if (!String(identity.stderr).split('\n').includes('Identifier=' + bundleId)) fail('code-signature-identity-mismatch')
  const readPlist = async name => JSON.parse((await io.execute('/usr/bin/plutil', ['-convert', 'json', '-o', '-', join(path, name)])).stdout)
  const metadata = await readPlist('Contents/Info.plist')
  if (metadata.CFBundleIdentifier !== bundleId || metadata.CFBundleExecutable !== 'GhostSentinelHelper' || metadata.LSUIElement !== true || metadata.GhostSentinelRuntimeVersion !== version) fail('helper-bundle-identity-mismatch')
  const executable = await lstat(join(path, main))
  if (!executable.isFile() || !(executable.mode & 0o111)) fail('helper-executable-required')
  const service = await readPlist('Contents/Library/LaunchAgents/com.ghostsentinel.helper.agent.plist')
  const expected = { Label: bundleId + '.agent', BundleProgram: main, ProgramArguments: ['GhostSentinelHelper', '--agent'], RunAtLoad: true, KeepAlive: { SuccessfulExit: false }, ThrottleInterval: 30, ProcessType: 'Background', ExitTimeOut: 15 }
  if (JSON.stringify(Object.keys(service).sort()) !== JSON.stringify(Object.keys(expected).sort()) || Object.entries(expected).some(([key, value]) => JSON.stringify(service[key]) !== JSON.stringify(value))) fail('helper-service-contract-mismatch')
  const manifestBytes = await bytes(join(path, manifestPath), 4194304), manifest = JSON.parse(manifestBytes)
  if (manifest.format !== 1 || manifest.bundleId !== bundleId || manifest.runtimeVersion !== version || manifest.installationConfigPath !== configPath || manifest.codeSignedExecutable !== main || manifest.nodeInputSHA256 !== '738852ca4f83d6a0485e4ae25e04b84243ae1d331e5f003b7cd0291cdf4b6821' || !manifest.sources || !manifest.packagedFiles || manifest.sourceSetSHA256 !== hash(JSON.stringify(manifest.sources))) fail('helper-inputs-invalid')
  const safeKey = key => typeof key === 'string' && key.length < 1024 && !key.startsWith('/') && !key.split('/').some(part => !part || part === '.' || part === '..') && !/[\x00-\x1f\x7f\\]/.test(key)
  for (const [key, value] of Object.entries(manifest.sources)) if (!safeKey(key) || !digest(value)) fail('source-manifest-invalid')
  const paths = await filesUnder(path)
  const payload = paths.filter(key => key !== main && key !== manifestPath && !key.startsWith('Contents/_CodeSignature/'))
  if (JSON.stringify(Object.keys(manifest.packagedFiles).sort()) !== JSON.stringify([...payload].sort())) fail('packaged-file-set-mismatch')
  let total = 0
  for (const key of payload) {
    if (!safeKey(key) || !digest(manifest.packagedFiles[key])) fail('packaged-manifest-invalid')
    const data = await bytes(join(path, key), 134217728)
    if ((total += data.length) > 268435456) fail('bundle-byte-limit')
    if (hash(data) !== manifest.packagedFiles[key]) fail('packaged-file-hash-mismatch')
    const sourceKey = key.startsWith('Contents/Resources/observer/') ? key.slice('Contents/Resources/observer/'.length) : null
    if (sourceKey && manifest.sources[sourceKey] !== hash(data)) fail('packaged-source-provenance-mismatch')
  }
  for (const key of endpointFiles) if (!Object.hasOwn(manifest.sources, key) || !payload.includes('Contents/Resources/observer/' + key)) fail('packaged-endpoint-prerequisite-missing')
  const node = await lstat(join(path, 'Contents/Resources/node/bin/node'))
  if (!node.isFile() || !(node.mode & 0o111)) fail('bundled-node-executable-required')
  const pointer = await json(join(path, 'Contents/Resources/helper-config.json'))
  if (Object.keys(pointer).length !== 1 || pointer.installationConfigPath !== configPath) fail('helper-config-pointer-mismatch')
  // The outer signature seals the main Mach-O and this manifest; hashing the main
  // here would create a resource-seal/signature cycle. No other payload is exempt.
  return { manifestSHA256: hash(manifestBytes), sourceSetSHA256: manifest.sourceSetSHA256, runtimeVersion: version }
}
async function inspect(options, io) {
  const { operation, configPath, destinationBundle, candidateBundle } = options
  if (!['install', 'upgrade', 'uninstall'].includes(operation)) fail('unknown-helper-operation')
  if (!absolute(destinationBundle) || !destinationBundle.endsWith('.app') || basename(destinationBundle) !== 'GhostSentinel.app') fail('explicit-helper-destination-required')
  await noLinks(dirname(destinationBundle))
  const parent = await lstat(dirname(destinationBundle))
  if (!parent.isDirectory() || parent.uid !== uid() || parent.mode & 0o022) fail('untrusted-destination-parent')
  const config = await configuration(configPath)
  const exists = await optional(destinationBundle, lstat)
  const installed = exists ? await bundle(destinationBundle, configPath, io) : null
  const receipt = await optional(join(config.dataDir, 'helper-installation.json'), json)
  if (options.resume !== undefined && !uuid(options.resume)) fail('invalid-operation-resume')
  if (operation === 'install' && !options.resume && (installed || receipt)) fail('installation-already-exists')
  if (operation === 'install' && !options.resume) {
    for (const name of ['runtime.json', 'runtime.lock', 'plugin/.mcp.json', 'plugin/.codex-plugin/plugin.json', 'cursors', 'health-outbox.sqlite', 'health-outbox.sqlite-wal', 'health-outbox.sqlite-shm']) if (await optional(join(config.dataDir, name), lstat)) fail('unmanaged-runtime-or-plugin-requires-migration')
  }
  if ((operation !== 'install' || options.resume) && (!installed || receipt?.format !== 1 || receipt.bundleId !== bundleId || receipt.installationId !== config.installationId || receipt.configPath !== configPath || receipt.destinationBundle !== destinationBundle || receipt.manifestSHA256 !== installed.manifestSHA256 || receipt.configIdentity !== configIdentity(config) || receipt.knownPort !== null && receipt.knownPort !== config.port)) fail('unknown-managed-installation')
  if (operation !== 'install' && !options.resume && (receipt.configFileIdentity !== sameFile(await lstat(configPath)) || receipt.configSHA256 !== hash(await bytes(configPath, 16384, true)))) fail('managed-config-file-identity-mismatch')
  const candidate = options.resume ? installed : operation === 'uninstall' ? null : await bundle(candidateBundle, configPath, io)
  if (candidateBundle === destinationBundle) fail('candidate-must-be-separate')
  return { config, installed, receipt, candidate, options }
}
export async function previewHelperChange(options, io = defaultIO) {
  if (options.operation === 'uninstall' && options.resume) return resumeUninstall(options, io, true)
  const { candidate, installed, config } = await inspect(options, io)
  return { mode: 'preview', operation: options.operation, activation: 'not-requested', bundleId, runtimeVersion: version, destinationBundle: options.destinationBundle, candidate: candidate ?? installed, pluginDir: join(config.dataDir, 'plugin'), pluginRegistration: 'pending-first-party-tool', pending: ['live-runtime-ownership', 'existing-core-capability-and-disconnected-scope', 'SMAppService-status-and-user-approval', 'actual-process-stop'] }
}

async function save(path, value, exclusive = false) {
  if (exclusive) { const file = await open(path, 'wx', 0o600); try { await file.writeFile(JSON.stringify(value) + '\n') } finally { await file.close() }; return }
  const temporary = path + '.' + randomUUID() + '.tmp'
  try { await save(temporary, value, true); await rename(temporary, path) }
  finally { await rm(temporary, { force: true }) }
}
async function control(destination, command, io) {
  const response = JSON.parse((await io.execute(join(destination, main), [command])).stdout)
  if (response.ok !== true || response.bundleId !== bundleId || response.command !== command || !['notRegistered', 'enabled', 'requiresApproval', 'notFound'].includes(response.serviceStatus) || typeof response.unregisterCompleted !== 'boolean') fail('helper-control-protocol-invalid')
  return response
}
async function processRecord(pid, expectedCommand, expectedArguments, io) {
  const record = (await io.execute('/bin/ps', ['-p', String(pid), '-o', 'uid=,ppid=,lstart=', '-ww'])).stdout.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)
  const command = (await io.execute('/bin/ps', ['-p', String(pid), '-o', 'comm=', '-ww'])).stdout.trim()
  const args = (await io.execute('/bin/ps', ['-p', String(pid), '-o', 'args=', '-ww'])).stdout.trim()
  if (!record || Number(record[1]) !== uid() || command !== expectedCommand || args !== expectedArguments) fail('helper-process-ownership-unconfirmed')
  return { pid, parent: Number(record[2]), started: record[3] }
}
function hasManagedProcess(output, options) {
  return output.split('\n').some(line => {
    const row = line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)
    return row && Number(row[1]) === uid() && (row[3] === join(options.destinationBundle, main) + ' --agent' || row[3].includes('helper-main.mjs') && row[3].endsWith(' --config ' + options.configPath))
  })
}
async function runtimeIdentity(state, io) {
  const { config, options } = state, root = options.destinationBundle
  await privateDirectory(join(config.dataDir, 'runtime.lock'))
  const owner = await json(join(config.dataDir, 'runtime.lock/owner.json')), runtime = await json(join(config.dataDir, 'runtime.json'))
  const fields = ['protocol', 'installationId', 'runId', 'pid', 'version']
  if (owner.protocol !== 'ghost-sentinel-chatgpt-helper/1' || owner.installationId !== config.installationId || owner.version !== version || !uuid(owner.runId) || !Number.isSafeInteger(owner.pid) || owner.pid < 2 || runtime.format !== 1 || !fields.every(key => owner[key] === runtime[key]) || runtime.url !== 'http://127.0.0.1:' + config.port) fail('helper-runtime-identity-unconfirmed')
  const connection = await json(join(config.dataDir, 'plugin/.mcp.json'))
  const entry = connection.mcpServers?.sentinel
  if (Object.keys(connection).length !== 1 || Object.keys(connection.mcpServers ?? {}).length !== 1 || entry?.url !== runtime.url + '/mcp' || entry.http_headers?.Authorization !== 'Bearer ' + config.token) fail('helper-private-endpoint-unconfirmed')
  const health = await io.request({ url: runtime.url, token: config.token }, '/health')
  if (!fields.every(key => owner[key] === health[key])) fail('helper-health-identity-unconfirmed')
  const node = join(root, 'Contents/Resources/node/bin/node'), entryPath = join(root, 'Contents/Resources/observer/chatgpt/helper-main.mjs'), executable = join(root, main)
  const child = await processRecord(owner.pid, node, node + ' ' + entryPath + ' --config ' + options.configPath, io)
  const wrapper = await processRecord(child.parent, executable, executable + ' --agent', io)
  if (wrapper.parent !== 1) fail('helper-managed-parent-unconfirmed')
  const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout.split('\n').map(line => line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean)
  const matching = processes.filter(row => Number(row[1]) === uid() && row[3].includes('helper-main.mjs') && row[3].endsWith(' --config ' + options.configPath))
  if (matching.length !== 1 || Number(matching[0][2]) !== owner.pid) fail('parallel-helper-process-unconfirmed')
  return { owner, child, wrapper }
}
async function gone(pid, io) {
  try { await io.execute('/bin/ps', ['-p', String(pid), '-o', 'uid=,ppid=,lstart=', '-ww']); return false }
  catch (error) { if (error.code === 1) return true; throw error }
}
async function waitFor(check, code) {
  const deadline = Date.now() + 8000
  do { if (await check()) return; await delay(100) } while (Date.now() < deadline)
  fail(code)
}
async function endpointClosed(config, io) {
  try { await io.request({ url: 'http://127.0.0.1:' + config.port, token: config.token }, '/health'); return false }
  catch (error) { if ((error.code ?? error.cause?.code) === 'ECONNREFUSED') return true; throw error }
}
const validCoreTarget = target => target?.protocol === 1 && digest(target.token) && uuid(target.instanceId) && Number.isSafeInteger(target.pid) && target.pid > 1 && /^http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4}$/.test(target.url) && Number(new URL(target.url).port) <= 65535
async function existingCore(config, io) {
  await privateDirectory(config.coreDataDir)
  const target = await json(join(config.coreDataDir, 'runtime.json'))
  if (!validCoreTarget(target)) fail('existing-core-runtime-unconfirmed')
  const health = await io.request(target, '/v1/health')
  if (health.protocol !== 1 || health.instanceId !== target.instanceId) fail('existing-core-health-unconfirmed')
  return { target, health }
}
async function managementCore(config, io) {
  try { return await existingCore(config, io) }
  catch (error) { if (!['ENOENT', 'ECONNREFUSED'].includes(error.code ?? error.cause?.code)) throw error }
  const directory = await optional(config.coreDataDir, lstat)
  if (directory) await privateDirectory(config.coreDataDir)
  else {
    await noLinks(dirname(config.coreDataDir))
    const parent = await lstat(dirname(config.coreDataDir))
    if (!parent.isDirectory() || parent.uid !== uid() || parent.mode & 0o022) fail('core-profile-parent-unconfirmed')
  }
  const target = directory ? await optional(join(config.coreDataDir, 'runtime.json'), json) : null
  if (target && !validCoreTarget(target)) fail('existing-core-runtime-unconfirmed')
  if (target && (!Number.isSafeInteger(target.pid) || target.pid < 2 || !await gone(target.pid, io))) fail('core-profile-live-owner-unconfirmed')
  for (const name of ['core.lock', 'startup.lock']) {
    const path = join(config.coreDataDir, name)
    if (!await optional(path, lstat)) continue
    await privateDirectory(path)
    const owner = await json(join(path, 'owner.json'))
    if (name === 'startup.lock' || !Number.isSafeInteger(owner.pid) || owner.pid < 2 || !uuid(owner.instanceId) || !await gone(owner.pid, io)) fail('core-profile-owner-unconfirmed')
  }
  const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
  if (processes.split('\n').some(line => line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)?.[3].endsWith(' --data-dir ' + config.coreDataDir))) fail('core-profile-process-unconfirmed')
  if (typeof io.connectCore !== 'function') fail('management-core-connect-unavailable')
  const controller = new AbortController(), timer = setTimeout(() => controller.abort(new Error('management Core connection timed out')), 12000)
  let client
  try {
    client = await io.connectCore({ dataDir: config.coreDataDir, signal: controller.signal })
    const core = await existingCore(config, io), health = await client.health(controller.signal)
    if (health.protocol !== 1 || health.instanceId !== core.target.instanceId) fail('reconnected-core-identity-unconfirmed')
    return { ...core, client }
  } catch (error) { await client?.close(); throw error }
  finally { clearTimeout(timer) }
}
async function disconnected(config, core, io) {
  const sources = await io.request(core.target, '/v1/sources')
  if (!Array.isArray(sources.entries) || sources.entries.length > 5000) fail('existing-core-source-plan-unconfirmed')
  return !sources.entries.some(entry => entry.registrationId === 'chatgpt:' + config.installationId && ['connecting', 'connected'].includes(entry.status))
}
async function stopNode(state, transaction, io) {
  const owned = await runtimeIdentity(state, io)
  const again = await runtimeIdentity(state, io)
  if (JSON.stringify(again) !== JSON.stringify(owned)) fail('helper-process-changed-before-stop')
  await io.signal(owned.owner.pid, 'SIGTERM')
  transaction.completed.push('owned-node-signalled')
  await waitFor(async () => await gone(owned.owner.pid, io) && await endpointClosed(state.config, io) && !await optional(join(state.config.dataDir, 'runtime.json'), lstat) && !await optional(join(state.config.dataDir, 'runtime.lock'), lstat), 'owned-node-stop-unconfirmed')
  transaction.completed.push('owned-node-stopped')
  await waitFor(() => gone(owned.wrapper.pid, io), 'owned-wrapper-stop-unconfirmed')
  const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
  if (hasManagedProcess(processes, state.options)) fail('owned-stop-generation-unconfirmed')
  transaction.ownedStop = { format: 1, protocol: owned.owner.protocol, runId: owned.owner.runId, pid: owned.owner.pid, version: owned.owner.version, child: owned.child, wrapper: owned.wrapper, uid: uid(), nodeGone: true, wrapperGone: true, verifiedAt: Date.now() }
  transaction.phase = 'owned-stop-confirmed'
  await save(join(state.config.dataDir, 'helper-operation.json'), transaction)
  return owned
}
async function stopManaged(state, transaction, io) {
  const owned = await stopNode(state, transaction, io)
  const core = await managementCore(state.config, io)
  try {
    await waitFor(() => disconnected(state.config, core, io), 'owned-source-disconnect-unconfirmed')
    transaction.completed.push('owned-source-disconnected')
    return { owned, core }
  } catch (error) { await core.client?.close(); throw error }
}
async function unregister(state, owned, transaction, io) {
  const status = await control(state.options.destinationBundle, '--unregister', io)
  if (!status.unregisterCompleted || status.serviceStatus !== 'notRegistered') fail('helper-unregister-unconfirmed')
  await waitFor(() => gone(owned.wrapper.pid, io), 'owned-wrapper-stop-unconfirmed')
  const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
  if (hasManagedProcess(processes, state.options)) fail('residual-helper-process-unconfirmed')
  if (!await endpointClosed(state.config, io) || await optional(join(state.config.dataDir, 'runtime.json'), lstat) || await optional(join(state.config.dataDir, 'runtime.lock'), lstat)) fail('helper-stop-residual')
  transaction.completed.push('SMAppService-unregistered', 'owned-processes-stopped')
}
async function purgeNotifications(config, io) {
  if (!config.notificationBundlePath) return { status: 'not-configured', deliveredPreserved: true }
  await noLinks(config.notificationBundlePath)
  const metadata = JSON.parse((await io.execute('/usr/bin/plutil', ['-convert', 'json', '-o', '-', join(config.notificationBundlePath, 'Contents/Info.plist')])).stdout)
  if (metadata.CFBundleIdentifier !== 'com.ghostsentinel.notifications') fail('notification-component-identity-mismatch')
  await io.execute('/usr/bin/codesign', ['--verify', '--deep', '--strict', config.notificationBundlePath])
  const identity = await io.execute('/usr/bin/codesign', ['--display', '--verbose=4', config.notificationBundlePath])
  if (!String(identity.stderr).split('\n').includes('Identifier=com.ghostsentinel.notifications')) fail('notification-code-signature-identity-mismatch')
  const notifier = createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId, run: (file, args, input) => io.execute(file, args, input) })
  let result, requestedCancellations = 0, clearedActions = 0
  try {
    for (let attempt = 0; attempt < 4; attempt++) {
      result = await notifier.purgeInstallation(); requestedCancellations += result.requestedCancellations; clearedActions += result.clearedActions
      if (result.complete) return { status: 'complete', complete: true, requestedCancellations, remainingPending: 0, clearedActions, deliveredPreserved: true, operations: attempt + 1 }
    }
    return { status: 'incomplete', complete: false, requestedCancellations, remainingPending: result.remainingPending, clearedActions, deliveredPreserved: true, operations: 4 }
  } finally { notifier.close() }
}
async function purgeCore(config, core, io) {
  if (!core.health.features?.includes('chatgpt-installation-purge/1')) fail('core-installation-purge-feature-missing')
  let result
  for (let attempt = 0; attempt < 3; attempt++) {
    result = await io.request(core.target, '/v1/chatgpt/installation/purge', { installationId: config.installationId })
    if (result.installationId !== config.installationId || result.registrationId !== 'chatgpt:' + config.installationId || !['complete', 'incomplete', 'failed'].includes(result.status)) fail('core-installation-purge-protocol-invalid')
    if (result.status === 'complete') {
      if (result.logical?.status !== 'purged' || result.physical?.status !== 'checkpointed' || result.physical.secureDelete !== true) fail('core-installation-purge-completion-unconfirmed')
      break
    }
    if (result.retryable !== true) break
  }
  const counts = value => {
    if (value === null || value === undefined) return value
    if (typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length > 64 || Object.entries(value).some(([key, count]) => !/^[a-zA-Z][a-zA-Z0-9]{0,63}$/.test(key) || !Number.isSafeInteger(count) || count < 0)) fail('core-purge-counts-unconfirmed')
    return value
  }
  const physical = { status: result.physical?.status, secureDelete: result.physical?.secureDelete === true }
  if (result.physical?.wal) physical.wal = counts(result.physical.wal)
  return { status: result.status, logical: { status: result.logical?.status, removed: counts(result.logical?.removed), remaining: counts(result.logical?.remaining) }, physical, retryable: result.retryable === true, errors: (result.errors ?? []).map(error => ({ stage: error.stage, code: error.code })) }
}
async function purgeManagedInstallation(config, transaction, core, io) {
  if (!core.health.features?.includes('chatgpt-installation-purge/1')) fail('core-installation-purge-feature-missing')
  transaction.notifications = await purgeNotifications(config, io)
  if (transaction.notifications.status === 'incomplete') fail('native-notification-purge-incomplete')
  if (!transaction.completed.includes('installation-notifications-purged')) transaction.completed.push('installation-notifications-purged')
  transaction.core = await purgeCore(config, core, io)
  if (transaction.core.status !== 'complete') fail('core-installation-purge-incomplete')
  if (!transaction.completed.includes('installation-core-purged')) transaction.completed.push('installation-core-purged')
}
async function removePlugin(config, neverReady = false) {
  const pluginDir = join(config.dataDir, 'plugin'), connectionPath = join(pluginDir, '.mcp.json'), manifestPath = join(pluginDir, '.codex-plugin/plugin.json')
  if (await optional(pluginDir, lstat)) await privateDirectory(pluginDir)
  if (await optional(join(pluginDir, '.codex-plugin'), lstat)) await privateDirectory(join(pluginDir, '.codex-plugin'))
  const connection = await optional(connectionPath, json), metadata = await optional(manifestPath, json), receipt = await optional(join(config.dataDir, 'helper-installation.json'), json)
  if (!neverReady && (!connection || !metadata)) fail('plugin-files-missing-since-ready')
  let removedFiles = 0
  for (const [name, path, value] of [['connection', connectionPath, connection], ['manifest', manifestPath, metadata]]) {
    if (!value) continue
    if (!neverReady && (!digest(receipt.pluginFiles?.[name]) || receipt.pluginFiles[name] !== hash(await bytes(path, 16384, true)))) fail('plugin-files-changed-since-ready')
    if (name === 'connection' ? Object.keys(value.mcpServers ?? {}).length !== 1 || value.mcpServers.sentinel?.url !== 'http://127.0.0.1:' + config.port + '/mcp' || value.mcpServers.sentinel.http_headers?.Authorization !== 'Bearer ' + config.token : value.name !== 'ghost-sentinel' || value.version !== version || value.mcpServers !== './.mcp.json') fail('plugin-files-ownership-unconfirmed')
  }
  for (const path of [connectionPath, manifestPath]) if (await optional(path, lstat)) { await rm(path); removedFiles++ }
  for (const path of [join(pluginDir, '.codex-plugin'), pluginDir]) try { await rmdir(path) } catch (error) { if (!['ENOTEMPTY', 'ENOENT'].includes(error.code)) throw error }
  return { removedFiles, foreignFilesPreserved: Boolean(await optional(pluginDir, lstat)), registration: 'pending-first-party-tool' }
}
async function removePrivateRecords(config) {
  let cursors = 0, outboxFiles = 0, unknownCursorFiles = 0
  const directory = join(config.dataDir, 'cursors')
  if (await optional(directory, lstat)) {
    await privateDirectory(directory)
    const entries = await readdir(directory)
    if (entries.length > 1024) fail('private-record-count-limit')
    for (const name of entries) {
      if (!/^[a-f0-9]{64}\.json$/.test(name)) { unknownCursorFiles++; continue }
      const path = join(directory, name), value = await json(path, 262144)
      await loadCursor(path, { fork: value.format === 3 }); await rm(path); cursors++
    }
    try { await rmdir(directory) } catch (error) { if (error.code !== 'ENOTEMPTY') throw error }
  }
  const database = join(config.dataDir, 'health-outbox.sqlite')
  if (await optional(database, lstat)) {
    const data = await bytes(database, 33554432, true)
    if (!data.subarray(0, 16).equals(Buffer.from('SQLite format 3\0'))) fail('private-health-outbox-unconfirmed')
    for (const suffix of ['-wal', '-shm', '']) { const path = database + suffix; if (await optional(path, lstat)) { await bytes(path, 33554432, true); await rm(path); outboxFiles++ } }
  } else if (await optional(database + '-wal', lstat) || await optional(database + '-shm', lstat)) fail('orphan-private-health-outbox-unconfirmed')
  if (await optional(directory, lstat)) for (const name of await readdir(directory)) if (/^[a-f0-9]{64}\.json$/.test(name)) fail('private-cursor-residual')
  for (const suffix of ['', '-wal', '-shm']) if (await optional(database + suffix, lstat)) fail('private-health-outbox-residual')
  return { cursors, outboxFiles, unknownCursorFilesPreserved: unknownCursorFiles }
}
async function removeReadyInstallation(state, transaction, io) {
  const { config, options } = state, { operationId, completed } = transaction
  const markerPath = join(config.dataDir, 'maintenance.json'), operationPath = join(config.dataDir, 'helper-operation.json')
  transaction.phase = 'removing-installation-files'; await save(operationPath, transaction)
  if ((await bundle(options.destinationBundle, options.configPath, io)).manifestSHA256 !== state.installed.manifestSHA256) fail('installed-bundle-changed')
  await verifyRemovalConfig(state)
  transaction.privateRecords = await removePrivateRecords(config)
  transaction.plugin = await removePlugin(config)
  await rm(options.destinationBundle, { recursive: true }); completed.push('owned-bundle-and-plugin-files-removed')
  await verifyRemovalConfig(state)
  await rm(options.configPath); transaction.configRevoked = true; completed.push('installation-config-and-token-revoked')
  await rm(join(config.dataDir, 'helper-installation.json'))
  const marker = await json(markerPath, 1024)
  if (Object.keys(marker).length !== 3 || marker.operationId !== operationId || marker.installationId !== config.installationId || marker.format !== 1) fail('maintenance-marker-changed')
  await rm(markerPath)
  const unknownEntries = (await readdir(config.dataDir)).filter(name => name !== 'helper-operation.json')
  transaction.status = unknownEntries.length ? 'incomplete' : 'complete'; transaction.residual = { unknownPrivateEntries: unknownEntries.length }
  await save(operationPath, transaction)
  return { status: transaction.status, operationId, completed, collectionResumed: false, processExitCode: null, configRevoked: true, configPreserved: false, maintenancePreserved: false, ...(transaction.error ? { recoveredFailure: transaction.error } : {}), residual: transaction.residual, notifications: transaction.notifications, core: transaction.core, privateRecords: transaction.privateRecords, plugin: transaction.plugin }
}
async function resumeChange(state, io) {
  const { config, options, receipt } = state, operationPath = join(config.dataDir, 'helper-operation.json'), markerPath = join(config.dataDir, 'maintenance.json')
  const transaction = await json(operationPath), marker = await json(markerPath, 1024)
  const failedRegistration = transaction.operation === 'upgrade' && transaction.status === 'failed' && transaction.phase === 'registering-candidate' && ['SMAppService-unregistered', 'owned-processes-stopped', 'old-bundle-preserved', 'candidate-installed'].every(step => transaction.completed.includes(step))
  if (transaction.format !== 1 || transaction.operationId !== options.resume || transaction.operation !== options.operation || transaction.installationId !== config.installationId || transaction.configPath !== options.configPath || transaction.destinationBundle !== options.destinationBundle || transaction.manifestSHA256 !== state.installed.manifestSHA256 || transaction.configIdentity !== configIdentity(config) || receipt.operationId !== options.resume || !['pending-approval', 'pending-runtime-verification'].includes(transaction.status) && !failedRegistration || Object.keys(marker).length !== 3 || marker.format !== 1 || marker.operationId !== options.resume || marker.installationId !== config.installationId) fail('resume-operation-identity-mismatch')
  if (failedRegistration) {
    const expected = join(dirname(options.destinationBundle), '.GhostSentinel-backup-' + options.resume + '.app')
    if (transaction.backupBundle !== expected || !digest(transaction.previousManifestSHA256) || (await bundle(expected, options.configPath, io)).manifestSHA256 !== transaction.previousManifestSHA256) fail('failed-upgrade-backup-unconfirmed')
  }
  try {
    let status = await control(options.destinationBundle, '--status', io)
    if (failedRegistration && status.serviceStatus === 'notRegistered') {
      if (transaction.registrationRetryAttempted) fail('explicit-registration-retry-exhausted')
      const current = await configuration(options.configPath)
      if (configIdentity(current) !== transaction.configIdentity || current.port !== config.port || JSON.stringify(await json(markerPath, 1024)) !== JSON.stringify(marker)) fail('resume-registration-input-changed')
      transaction.registrationRetryAttempted = true; await save(operationPath, transaction)
      status = await control(options.destinationBundle, '--register', io)
      transaction.completed.push('explicit-register-retry-returned-' + status.serviceStatus)
    }
    if (status.serviceStatus !== 'enabled') { transaction.status = status.serviceStatus === 'requiresApproval' ? 'pending-approval' : 'pending-runtime-verification'; fail('helper-service-not-enabled') }
    await runtimeIdentity(state, io)
    const current = await configuration(options.configPath)
    if (configIdentity(current) !== transaction.configIdentity || current.port !== config.port || transaction.knownPort !== null && transaction.knownPort !== current.port) fail('installation-config-changed')
    if (transaction.operation === 'upgrade') {
      const expectedBackup = join(dirname(options.destinationBundle), '.GhostSentinel-backup-' + options.resume + '.app')
      if (transaction.backupBundle !== expectedBackup || !digest(transaction.previousManifestSHA256) || !transaction.completed.includes('SMAppService-unregistered')) fail('upgrade-backup-ownership-unconfirmed')
      if (await optional(expectedBackup, lstat)) {
        if ((await bundle(expectedBackup, options.configPath, io)).manifestSHA256 !== transaction.previousManifestSHA256) fail('upgrade-backup-fingerprint-mismatch')
        const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
        if (processes.includes(expectedBackup + '/Contents/')) fail('upgrade-backup-process-still-present')
        await rm(expectedBackup, { recursive: true })
      }
      transaction.completed.push('owned-backup-retired-after-ready')
    }
    const currentMarker = await json(markerPath, 1024)
    if (JSON.stringify(currentMarker) !== JSON.stringify(marker)) fail('maintenance-marker-changed')
    const pluginFiles = { connection: hash(await bytes(join(config.dataDir, 'plugin/.mcp.json'), 16384, true)), manifest: hash(await bytes(join(config.dataDir, 'plugin/.codex-plugin/plugin.json'), 16384, true)) }
    await save(join(config.dataDir, 'helper-installation.json'), { ...receipt, knownPort: current.port, pluginFiles, configFileIdentity: sameFile(await lstat(options.configPath)), configSHA256: hash(await bytes(options.configPath, 16384, true)) })
    transaction.status = 'complete'; transaction.completed.push('runtime-identity-verified', 'maintenance-released')
    await save(operationPath, transaction)
    if (JSON.stringify(await json(markerPath, 1024)) !== JSON.stringify(marker)) fail('maintenance-marker-changed')
    await rm(markerPath)
    let cleanupError
    try { await rm(operationPath) } catch (error) { cleanupError = safeError(error, config, options.configPath) }
    return { status: 'complete', operationId: options.resume, completed: transaction.completed, collectionResumed: true, ...(transaction.error ? { recoveredFailure: transaction.error } : {}), ...(cleanupError ? { cleanupError } : {}), pluginDir: join(config.dataDir, 'plugin'), pluginRegistration: 'pending-first-party-tool' }
  } catch (error) {
    if (transaction.status !== 'pending-approval') transaction.status = 'pending-runtime-verification'
    transaction.diagnostic = safeError(error, config, options.configPath)
    await save(operationPath, transaction)
    return { status: transaction.status, operationId: options.resume, completed: transaction.completed, collectionResumed: false, diagnostic: transaction.diagnostic }
  }
}
function safeError(error, config, configPath) {
  let message = String(error.message ?? error).slice(0, 2048)
  for (const secret of [config.token, config.installationId, config.pluginId, config.coreDataDir, config.metadataSocket, config.sessionsRoot, configPath]) message = message.replaceAll(secret, '[private]')
  message = message.replace(/[a-f0-9]{64}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/g, '[private]')
  return { name: String(error.name ?? 'Error'), code: typeof error.code === 'string' ? error.code : 'helper-operation-failed', ...(Number.isInteger(error.code) ? { exitCode: error.code } : {}), ...(Number.isInteger(error.status) ? { status: error.status } : {}), message }
}
async function verifyRemovalConfig(state) {
  const { config, options, receipt } = state
  const current = await configuration(options.configPath)
  if (configIdentity(current) !== receipt.configIdentity || current.port !== receipt.knownPort || receipt.configFileIdentity !== sameFile(await lstat(options.configPath)) || receipt.configSHA256 !== hash(await bytes(options.configPath, 16384, true))) fail('managed-config-changed-before-removal')
}
async function resumeUninstall(options, io, previewOnly = false) {
  const config = await configuration(options.configPath), operationPath = join(config.dataDir, 'helper-operation.json')
  const transaction = await json(operationPath)
  if (transaction.operation !== 'uninstall') return cancelInstall(options, io, previewOnly)
  const state = await inspect(options, io), marker = await json(join(config.dataDir, 'maintenance.json'), 1024), witness = transaction.ownedStop
  if (!uuid(options.resume) || transaction.format !== 1 || transaction.operationId !== options.resume || transaction.status !== 'failed' || transaction.phase !== 'owned-stop-confirmed' || transaction.installationId !== config.installationId || transaction.configIdentity !== configIdentity(config) || transaction.configPath !== options.configPath || transaction.destinationBundle !== options.destinationBundle || transaction.manifestSHA256 !== state.installed.manifestSHA256 || transaction.knownPort !== config.port || Object.keys(marker).length !== 3 || marker.format !== 1 || marker.operationId !== options.resume || marker.installationId !== config.installationId) fail('uninstall-resume-identity-mismatch')
  if (!witness || witness.format !== 1 || Object.keys(witness).length !== 11 || witness.protocol !== 'ghost-sentinel-chatgpt-helper/1' || witness.version !== version || !uuid(witness.runId) || witness.uid !== uid() || witness.nodeGone !== true || witness.wrapperGone !== true || !Number.isSafeInteger(witness.pid) || witness.pid < 2 || witness.child?.pid !== witness.pid || witness.child?.parent !== witness.wrapper?.pid || !Number.isSafeInteger(witness.wrapper?.pid) || witness.wrapper.pid < 2 || witness.wrapper.parent !== 1 || Object.keys(witness.child).length !== 3 || Object.keys(witness.wrapper).length !== 3 || !Number.isSafeInteger(witness.verifiedAt) || witness.verifiedAt < 1 || witness.verifiedAt > Date.now() || ![witness.child.started, witness.wrapper.started].every(value => typeof value === 'string' && value.length > 0 && value.length < 128 && !/[\x00-\x1f\x7f]/.test(value))) fail('uninstall-owned-stop-witness-unconfirmed')
  await verifyRemovalConfig(state)
  if (previewOnly) return { mode: 'preview', operation: 'uninstall', activation: 'not-requested', pending: ['recorded-owned-stop-revalidation', 'same-profile-core-cleanup', 'SM-and-file-cleanup'] }
  let core
  try {
    if (!await gone(witness.pid, io) || !await gone(witness.wrapper.pid, io) || !await endpointClosed(config, io) || await optional(join(config.dataDir, 'runtime.json'), lstat) || await optional(join(config.dataDir, 'runtime.lock'), lstat) || hasManagedProcess((await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout, options)) fail('uninstall-owned-stop-no-longer-confirmed')
    core = await managementCore(config, io)
    await waitFor(() => disconnected(config, core, io), 'owned-source-disconnect-unconfirmed')
    if (!transaction.completed.includes('owned-source-disconnected')) transaction.completed.push('owned-source-disconnected')
    await purgeManagedInstallation(config, transaction, core, io)
    await unregister(state, { wrapper: witness.wrapper }, transaction, io)
    return await removeReadyInstallation(state, transaction, io)
  } catch (error) {
    transaction.resumeError = safeError(error, config, options.configPath); transaction.status = 'failed'
    await save(operationPath, transaction)
    return { status: 'failed', operationId: options.resume, completed: transaction.completed, error: transaction.resumeError, originalFailure: transaction.error, collectionResumed: false, configRevoked: transaction.configRevoked === true, recovery: { operationReceipt: operationPath } }
  } finally { await core?.client?.close() }
}
async function cancelInstall(options, io, previewOnly = false) {
  if (!uuid(options.resume) || !absolute(options.destinationBundle) || basename(options.destinationBundle) !== 'GhostSentinel.app') fail('invalid-cancellation-options')
  await noLinks(dirname(options.destinationBundle))
  const config = await configuration(options.configPath), operationPath = join(config.dataDir, 'helper-operation.json'), markerPath = join(config.dataDir, 'maintenance.json')
  const transaction = await json(operationPath), marker = await json(markerPath, 1024)
  const staging = join(dirname(options.destinationBundle), '.GhostSentinel-' + options.resume + '.app')
  if (transaction.format !== 1 || transaction.operation !== 'install' || transaction.operationId !== options.resume || transaction.installationId !== config.installationId || transaction.configIdentity !== configIdentity(config) || transaction.configPath !== options.configPath || transaction.destinationBundle !== options.destinationBundle || transaction.staging !== staging || transaction.backupBundle !== null || !digest(transaction.manifestSHA256) || transaction.knownPort !== null && transaction.knownPort !== config.port || Object.keys(marker).length !== 3 || marker.format !== 1 || marker.installationId !== config.installationId || marker.operationId !== options.resume) fail('cancellation-operation-identity-mismatch')
  if (transaction.status === 'cancelled') {
    if (await optional(options.destinationBundle, lstat) || await optional(staging, lstat)) fail('cancelled-installation-residual-changed')
    return previewOnly ? { mode: 'preview', operation: 'uninstall', activation: 'not-requested', cancellation: 'already-cancelled' } : transaction.cancellationResult
  }
  if (!['preparing', 'pending-approval', 'pending-runtime-verification', 'failed'].includes(transaction.status) || !transaction.registrationWasAbsent || transaction.completed.includes('maintenance-released')) fail('cancellation-phase-unconfirmed')
  const receipt = await optional(join(config.dataDir, 'helper-installation.json'), json)
  if (receipt && (receipt.operationId !== options.resume || receipt.installationId !== config.installationId || receipt.configIdentity !== transaction.configIdentity || receipt.destinationBundle !== options.destinationBundle || receipt.manifestSHA256 !== transaction.manifestSHA256)) fail('cancellation-managed-receipt-mismatch')
  const ownedBundles = []
  for (const path of [options.destinationBundle, staging]) if (await optional(path, lstat)) {
    if ((await bundle(path, options.configPath, io)).manifestSHA256 !== transaction.manifestSHA256) fail('cancellation-bundle-fingerprint-mismatch')
    ownedBundles.push(path)
  }
  if (previewOnly) return { mode: 'preview', operation: 'uninstall', activation: 'not-requested', cancellation: 'own-never-ready-install', destinationBundle: options.destinationBundle, pending: ['owned-runtime-and-service-stop', 'available-core-and-native-cleanup', 'first-party-plugin-unregistration'] }
  const state = { config, options }, originalFailure = transaction.error
  try {
    const owner = await optional(join(config.dataDir, 'runtime.lock'), lstat), runtime = await optional(join(config.dataDir, 'runtime.json'), lstat)
    let owned
    if (owner || runtime) {
      if (!owner || !runtime || !ownedBundles.includes(options.destinationBundle)) fail('cancellation-runtime-partial-unconfirmed')
      owned = await stopNode(state, transaction, io)
      transaction.confirmedPort = config.port
    } else {
      if (transaction.knownPort === null && config.port !== undefined && transaction.confirmedPort !== config.port) fail('cancellation-first-port-unconfirmed')
      const processes = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
      if (hasManagedProcess(processes, options) || config.port !== undefined && !await endpointClosed(config, io)) fail('cancellation-absent-runtime-unconfirmed')
      transaction.completed.push('runtime-never-ready-or-already-stopped')
    }
    const coreRecord = await optional(join(config.coreDataDir, 'runtime.json'), json)
    let coreResult = { status: 'not-required-before-first-qualification' }
    if (coreRecord) {
      const core = await existingCore(config, io)
      await waitFor(() => disconnected(config, core, io), 'owned-source-disconnect-unconfirmed')
      coreResult = await purgeCore(config, core, io)
      if (coreResult.status !== 'complete') fail('core-installation-purge-incomplete')
    }
    const notifications = await purgeNotifications(config, io)
    if (notifications.status === 'incomplete') fail('native-notification-purge-incomplete')
    const controlRoot = ownedBundles.includes(options.destinationBundle) ? options.destinationBundle : ownedBundles[0]
    if (controlRoot) {
      const status = await control(controlRoot, '--status', io)
      if (status.serviceStatus !== 'notRegistered') {
        const unregistered = await control(controlRoot, '--unregister', io)
        if (!unregistered.unregisterCompleted || unregistered.serviceStatus !== 'notRegistered') fail('helper-unregister-unconfirmed')
      }
    } else if (transaction.completed.includes('candidate-installed')) fail('cancellation-installed-bundle-missing')
    if (owned) await waitFor(() => gone(owned.wrapper.pid, io), 'owned-wrapper-stop-unconfirmed')
    const remaining = (await io.execute('/bin/ps', ['-axo', 'uid=,pid=,args=', '-ww'])).stdout
    if (hasManagedProcess(remaining, options) || config.port !== undefined && !await endpointClosed(config, io) || await optional(join(config.dataDir, 'runtime.json'), lstat) || await optional(join(config.dataDir, 'runtime.lock'), lstat)) fail('cancellation-process-stop-unconfirmed')
    transaction.completed.push('SMAppService-unregistered', 'owned-processes-stopped')
    const privateRecords = await removePrivateRecords(config), plugin = await removePlugin(config, true)
    for (const path of ownedBundles) { if ((await bundle(path, options.configPath, io)).manifestSHA256 !== transaction.manifestSHA256) fail('cancellation-bundle-changed'); await rm(path, { recursive: true }) }
    if (receipt) await rm(join(config.dataDir, 'helper-installation.json'))
    const result = { status: 'cancelled', operationId: options.resume, cancelledOperation: 'install', completed: transaction.completed, collectionResumed: false, processExitCode: null, configPreserved: true, maintenancePreserved: true, core: coreResult, notifications, privateRecords, plugin, ...(originalFailure ? { originalFailure } : {}) }
    transaction.status = 'cancelled'; transaction.cancellationResult = result; await save(operationPath, transaction)
    return result
  } catch (error) {
    transaction.cancellationError = safeError(error, config, options.configPath); await save(operationPath, transaction)
    return { status: 'failed', operationId: options.resume, completed: transaction.completed, error: transaction.cancellationError, collectionResumed: false, configPreserved: true, recovery: { operationReceipt: operationPath } }
  }
}
export async function applyHelperChange(options, io = defaultIO) {
  if (io === defaultIO && (process.platform !== 'darwin' || process.version !== 'v24.21.0')) fail('macOS-Node24.21.0-required-for-apply')
  if (options.operation === 'uninstall' && options.resume) return resumeUninstall(options, io)
  const state = await inspect(options, io), { config } = state
  if (options.resume) return resumeChange(state, io)
  if (options.operation === 'install') {
    if (config.port !== undefined && !await endpointClosed(config, io)) fail('unmanaged-endpoint-requires-migration')
    if ((await control(options.candidateBundle, '--status', io)).serviceStatus !== 'notRegistered') fail('existing-helper-service-requires-migration')
  }
  const operationId = randomUUID(), completed = [], markerPath = join(config.dataDir, 'maintenance.json'), operationPath = join(config.dataDir, 'helper-operation.json')
  const staging = join(dirname(options.destinationBundle), '.GhostSentinel-' + operationId + '.app')
  const backupBundle = options.operation === 'upgrade' ? join(dirname(options.destinationBundle), '.GhostSentinel-backup-' + operationId + '.app') : null
  const transaction = { format: 1, operationId, operation: options.operation, installationId: config.installationId, configPath: options.configPath, configIdentity: configIdentity(config), knownPort: config.port ?? null, destinationBundle: options.destinationBundle, manifestSHA256: state.candidate?.manifestSHA256 ?? state.installed?.manifestSHA256, previousManifestSHA256: state.installed?.manifestSHA256, staging, backupBundle, registrationWasAbsent: options.operation === 'install', completed, status: 'preparing' }
  if (await optional(markerPath, lstat) || await optional(operationPath, lstat)) fail('existing-maintenance-operation-requires-review')
  await save(markerPath, { format: 1, installationId: config.installationId, operationId }, true)
  await save(operationPath, transaction, true)
  let coreClient
  try {
    completed.push('maintenance-established')
    if (options.operation !== 'install') {
      const stopped = await stopManaged(state, transaction, io)
      coreClient = stopped.core.client
      if (options.operation === 'uninstall') {
        await purgeManagedInstallation(config, transaction, stopped.core, io)
      }
      await unregister(state, stopped.owned, transaction, io)
    }
    if (options.operation === 'uninstall') return await removeReadyInstallation(state, transaction, io)
    await cp(options.candidateBundle, staging, { recursive: true, force: false, errorOnExist: true, dereference: false })
    if ((await bundle(staging, options.configPath, io)).manifestSHA256 !== state.candidate.manifestSHA256) fail('staged-candidate-changed')
    const current = await configuration(options.configPath)
    if (configIdentity(current) !== transaction.configIdentity || current.port !== config.port) fail('installation-config-changed')
    if (options.operation === 'upgrade') {
      if ((await bundle(options.destinationBundle, options.configPath, io)).manifestSHA256 !== state.installed.manifestSHA256) fail('installed-bundle-changed')
      if (await optional(backupBundle, lstat)) fail('backup-destination-already-exists')
      await rename(options.destinationBundle, backupBundle); completed.push('old-bundle-preserved')
    } else if (await optional(options.destinationBundle, lstat)) fail('destination-created-concurrently')
    await rename(staging, options.destinationBundle); completed.push('candidate-installed')
    const receipt = { format: 1, bundleId, installationId: config.installationId, configPath: options.configPath, configIdentity: configIdentity(config), knownPort: config.port ?? null, destinationBundle: options.destinationBundle, manifestSHA256: state.candidate.manifestSHA256, runtimeVersion: version, operationId }
    await save(join(config.dataDir, 'helper-installation.json'), receipt)
    transaction.phase = 'registering-candidate'; await save(operationPath, transaction)
    const registered = await control(options.destinationBundle, '--register', io)
    completed.push('register-returned-' + registered.serviceStatus)
    transaction.phase = 'awaiting-runtime'
    transaction.status = registered.serviceStatus === 'requiresApproval' ? 'pending-approval' : 'pending-runtime-verification'
    await save(operationPath, transaction)
    return { status: transaction.status, operationId, completed, collectionResumed: false, processExitCode: null, recovery: { backupBundle }, pluginDir: join(config.dataDir, 'plugin'), pluginRegistration: 'pending-first-party-tool' }
  } catch (error) {
    transaction.status = 'failed'; transaction.error = safeError(error, config, options.configPath)
    await save(operationPath, transaction)
    return { status: 'failed', operationId, completed, error: transaction.error, collectionResumed: false, processExitCode: null, configRevoked: transaction.configRevoked === true, ...(transaction.notifications ? { notifications: transaction.notifications } : {}), ...(transaction.core ? { core: transaction.core } : {}), recovery: { operationReceipt: operationPath, staging, backupBundle } }
  } finally { await coreClient?.close() }
}
