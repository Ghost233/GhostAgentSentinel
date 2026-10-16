import { mkdir, open, writeFile, rename, lstat, rm } from 'node:fs/promises'
import { join, resolve, isAbsolute } from 'node:path'
import { homedir } from 'node:os'
import { randomBytes, randomUUID } from 'node:crypto'
import { constants } from 'node:fs'
import { setTimeout as delay } from 'node:timers/promises'
import { createLazyCore, retentionProfileSignature } from './lazy-core.mjs'
import { createNativeIO } from './native-io.mjs'
import { startChatGPTAdapter } from './adapter.mjs'
import { createNativeNotificationIO } from './native-notifications.mjs'

async function readPrivateConfig(path) {
  const info = await lstat(path)
  if (!info.isFile() || info.size > 16384 || (info.mode & 0o077) || typeof process.getuid === 'function' && info.uid !== process.getuid()) throw new Error('private configuration required')
  const file = await open(path, 'r')
  let value
  try { const buffer = Buffer.alloc(16385); const result = await file.read(buffer, 0, buffer.length, 0); if (result.bytesRead > 16384) throw new Error('private configuration over limit'); value = JSON.parse(buffer.subarray(0, result.bytesRead)) } finally { await file.close() }
  if (value.format !== 1 || !/^[a-f0-9]{64}$/.test(value.token ?? '') || typeof value.installationId !== 'string' || !value.installationId || typeof value.pluginId !== 'string' || !value.pluginId || !['dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket'].every(key => typeof value[key] === 'string' && isAbsolute(value[key]))) throw new Error('invalid installation configuration')
  if (value.port !== undefined && (!Number.isSafeInteger(value.port) || value.port < 1 || value.port > 65535)) throw new Error('invalid installation port')
  if (value.notificationBundlePath !== undefined && (typeof value.notificationBundlePath !== 'string' || !isAbsolute(value.notificationBundlePath) || !value.notificationBundlePath.endsWith('.app'))) throw new Error('invalid notification bundle configuration')
  return value
}

export async function prepareInstallation({ dataDir, coreDataDir, sessionsRoot = join(homedir(), '.codex/sessions'), metadataSocket = join(homedir(), '.codex/app-server-control/app-server-control.sock'), pluginId, notificationBundlePath } = {}) {
  if (typeof dataDir !== 'string' || typeof coreDataDir !== 'string' || !isAbsolute(dataDir) || !isAbsolute(coreDataDir) || !isAbsolute(sessionsRoot) || !isAbsolute(metadataSocket) || typeof pluginId !== 'string' || !pluginId.trim()) throw new Error('explicit installation paths and plugin identity required')
  if (notificationBundlePath !== undefined && (typeof notificationBundlePath !== 'string' || !isAbsolute(notificationBundlePath) || !notificationBundlePath.endsWith('.app'))) throw new Error('explicit notification bundle path required')
  if (notificationBundlePath !== undefined) notificationBundlePath = resolve(notificationBundlePath)
  dataDir = resolve(dataDir)
  await mkdir(dataDir, { recursive: true, mode: 0o700 })
  const info = await lstat(dataDir)
  if (!info.isDirectory() || info.mode & 0o077) throw new Error('private installation directory required')
  const configPath = join(dataDir, 'installation.json'), pluginDir = join(dataDir, 'plugin')
  try {
    const file = await open(configPath, 'wx', 0o600)
    try { await file.writeFile(JSON.stringify({ format: 1, installationId: randomUUID(), dataDir, coreDataDir: resolve(coreDataDir), sessionsRoot: resolve(sessionsRoot), metadataSocket, pluginId, ...(notificationBundlePath === undefined ? {} : { notificationBundlePath }), token: randomBytes(32).toString('hex') }) + '\n') } finally { await file.close() }
  } catch (error) { if (error.code !== 'EEXIST') throw error }
  const saved = await readPrivateConfig(configPath)
  if (['dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket', 'pluginId'].some(key => saved[key] !== ({ dataDir, coreDataDir: resolve(coreDataDir), sessionsRoot: resolve(sessionsRoot), metadataSocket, pluginId })[key])) throw new Error('installation configuration already exists with different settings')
  if (notificationBundlePath !== undefined && saved.notificationBundlePath !== notificationBundlePath) throw new Error('installation configuration already exists with different settings')
  await mkdir(join(pluginDir, '.codex-plugin'), { recursive: true, mode: 0o700 })
  return { configPath, pluginDir }
}

export const HELPER_PROTOCOL = 'ghost-sentinel-chatgpt-helper/1'
export const HELPER_VERSION = '0.1.0-dev.1'

const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/
async function maintenance(config) {
  const path = join(config.dataDir, 'maintenance.json'), unconfirmed = 'installation-maintenance-unconfirmed'
  const identity = info => JSON.stringify([info.dev, info.ino, info.size, info.mtimeMs, info.ctimeMs, info.uid, info.mode])
  let file, before
  try { before = await lstat(path) } catch (error) { return error.code === 'ENOENT' ? null : unconfirmed }
  if (!before.isFile() || before.size > 1024 || (before.mode & 0o7777) !== 0o600 || typeof process.getuid === 'function' && before.uid !== process.getuid()) return unconfirmed
  try {
    file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
    const opened = await file.stat(), bytes = Buffer.alloc(1025), read = await file.read(bytes, 0, bytes.length, 0)
    const [after, current] = await Promise.all([file.stat(), lstat(path)])
    if (read.bytesRead > 1024 || read.bytesRead !== opened.size || [opened, after, current].some(info => identity(info) !== identity(before))) return unconfirmed
    const value = JSON.parse(bytes.subarray(0, read.bytesRead))
    return value && Object.keys(value).length === 3 && value.format === 1 && value.installationId === config.installationId && uuid.test(value.operationId) ? 'installation-maintenance' : unconfirmed
  } catch { return unconfirmed }
  finally { await file?.close().catch(() => {}) }
}

async function writePrivateJson(path, value) {
  const temporary = path + '.' + randomUUID() + '.tmp'
  try { await writeFile(temporary, JSON.stringify(value, null, 2) + '\n', { mode: 0o600, flag: 'wx' }); await rename(temporary, path) }
  finally { await rm(temporary, { force: true }) }
}
async function record(path) {
  try {
    const info = await lstat(path)
    if (!info.isFile() || info.size > 16384 || info.mode & 0o077 || typeof process.getuid === 'function' && info.uid !== process.getuid()) throw new Error('untrusted helper record')
    const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
    try { const buffer = Buffer.alloc(16385), value = await file.read(buffer, 0, buffer.length, 0); if (value.bytesRead > 16384) throw new Error('helper record over limit'); return JSON.parse(buffer.subarray(0, value.bytesRead)) }
    finally { await file.close() }
  } catch (error) { if (error.code === 'ENOENT') return null; throw error }
}
const ownIdentity = (value, config) => value?.protocol === HELPER_PROTOCOL && value.installationId === config.installationId && value.version === HELPER_VERSION && Number.isSafeInteger(value.pid) && value.pid > 0 && typeof value.runId === 'string' && /^[a-f0-9-]{36}$/.test(value.runId)
const identityMatches = (a, b) => ['protocol', 'installationId', 'version', 'pid', 'runId'].every(key => a[key] === b[key])
const portOf = url => { const parsed = new URL(url); if (parsed.protocol !== 'http:' || parsed.hostname !== '127.0.0.1' || parsed.username || parsed.password || parsed.pathname !== '/' || parsed.search || parsed.hash || !parsed.port) throw new Error('untrusted helper endpoint'); return Number(parsed.port) }
async function stablePort(config) {
  const connection = await record(join(config.dataDir, 'plugin/.mcp.json'))
  if (!connection) return config.port ?? 0
  const entry = connection.mcpServers?.sentinel
  if (Object.keys(connection.mcpServers ?? {}).length !== 1 || entry?.http_headers?.Authorization !== 'Bearer ' + config.token) throw new Error('untrusted helper endpoint')
  const port = portOf(entry.url.replace(/\/mcp$/, '/'))
  if (entry.url !== `http://127.0.0.1:${port}/mcp` || config.port !== undefined && config.port !== port) throw new Error('helper endpoint differs from installation')
  return port
}
async function existing(config, owner, port) {
  if (!ownIdentity(owner, config)) throw new Error('untrusted helper owner')
  const runtime = await record(join(config.dataDir, 'runtime.json'))
  if (runtime) {
    if (!ownIdentity(runtime, config) || !identityMatches(runtime, owner)) throw new Error('untrusted helper endpoint')
    const recordedPort = portOf(runtime.url)
    if (port && recordedPort !== port) throw new Error('untrusted helper endpoint')
  }
  try { process.kill(owner.pid, 0) } catch (error) { if (error.code === 'ESRCH') return { dead: true }; throw new Error('helper owner cannot be verified') }
  if (!runtime) return null
  let response
  try { response = await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token }, redirect: 'error', signal: AbortSignal.timeout(750) }) }
  catch { throw new Error('live helper endpoint cannot be verified') }
  if (response.status !== 200 || !identityMatches(await response.json(), owner)) throw new Error('live helper endpoint identity mismatch')
  return runtime
}

export async function startFromInstallation({ configPath, pollMs = 1500, native, notificationIO } = {}) {
  if (typeof configPath !== 'string' || !isAbsolute(configPath)) throw new Error('explicit private configuration path required')
  const config = await readPrivateConfig(configPath), pluginDir = join(config.dataDir, 'plugin')
  const info = await lstat(config.dataDir)
  if (!info.isDirectory() || info.mode & 0o077 || typeof process.getuid === 'function' && info.uid !== process.getuid()) throw new Error('private installation directory required')
  const lock = join(config.dataDir, 'runtime.lock'), port = await stablePort(config), runId = randomUUID()
  const identity = { protocol: HELPER_PROTOCOL, installationId: config.installationId, runId, pid: process.pid, version: HELPER_VERSION }
  const deadline = Date.now() + 2500
  while (true) {
    try { await mkdir(lock, { mode: 0o700 }); break }
    catch (error) { if (error.code !== 'EEXIST') throw error }
    const owner = await record(join(lock, 'owner.json'))
    if (owner) {
      const live = await existing(config, owner, port)
      if (live?.dead) {
        const current = await record(join(lock, 'owner.json'))
        if (current && identityMatches(current, owner)) { await rm(join(config.dataDir, 'runtime.json'), { force: true }); await rm(lock, { recursive: true, force: true }) }
        continue
      }
      if (live) return { ...live, reused: true, pluginDir, refresh: async () => {}, diagnostics: () => ({ reused: true }), close: async () => {} }
    }
    if (Date.now() >= deadline) throw new Error('helper owner startup or endpoint cannot be verified')
    await delay(25)
  }
  let adapter, closing = null, stopping = false, retentionTimer = null, retentionPending = null, retentionSignature = null, retentionNextAt = null, retentionError = null
  function retentionObserved(value, error) {
    if (stopping) return
    const next = value?.retention ? value.retention.nextExpiryAt : value?.nextExpiryAt, generatedAt = value?.generatedAt
    if (error || !Number.isSafeInteger(generatedAt) || next !== null && (!Number.isSafeInteger(next) || next <= generatedAt)) {
      retentionError = error?.code ?? 'retention_deadline_unconfirmed'; retentionNextAt = Date.now() + 60000
    } else { retentionError = null; retentionNextAt = next === null ? null : Date.now() + next - generatedAt }
    scheduleRetention()
  }
  const core = createLazyCore({ dataDir: config.coreDataDir, onRetention: retentionObserved })
  function scheduleRetention() {
    clearTimeout(retentionTimer); retentionTimer = null
    if (stopping) return
    const delay = retentionNextAt === null ? 60000 : Math.max(1000, Math.min(60000, retentionNextAt - Date.now()))
    retentionTimer = setTimeout(() => { retentionTimer = null; void maintainRetention() }, delay)
    retentionTimer.unref()
  }
  function maintainRetention(force = false) {
    if (stopping) return Promise.resolve()
    if (retentionPending) return retentionPending
    retentionPending = (async () => {
      try {
        adapter?.diagnostics()
        const signature = await retentionProfileSignature(config.coreDataDir)
        if (stopping) return
        if (!signature) { retentionSignature = null; retentionNextAt = null; retentionError = null; return }
        if (!force && signature === retentionSignature && (retentionNextAt === null || retentionNextAt > Date.now())) return
        const result = await core.maintenance()
        if (stopping) return
        retentionSignature = await retentionProfileSignature(config.coreDataDir)
        if (result) retentionObserved(result)
      } catch (error) { retentionError = error.code ?? 'retention_maintenance_unavailable'; retentionNextAt = Date.now() + 60000 }
    })().finally(() => { retentionPending = null; scheduleRetention() })
    return retentionPending
  }
  const close = () => closing ??= (async () => {
    stopping = true; clearTimeout(retentionTimer); retentionTimer = null
    try { const stopped = adapter?.close(); await core.cancelPending(); await retentionPending; await stopped } finally {
      try { await core.close() } finally {
        const owner = await record(join(lock, 'owner.json'))
        if (owner && identityMatches(owner, identity)) {
          const value = await record(join(config.dataDir, 'runtime.json'))
          if (value && identityMatches(value, identity)) await rm(join(config.dataDir, 'runtime.json'), { force: true })
          await rm(lock, { recursive: true, force: true })
        }
      }
    }
  })()
  try {
    await writePrivateJson(join(lock, 'owner.json'), identity)
    await maintainRetention(true)
    const rawHostIO = native ?? createNativeIO({ pluginId: config.pluginId, socketPath: config.metadataSocket })
    const hostIO = { ...rawHostIO, async sampleHost() { const reason = await maintenance(config); return reason ? { available: false, reason } : rawHostIO.sampleHost() } }
    const notifier = notificationIO ?? (config.notificationBundlePath ? createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId }) : undefined)
    adapter = await startChatGPTAdapter({ core, native: hostIO, notificationIO: notifier, sessionsRoot: config.sessionsRoot, dataDir: config.dataDir, installationId: config.installationId, token: config.token, port, pollMs, collectMaterials: true, nativeOriginVersion: 2, runtimeStatus: () => ({ ...identity }) })
    adapter.diagnostics()
    const selectedPort = portOf(adapter.url)
    if (port && selectedPort !== port) throw new Error('helper changed its configured endpoint')
    if (config.port === undefined) await writePrivateJson(configPath, { ...config, port: selectedPort })
    await mkdir(join(pluginDir, '.codex-plugin'), { recursive: true, mode: 0o700 })
    await writePrivateJson(join(pluginDir, '.codex-plugin/plugin.json'), { name: 'ghost-sentinel', version: HELPER_VERSION, description: 'ChatGPT.app 本机主会话只读检查', mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查', shortDescription: '本机主会话 · 只读检查' } })
    await writePrivateJson(join(pluginDir, '.mcp.json'), { mcpServers: { sentinel: { url: adapter.url + '/mcp', http_headers: { Authorization: 'Bearer ' + config.token } } } })
    const value = { format: 1, ...identity, url: adapter.url, startedAt: Date.now() }
    await writePrivateJson(join(config.dataDir, 'runtime.json'), value)
    return { ...value, reused: false, pluginDir, refresh: adapter.refresh, maintainRetention, diagnostics: () => ({ ...adapter.diagnostics(), retention: { timers: retentionTimer ? 1 : 0, pending: retentionPending ? 1 : 0, nextAt: retentionNextAt, errorCode: retentionError } }), close }
  } catch (error) { await close(); throw error }
}
