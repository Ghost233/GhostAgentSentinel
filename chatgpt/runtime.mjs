import { mkdir, open, readFile, writeFile, rename, lstat, rm } from 'node:fs/promises'
import { join, resolve, isAbsolute } from 'node:path'
import { homedir } from 'node:os'
import { randomBytes, randomUUID } from 'node:crypto'
import { connectCore } from '../src/core-client.mjs'
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

async function writePrivateJson(path, value) { await writeFile(path + '.tmp', JSON.stringify(value, null, 2) + '\n', { mode: 0o600 }); await rename(path + '.tmp', path) }

export async function startFromInstallation({ configPath, pollMs = 1500, native, notificationIO } = {}) {
  if (typeof configPath !== 'string' || !isAbsolute(configPath)) throw new Error('explicit private configuration path required')
  const config = await readPrivateConfig(configPath)
  const lock = join(config.dataDir, 'runtime.lock'), runId = randomUUID()
  try { await mkdir(lock, { mode: 0o700 }) } catch (error) { if (error.code === 'EEXIST') throw new Error('installation helper already running or requires lifecycle recovery'); throw error }
  let core, adapter, closing = null
  await writePrivateJson(join(lock, 'owner.json'), { pid: process.pid, runId })
  const close = () => closing ??= (async () => {
    try { await adapter?.close() } finally {
      await core?.close()
      const owner = JSON.parse(await readFile(join(lock, 'owner.json'), 'utf8'))
      if (owner.runId === runId) { await rm(join(config.dataDir, 'runtime.json'), { force: true }); await rm(lock, { recursive: true, force: true }) }
    }
  })()
  try {
    core = await connectCore({ dataDir: config.coreDataDir })
    const hostIO = native ?? createNativeIO({ pluginId: config.pluginId, socketPath: config.metadataSocket })
    const notifier = notificationIO ?? (config.notificationBundlePath ? createNativeNotificationIO({ bundlePath: config.notificationBundlePath, installationId: config.installationId }) : undefined)
    adapter = await startChatGPTAdapter({ core, native: hostIO, notificationIO: notifier, sessionsRoot: config.sessionsRoot, dataDir: config.dataDir, installationId: config.installationId, token: config.token, pollMs, collectMaterials: true })
    const pluginDir = join(config.dataDir, 'plugin')
    await mkdir(join(pluginDir, '.codex-plugin'), { recursive: true, mode: 0o700 })
    await writePrivateJson(join(pluginDir, '.codex-plugin/plugin.json'), { name: 'ghost-sentinel', version: '0.1.0-dev.0', description: 'ChatGPT.app 本机主会话只读检查；当前开发候选不含正式常驻安装。', mcpServers: './.mcp.json', interface: { displayName: 'Sentinel 会话检查', shortDescription: '本机主会话 · 只读检查' } })
    await writePrivateJson(join(pluginDir, '.mcp.json'), { mcpServers: { sentinel: { url: adapter.url + '/mcp', http_headers: { Authorization: 'Bearer ' + config.token } } } })
    await writePrivateJson(join(config.dataDir, 'runtime.json'), { format: 1, pid: process.pid, runId, installationId: config.installationId, url: adapter.url, startedAt: Date.now() })
    return { url: adapter.url, pluginDir, refresh: adapter.refresh, diagnostics: adapter.diagnostics, close }
  } catch (error) { await close(); throw error }
}
