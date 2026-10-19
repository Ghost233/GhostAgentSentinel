import { open, lstat, readdir, mkdir, mkdtemp, writeFile, chmod, rename, rm, realpath, rmdir } from 'node:fs/promises'
import { constants } from 'node:fs'
import { join, resolve, dirname, basename, isAbsolute } from 'node:path'
import { fileURLToPath } from 'node:url'
import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { HELPER_VERSION } from './runtime.mjs'

export const PLUGIN_NODE = '/Applications/DSH Workflow.app/Contents/Resources/desktop/DeepSeek Harness.app/Contents/Resources/runtime/primary-runtime/dependencies/node/bin/node'
export const PLUGIN_NODE_SHA256 = '43aa9baf363e1e1ede7f7e4dd16e175793312d8ef0abd7b10868ba2ba3fe27f1'
const acceptedNodeHashes = new Set([PLUGIN_NODE_SHA256, '738852ca4f83d6a0485e4ae25e04b84243ae1d331e5f003b7cd0291cdf4b6821'])
const root = fileURLToPath(new URL('../', import.meta.url)), manifestName = 'plugin-package.json'
const maxFiles = 20000, maxSourceBytes = 100 * 1024 * 1024, maxBytes = 256 * 1024 * 1024, maxManifestBytes = 8 * 1024 * 1024
const hash = bytes => createHash('sha256').update(bytes).digest('hex'), execute = promisify(execFile)
const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/
const validVersion = value => typeof value === 'string' && value.length <= 128 && !/[\x00-\x20\x7f]/.test(value) && semver.test(value)
const required = ['package.json', 'package-lock.json', 'src/daemon.mjs', 'src/server.mjs', 'src/core-client.mjs', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'chatgpt/plugin-main.mjs', 'chatgpt/plugin-bridge.mjs', 'chatgpt/plugin-host.mjs', 'chatgpt/plugin-installation.mjs', 'chatgpt/plugin-package.mjs', 'scripts/manage-chatgpt-plugin.mjs', 'chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/adapter.mjs', 'chatgpt/bounded-reader.mjs', 'chatgpt/panel.html']
const excluded = new Set(['helper-main.mjs', 'helper-installation.mjs', 'run.mjs', '.mcp.json', 'installation.json'])
const identity = info => JSON.stringify([info.dev, info.ino, info.mode, info.uid, info.nlink, info.size, info.mtimeMs, info.ctimeMs])
const own = info => typeof process.getuid !== 'function' || info.uid === process.getuid()
const safe = (info, path) => { if (!own(info) || info.mode & 0o022) throw new Error('foreign writable package entry: ' + path) }
const validPath = path => typeof path === 'string' && path.length > 0 && path.length < 1024 && !/[\x00-\x1f\x7f\\]/.test(path) && !path.startsWith('/') && path.split('/').every(part => part && part !== '.' && part !== '..')
const ordered = entries => Object.fromEntries([...entries].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0))
const sourceEntries = files => ordered(Object.entries(files).filter(([path]) => path !== 'runtime/node' && !path.startsWith('notification/')).map(([path, value]) => [path, { sha256: value.sha256, size: value.size, mode: value.sourceMode }]))

async function regular(path, limit) {
  const before = await lstat(path)
  if (!before.isFile() || before.isSymbolicLink()) throw new Error('regular package file required: ' + path)
  safe(before, path)
  if (before.size > limit) throw new Error('package file size over budget: ' + path)
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW)
  try {
    const opened = await file.stat(), buffer = Buffer.alloc(before.size + 1)
    let length = 0
    while (length < buffer.length) { const result = await file.read(buffer, length, buffer.length - length, length); if (!result.bytesRead) break; length += result.bytesRead }
    const bytes = buffer.subarray(0, length)
    const [after, current] = await Promise.all([file.stat(), lstat(path)])
    if (bytes.length !== before.size || [opened, after, current].some(info => identity(info) !== identity(before))) throw new Error('package file changed during read: ' + path)
    return { bytes, sourceMode: before.mode & 0o7777, identity: identity(before) }
  } finally { await file.close() }
}

async function walk(directory, prefix = '', source = false, budget = { count: 0 }) {
  const before = await lstat(directory)
  if (!before.isDirectory() || before.isSymbolicLink()) throw new Error('regular package directory required: ' + directory)
  safe(before, directory)
  const files = [], directories = [{ path: prefix, mode: before.mode & 0o7777 }]
  for (const name of (await readdir(directory)).sort()) {
    if (source && ['.cache', '.bin', '.git', '.DS_Store'].includes(name)) continue
    if (++budget.count > maxFiles * 2) throw new Error('package entry count over budget')
    const path = join(directory, name), key = prefix ? prefix + '/' + name : name
    if (!validPath(key)) throw new Error('invalid package path: ' + key)
    const info = await lstat(path)
    if (info.isSymbolicLink()) throw new Error('unexpected package symlink: ' + key)
    safe(info, path)
    if (info.isDirectory()) { const nested = await walk(path, key, source, budget); files.push(...nested.files); directories.push(...nested.directories) }
    else if (info.isFile()) files.push({ path, key, identity: identity(info) })
    else throw new Error('nonregular package entry: ' + key)
    if (files.length > maxFiles) throw new Error('package file count over budget')
  }
  if (identity(await lstat(directory)) !== identity(before)) throw new Error('package directory changed during scan: ' + directory)
  return { files, directories }
}

async function sources() {
  const files = (await walk(join(root, 'src'), 'src', true)).files.filter(file => /\.(mjs|js|cjs|json)$/.test(file.key))
  for (const name of (await readdir(join(root, 'chatgpt'))).sort()) {
    if (excluded.has(name) || !/\.(mjs|js|cjs|html)$/.test(name) && !['package.json', 'package-lock.json'].includes(name)) continue
    const path = join(root, 'chatgpt', name), info = await lstat(path)
    if (!info.isFile() || info.isSymbolicLink()) throw new Error('regular production source required: ' + name)
    files.push({ path, key: 'chatgpt/' + name })
  }
  files.push(...(await walk(join(root, 'chatgpt/node_modules'), 'chatgpt/node_modules', true)).files)
  files.push(...['package.json', 'package-lock.json', 'scripts/manage-chatgpt-plugin.mjs'].map(key => ({ path: join(root, key), key })))
  if (files.length > maxFiles) throw new Error('package source count over budget')
  const entries = new Map(), bytes = new Map(), identities = new Map()
  let total = 0
  for (const file of files) {
    const value = await regular(file.path, maxSourceBytes - total)
    total += value.bytes.length
    entries.set(file.key, { sha256: hash(value.bytes), size: value.bytes.length, mode: value.sourceMode & 0o111 ? 0o700 : 0o600, sourceMode: value.sourceMode })
    bytes.set(file.key, value.bytes); identities.set(file.key, value.identity)
  }
  const values = ordered(entries)
  for (const path of required) if (!values[path]) throw new Error('missing production runtime source: ' + path)
  for (const name of ['@modelcontextprotocol/sdk', '@modelcontextprotocol/ext-apps', 'stream-chain', 'stream-json', 'ws', 'zod']) if (!values['chatgpt/node_modules/' + name + '/package.json']) throw new Error('missing production dependency: ' + name)
  return { files: values, bytes, identities: ordered(identities) }
}

function report(directory, manifest, digest) {
  return { root: directory, digest, manifest, nodePath: join(directory, 'runtime/node'), bridgePath: join(directory, 'chatgpt/plugin-bridge.mjs'), ownerPath: join(directory, 'chatgpt/plugin-main.mjs'), notificationApp: manifest.notificationPath ? join(directory, manifest.notificationPath) : null, signed: false, notarized: false, installed: false, registered: false, launched: false }
}

export async function inspectPluginPackage(directory, { digest } = {}) {
  if (typeof directory !== 'string' || !isAbsolute(directory) || /[\n\0]/.test(directory)) throw new Error('absolute package root required')
  directory = resolve(directory)
  const rootInfo = await lstat(directory)
  if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) throw new Error('regular package directory required')
  safe(rootInfo, directory)
  directory = await realpath(directory)
  const manifestFile = await regular(join(directory, manifestName), maxManifestBytes), actualDigest = hash(manifestFile.bytes)
  if (digest !== undefined && (!/^[a-f0-9]{64}$/.test(digest) || digest !== actualDigest)) throw new Error('package manifest digest mismatch')
  const manifest = JSON.parse(manifestFile.bytes)
  if (!manifest || Object.keys(manifest).sort().join(',') !== 'files,format,kind,node,notificationPath,runtimeVersion,sourceSetSHA256,version' || manifest.format !== 1 || manifest.kind !== 'ghost-sentinel-plugin-payload' || !validVersion(manifest.version) || manifest.runtimeVersion !== HELPER_VERSION || manifest.node?.path !== 'runtime/node' || manifest.node.version !== 'v24.21.0' || !acceptedNodeHashes.has(manifest.node.sha256) || Object.keys(manifest.node).sort().join(',') !== 'path,sha256,version' || ![null, 'notification/GhostSentinelNotifications.app'].includes(manifest.notificationPath)) throw new Error('invalid package manifest')
  if (!manifest.files || Array.isArray(manifest.files) || typeof manifest.files !== 'object' || Object.keys(manifest.files).length > maxFiles) throw new Error('invalid package file set')
  let total = 0, sourceTotal = 0
  for (const [path, value] of Object.entries(manifest.files)) {
    const allowed = path === 'runtime/node' || ['package.json', 'package-lock.json', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'scripts/manage-chatgpt-plugin.mjs'].includes(path) || path.startsWith('src/') && /\.(mjs|js|cjs|json)$/.test(path) || path.startsWith('chatgpt/node_modules/') || /^chatgpt\/[^/]+\.(mjs|js|cjs|html)$/.test(path) && !excluded.has(path.slice('chatgpt/'.length)) || path.startsWith('notification/GhostSentinelNotifications.app/')
    if (!validPath(path) || !allowed || path === manifestName || path.split('/').includes('LaunchAgents') || /\.(swift|plist)$/.test(path) && !path.startsWith('notification/')) throw new Error('invalid package file path: ' + path)
    if (!value || Object.keys(value).sort().join(',') !== 'mode,sha256,size,sourceMode' || !/^[a-f0-9]{64}$/.test(value.sha256) || !Number.isSafeInteger(value.size) || value.size < 0 || ![0o600, 0o700].includes(value.mode) || !Number.isSafeInteger(value.sourceMode) || value.sourceMode < 0 || value.sourceMode > 0o7777 || value.sourceMode & 0o022 || value.mode !== (value.sourceMode & 0o111 ? 0o700 : 0o600)) throw new Error('invalid package file metadata: ' + path)
    if ((total += value.size) > maxBytes) throw new Error('package bytes over budget')
    if (path !== 'runtime/node' && !path.startsWith('notification/') && (sourceTotal += value.size) > maxSourceBytes) throw new Error('package source bytes over budget')
  }
  for (const path of [...required, 'runtime/node']) if (!manifest.files[path]) throw new Error('missing production runtime file: ' + path)
  if (manifest.files['runtime/node'].sha256 !== manifest.node.sha256 || manifest.files['runtime/node'].mode !== 0o700) throw new Error('pinned executable Node required')
  if (manifest.notificationPath && !manifest.files[manifest.notificationPath + '/Contents/MacOS/GhostSentinelNotifications']) throw new Error('missing notification executable')
  if (Object.keys(manifest.files).some(path => path.startsWith('notification/')) !== Boolean(manifest.notificationPath)) throw new Error('unexpected notification file set')
  if (hash(JSON.stringify(sourceEntries(manifest.files))) !== manifest.sourceSetSHA256) throw new Error('package source set hash mismatch')
  const tree = await walk(directory), expectedPaths = [...Object.keys(manifest.files), manifestName].sort()
  if (JSON.stringify(tree.files.map(file => file.key).sort()) !== JSON.stringify(expectedPaths)) throw new Error('package file set mismatch')
  const expectedDirectories = new Set([''])
  for (const path of expectedPaths) { let parent = dirname(path); while (parent !== '.') { expectedDirectories.add(parent); parent = dirname(parent) } }
  if (tree.directories.some(value => !expectedDirectories.has(value.path) || value.mode !== 0o700)) throw new Error('unexpected package directory or mode')
  for (const file of tree.files) {
    const value = await regular(file.path, file.key === manifestName ? maxManifestBytes : manifest.files[file.key].size)
    if (value.identity !== file.identity) throw new Error('package file changed during inspection: ' + file.key)
    if (file.key === manifestName) { if (hash(value.bytes) !== actualDigest || value.sourceMode !== 0o600) throw new Error('package manifest changed during inspection'); continue }
    const expected = manifest.files[file.key]
    if (value.sourceMode !== expected.mode) throw new Error('package file mode mismatch: ' + file.key)
    if (value.bytes.length !== expected.size || hash(value.bytes) !== expected.sha256) throw new Error('package file hash or size changed: ' + file.key)
  }
  const after = await walk(directory)
  if (JSON.stringify(after.files) !== JSON.stringify(tree.files) || JSON.stringify(after.directories) !== JSON.stringify(tree.directories)) throw new Error('package file set changed during inspection')
  return report(directory, manifest, actualDigest)
}

export async function buildPluginPackage({ outputDir, notificationApp, version } = {}) {
  if (process.platform !== 'darwin' || process.version !== 'v24.21.0' || await realpath(process.execPath) !== await realpath(PLUGIN_NODE)) throw new Error('fixed macOS Node24.21.0 builder required')
  if (typeof outputDir !== 'string' || !isAbsolute(outputDir) || /[\n\0]/.test(outputDir)) throw new Error('absolute new package output required')
  if (version !== undefined && !validVersion(version)) throw new Error('finite semantic package version required')
  outputDir = resolve(outputDir)
  if (await lstat(outputDir).catch(error => { if (error.code === 'ENOENT') return null; throw error })) throw new Error('package output already exists')
  const parent = dirname(outputDir), parentInfo = await lstat(parent)
  if (!parentInfo.isDirectory() || parentInfo.isSymbolicLink()) throw new Error('regular package output parent required')
  safe(parentInfo, parent)
  outputDir = join(await realpath(parent), basename(outputDir))
  const node = await regular(PLUGIN_NODE, maxBytes)
  if (hash(node.bytes) !== PLUGIN_NODE_SHA256 || !(node.sourceMode & 0o111)) throw new Error('pinned Node executable changed')
  const source = await sources(), declared = JSON.parse(source.bytes.get('chatgpt/package.json'))
  version ??= declared.version
  if (!validVersion(version)) throw new Error('finite semantic package version required')
  let notification, notificationPath = null
  if (notificationApp !== undefined) {
    if (typeof notificationApp !== 'string' || !isAbsolute(notificationApp) || !notificationApp.endsWith('.app')) throw new Error('absolute existing notification app required')
    notification = await walk(notificationApp, 'notification/GhostSentinelNotifications.app')
    const { stdout } = await execute('/usr/bin/plutil', ['-extract', 'CFBundleIdentifier', 'raw', '-o', '-', join(notificationApp, 'Contents/Info.plist')], { timeout: 10000, maxBuffer: 1048576 })
    if (stdout.trim() !== 'com.ghostsentinel.notifications') throw new Error('notification component identity mismatch')
    await execute('/usr/bin/codesign', ['--verify', '--strict', notificationApp], { timeout: 10000, maxBuffer: 1048576 })
    notificationPath = 'notification/GhostSentinelNotifications.app'
  }
  const bytes = new Map(source.bytes), entries = new Map(Object.entries(source.files))
  bytes.set('runtime/node', node.bytes); entries.set('runtime/node', { sha256: PLUGIN_NODE_SHA256, size: node.bytes.length, mode: 0o700, sourceMode: node.sourceMode })
  const notificationIdentities = []
  let notificationBytesRemaining = maxBytes - [...bytes.values()].reduce((total, value) => total + value.length, 0)
  if (notification) for (const file of notification.files) {
    const value = await regular(file.path, notificationBytesRemaining)
    notificationBytesRemaining -= value.bytes.length
    bytes.set(file.key, value.bytes); entries.set(file.key, { sha256: hash(value.bytes), size: value.bytes.length, mode: value.sourceMode & 0o111 ? 0o700 : 0o600, sourceMode: value.sourceMode }); notificationIdentities.push([file.path, value.identity])
  }
  if (entries.size > maxFiles || [...bytes.values()].reduce((total, value) => total + value.length, 0) > maxBytes) throw new Error('package count or bytes over budget')
  const files = ordered(entries), manifest = { format: 1, kind: 'ghost-sentinel-plugin-payload', version, runtimeVersion: HELPER_VERSION, node: { path: 'runtime/node', version: 'v24.21.0', sha256: PLUGIN_NODE_SHA256 }, sourceSetSHA256: hash(JSON.stringify(sourceEntries(files))), files, notificationPath }
  const temporary = await mkdtemp(join(parent, '.' + basename(outputDir) + '.partial-'))
  let reserved = false
  try {
    await chmod(temporary, 0o700)
    for (const [path, value] of Object.entries(files)) {
      const target = join(temporary, path)
      await mkdir(dirname(target), { recursive: true, mode: 0o700 }); await writeFile(target, bytes.get(path), { mode: value.mode, flag: 'wx' }); await chmod(target, value.mode)
    }
    await writeFile(join(temporary, manifestName), JSON.stringify(manifest, null, 2) + '\n', { mode: 0o600, flag: 'wx' })
    const verified = await inspectPluginPackage(temporary)
    const current = await sources()
    if (JSON.stringify(current.files) !== JSON.stringify(source.files) || JSON.stringify(current.identities) !== JSON.stringify(source.identities)) throw new Error('production source changed during package build')
    if ((await regular(PLUGIN_NODE, maxBytes)).identity !== node.identity) throw new Error('pinned Node changed during package build')
    if (notification) {
      const currentNotification = await walk(notificationApp, notificationPath)
      if (JSON.stringify(currentNotification.files) !== JSON.stringify(notification.files)) throw new Error('notification file set changed during package build')
      for (const [path, before] of notificationIdentities) if ((await regular(path, maxBytes)).identity !== before) throw new Error('notification changed during package build')
    }
    await mkdir(outputDir, { mode: 0o700 }); reserved = true
    await rename(temporary, outputDir); reserved = false
    return report(outputDir, manifest, verified.digest)
  } finally {
    await rm(temporary, { recursive: true, force: true })
    if (reserved) await rmdir(outputDir).catch(() => {})
  }
}
