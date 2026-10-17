import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { mkdir, readFile, writeFile, readdir, lstat, realpath, copyFile, chmod, access } from 'node:fs/promises'
import { constants } from 'node:fs'
import { join, resolve, isAbsolute, sep, dirname, basename } from 'node:path'
import { fileURLToPath } from 'node:url'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'

const execute = promisify(execFile)
const root = fileURLToPath(new URL('../', import.meta.url))
export const HELPER_BUNDLE_ID = 'com.ghostsentinel.helper'
export const HELPER_VERSION = '0.1.0-dev.1'
export const HELPER_NODE = '/Applications/DSH Workflow.app/Contents/Resources/desktop/DeepSeek Harness.app/Contents/Resources/runtime/primary-runtime/dependencies/node/bin/node'
export const HELPER_NODE_SHA256 = '738852ca4f83d6a0485e4ae25e04b84243ae1d331e5f003b7cd0291cdf4b6821'
const hash = bytes => createHash('sha256').update(bytes).digest('hex')
const run = (file, args, options = {}) => execute(file, args, { shell: false, timeout: 120000, maxBuffer: 1048576, ...options })
const required = ['chatgpt/helper-main.mjs', 'chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/panel.html', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'src/daemon.mjs', 'src/server.mjs', 'src/core-client.mjs', 'package.json', 'package-lock.json']
const swiftFiles = ['chatgpt/native/GhostSentinelHelper.swift', 'chatgpt/native/HelperProcess.swift']
const agent = 'com.ghostsentinel.helper.agent.plist'
const xml = value => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')

export function parseHelperBuildArguments(args) {
  const values = {}
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index], value = args[index + 1]
    if (!['--out', '--config', '--sign-identity', '--notification-app'].includes(key) || typeof value !== 'string' || !value || Object.hasOwn(values, key)) throw new Error('usage: --out <scratch-dir> --config <absolute installation.json> [--sign-identity <identity>] [--notification-app <existing.app>]')
    values[key] = value
  }
  if (!values['--out'] || !values['--config']) throw new Error('explicit output and installation configuration required')
  return { outputDir: values['--out'], configPath: values['--config'], signIdentity: values['--sign-identity'] ?? '-', notificationApp: values['--notification-app'] }
}

export async function validateHelperBuildOptions({ outputDir, configPath, signIdentity = '-', notificationApp } = {}) {
  if (typeof outputDir !== 'string' || !outputDir || /[\n\0]/.test(outputDir) || !isAbsolute(configPath ?? '') || configPath.includes('\n') || configPath.includes('\0') || configPath.length > 2048) throw new Error('explicit output and absolute installation configuration required')
  outputDir = resolve(outputDir)
  let ancestor = outputDir
  const missing = []
  while (true) {
    try { outputDir = join(await realpath(ancestor), ...missing.reverse()); break }
    catch (error) { if (error.code !== 'ENOENT') throw error; missing.push(basename(ancestor)); ancestor = dirname(ancestor) }
  }
  const scratch = resolve(root, '.scratch'), scratchInfo = await lstat(scratch).catch(error => { if (error.code === 'ENOENT') return null; throw error })
  if (scratchInfo?.isSymbolicLink()) throw new Error('helper development bundle must use isolated scratch or temporary output')
  const roots = [scratchInfo ? await realpath(scratch) : scratch, await realpath(tmpdir())]
  if (process.platform === 'darwin') roots.push(await realpath('/private/tmp'))
  if (!roots.some(base => outputDir.startsWith(base + sep))) throw new Error('helper development bundle must use isolated scratch or temporary output')
  if (typeof signIdentity !== 'string' || !signIdentity || signIdentity.length > 200 || /[\n\0]/.test(signIdentity) || signIdentity.startsWith('-') && signIdentity !== '-') throw new Error('invalid signing identity')
  const info = await lstat(configPath)
  if (!info.isFile() || info.isSymbolicLink() || info.size < 1 || info.size > 16384 || info.mode & 0o077 || info.uid !== process.getuid()) throw new Error('private installation configuration required')
  configPath = await realpath(configPath)
  const config = JSON.parse(await readFile(configPath, 'utf8'))
  if (config.format !== 1 || !/^[a-f0-9]{64}$/.test(config.token ?? '') || typeof config.installationId !== 'string' || !config.installationId || typeof config.pluginId !== 'string' || !config.pluginId || !['dataDir', 'coreDataDir', 'sessionsRoot', 'metadataSocket'].every(key => isAbsolute(config[key] ?? '')) || config.port !== undefined && (!Number.isSafeInteger(config.port) || config.port < 1 || config.port > 65535)) throw new Error('installation configuration endpoint prerequisites invalid')
  if (notificationApp !== undefined) {
    if (!isAbsolute(notificationApp) || !notificationApp.endsWith('.app')) throw new Error('absolute existing notification app required')
    notificationApp = await realpath(notificationApp)
    const { stdout } = await run('/usr/bin/plutil', ['-extract', 'CFBundleIdentifier', 'raw', '-o', '-', join(notificationApp, 'Contents/Info.plist')])
    if (stdout.trim() !== 'com.ghostsentinel.notifications') throw new Error('notification component identity mismatch')
    await run('/usr/bin/codesign', ['--verify', '--strict', notificationApp])
  }
  return { outputDir, configPath, signIdentity, notificationApp }
}

export async function validateHelperSourceTree(directory) {
  for (const path of required) {
    const info = await lstat(join(directory, path))
    if (!info.isFile() || info.isSymbolicLink()) throw new Error('regular helper endpoint source required: ' + path)
  }
  for (const name of ['@modelcontextprotocol/sdk', '@modelcontextprotocol/ext-apps', 'stream-chain', 'stream-json', 'ws', 'zod']) {
    await access(join(directory, 'chatgpt/node_modules', name, 'package.json'), constants.R_OK)
  }
  return { entry: 'chatgpt/helper-main.mjs', daemon: 'src/daemon.mjs', panel: 'chatgpt/panel.html' }
}

async function filesUnder(directory, prefix = '', complete = false) {
  const values = []
  for (const entry of (await readdir(directory, { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) {
    if (!complete && ['.cache', '.bin', '.git', '.DS_Store'].includes(entry.name)) continue
    const path = join(directory, entry.name), key = prefix ? prefix + '/' + entry.name : entry.name
    if (entry.isSymbolicLink()) throw new Error('unexpected source symlink: ' + key)
    if (entry.isDirectory()) values.push(...await filesUnder(path, key, complete))
    else if (entry.isFile()) values.push({ path, key })
    else if (complete) throw new Error('unexpected nonregular bundle entry: ' + key)
  }
  return values
}

export async function collectHelperPayload(bundle) {
  const main = await lstat(join(bundle, 'Contents/MacOS/GhostSentinelHelper'))
  if (!main.isFile() || !(main.mode & 0o111)) throw new Error('fixed code-signed helper executable required')
  const packagedFiles = {}
  for (const file of await filesUnder(bundle, '', true)) {
    if (file.key === 'Contents/Resources/helper-inputs.json' || file.key === 'Contents/MacOS/GhostSentinelHelper' || file.key.startsWith('Contents/_CodeSignature/')) continue
    packagedFiles[file.key] = hash(await readFile(file.path))
  }
  return packagedFiles
}

export async function buildHelperApp(options) {
  if (process.platform !== 'darwin' || process.execPath !== HELPER_NODE || process.version !== 'v24.21.0') throw new Error('fixed macOS Node24.21.0 runtime required')
  const { outputDir, configPath, signIdentity, notificationApp } = await validateHelperBuildOptions(options)
  const nodeBytes = await readFile(HELPER_NODE)
  if (hash(nodeBytes) !== HELPER_NODE_SHA256) throw new Error('pinned Node binary changed')
  await validateHelperSourceTree(root)
  const sources = []
  for (const file of await filesUnder(join(root, 'src'), 'src')) if (/\.(mjs|js|cjs|json)$/.test(file.key)) sources.push(file)
  for (const entry of (await readdir(join(root, 'chatgpt'), { withFileTypes: true })).sort((a, b) => a.name.localeCompare(b.name))) if (entry.isFile() && /\.(mjs|js|cjs|json|html)$/.test(entry.name)) sources.push({ path: join(root, 'chatgpt', entry.name), key: 'chatgpt/' + entry.name })
  sources.push(...await filesUnder(join(root, 'chatgpt/node_modules'), 'chatgpt/node_modules'))
  for (const name of ['package.json', 'package-lock.json']) sources.push({ path: join(root, name), key: name })
  if (sources.length > 20000) throw new Error('runtime dependency count over budget')
  const inputs = {}, sourceBytes = new Map()
  let total = 0
  for (const file of sources) { const bytes = await readFile(file.path); if ((total += bytes.length) > 100 * 1024 * 1024) throw new Error('runtime dependency bytes over budget'); inputs[file.key] = hash(bytes); sourceBytes.set(file.key, bytes) }
  for (const path of [...swiftFiles, 'chatgpt/native/' + agent, 'scripts/build-chatgpt-helper.mjs']) inputs[path] = hash(await readFile(join(root, path)))
  const bundle = join(outputDir, 'GhostSentinel.app'), contents = join(bundle, 'Contents'), resources = join(contents, 'Resources'), observer = join(resources, 'observer'), cache = join(outputDir, 'module-cache')
  await mkdir(outputDir, { recursive: true, mode: 0o700 }); await mkdir(bundle, { mode: 0o700 })
  for (const path of ['MacOS', 'Resources/node/bin', 'Resources/observer', 'Library/LaunchAgents']) await mkdir(join(contents, path), { recursive: true, mode: 0o700 })
  await mkdir(cache, { recursive: true, mode: 0o700 })
  for (const file of sources) { const target = join(observer, file.key); await mkdir(resolve(target, '..'), { recursive: true, mode: 0o700 }); await writeFile(target, sourceBytes.get(file.key), { mode: 0o600 }) }
  const bundledNode = join(resources, 'node/bin/node'); await writeFile(bundledNode, nodeBytes, { mode: 0o755 }); await chmod(bundledNode, 0o755)
  await copyFile(join(root, 'chatgpt/native', agent), join(contents, 'Library/LaunchAgents', agent))
  const info = '<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"><dict><key>CFBundleIdentifier</key><string>' + HELPER_BUNDLE_ID + '</string><key>CFBundleExecutable</key><string>GhostSentinelHelper</string><key>CFBundleDisplayName</key><string>GhostSentinel</string><key>CFBundleName</key><string>GhostSentinel</string><key>CFBundlePackageType</key><string>APPL</string><key>CFBundleVersion</key><string>1</string><key>CFBundleShortVersionString</key><string>0.1.0</string><key>GhostSentinelRuntimeVersion</key><string>' + xml(HELPER_VERSION) + '</string><key>LSMinimumSystemVersion</key><string>13.0</string><key>LSUIElement</key><true/></dict></plist>'
  await writeFile(join(contents, 'Info.plist'), info)
  await writeFile(join(resources, 'helper-config.json'), JSON.stringify({ installationConfigPath: configPath }) + '\n', { mode: 0o600 })
  let nestedNotificationPath = null
  if (notificationApp) {
    const { cp } = await import('node:fs/promises'); nestedNotificationPath = 'Contents/Helpers/GhostSentinelNotifications.app'
    await mkdir(join(contents, 'Helpers'), { mode: 0o700 }); await cp(notificationApp, join(bundle, nestedNotificationPath), { recursive: true, errorOnExist: true, force: false })
  }
  const sdk = (await run('/usr/bin/xcrun', ['--sdk', 'macosx', '--show-sdk-path'])).stdout.trim(), executable = join(contents, 'MacOS/GhostSentinelHelper')
  await run('/usr/bin/xcrun', ['swiftc', '-swift-version', '5', '-O', '-parse-as-library', '-target', process.arch === 'arm64' ? 'arm64-apple-macosx13.0' : 'x86_64-apple-macosx13.0', '-sdk', sdk, '-module-cache-path', cache, '-framework', 'ServiceManagement', ...swiftFiles.map(path => join(root, path)), '-o', executable])
  await run('/usr/bin/codesign', ['--force', '--sign', signIdentity, '--timestamp=none', '--identifier', HELPER_BUNDLE_ID + '.node', bundledNode])
  const manifest = { format: 1, bundleId: HELPER_BUNDLE_ID, runtimeVersion: HELPER_VERSION, installationConfigPath: configPath, nodeInputSHA256: HELPER_NODE_SHA256, sources: inputs, sourceSetSHA256: hash(JSON.stringify(inputs)), nestedNotificationPath, codeSignedExecutable: 'Contents/MacOS/GhostSentinelHelper', packagedFiles: await collectHelperPayload(bundle) }
  await writeFile(join(resources, 'helper-inputs.json'), JSON.stringify(manifest, null, 2) + '\n', { mode: 0o600 })
  await run('/usr/bin/codesign', ['--force', '--sign', signIdentity, '--timestamp=none', '--identifier', HELPER_BUNDLE_ID, bundle])
  await run('/usr/bin/codesign', ['--verify', '--deep', '--strict', bundle])
  for (const [path, digest] of Object.entries(inputs)) if (hash(await readFile(join(root, path))) !== digest) throw new Error('source changed during helper build: ' + path)
  const bundleFiles = {}
  for (const file of await filesUnder(bundle, '', true)) bundleFiles[file.key] = hash(await readFile(file.path))
  const report = { bundle, executable, bundleId: HELPER_BUNDLE_ID, runtimeVersion: HELPER_VERSION, installationConfigPath: configPath, sourceCount: Object.keys(inputs).length, sourceSetSHA256: manifest.sourceSetSHA256, nodeInputSHA256: HELPER_NODE_SHA256, bundledNodeSHA256: hash(await readFile(bundledNode)), executableSHA256: hash(await readFile(executable)), servicePlistSHA256: hash(await readFile(join(contents, 'Library/LaunchAgents', agent))), bundleSHA256: hash(JSON.stringify(bundleFiles)), nestedNotificationPath, signed: signIdentity === '-' ? 'adhoc-development' : 'supplied-identity', notarized: false, installed: false, registered: false, helperLaunched: false }
  await writeFile(join(outputDir, 'helper-build.json'), JSON.stringify(report, null, 2) + '\n', { mode: 0o600 })
  return report
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) process.stdout.write(JSON.stringify(await buildHelperApp(parseHelperBuildArguments(process.argv.slice(2)))) + '\n')
