import test, { before, after } from 'node:test'
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { mkdtemp, writeFile, readFile, rm, chmod, rename, symlink, lstat, readdir, realpath, copyFile, cp, mkdir } from 'node:fs/promises'
import { constants } from 'node:fs'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { createHash } from 'node:crypto'
import { buildPluginPackage, inspectPluginPackage, PLUGIN_NODE, PLUGIN_NODE_SHA256 } from '../chatgpt/plugin-package.mjs'
import { parsePluginBuildArguments } from '../scripts/build-chatgpt-plugin.mjs'

const execute = promisify(execFile), directory = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-plugin-package-')))
const packageRoot = join(directory, 'payload'), sourceRoot = resolve('.'), hash = bytes => createHash('sha256').update(bytes).digest('hex')
const mac = process.platform === 'darwin', packageTest = { skip: !mac && 'fixed macOS Mach-O packaging is unsupported on this platform' }
let built
before(async () => { if (mac) built = await buildPluginPackage({ outputDir: packageRoot }) })
after(() => rm(directory, { recursive: true, force: true }))

test('plugin package CLI only accepts output and an optional existing notification app', () => {
  assert.deepEqual(parsePluginBuildArguments(['--out', packageRoot]), { outputDir: packageRoot, notificationApp: undefined })
  assert.deepEqual(parsePluginBuildArguments(['--out', packageRoot, '--version', '0.1.0-dev.2']), { outputDir: packageRoot, notificationApp: undefined, version: '0.1.0-dev.2' })
  for (const args of [[], ['--out'], ['--out', packageRoot, '--config', '/tmp/config'], ['--out', packageRoot, '--node', '/bin/sh'], ['--out', packageRoot, '--out', packageRoot]]) assert.throws(() => parsePluginBuildArguments(args))
})

test('unfinished, path-escaping, legacy service and foreign-writable manifests fail before any runtime execution', async () => {
  const incomplete = join(directory, 'incomplete'); await mkdir(incomplete, { mode: 0o700 })
  await assert.rejects(inspectPluginPackage(incomplete), { code: 'ENOENT' })
  const manifestPath = join(incomplete, 'plugin-package.json')
  const invalid = { format: 1, kind: 'ghost-sentinel-plugin-payload', version: '0.1.0-dev.1', runtimeVersion: '0.1.0-dev.1', node: { path: 'runtime/node', version: 'v24.21.0', sha256: PLUGIN_NODE_SHA256 }, sourceSetSHA256: '0'.repeat(64), notificationPath: null }
  for (const path of ['../escape', '/absolute', 'chatgpt/helper-main.mjs', '.codex-plugin/plugin.json', 'hooks/hooks.json']) {
    await writeFile(manifestPath, JSON.stringify({ ...invalid, files: { [path]: { sha256: '0'.repeat(64), size: 0, mode: 0o600, sourceMode: 0o644 } } }), { mode: 0o600 })
    await assert.rejects(inspectPluginPackage(incomplete), /path/)
  }
  await chmod(manifestPath, 0o620); await assert.rejects(inspectPluginPackage(incomplete), /writ/); await chmod(manifestPath, 0o600)
  const linked = join(directory, 'incomplete-link'); await symlink(incomplete, linked)
  await assert.rejects(inspectPluginPackage(linked), /regular|symlink/)
})

test('payload is self contained and binds the complete pinned runtime without old service entries or installation secrets', packageTest, async t => {
  const value = await inspectPluginPackage(packageRoot, { digest: built.digest })
  assert.equal(value.root, packageRoot); assert.equal(value.nodePath, join(packageRoot, 'runtime/node'))
  assert.equal(value.bridgePath, join(packageRoot, 'chatgpt/plugin-bridge.mjs'))
  assert.equal(value.ownerPath, join(packageRoot, 'chatgpt/plugin-main.mjs'))
  assert.equal(value.manifest.version, '0.1.0-dev.1'); assert.equal(value.manifest.runtimeVersion, '0.1.0-dev.1')
  assert.equal(value.notificationApp, null)
  assert.equal(value.signed, false); assert.equal(value.notarized, false)
  assert.equal(value.installed, false); assert.equal(value.registered, false); assert.equal(value.launched, false)
  assert.equal(value.manifest.node.sha256, PLUGIN_NODE_SHA256)
  assert.equal(hash(await readFile(value.nodePath)), PLUGIN_NODE_SHA256)
  assert.equal((await execute(value.nodePath, ['--version'])).stdout.trim(), 'v24.21.0')
  for (const path of ['src/daemon.mjs', 'src/server.mjs', 'chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/plugin-host.mjs', 'chatgpt/plugin-installation.mjs', 'chatgpt/plugin-package.mjs', 'scripts/manage-chatgpt-plugin.mjs', 'chatgpt/panel.html', 'chatgpt/settings.html', 'package.json', 'package-lock.json', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'chatgpt/node_modules/@modelcontextprotocol/sdk/package.json']) assert.ok(value.manifest.files[path], path)
  for (const path of Object.keys(value.manifest.files)) assert.doesNotMatch(path, /helper-main|helper-installation|\.swift$|\.plist$|LaunchAgents|^hooks\/|installation\.json|\.mcp\.json/)
  assert.equal((await lstat(value.nodePath)).mode & 0o777, value.manifest.files['runtime/node'].mode)
  assert.equal(value.manifest.files['chatgpt/runtime.mjs'].sourceMode, (await lstat(join(sourceRoot, 'chatgpt/runtime.mjs'))).mode & 0o7777)
  await assert.rejects(inspectPluginPackage(packageRoot, { digest: '0'.repeat(64) }), /digest/)
  assert.equal((await inspectPluginPackage(packageRoot.replace(/^\/private\/var\//, '/var/'), { digest: built.digest })).root, packageRoot)
  t.diagnostic(JSON.stringify({ digest: value.digest, sourceSetSHA256: value.manifest.sourceSetSHA256, fileCount: Object.keys(value.manifest.files).length, nodeSHA256: value.manifest.node.sha256, sources: Object.fromEntries(['chatgpt/runtime.mjs', 'chatgpt/plugin-main.mjs', 'chatgpt/plugin-bridge.mjs', 'chatgpt/plugin-package.mjs', 'chatgpt/plugin-host.mjs', 'chatgpt/plugin-installation.mjs', 'scripts/manage-chatgpt-plugin.mjs'].map(path => [path, value.manifest.files[path]])) }))
})

test('a self-consistent payload without the global settings resource is incomplete', packageTest, async () => {
  const manifestPath = join(packageRoot, 'plugin-package.json'), settingsPath = join(packageRoot, 'chatgpt/settings.html')
  const [manifestBytes, settingsBytes] = await Promise.all([readFile(manifestPath), readFile(settingsPath)])
  try {
    const manifest = JSON.parse(manifestBytes)
    delete manifest.files['chatgpt/settings.html']
    const sources = Object.fromEntries(Object.entries(manifest.files).filter(([path]) => path !== 'runtime/node' && !path.startsWith('notification/')).map(([path, value]) => [path, { sha256: value.sha256, size: value.size, mode: value.sourceMode }]))
    manifest.sourceSetSHA256 = hash(JSON.stringify(sources))
    await writeFile(manifestPath, JSON.stringify(manifest)); await rm(settingsPath)
    await assert.rejects(inspectPluginPackage(packageRoot), /missing production runtime file: chatgpt\/settings.html/)
  } finally {
    await writeFile(settingsPath, settingsBytes, { mode: 0o600 }); await writeFile(manifestPath, manifestBytes)
  }
})

test('tampering, missing modules, extra files, modes, unsafe paths and symlinks are rejected', packageTest, async () => {
  const module = join(packageRoot, 'chatgpt/runtime.mjs'), original = await readFile(module)
  await writeFile(module, Buffer.concat([original, Buffer.from('\n// changed')]))
  await assert.rejects(inspectPluginPackage(packageRoot), /changed|hash|size/)
  await writeFile(module, original)
  await rename(module, module + '.held')
  await assert.rejects(inspectPluginPackage(packageRoot), /file|payload|set/)
  await rename(module + '.held', module)
  const foreign = join(packageRoot, 'unlisted.txt'); await writeFile(foreign, 'unexpected', { mode: 0o600 })
  await assert.rejects(inspectPluginPackage(packageRoot), /file|payload|set/); await rm(foreign)
  await chmod(module, 0o620); await assert.rejects(inspectPluginPackage(packageRoot), /writ|mode|permission/); await chmod(module, 0o600)
  await chmod(built.nodePath, 0o600); await assert.rejects(inspectPluginPackage(packageRoot), /mode|executable/); await chmod(built.nodePath, 0o700)
  await chmod(packageRoot, 0o750); await assert.rejects(inspectPluginPackage(packageRoot), /mode/); await chmod(packageRoot, 0o700)
  const linkedRoot = join(directory, 'linked-payload'); await symlink(packageRoot, linkedRoot)
  await assert.rejects(inspectPluginPackage(linkedRoot), /regular|symlink/); await rm(linkedRoot)
  await rename(module, module + '.held'); await symlink(module + '.held', module)
  await assert.rejects(inspectPluginPackage(packageRoot), /symlink|regular/); await rm(module); await rename(module + '.held', module)
  const manifestPath = join(packageRoot, 'plugin-package.json'), manifestBytes = await readFile(manifestPath), manifest = JSON.parse(manifestBytes)
  manifest.files['../escape'] = { sha256: 'a'.repeat(64), size: 1, mode: 0o600 }
  await writeFile(manifestPath, JSON.stringify(manifest)); await assert.rejects(inspectPluginPackage(packageRoot), /path/)
  await writeFile(manifestPath, manifestBytes)
  assert.equal((await inspectPluginPackage(packageRoot, { digest: built.digest })).digest, built.digest)
})

test('building does not replace an existing destination or start any process', packageTest, async () => {
  await assert.rejects(buildPluginPackage({ outputDir: packageRoot }), /exist/)
  for (const version of ['../escape', '1.2', '1.2.3\n', '1.2.3-01', '1.2.3' + 'x'.repeat(200)]) await assert.rejects(buildPluginPackage({ outputDir: join(directory, 'invalid'), version }), /version/)
  const guarded = join(directory, 'guarded-build.mjs'), output = join(directory, 'guarded-payload'), source = join(directory, 'isolated-source')
  await cp(packageRoot, source, { recursive: true, force: false, errorOnExist: true, mode: constants.COPYFILE_FICLONE })
  await copyFile(join(sourceRoot, 'chatgpt/plugin-package.mjs'), join(source, 'chatgpt/plugin-package.mjs'))
  for (const name of ['installation.json', 'runtime.json', '.mcp.json', 'hooks.json']) await writeFile(join(source, 'chatgpt', name), '{"token":"installation-only-fixture"}\n', { mode: 0o600 })
  await writeFile(guarded, `import childProcess from 'node:child_process'\nimport { syncBuiltinESMExports } from 'node:module'\nimport { pathToFileURL } from 'node:url'\nfor (const name of ['spawn','spawnSync','exec','execSync','execFile','execFileSync','fork']) childProcess[name] = () => { throw new Error('unexpected process launch: ' + name) }\nsyncBuiltinESMExports()\nconst { buildPluginPackage } = await import(pathToFileURL(process.argv[3]).href)\nconst result = await buildPluginPackage({outputDir:process.argv[2],version:'0.1.0-dev.2'})\nprocess.stdout.write(JSON.stringify({digest:result.digest,launched:result.launched,version:result.manifest.version,runtimeVersion:result.manifest.runtimeVersion}))\n`, { mode: 0o600 })
  const result = JSON.parse((await execute(PLUGIN_NODE, [guarded, output, join(source, 'chatgpt/plugin-package.mjs')], { cwd: directory, timeout: 30000, maxBuffer: 1048576 })).stdout)
  const verified = await inspectPluginPackage(output)
  assert.equal(result.launched, false); assert.equal(verified.digest, result.digest)
  for (const name of ['installation.json', 'runtime.json', '.mcp.json', 'hooks.json']) assert.equal(Object.hasOwn(verified.manifest.files, 'chatgpt/' + name), false)
  assert.equal(result.version, '0.1.0-dev.2'); assert.equal(result.runtimeVersion, '0.1.0-dev.1')
  await copyFile(join(sourceRoot, 'chatgpt/plugin-package.mjs'), join(output, 'chatgpt/plugin-package.mjs'))
  const changing = join(directory, 'changing-build.mjs'), racedOutput = join(directory, 'raced-payload')
  await copyFile(new URL('./fixtures/chatgpt-plugin-package/changing-build.mjs', import.meta.url), changing)
  const rejected = JSON.parse((await execute(PLUGIN_NODE, [changing, output, racedOutput], { cwd: directory, timeout: 30000, maxBuffer: 1048576 })).stdout)
  assert.equal(rejected.changed, true); assert.equal(rejected.rejected, true)
  await assert.rejects(lstat(racedOutput), { code: 'ENOENT' })
  await rm(output, { recursive: true })
  await rm(source, { recursive: true })
  assert.equal((await readdir(directory)).some(name => name.includes('.partial-')), false)
})

test('notification resources exceeding the remaining payload budget are rejected before open or allocation', packageTest, async t => {
  const source = join(directory, 'notification-source')
  await cp(packageRoot, source, { recursive: true, force: false, errorOnExist: true, mode: constants.COPYFILE_FICLONE })
  const notificationApp = join(directory, 'BudgetFixture.app'), contents = join(notificationApp, 'Contents')
  await mkdir(join(contents, 'MacOS'), { recursive: true, mode: 0o700 })
  await copyFile('/usr/bin/true', join(contents, 'MacOS/GhostSentinelNotifications'))
  await chmod(join(contents, 'MacOS/GhostSentinelNotifications'), 0o700)
  await writeFile(join(contents, 'Info.plist'), '<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"><dict><key>CFBundleIdentifier</key><string>com.ghostsentinel.notifications</string><key>CFBundleExecutable</key><string>GhostSentinelNotifications</string><key>CFBundlePackageType</key><string>APPL</string><key>CFBundleVersion</key><string>1</string></dict></plist>', { mode: 0o600 })
  await execute('/usr/bin/codesign', ['--force', '--sign', '-', '--timestamp=none', notificationApp], { timeout: 10000, maxBuffer: 1048576 })
  const probe = join(directory, 'notification-budget-probe.mjs')
  await writeFile(probe, `import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import { syncBuiltinESMExports } from 'node:module'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
const [modulePath, notificationApp, directory] = process.argv.slice(2)
const resource = join(notificationApp, 'Contents/Info.plist'), virtualSize = 200 * 1024 * 1024
const originalStat = fs.lstat, originalOpen = fs.open, originalAlloc = Buffer.alloc
let opened = 0, allocated = 0
fs.lstat = async (path, ...args) => {
  const info = await originalStat(path, ...args)
  return path === resource ? Object.assign(Object.create(Object.getPrototypeOf(info)), info, { size: virtualSize }) : info
}
fs.open = async (path, ...args) => {
  if (path === resource) { opened++; throw new Error('oversized notification resource opened before remaining-budget rejection') }
  return originalOpen(path, ...args)
}
Buffer.alloc = (size, ...args) => {
  if (size >= virtualSize) { allocated++; throw new Error('oversized notification resource allocated before remaining-budget rejection') }
  return originalAlloc(size, ...args)
}
syncBuiltinESMExports()
const { buildPluginPackage, inspectPluginPackage } = await import(pathToFileURL(modulePath).href)
try {
  await assert.rejects(buildPluginPackage({ outputDir: join(directory, 'over-budget-notification'), notificationApp }), /over budget/)
  assert.equal(opened, 0); assert.equal(allocated, 0)
} finally {
  fs.lstat = originalStat; fs.open = originalOpen; Buffer.alloc = originalAlloc; syncBuiltinESMExports()
}
const normal = await buildPluginPackage({ outputDir: join(directory, 'small-notification'), notificationApp })
const verified = await inspectPluginPackage(normal.root, { digest: normal.digest })
assert.equal(verified.notificationApp, join(normal.root, 'notification/GhostSentinelNotifications.app'))
assert.ok(verified.manifest.files['notification/GhostSentinelNotifications.app/Contents/Info.plist'].size < 1024)
await assert.rejects(originalStat(join(directory, 'over-budget-notification')), { code: 'ENOENT' })
process.stdout.write(JSON.stringify({ opened, allocated, virtualSize, smallPackageVerified: true, signatureValidation: 'real-codesign', notificationComponentLaunched: false }) + '\\n')
`, { mode: 0o600 })
  const result = JSON.parse((await execute(PLUGIN_NODE, [probe, join(source, 'chatgpt/plugin-package.mjs'), notificationApp, directory], { cwd: directory, timeout: 30000, maxBuffer: 1048576 })).stdout)
  assert.equal(result.opened, 0); assert.equal(result.allocated, 0); assert.equal(result.smallPackageVerified, true)
  assert.equal(result.notificationComponentLaunched, false)
  t.diagnostic(JSON.stringify(result))
})

test('packaged production runtime and real SDK stdio bridge run outside the checkout', packageTest, async () => {
  const runner = join(directory, 'outside-runtime.mjs')
  const fixture = await readFile(new URL('./fixtures/chatgpt-plugin-package/outside-runtime.mjs', import.meta.url))
  await writeFile(runner, fixture, { mode: 0o600 })
  const result = await execute(built.nodePath, [runner, packageRoot, directory], { cwd: directory, env: { PATH: '/usr/bin:/bin', HOME: directory }, timeout: 20000, maxBuffer: 1048576 })
  const value = JSON.parse(result.stdout)
  assert.equal(value.version, 'v24.21.0'); assert.equal(value.sourceRootSeen, false)
  assert.equal(value.ownerPid, value.pid); assert.equal(value.currentThread, 'package-main')
  assert.equal(value.panel, true); assert.equal(value.disconnected, true)
  assert.equal(value.managementImports, true)
  assert.equal(JSON.stringify(value).includes(sourceRoot), false)
  const help = JSON.parse((await execute(built.nodePath, [join(packageRoot, 'scripts/manage-chatgpt-plugin.mjs'), '--help'], { cwd: directory, env: { PATH: '/usr/bin:/bin', HOME: directory }, timeout: 5000, maxBuffer: 1048576 })).stdout)
  assert.equal(help.exitCode, 0); assert.match(help.help, /install\|upgrade\|uninstall/)
})
