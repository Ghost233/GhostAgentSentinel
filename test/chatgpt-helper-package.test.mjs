import test, { before, after } from 'node:test'
import assert from 'node:assert/strict'
import { execFile, spawn } from 'node:child_process'
import { promisify } from 'node:util'
import { mkdir, writeFile, readFile, chmod, rm, copyFile, rename, symlink, realpath } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { randomUUID } from 'node:crypto'
import { parseHelperBuildArguments, validateHelperBuildOptions, validateHelperSourceTree, collectHelperPayload, HELPER_BUNDLE_ID, HELPER_NODE } from '../scripts/build-chatgpt-helper.mjs'

const execute = promisify(execFile), directory = resolve('.scratch/chatgpt-sentinel-delivery/execution/stage05/ticket09/native/test-' + randomUUID())
const configPath = join(directory, 'installation.json'), outputDir = join(directory, 'output'), harness = join(directory, 'HelperHarness')
const config = { format: 1, installationId: 'isolated-package-fixture', pluginId: 'sentinel@fixture', token: 'a'.repeat(64), dataDir: join(directory, 'data'), coreDataDir: join(directory, 'core'), sessionsRoot: join(directory, 'sessions'), metadataSocket: join(directory, 'metadata.sock'), port: 52184 }
const mac = process.platform === 'darwin'
before(async () => {
  await mkdir(directory, { recursive: true, mode: 0o700 }); await writeFile(configPath, JSON.stringify(config), { mode: 0o600 })
  if (mac) {
    const sdk = (await execute('/usr/bin/xcrun', ['--sdk', 'macosx', '--show-sdk-path'])).stdout.trim(), cache = join(directory, 'cache')
    await mkdir(cache, { mode: 0o700 })
    const common = ['swiftc', '-swift-version', '5', '-parse-as-library', '-target', process.arch === 'arm64' ? 'arm64-apple-macosx13.0' : 'x86_64-apple-macosx13.0', '-sdk', sdk, '-module-cache-path', cache, resolve('chatgpt/native/HelperProcess.swift')]
    // Compile/link the real SMAppService entry; never execute it in offline tests.
    await execute('/usr/bin/xcrun', [...common, '-framework', 'ServiceManagement', resolve('chatgpt/native/GhostSentinelHelper.swift'), '-o', join(directory, 'GhostSentinelHelper-compile-only')], { timeout: 120000, maxBuffer: 1048576 })
    // This isolated harness has no ServiceManagement import or real observer entry.
    await execute('/usr/bin/xcrun', [...common, resolve('test/fixtures/chatgpt-helper/HelperHarness.swift'), '-o', harness], { timeout: 120000, maxBuffer: 1048576 })
  }
})
after(() => rm(directory, { recursive: true, force: true }))

test('helper build CLI requires explicit installation and rejects executable/argv overrides', () => {
  assert.deepEqual(parseHelperBuildArguments(['--out', outputDir, '--config', configPath]), { outputDir, configPath, signIdentity: '-', notificationApp: undefined })
  for (const args of [[], ['--out', outputDir], ['--out', outputDir, '--config', configPath, '--node', '/bin/sh'], ['--out', outputDir, '--config', configPath, '--entry', '/tmp/other'], ['--out', outputDir, '--config', configPath, '--config', configPath], ['--out', outputDir, '--config']]) assert.throws(() => parseHelperBuildArguments(args))
})

test('helper build accepts a private config path without copying or modifying its secret contents', async () => {
  const before = await readFile(configPath)
  const value = await validateHelperBuildOptions({ outputDir, configPath })
  assert.equal(value.configPath, await realpath(configPath)); assert.equal(value.signIdentity, '-')
  assert.deepEqual(await readFile(configPath), before)
  assert.equal(JSON.stringify(value).includes(config.token), false)
  const literal = join(directory, 'literal $(never-execute).json')
  await writeFile(literal, before, { mode: 0o600 })
  assert.equal((await validateHelperBuildOptions({ outputDir, configPath: literal })).configPath, await realpath(literal))
})

test('helper build rejects unsafe config, output and signing substitutions', async () => {
  await assert.rejects(validateHelperBuildOptions({ outputDir: '/Applications', configPath }), /isolated/)
  const escapedOutput = join(directory, 'output-link'); await symlink('/', escapedOutput)
  await assert.rejects(validateHelperBuildOptions({ outputDir: join(escapedOutput, 'unowned-helper'), configPath }), /isolated/)
  await assert.rejects(validateHelperBuildOptions({ outputDir, configPath: 'relative.json' }), /absolute/)
  await assert.rejects(validateHelperBuildOptions({ outputDir, configPath, signIdentity: '--deep' }), /signing/)
  const publicConfig = join(directory, 'public.json'); await copyFile(configPath, publicConfig); await chmod(publicConfig, 0o644)
  await assert.rejects(validateHelperBuildOptions({ outputDir, configPath: publicConfig }), /private/)
  const link = join(directory, 'linked.json'); await symlink(configPath, link)
  await assert.rejects(validateHelperBuildOptions({ outputDir, configPath: link }), /private/)
  const bad = join(directory, 'invalid-port.json'); await writeFile(bad, JSON.stringify({ ...config, port: 0 }), { mode: 0o600 })
  await assert.rejects(validateHelperBuildOptions({ outputDir, configPath: bad }), /endpoint/)
})

test('packaged endpoint prerequisites include dynamic shared daemon, panel and declared JS dependencies', async () => {
  const tree = join(directory, 'source-tree')
  const files = ['chatgpt/helper-main.mjs', 'chatgpt/runtime.mjs', 'chatgpt/lazy-core.mjs', 'chatgpt/panel.html', 'chatgpt/package.json', 'chatgpt/package-lock.json', 'src/daemon.mjs', 'src/server.mjs', 'src/core-client.mjs', 'package.json', 'package-lock.json']
  for (const file of files) { const target = join(tree, file); await mkdir(resolve(target, '..'), { recursive: true }); await writeFile(target, '{}') }
  for (const name of ['@modelcontextprotocol/sdk', '@modelcontextprotocol/ext-apps', 'stream-chain', 'stream-json', 'ws', 'zod']) { const target = join(tree, 'chatgpt/node_modules', name); await mkdir(target, { recursive: true }); await writeFile(join(target, 'package.json'), '{}') }
  assert.deepEqual(await validateHelperSourceTree(tree), { entry: 'chatgpt/helper-main.mjs', daemon: 'src/daemon.mjs', panel: 'chatgpt/panel.html' })
  for (const file of ['src/daemon.mjs', 'chatgpt/panel.html', 'chatgpt/node_modules/ws/package.json']) { await rename(join(tree, file), join(tree, file + '.held')); await assert.rejects(validateHelperSourceTree(tree)); await rename(join(tree, file + '.held'), join(tree, file)) }
})

test('packaged payload hashes include signed nested content and only fixed signature-cycle exclusions', async () => {
  const bundle = join(directory, 'payload.app')
  const files = { 'Contents/MacOS/GhostSentinelHelper': 'code-signed main', 'Contents/Resources/node/bin/node': 'signed Node', 'Contents/Resources/.DS_Store': 'bundled metadata also covered', 'Contents/Resources/helper-config.json': '{}', 'Contents/Resources/observer/chatgpt/helper-main.mjs': 'entry', 'Contents/Helpers/Notification.app/Contents/_CodeSignature/CodeResources': 'nested seal', 'Contents/_CodeSignature/CodeResources': 'outer seal', 'Contents/Resources/helper-inputs.json': '{}' }
  for (const [name, body] of Object.entries(files)) { const target = join(bundle, name); await mkdir(resolve(target, '..'), { recursive: true }); await writeFile(target, body, { mode: name.includes('/MacOS/') ? 0o755 : 0o600 }) }
  const payload = await collectHelperPayload(bundle)
  assert.equal(Object.hasOwn(payload, 'Contents/MacOS/GhostSentinelHelper'), false)
  assert.equal(Object.hasOwn(payload, 'Contents/Resources/helper-inputs.json'), false)
  assert.equal(Object.hasOwn(payload, 'Contents/_CodeSignature/CodeResources'), false)
  assert.ok(Object.hasOwn(payload, 'Contents/Helpers/Notification.app/Contents/_CodeSignature/CodeResources'))
  assert.ok(Object.hasOwn(payload, 'Contents/Resources/node/bin/node'))
  assert.ok(Object.hasOwn(payload, 'Contents/Resources/.DS_Store'))
  const before = payload['Contents/Resources/node/bin/node']; await writeFile(join(bundle, 'Contents/Resources/node/bin/node'), 'changed signature')
  assert.notEqual((await collectHelperPayload(bundle))['Contents/Resources/node/bin/node'], before)
})

test('SMAppService plist uses the helper bundle identity, relative program and one agent role', async () => {
  const plist = await readFile('chatgpt/native/com.ghostsentinel.helper.agent.plist', 'utf8')
  assert.match(plist, /<key>Label<\/key><string>com\.ghostsentinel\.helper\.agent<\/string>/)
  assert.match(plist, /<key>BundleProgram<\/key><string>Contents\/MacOS\/GhostSentinelHelper<\/string>/)
  assert.match(plist, /<key>ProgramArguments<\/key><array><string>GhostSentinelHelper<\/string><string>--agent<\/string><\/array>/)
  assert.match(plist, /<key>SuccessfulExit<\/key><false\/>/)
  assert.equal(plist.includes('com.ghostsentinel.notifications'), false)
  assert.equal(plist.includes('/Library/LaunchAgents'), false)
  assert.equal(HELPER_BUNDLE_ID, 'com.ghostsentinel.helper')
})

test('isolated Swift contract accepts only control roles and fixed bundle paths', { skip: !mac && 'Apple compile/contract validation requires macOS SDK' }, async () => {
  for (const flag of ['--register', '--unregister', '--status', '--agent']) assert.equal((await execute(harness, ['parse', flag])).stdout.trim(), flag)
  assert.equal((await execute(harness, ['parse'])).stdout.trim(), '--status')
  for (const args of [['--register', '--agent'], ['--node', '/bin/sh'], ['--config', configPath], ['--unknown']]) await assert.rejects(execute(harness, ['parse', ...args]))
  const bundle = join(directory, 'Isolated bundle.app')
  for (const path of ['Contents/Resources/node/bin', 'Contents/Resources/observer/chatgpt', 'Contents/Library/LaunchAgents']) await mkdir(join(bundle, path), { recursive: true })
  const node = join(bundle, 'Contents/Resources/node/bin/node'), entry = join(bundle, 'Contents/Resources/observer/chatgpt/helper-main.mjs')
  await writeFile(node, 'fixture only', { mode: 0o755 }); await writeFile(entry, '// fixture only')
  await copyFile('chatgpt/native/com.ghostsentinel.helper.agent.plist', join(bundle, 'Contents/Library/LaunchAgents/com.ghostsentinel.helper.agent.plist'))
  await writeFile(join(bundle, 'Contents/Resources/helper-config.json'), JSON.stringify({ installationConfigPath: await realpath(configPath) }))
  const value = JSON.parse((await execute(harness, ['contract', bundle, HELPER_BUNDLE_ID])).stdout)
  assert.deepEqual(value.arguments, [await realpath(entry), '--config', await realpath(configPath)])
  assert.equal(value.node, await realpath(node))
  await assert.rejects(execute(harness, ['contract', bundle, 'com.ghostsentinel.notifications']))
  await rm(node); await symlink(HELPER_NODE, node)
  await assert.rejects(execute(harness, ['contract', bundle, HELPER_BUNDLE_ID]))
})

async function supervise(t, mode) {
  const output = join(directory, 'signal-' + mode + '.json'), child = spawn(harness, ['supervise', HELPER_NODE, resolve('test/fixtures/chatgpt-helper/signal-child.mjs'), mode, output], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, NODE_OPTIONS: '--require /must-not-be-inherited.mjs', NODE_PATH: '/must-not-be-inherited' } })
  let stderr = '', stubPID
  child.stderr.on('data', chunk => { stderr += chunk })
  const exit = new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal })) })
  const ready = new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error('isolated child ready timeout: ' + stderr)), 10000); child.stdout.on('data', chunk => { try { const value = JSON.parse(chunk.toString().trim()); if (value.ready) { stubPID = value.pid; clearTimeout(timer); resolve() } } catch {} }); child.once('exit', () => { clearTimeout(timer); if (!stubPID) reject(new Error('isolated harness exited before ready: ' + stderr)) }) })
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); await exit; if (stubPID) { try { process.kill(stubPID, 0); process.kill(stubPID, 'SIGKILL') } catch (error) { if (error.code !== 'ESRCH') throw error } } })
  await ready; child.kill('SIGTERM')
  const timeout = setTimeout(() => child.kill('SIGKILL'), 5000)
  const result = await exit; clearTimeout(timeout)
  assert.equal(result.signal, null, stderr)
  assert.throws(() => process.kill(stubPID, 0), error => error.code === 'ESRCH')
  return { result, output }
}

test('isolated Swift supervisor forwards SIGTERM and strips external Node loader environment', { skip: !mac && 'Apple compile/signal fixture requires macOS' }, async t => {
  const { result, output } = await supervise(t, 'graceful')
  assert.equal(result.code, 0)
  assert.deepEqual(JSON.parse(await readFile(output)), { signal: 'SIGTERM', nodeOptionsPresent: false, nodePathPresent: false })
})

test('isolated Swift supervisor waits for and reaps an uncooperative owned child', { skip: !mac && 'Apple compile/signal fixture requires macOS' }, async t => {
  const { result } = await supervise(t, 'ignore')
  assert.equal(result.code, 137)
})
