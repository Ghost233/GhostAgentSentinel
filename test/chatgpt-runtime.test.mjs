import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm, stat, chmod } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { prepareInstallation, startFromInstallation } from '../chatgpt/runtime.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'

test('private production installation retains its notifier bundle and wires native IO without adding another panel entry', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-native-installation-')), notificationBundlePath = join(dir, 'GhostSentinel.app')
  const server = await startServer({ dataDir: join(dir, 'core') }); let runtime
  t.after(async () => { await runtime?.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const options = { dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'ghost-sentinel@test', notificationBundlePath }
  const prepared = await prepareInstallation(options), config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  assert.equal(config.notificationBundlePath, notificationBundlePath)
  await assert.rejects(prepareInstallation({ ...options, notificationBundlePath: join(dir, 'Different.app') }), /different settings/)
  assert.equal(JSON.parse(await readFile(prepared.configPath, 'utf8')).token, config.token)
  let settingsCalls = 0
  const notificationIO = { async settings() { settingsCalls++; return { authorization: 'denied', observedAt: Date.now() } }, async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: Date.now() } }, async cancel() { return { pendingIds: [], observedAt: Date.now() } }, close() {} }
  const started = Date.now() - 1000
  const native = { async sampleHost() { return { available: true, hostRunId: 'native-installation-host', hostStartedAt: started, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [], nextCursor: null } }, async close() {} }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, notificationIO, pollMs: 0 })
  await runtime.refresh()
  for (let wait = 0; !settingsCalls && wait < 100; wait++) await new Promise(resolve => setTimeout(resolve, 2))
  assert.equal(settingsCalls, 1)
  const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + config.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) }).then(value => value.json())
  assert.equal(response.result.tools.filter(tool => tool._meta?.['openai/ui']?.entrypoints).length, 1)
  assert.ok(response.result.tools.some(tool => tool.name === 'sentinel_request_notification_permission'))
  assert.ok(response.result.tools.some(tool => tool.name === 'sentinel_notification_records'))
  assert.equal(JSON.stringify(response).includes(config.token), false)
})

test('the production helper starts from private installation configuration and generates only a conversation MCP entry', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-installation-'))
  const server = await startServer({ dataDir: join(dir, 'core') })
  let runtime
  t.after(async () => { await runtime?.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'metadata.sock') })
  assert.equal((await stat(prepared.configPath)).mode & 0o777, 0o600)
  const initial = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const again = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'metadata.sock') })
  assert.equal(JSON.parse(await readFile(again.configPath, 'utf8')).token, initial.token)
  runtime = await startFromInstallation({ configPath: prepared.configPath, pollMs: 0 })
  const manifest = JSON.parse(await readFile(join(prepared.pluginDir, '.codex-plugin/plugin.json'), 'utf8'))
  assert.equal(manifest.name, 'ghost-sentinel')
  const connection = JSON.parse(await readFile(join(prepared.pluginDir, '.mcp.json'), 'utf8'))
  assert.equal(connection.mcpServers.sentinel.url, runtime.url + '/mcp')
  assert.equal(connection.mcpServers.sentinel.http_headers.Authorization, 'Bearer ' + initial.token)
  const response = await fetch(runtime.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + initial.token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) }).then(value => value.json())
  const entries = response.result.tools.filter(tool => tool._meta?.['openai/ui']?.entrypoints)
  assert.equal(entries.length, 1, 'the write control does not add another visible entry')
  assert.deepEqual(entries[0]._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  const control = response.result.tools.find(tool => tool.name === 'sentinel_set_chat_analysis')
  assert.deepEqual(control._meta.ui.visibility, ['app'])
  assert.equal(control.annotations.readOnlyHint, false)
  assert.equal(JSON.stringify(response).includes(initial.token), false)
  await runtime.close()
  await chmod(prepared.configPath, 0o644)
  await assert.rejects(startFromInstallation({ configPath: prepared.configPath }), /private configuration/)
})

test('a duplicate helper CLI exits after verified reuse without releasing the owner lock', async t => {
  const { spawn } = await import('node:child_process'), { once } = await import('node:events')
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-helper-entry-'))
  let runtime, child
  t.after(async () => { if (child && child.exitCode === null) { child.kill('SIGTERM'); await once(child, 'close') }; await runtime?.close(); await rm(dir, { recursive: true, force: true }) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, pluginId: 'ghost-sentinel@test', metadataSocket: join(dir, 'missing-metadata.sock') })
  const native = { async sampleHost() { return { available: false } }, async readEnablement() { assert.fail('no App metadata') }, async close() {} }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 })
  child = spawn(process.execPath, [new URL('../chatgpt/helper-main.mjs', import.meta.url).pathname, '--config', prepared.configPath], { stdio: ['ignore', 'pipe', 'pipe'] })
  const ended = once(child, 'close')
  let output = '', errors = '', timer
  child.stdout.on('data', bytes => { output += bytes }); child.stderr.on('data', bytes => { errors += bytes })
  let result
  try { result = await Promise.race([ended, new Promise((resolve, reject) => { timer = setTimeout(() => { child.kill('SIGTERM'); reject(new Error('duplicate helper stayed resident')) }, 5000) })]) }
  finally { clearTimeout(timer) }
  assert.deepEqual(result, [0, null], errors)
  const ready = JSON.parse(output.trim()), config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  assert.equal(ready.status, 'reused'); assert.equal(ready.pid, process.pid); assert.equal(ready.url, runtime.url)
  assert.equal(ready.policy, 'chatgpt-main/1')
  assert.equal(output.includes(config.token), false); assert.equal(output.includes(dir), false)
  assert.equal(JSON.parse(await readFile(join(dir, 'adapter/runtime.lock/owner.json'), 'utf8')).runId, runtime.runId)
  await assert.rejects(stat(join(dir, 'core/runtime.json')), { code: 'ENOENT' })
  await runtime.close()
  await assert.rejects(stat(join(dir, 'adapter/runtime.lock')), { code: 'ENOENT' })
})

test('existing-profile standby permits only retention maintenance and helper health remains independent of Core', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-standby-runtime-')), server = await startServer({ dataDir: join(dir, 'core') })
  let runtime; const coreHTTP = []
  t.after(async () => { await runtime?.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  const original = globalThis.fetch
  t.mock.method(globalThis, 'fetch', async function (url, ...args) { if (String(url).startsWith(server.url + '/')) coreHTTP.push({ path: new URL(url).pathname, method: args[0]?.method ?? 'GET' }); return original.call(this, url, ...args) })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'metadata.sock'), pluginId: 'ghost-sentinel@test' })
  const native = { async sampleHost() { return { available: false, reason: 'execution-source-unavailable' } }, async readEnablement() { assert.fail('metadata cannot be read while App is off') }, async close() {} }
  runtime = await startFromInstallation({ configPath: prepared.configPath, native, pollMs: 0 })
  await runtime.refresh()
  const maintenance = [{ path: '/v1/health', method: 'GET' }, { path: '/v1/health', method: 'GET' }, { path: '/v1/diagnostics', method: 'GET' }]
  assert.deepEqual(coreHTTP, maintenance, 'an existing profile permits cleanup only, without registration, analysis or notification writes')
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  const health = await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token } }).then(value => value.json())
  assert.equal(health.protocol, 'ghost-sentinel-chatgpt-helper/1')
  assert.equal(health.installationId, config.installationId)
  assert.equal(health.pid, process.pid)
  assert.equal(JSON.stringify(health).includes(config.token), false)
  assert.equal(JSON.stringify(health).includes(dir), false)
  assert.deepEqual(coreHTTP, maintenance, 'authenticated helper health is independent of Core')
  const firstURL = runtime.url
  await runtime.close()
  const legacy = { ...config }; delete legacy.port
  await writeFile(prepared.configPath, JSON.stringify(legacy), { mode: 0o600 })
  runtime = await startFromInstallation({ configPath: prepared.configPath, native: { ...native }, pollMs: 0 })
  assert.equal(runtime.url, firstURL, 'the existing own MCP endpoint survives an installation without a saved port')
  assert.equal(JSON.parse(await readFile(prepared.configPath, 'utf8')).port, config.port)
  assert.deepEqual(coreHTTP, [...maintenance, ...maintenance], 'restart checks existing retention once without reactivating collection')
})

test('a never-collected standby profile creates no Core while its helper endpoint remains available', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-new-standby-')); let runtime
  t.after(async () => { try { await runtime?.close() } finally { await rm(dir, { recursive: true, force: true }) } })
  const prepared = await prepareInstallation({ dataDir: join(dir, 'adapter'), coreDataDir: join(dir, 'core'), sessionsRoot: dir, metadataSocket: join(dir, 'missing.sock'), pluginId: 'sentinel@empty-standby' })
  const original = globalThis.fetch; let unexpected = 0
  t.mock.method(globalThis, 'fetch', async function (url, ...args) { if (!runtime || !String(url).startsWith(runtime.url + '/')) { unexpected++; assert.fail('an empty standby profile cannot contact or start Core') }; return original.call(this, url, ...args) })
  runtime = await startFromInstallation({ configPath: prepared.configPath, native: { async sampleHost() { return { available: false } }, async close() {} }, pollMs: 0 })
  await runtime.refresh(); await runtime.maintainRetention()
  const config = JSON.parse(await readFile(prepared.configPath, 'utf8'))
  assert.equal((await fetch(runtime.url + '/health', { headers: { authorization: 'Bearer ' + config.token } })).status, 200)
  assert.equal(unexpected, 0)
  await assert.rejects(stat(join(dir, 'core')), { code: 'ENOENT' })
})
