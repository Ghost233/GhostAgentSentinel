import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { createRequire } from 'node:module'
import { mkdtemp, mkdir, writeFile, rm, realpath, lstat, symlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import { createPluginHost } from '../chatgpt/plugin-host.mjs'

const { WebSocketServer } = createRequire(new URL('../chatgpt/package.json', import.meta.url))('ws')

async function socketBinding(path) {
  path = await realpath(path)
  const info = await lstat(path)
  return { path, identity: [info.dev, info.ino, info.mode, info.uid, info.ctimeMs] }
}

async function fixture(t, hostOptions = {}) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'sentinel-plugin-host-')))
  const pluginPath = join(root, 'plugin'), marketplacePath = join(root, '.agents/plugins/marketplace.json')
  await mkdir(pluginPath, { mode: 0o700 }); await mkdir(join(root, '.agents/plugins'), { recursive: true, mode: 0o700 })
  await writeFile(marketplacePath, JSON.stringify({ name: 'sentinel-host-test', plugins: [{ name: 'ghost-sentinel', source: { source: 'local', path: './plugin' } }] }), { mode: 0o600 })
  const options = { socketPath: join(tmpdir(), 'gs-ph-' + randomUUID().slice(0, 8) + '.sock'), marketplacePath, marketplaceName: 'sentinel-host-test', pluginName: 'ghost-sentinel', pluginId: 'ghost-sentinel@sentinel-host-test', pluginPath }
  const state = { registered: true, installed: true, enabled: true, revision: 'user-revision-1', otherEnabled: true, race: false, writes: 1, sourcePath: pluginPath }
  const calls = [], endpoints = [], websocket = new WebSocketServer({ noServer: true }), server = createServer()
  server.on('upgrade', (req, socket, head) => websocket.handleUpgrade(req, socket, head, ws => { ws.fixtureEndpoint = 'original'; websocket.emit('connection', ws, req) }))
  const summary = () => ({ id: options.pluginId, name: options.pluginName, installed: state.installed, enabled: state.enabled, localVersion: '0.1.0-dev.2', source: { type: 'local', path: state.sourcePath } })
  websocket.on('connection', ws => ws.on('message', bytes => {
    const message = JSON.parse(bytes); calls.push(message); endpoints.push({ endpoint: ws.fixtureEndpoint, method: message.method })
    if (!message.id) return
    if (state.disconnectOn === message.method) { ws.terminate(); return }
    if (state.holdMethod === message.method) { state.onHeld?.(); return }
    let result
    if (message.method === 'initialize') result = { serverInfo: { name: 'owned-plugin-host' } }
    else if (message.method === 'plugin/list') result = { marketplaces: state.registered ? [{ name: options.marketplaceName, path: marketplacePath, plugins: [summary()] }] : [], marketplaceLoadErrors: [] }
    else if (message.method === 'plugin/read') result = { plugin: { summary: summary(), marketplaceName: options.marketplaceName, marketplacePath, mcpServers: ['sentinel'] } }
    else if (message.method === 'config/read') {
      const config = { plugins: { ...(state.ownSettingMissing ? {} : { [options.pluginId]: { enabled: state.enabled } }), 'unrelated@other': { enabled: state.otherEnabled } }, ...(state.marketConfig ? { marketplaces: { [options.marketplaceName]: state.marketConfig } } : {}) }
      if (state.staticServer) config.mcp_servers = { sentinel: { command: 'unrelated-static-server' } }
      const layer = { name: { type: 'user', file: join(root, 'config.toml') }, version: state.revision, config }
      if (state.malformedOwn) config.plugins[options.pluginId] = 'untrusted'
      result = { config, layers: state.ambiguousUser ? [layer, { ...layer }] : [layer] }
    } else if (message.method === 'config/batchWrite') {
      if (state.race) { state.race = false; state.revision = 'concurrent-user-revision'; state.otherEnabled = false }
      if (message.params.expectedVersion !== state.revision) { ws.send(JSON.stringify({ id: message.id, error: { code: -32009, message: 'owned secret error Bearer MUST_NOT_APPEAR' } })); return }
      assert.equal(message.params.edits.length, 1)
      assert.equal(message.params.edits[0].keyPath, 'plugins."' + options.pluginId + '".enabled')
      state.enabled = message.params.edits[0].value
      state.revision = 'user-revision-' + ++state.writes; result = { status: 'ok', version: state.revision, filePath: join(root, 'config.toml') }
    } else if (message.method === 'marketplace/add') {
      assert.equal(message.params.source, root); state.registered = true; result = { marketplaceName: options.marketplaceName }
      if (state.nativeMarketWrite) { state.marketConfig = { source_type: 'local', source: root }; state.revision = 'user-revision-' + ++state.writes }
      if (state.foreignMarketField) state.marketConfig.userChoice = 'preserve'
      if (state.foreignOtherChoice) state.otherEnabled = false
      if (state.concurrentDuringAdd) { state.enabled = false; state.ownSettingMissing = false; state.revision = 'concurrent-user-disabled' }
    } else if (message.method === 'plugin/install') {
      assert.equal(message.params.pluginName, options.pluginName); assert.equal(message.params.marketplacePath, marketplacePath)
      assert.match(message.params.installAttemptId, /^[a-f0-9-]{36}$/)
      state.installed = true; state.enabled = true; state.ownSettingMissing = false; state.revision = 'user-revision-' + ++state.writes
      result = { authPolicy: 'ON_INSTALL', appsNeedingAuth: state.needsTrust ? [{}] : [] }
    } else if (message.method === 'plugin/uninstall') {
      assert.equal(message.params.pluginId, options.pluginId)
      state.installed = false; state.enabled = false; state.revision = 'user-revision-' + ++state.writes; result = {}
    } else if (message.method === 'marketplace/remove') {
      assert.equal(message.params.marketplaceName, options.marketplaceName); state.registered = false; result = { marketplaceName: options.marketplaceName, installedRoot: null }
    } else if (message.method === 'mcpServerStatus/list') {
      assert.deepEqual(message.params, { serverName: 'sentinel', detail: 'toolsAndAuthOnly', limit: 1 })
      state.discovered = true
      result = { data: state.discoveryData ?? [{ name: 'sentinel', pluginId: options.pluginId, tools: {}, authStatus: 'unsupported' }], nextCursor: null }
      if (state.discoveryConcurrentChange) { state.revision = 'user-disabled-during-discovery'; state.enabled = false; state.otherEnabled = false }
    } else { ws.send(JSON.stringify({ id: message.id, error: { code: -32601, message: 'unexpected mutation in read-only test' } })); return }
    ws.send(JSON.stringify({ id: message.id, result }))
  }))
  await new Promise(resolve => server.listen(options.socketPath, resolve))
  const host = createPluginHost({ ...options, connectionContext: async () => ({ scope: 'app', appHostRunId: 'owned-test-app', socketBinding: await socketBinding(options.socketPath) }), ...hostOptions })
  t.after(async () => { await host.close(); for (const ws of websocket.clients) ws.terminate(); await new Promise(resolve => websocket.close(resolve)); await new Promise(resolve => server.close(resolve)); await rm(options.socketPath, { force: true }); await rm(root, { recursive: true, force: true }) })
  return { host, calls, options, state, endpoints, websocket, connections: () => websocket.clients.size }
}

async function secondEndpoint(t, f, path = join(tmpdir(), 'gs-ph-' + randomUUID().slice(0, 8) + '.sock')) {
  const server = createServer()
  server.on('upgrade', (req, socket, head) => f.websocket.handleUpgrade(req, socket, head, ws => { ws.fixtureEndpoint = 'replacement'; f.websocket.emit('connection', ws, req) }))
  await new Promise(resolve => server.listen(path, resolve))
  t.after(async () => { await new Promise(resolve => server.close(resolve)); await rm(path, { force: true }) })
  return path
}

test('installation preview reads an authenticated local registration and enablement without changing the host', async t => {
  const f = await fixture(t), state = await f.host.inspect()
  assert.deepEqual(state, { pluginId: f.options.pluginId, installed: true, enabled: 'enabled', revision: 'user-revision-1', pluginPath: f.options.pluginPath, marketplaceRegistered: true, version: '0.1.0-dev.2' })
  assert.ok(f.calls.every(call => ['initialize', 'initialized', 'plugin/list', 'plugin/read', 'config/read'].includes(call.method)))
  assert.ok(f.calls.find(call => call.method === 'plugin/list').params.cwds === undefined, 'a discovered workspace market is not proof of a registered home market')
  await f.host.close()
  assert.equal(f.connections(), 0)
})

test('stopping one plugin writes only its enabled key with a configuration revision and verifies the result', async t => {
  const f = await fixture(t), before = await f.host.inspect()
  const after = await f.host.setEnabled(false, before.revision)
  assert.equal(after.enabled, 'disabled'); assert.equal(after.revision, 'user-revision-2')
  assert.equal(f.state.otherEnabled, true)
  assert.ok(!f.calls.some(call => call.method === 'mcpServerStatus/list'))
  const writes = f.calls.filter(call => call.method === 'config/batchWrite')
  assert.equal(writes.length, 1)
  assert.deepEqual(writes[0].params, { edits: [{ keyPath: 'plugins."' + f.options.pluginId + '".enabled', value: false, mergeStrategy: 'replace' }], expectedVersion: 'user-revision-1', filePath: join(f.options.pluginPath, '../config.toml'), reloadUserConfig: true })
})

test('enabling the managed plugin applies only its own revision-protected setting to loaded chats', async t => {
  const f = await fixture(t); f.state.enabled = false
  const before = await f.host.inspect(), after = await f.host.setEnabled(true, before.revision)
  assert.equal(after.enabled, 'enabled'); assert.equal(f.state.otherEnabled, true)
  const writes = f.calls.filter(call => call.method === 'config/batchWrite')
  assert.deepEqual(writes.map(call => call.params), [{ edits: [{ keyPath: 'plugins.' + JSON.stringify(f.options.pluginId) + '.enabled', value: true, mergeStrategy: 'replace' }], expectedVersion: before.revision, filePath: join(f.options.pluginPath, '../config.toml'), reloadUserConfig: true }])
  assert.ok(f.calls.every(call => ['initialize', 'initialized', 'plugin/list', 'plugin/read', 'config/read', 'config/batchWrite', 'mcpServerStatus/list'].includes(call.method)))
})

test('the installer initializes only the managed MCP connection after confirmed enable without a panel, tool call or global reload', async t => {
  const f = await fixture(t); f.state.enabled = false
  const enabled = await f.host.setEnabled(true, (await f.host.inspect()).revision)
  assert.equal(f.state.discovered, undefined, 'the write revision is returned before connection initialization can fail')
  const initialized = await f.host.initializeConnection(enabled.revision)
  assert.equal(initialized.appActivationRequired, false)
  assert.equal(initialized.connectionScope, 'app')
  assert.equal(f.state.discovered, true, 'persisted enablement alone does not initialize the plugin connection')
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 1)
  assert.ok(!f.calls.some(call => ['mcpServer/tool/call', 'config/mcpServer/reload', 'plugin/reconcile', 'thread/resume'].includes(call.method)))
  assert.equal(f.state.otherEnabled, true)
})

test('a separate configuration daemon never becomes an App activation witness or starts a replacement MCP connection', async t => {
  const f = await fixture(t, { connectionContext: async () => ({ scope: 'configuration-only', reason: 'different-process', appHostRunId: 'owned-test-app' }) })
  const before = await f.host.inspect(), result = await f.host.initializeConnection(before.revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'configuration-only')
  assert.equal(f.calls.some(call => call.method === 'mcpServerStatus/list'), false)
  assert.equal(f.state.enabled, true)
  assert.equal(f.state.otherEnabled, true)
})

test('an App socket alias that flips back cannot label an existing daemon WebSocket as App activation', async t => {
  const f = await fixture(t), appSocket = await secondEndpoint(t, f)
  const alias = join(tmpdir(), 'gs-al-' + randomUUID().slice(0, 8) + '.sock')
  await symlink(f.options.socketPath, alias)
  const host = createPluginHost({ ...f.options, socketPath: alias, connectionContext: async () => {
    await rm(alias); await symlink(appSocket, alias)
    const context = { scope: 'app', appHostRunId: 'owned-test-app', socketBinding: await socketBinding(alias) }
    await rm(alias); await symlink(f.options.socketPath, alias)
    return context
  } })
  t.after(async () => { await host.close(); await rm(alias, { force: true }) })
  const before = await host.inspect(), result = await host.initializeConnection(before.revision)
  assert.equal(f.endpoints.some(call => call.endpoint === 'original' && call.method === 'mcpServerStatus/list'), false, 'the still-open daemon connection must not be warmed by proof of the other endpoint')
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
})

test('replacing a live socket pathname cannot transfer the old RPC connection to the new App listener', async t => {
  const f = await fixture(t), before = await f.host.inspect(), original = await socketBinding(f.options.socketPath)
  await rm(f.options.socketPath)
  await secondEndpoint(t, f, f.options.socketPath)
  const replacement = await socketBinding(f.options.socketPath)
  assert.notDeepEqual(original.identity, replacement.identity)
  const result = await f.host.initializeConnection(before.revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
})

test('reconnecting the same exact endpoint between App proof and discovery requires a fresh proof', async t => {
  let f
  f = await fixture(t, { connectionContext: async () => {
    const context = { scope: 'app', appHostRunId: 'owned-test-app', socketBinding: await socketBinding(f.options.socketPath) }
    await Promise.all([...f.websocket.clients].map(ws => new Promise(resolve => { ws.once('close', resolve); ws.close() })))
    await f.host.inspect()
    return context
  } })
  const before = await f.host.inspect(), result = await f.host.initializeConnection(before.revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
  assert.equal(f.calls.filter(call => call.method === 'initialize').length, 2, 'the fixture actually opened another RPC connection to the unchanged socket')
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
})

test('a connection replacement during final App sampling cannot complete or warm the replacement connection', async t => {
  let f, samples = 0
  f = await fixture(t, { connectionContext: async () => {
    if (++samples === 2) {
      await rm(f.options.socketPath)
      await secondEndpoint(t, f, f.options.socketPath)
      await Promise.all([...f.websocket.clients].map(ws => new Promise(resolve => { ws.once('close', resolve); ws.close() })))
      await f.host.inspect()
    }
    return { scope: 'app', appHostRunId: 'owned-test-app', socketBinding: await socketBinding(f.options.socketPath) }
  } })
  const result = await f.host.initializeConnection((await f.host.inspect()).revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
  assert.equal(f.endpoints.filter(call => call.endpoint === 'original' && call.method === 'mcpServerStatus/list').length, 1)
  assert.equal(f.endpoints.filter(call => call.endpoint === 'replacement' && call.method === 'mcpServerStatus/list').length, 0)
})

test('App scope without an actual matching socket binding never initializes MCP', async t => {
  const f = await fixture(t, { connectionContext: async () => ({ scope: 'app', appHostRunId: 'owned-test-app' }) })
  const result = await f.host.initializeConnection((await f.host.inspect()).revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
})

test('unknown connection facts require native App activation without discovering MCP', async t => {
  for (const context of [null, { scope: 'unknown' }, { scope: 'daemon', reason: 'Bearer MUST_NOT_APPEAR' }]) {
    const f = await fixture(t, { connectionContext: async () => context })
    const result = await f.host.initializeConnection((await f.host.inspect()).revision)
    assert.equal(result.appActivationRequired, true)
    assert.equal(result.connectionScope, 'unknown')
    assert.equal(JSON.stringify(result).includes('MUST_NOT_APPEAR'), false)
    assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
  }
})

test('configuration-only activation rechecks revision and qualification after connection sampling', async t => {
  for (const mutation of ['revision', 'enabled', 'staticServer']) {
    let state
    const f = await fixture(t, { connectionContext: async () => { state[mutation] = mutation === 'revision' ? 'new-user-choice' : mutation === 'staticServer'; return { scope: 'configuration-only' } } })
    state = f.state
    await assert.rejects(f.host.initializeConnection((await f.host.inspect()).revision), { code: mutation === 'revision' ? 'plugin_host_revision_conflict' : 'plugin_host_activation_qualification_changed' })
    assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
    assert.equal(f.calls.filter(call => call.method === 'config/batchWrite').length, 0)
  }
})

test('an App generation change during discovery cannot be reported as App activation', async t => {
  let samples = 0
  const f = await fixture(t, { connectionContext: async () => ({ scope: 'app', appHostRunId: ++samples === 1 ? 'old-app' : 'new-app', socketBinding: await socketBinding(f.options.socketPath) }) })
  const result = await f.host.initializeConnection((await f.host.inspect()).revision)
  assert.equal(result.appActivationRequired, true)
  assert.equal(result.connectionScope, 'unknown')
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 1)
})

test('an existing static server with the same name blocks activation before any configuration write or connection', async t => {
  const f = await fixture(t); f.state.enabled = false; f.state.staticServer = true
  await assert.rejects(f.host.setEnabled(true, (await f.host.inspect()).revision), { code: 'plugin_host_server_name_conflict' })
  assert.equal(f.state.enabled, false)
  assert.equal(f.state.otherEnabled, true)
  assert.ok(!f.calls.some(call => ['config/batchWrite', 'mcpServerStatus/list'].includes(call.method)))
})

test('missing or unrelated discovery results cannot complete plugin activation', async t => {
  for (const data of [[], [{ name: 'unrelated' }], [{ name: 'sentinel' }, { name: 'sentinel' }], { length: 1, 0: { name: 'sentinel' } }, [{ name: 'sentinel', pluginId: 'unrelated@other' }], [{ name: 'sentinel' }], [{ name: 'sentinel', pluginId: null }]]) {
    const f = await fixture(t); f.state.enabled = false; f.state.discoveryData = data
    const enabled = await f.host.setEnabled(true, (await f.host.inspect()).revision)
    await assert.rejects(f.host.initializeConnection(enabled.revision), { code: 'plugin_host_discovery_unconfirmed' })
    assert.equal(f.state.enabled, true, 'the confirmed configuration write is retained for the installation transaction to recover')
    assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 1)
    assert.equal(f.state.otherEnabled, true)
  }
})

test('a lost discovery connection preserves the written setting and is not retried or called successful', async t => {
  const f = await fixture(t); f.state.enabled = false; f.state.disconnectOn = 'mcpServerStatus/list'
  const enabled = await f.host.setEnabled(true, (await f.host.inspect()).revision)
  await assert.rejects(f.host.initializeConnection(enabled.revision), { code: 'plugin_host_unavailable' })
  assert.equal(f.state.enabled, true)
  assert.equal(f.state.otherEnabled, true)
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 1)
})

test('connection initialization requires current enabled qualification and preserves a concurrent user change', async t => {
  const f = await fixture(t), before = await f.host.inspect()
  f.state.enabled = false
  await assert.rejects(f.host.initializeConnection(before.revision), { code: 'plugin_host_activation_qualification_changed' })
  f.state.revision = 'changed-before-initialization'
  await assert.rejects(f.host.initializeConnection(before.revision), { code: 'plugin_host_revision_conflict' })
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 0)
  f.state.enabled = true; f.state.discoveryConcurrentChange = true
  await assert.rejects(f.host.initializeConnection(f.state.revision), { code: 'plugin_host_revision_conflict' })
  assert.equal(f.state.enabled, false)
  assert.equal(f.state.otherEnabled, false)
  assert.equal(f.calls.filter(call => call.method === 'mcpServerStatus/list').length, 1)
  assert.equal(f.calls.filter(call => call.method === 'config/batchWrite').length, 0)
})

test('a concurrent user write is preserved rather than retried or reported as a completed toggle', async t => {
  const f = await fixture(t), before = await f.host.inspect(); f.state.race = true
  await assert.rejects(f.host.setEnabled(false, before.revision), error => error.code === 'plugin_host_rpc_failed' && error.rpcCode === -32009 && !error.message.includes('MUST_NOT_APPEAR'))
  assert.equal(f.state.enabled, true); assert.equal(f.state.otherEnabled, false)
  assert.equal(f.calls.filter(call => call.method === 'config/batchWrite').length, 1)
})

test('a managed local plugin can be registered and fully unregistered without changing another plugin', async t => {
  const f = await fixture(t); f.state.registered = false; f.state.installed = false; f.state.enabled = false
  const initial = await f.host.inspect()
  assert.equal(initial.marketplaceRegistered, false); assert.equal(initial.pluginPath, null)
  const installed = await f.host.register()
  assert.equal(installed.installed, true); assert.equal(installed.marketplaceRegistered, true); assert.equal(installed.pluginPath, f.options.pluginPath)
  const removed = await f.host.unregister()
  assert.equal(removed.installed, false); assert.equal(removed.marketplaceRegistered, false); assert.equal(removed.pluginPath, null)
  assert.equal(f.state.otherEnabled, true)
  assert.equal(f.calls.filter(call => call.method === 'plugin/uninstall').length, 1)
  assert.ok(!f.calls.some(call => ['plugin/reconcile', 'config/mcpServer/reload', 'thread/resume'].includes(call.method)))
})

test('a same-name registration pointing elsewhere is never installed over or deleted', async t => {
  const f = await fixture(t); f.state.sourcePath = join(f.options.pluginPath, 'foreign')
  await assert.rejects(f.host.register(), { code: 'plugin_host_registration_conflict' })
  await assert.rejects(f.host.unregister(), { code: 'plugin_host_registration_conflict' })
  assert.ok(f.calls.every(call => ['initialize', 'initialized', 'plugin/list', 'plugin/read', 'config/read'].includes(call.method)))
})

test('ambiguous or malformed user configuration cannot be overwritten during installation or a toggle', async t => {
  const f = await fixture(t)
  f.state.ambiguousUser = true
  assert.equal((await f.host.inspect()).revision, null)
  await assert.rejects(f.host.setEnabled(false, 'user-revision-1'), { code: 'plugin_host_revision_conflict' })
  f.state.installed = false; f.state.enabled = false
  await assert.rejects(f.host.register(), { code: 'plugin_host_enablement_unconfirmed' })
  f.state.ambiguousUser = false; f.state.malformedOwn = true
  await assert.rejects(f.host.register(), { code: 'plugin_host_enablement_unconfirmed' })
  assert.ok(!f.calls.some(call => ['config/batchWrite', 'marketplace/add', 'plugin/install', 'plugin/uninstall'].includes(call.method)))
})

test('a transport loss during registration records an unknown mutation instead of retrying installation', async t => {
  const f = await fixture(t); f.state.registered = false; f.state.installed = false; f.state.enabled = false; f.state.disconnectOn = 'marketplace/add'
  await assert.rejects(f.host.register(), error => error.code === 'plugin_host_unavailable' && error.mutationOutcome === 'unknown')
  assert.equal(f.calls.filter(call => call.method === 'marketplace/add').length, 1)
  assert.equal(f.calls.filter(call => call.method === 'plugin/install').length, 0)
  await f.host.close()
  await assert.rejects(f.host.inspect(), { code: 'plugin_host_closed' })
})

test('an installed plugin is refreshed only while disabled and explicit trust is not reported as ready', async t => {
  const f = await fixture(t)
  await assert.rejects(f.host.register(), { code: 'plugin_host_disable_required' })
  assert.ok(!f.calls.some(call => call.method === 'plugin/uninstall'))
  await f.host.setEnabled(false, (await f.host.inspect()).revision)
  const refreshed = await f.host.register()
  assert.equal(refreshed.enabled, 'enabled')
  const mutations = f.calls.filter(call => ['plugin/install', 'plugin/uninstall'].includes(call.method)).map(call => call.method)
  assert.deepEqual(mutations, ['plugin/uninstall', 'plugin/install'])
  await f.host.setEnabled(false, refreshed.revision); f.state.needsTrust = true
  await assert.rejects(f.host.register(), { code: 'plugin_trust_required' })
})

test('closing the host client rejects outstanding reads and releases its only connection', async t => {
  const f = await fixture(t)
  f.state.holdMethod = 'plugin/list'
  const held = new Promise(resolve => { f.state.onHeld = resolve })
  const request = f.host.inspect(), rejection = assert.rejects(request, { code: 'plugin_host_closed' })
  await held; await f.host.close(); await rejection
  assert.equal(f.connections(), 0)
})

test('the registration binding cannot place the managed plugin outside its marketplace state', async t => {
  const f = await fixture(t)
  assert.throws(() => createPluginHost({ ...f.options, pluginPath: join(f.options.pluginPath, '../../unrelated') }), { code: 'plugin_host_binding_invalid' })
})

test('registration does not continue with an installation after configuration changes during marketplace registration', async t => {
  const f = await fixture(t); f.state.registered = false; f.state.installed = false; f.state.enabled = false; f.state.ownSettingMissing = true; f.state.nativeMarketWrite = true; f.state.concurrentDuringAdd = true
  await assert.rejects(f.host.register(), { code: 'plugin_host_revision_conflict' })
  assert.equal(f.state.enabled, false)
  assert.equal(f.state.revision, 'concurrent-user-disabled')
  assert.ok(!f.calls.some(call => call.method === 'plugin/install'))
})

test('the native marketplace registration revision is accepted only for its exact local source record', async t => {
  const f = await fixture(t); f.state.registered = false; f.state.installed = false; f.state.enabled = false; f.state.ownSettingMissing = true; f.state.nativeMarketWrite = true
  const installed = await f.host.register()
  assert.equal(installed.installed, true)
  assert.equal(installed.enabled, 'enabled')
  assert.equal(f.calls.filter(call => call.method === 'plugin/install').length, 1)
  assert.equal(f.state.otherEnabled, true)
})

for (const mutation of ['foreignMarketField', 'foreignOtherChoice']) test('native registration does not hide a concurrent choice: ' + mutation, async t => {
  const f = await fixture(t); f.state.registered = false; f.state.installed = false; f.state.enabled = false; f.state.nativeMarketWrite = true; f.state[mutation] = true
  await assert.rejects(f.host.register(), { code: 'plugin_host_revision_conflict' })
  assert.ok(!f.calls.some(call => call.method === 'plugin/install'))
  if (mutation === 'foreignMarketField') assert.equal(f.state.marketConfig.userChoice, 'preserve')
  else assert.equal(f.state.otherEnabled, false)
})
