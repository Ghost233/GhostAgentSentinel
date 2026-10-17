import WebSocket from 'ws'
import { isAbsolute, resolve, dirname, join } from 'node:path'
import { realpath, lstat, open } from 'node:fs/promises'
import { constants } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'

const failure = code => Object.assign(new Error(code), { code })
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value)
const absolute = value => typeof value === 'string' && isAbsolute(value) && resolve(value) === value && !/[\x00-\x1f\x7f]/.test(value)
const setting = (config, id) => object(config?.plugins) && object(config.plugins[id]) && typeof config.plugins[id].enabled === 'boolean' ? config.plugins[id].enabled : null
const editable = (config, id) => object(config) && (!Object.hasOwn(config, 'plugins') || object(config.plugins) && (!Object.hasOwn(config.plugins, id) || object(config.plugins[id]) && (!Object.hasOwn(config.plugins[id], 'enabled') || typeof config.plugins[id].enabled === 'boolean')))

// Installation control is deliberately separate from the observation-only client.
// It connects to the existing App; it cannot launch an App or operate a chat.
function hostConnection(socketPath) {
  let socket = null, opening = null, closed = false, sequence = 0
  const pending = new Map()
  const rejectAll = code => { for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(failure(code)) }; pending.clear() }
  async function dispose(ws) {
    if (!ws || ws.readyState === WebSocket.CLOSED) return
    await new Promise(resolve => { const timer = setTimeout(() => ws.terminate(), 1000); ws.once('close', () => { clearTimeout(timer); resolve() }); ws.close() })
  }
  function wire(method, params) {
    if (closed) return Promise.reject(failure('plugin_host_closed'))
    if (socket?.readyState !== WebSocket.OPEN) return Promise.reject(failure('plugin_host_unavailable'))
    if (pending.size >= 16) return Promise.reject(failure('plugin_host_over_limit'))
    return new Promise((resolve, reject) => {
      const id = ++sequence, timer = setTimeout(() => { pending.delete(id); reject(failure('plugin_host_deadline')) }, 5000)
      pending.set(id, { resolve, reject, timer })
      socket.send(JSON.stringify({ id, method, params }), error => { if (!error) return; clearTimeout(timer); pending.delete(id); reject(failure('plugin_host_send_failed')) })
    })
  }
  async function connect() {
    if (closed) throw failure('plugin_host_closed')
    if (opening) return opening
    if (socket?.readyState === WebSocket.OPEN) return
    opening = (async () => {
      const ws = new WebSocket(`ws+unix://${socketPath}:/`, { handshakeTimeout: 3000, maxPayload: 2097152 })
      socket = ws
      ws.on('error', () => { if (socket === ws) rejectAll('plugin_host_unavailable') })
      ws.on('close', () => { if (socket === ws) { rejectAll('plugin_host_unavailable'); socket = null } })
      ws.on('message', bytes => {
        let response
        try { response = JSON.parse(bytes) } catch { rejectAll('plugin_host_invalid_response'); ws.terminate(); return }
        const entry = pending.get(response?.id)
        if (!entry) return
        pending.delete(response.id); clearTimeout(entry.timer)
        if (response.error) entry.reject(Object.assign(failure('plugin_host_rpc_failed'), { rpcCode: response.error.code }))
        else entry.resolve(response.result)
      })
      try {
        await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', () => reject(failure('plugin_host_unavailable'))) })
        await wire('initialize', { clientInfo: { name: 'ghost_sentinel_plugin_installer', version: '0.1.0-dev.1' }, capabilities: { experimentalApi: true } })
        if (closed || socket !== ws) throw failure('plugin_host_closed')
        ws.send(JSON.stringify({ method: 'initialized' }))
      } catch (error) { if (socket === ws) { rejectAll('plugin_host_unavailable'); socket = null }; await dispose(ws); throw error }
    })().finally(() => { opening = null })
    return opening
  }
  return {
    async request(method, params) { await connect(); return wire(method, params) },
    async close() { closed = true; rejectAll('plugin_host_closed'); const ws = socket; socket = null; await dispose(ws); await opening?.catch(() => {}) },
  }
}

export function createPluginHost({ socketPath, marketplacePath, marketplaceName, pluginName, pluginId, pluginPath } = {}) {
  if (![socketPath, marketplacePath, pluginPath].every(absolute) || ![marketplaceName, pluginName].every(value => typeof value === 'string' && /^[a-z0-9][a-z0-9._-]{0,127}$/.test(value)) || pluginId !== pluginName + '@' + marketplaceName) throw failure('plugin_host_binding_invalid')
  const marketplaceRoot = dirname(dirname(dirname(marketplacePath)))
  if (marketplacePath !== join(marketplaceRoot, '.agents/plugins/marketplace.json') || pluginPath !== join(marketplaceRoot, 'plugin')) throw failure('plugin_host_binding_invalid')
  const rpc = hostConnection(socketPath)
  async function readState() {
    const list = await rpc.request('plugin/list', { forceRefetch: false, marketplaceKinds: ['local'] })
    if (!Array.isArray(list?.marketplaces)) throw failure('plugin_host_catalog_unconfirmed')
    if (list.marketplaceLoadErrors?.some(error => error.marketplacePath === marketplacePath)) throw failure('plugin_host_catalog_unconfirmed')
    const matches = list.marketplaces.filter(market => market.name === marketplaceName)
    if (matches.length > 1 || matches.some(market => market.path !== marketplacePath)) throw failure('plugin_host_registration_conflict')
    let summary = null
    if (matches.length) {
      if (matches[0].plugins?.length !== 1 || matches[0].plugins[0].id !== pluginId) throw failure('plugin_host_registration_conflict')
      const detail = (await rpc.request('plugin/read', { marketplacePath, pluginName }))?.plugin
      summary = detail?.summary
      if (detail?.marketplaceName !== marketplaceName || detail.marketplacePath !== marketplacePath || summary?.id !== pluginId || summary.name !== pluginName || summary.source?.type !== 'local' || summary.source.path !== pluginPath || typeof summary.installed !== 'boolean' || typeof summary.enabled !== 'boolean' || await realpath(pluginPath) !== pluginPath) throw failure('plugin_host_registration_conflict')
    }
    const config = await rpc.request('config/read', { cwd: null, includeLayers: true })
    const users = Array.isArray(config?.layers) ? config.layers.filter(layer => layer.name?.type === 'user' && !layer.disabledReason) : []
    const user = users.length === 1 && typeof users[0].version === 'string' && users[0].version.length > 0 && absolute(users[0].name.file) ? users[0] : null
    const own = user ? setting(user.config, pluginId) : null, effective = setting(config?.config, pluginId)
    const denied = Array.isArray(config?.layers) && config.layers.some(layer => !layer.disabledReason && setting(layer.config, pluginId) === false)
    const enabled = own === false || effective === false || denied || summary?.enabled === false ? 'disabled' : own === true && effective === true && summary?.enabled === true ? 'enabled' : 'unknown'
    return { view: { pluginId, installed: summary?.installed ?? false, enabled, revision: user?.version ?? null, pluginPath: summary ? pluginPath : null, marketplaceRegistered: matches.length === 1, version: summary?.localVersion ?? summary?.version ?? null }, user }
  }
  const inspect = async () => (await readState()).view
  async function mutate(method, params) {
    try { return await rpc.request(method, params) }
    catch (error) { if (error.rpcCode === undefined) error.mutationOutcome = 'unknown'; throw error }
  }
  async function checkSource() {
    if (await realpath(marketplaceRoot) !== marketplaceRoot || await realpath(pluginPath) !== pluginPath || await realpath(marketplacePath) !== marketplacePath) throw failure('plugin_host_source_unconfirmed')
    const before = await lstat(marketplacePath)
    if (!before.isFile() || before.size > 1048576 || before.mode & 0o077 || typeof process.getuid === 'function' && before.uid !== process.getuid()) throw failure('plugin_host_source_unconfirmed')
    const file = await open(marketplacePath, constants.O_RDONLY | constants.O_NOFOLLOW)
    try {
      const bytes = await file.readFile(), after = await file.stat(), pathNow = await lstat(marketplacePath)
      if (bytes.length !== before.size || [after, pathNow].some(info => ['dev', 'ino', 'size', 'mtimeMs', 'ctimeMs', 'mode', 'uid'].some(key => info[key] !== before[key]))) throw failure('plugin_host_source_unconfirmed')
      let market
      try { market = JSON.parse(bytes) } catch { throw failure('plugin_host_source_unconfirmed') }
      const entry = market?.plugins?.[0]
      if (market?.name !== marketplaceName || !Array.isArray(market.plugins) || market.plugins.length !== 1 || entry?.name !== pluginName || entry.source?.source !== 'local' || entry.source.path !== './plugin') throw failure('plugin_host_source_unconfirmed')
    } finally { await file.close() }
  }
  async function register() {
    const before = await readState()
    if (!before.user || !editable(before.user.config, pluginId)) throw failure('plugin_host_enablement_unconfirmed')
    await checkSource()
    if ((await readState()).view.revision !== before.view.revision) throw failure('plugin_host_revision_conflict')
    if (before.view.installed && before.view.enabled !== 'disabled') throw failure('plugin_host_disable_required')
    if (!before.view.marketplaceRegistered) {
      await mutate('marketplace/add', { source: marketplaceRoot })
      const after = await readState()
      if (after.view.revision !== before.view.revision) {
        const expected = structuredClone(before.user.config)
        if (Object.hasOwn(expected, 'marketplaces') && !object(expected.marketplaces) || Object.hasOwn(expected.marketplaces ?? {}, marketplaceName)) throw failure('plugin_host_revision_conflict')
        expected.marketplaces ??= {}
        // The current native API writes this exact local registration itself.
        // No plugin setting or other user field may change in the same window.
        expected.marketplaces[marketplaceName] = { source_type: 'local', source: marketplaceRoot }
        if (!after.user || after.user.name.file !== before.user.name.file || !isDeepStrictEqual(after.user.config, expected)) throw failure('plugin_host_revision_conflict')
      }
    }
    if (before.view.installed) {
      await mutate('plugin/uninstall', { pluginId })
      if ((await readState()).view.installed) throw failure('plugin_host_uninstall_unconfirmed')
    }
    const result = await mutate('plugin/install', { marketplacePath, pluginName, installAttemptId: randomUUID() })
    if (!Array.isArray(result?.appsNeedingAuth)) throw failure('plugin_host_install_unconfirmed')
    if (result.appsNeedingAuth.length) throw failure('plugin_trust_required')
    const after = await readState()
    if (!after.view.installed || !after.view.marketplaceRegistered) throw failure('plugin_host_install_unconfirmed')
    return after.view
  }
  async function unregister() {
    const before = await readState()
    if (before.view.installed) {
      await mutate('plugin/uninstall', { pluginId })
      if ((await readState()).view.installed) throw failure('plugin_host_uninstall_unconfirmed')
    }
    if (before.view.marketplaceRegistered) await mutate('marketplace/remove', { marketplaceName })
    const after = await readState()
    if (after.view.installed || after.view.marketplaceRegistered) throw failure('plugin_host_uninstall_unconfirmed')
    return after.view
  }
  async function setEnabled(value, expectedRevision) {
    if (typeof value !== 'boolean' || typeof expectedRevision !== 'string' || !expectedRevision) throw failure('plugin_host_revision_required')
    const before = await readState()
    if (!before.view.installed || !before.view.marketplaceRegistered) throw failure('plugin_host_not_installed')
    if (before.view.revision !== expectedRevision) throw failure('plugin_host_revision_conflict')
    if (!before.user || !editable(before.user.config, pluginId)) throw failure('plugin_host_enablement_unconfirmed')
    if (setting(before.user.config, pluginId) === value) return before.view
    let result
    try {
      result = await rpc.request('config/batchWrite', { edits: [{ keyPath: 'plugins.' + JSON.stringify(pluginId) + '.enabled', value, mergeStrategy: 'replace' }], expectedVersion: expectedRevision, filePath: before.user.name.file, reloadUserConfig: true })
    } catch (error) { if (error.rpcCode === undefined) error.mutationOutcome = 'unknown'; throw error }
    const after = await readState()
    if (typeof result?.version !== 'string' || result.filePath !== before.user.name.file || after.view.revision !== result.version || setting(after.user?.config, pluginId) !== value) throw failure('plugin_host_write_unconfirmed')
    return after.view
  }
  return { inspect, register, setEnabled, unregister, close: () => rpc.close() }
}
