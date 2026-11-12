import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createPanelMCPServer, startPanelServer } from '../chatgpt/panel-server.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/bounded-reader.mjs'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createNativeNotificationIO, notificationPrefix } from '../chatgpt/native-notifications.mjs'
import { Client } from '../chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/client/index.js'
import { InMemoryTransport } from '../chatgpt/node_modules/@modelcontextprotocol/sdk/dist/esm/inMemory.js'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const token = 'a'.repeat(64)
const identity = { installationId: 'presentation-installation', runId: 'presentation-run', pid: 123, version: '0.1.0-preview', ownership: 'plugin' }
async function panel(t, options = {}) {
  const server = await startPanelServer({ token, read: async () => ({ summary: '只读观察' }), ...options })
  t.after(() => server.close())
  return {
    server,
    async rpc(method, params = {}) {
      const response = await fetch(server.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
      assert.equal(response.status, 200)
      return (await response.json()).result
    },
  }
}

test('an upgraded chat panel keeps its address and shows three rows and its JEV state', async t => {
  const currentHtml = await readFile(new URL('../chatgpt/panel.html', import.meta.url), 'utf8')
  const nextHtml = currentHtml.replace('查看判断依据</summary>', '查看原始判断依据</summary>')
  assert.notEqual(nextHtml, currentHtml)
  const cache = new Map()
  async function connect(html) {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    const server = await createPanelMCPServer({ resources: { html }, displayName: 'GhostSentinel', read: async () => ({ summary: '只读观察' }) })
    const client = new Client({ name: 'panel-cache-regression', version: '1' })
    t.after(async () => { await client.close(); await server.close() })
    await server.connect(serverTransport)
    await client.connect(clientTransport)
    const tools = await client.listTools()
    const uri = tools.tools.find(tool => tool.name === 'sentinel_current_chat')._meta.ui.resourceUri
    assert.ok((await client.listResources()).resources.some(resource => resource.uri === uri))
    return { uri, client }
  }
  const tabUri = 'ui://ghost-sentinel/current-chat-e2dde0133a9f3247f746d169729c5afcb8a40110cf52b99a1a4203ae5c97e6c6.html'
  async function loadTab(connection, pluginRevision) {
    assert.equal(connection.uri, tabUri, 'the existing 0.4.6 tab still matches the advertised resource address')
    const key = JSON.stringify([tabUri, pluginRevision])
    if (!cache.has(key)) {
      const resource = (await connection.client.readResource({ uri: tabUri })).contents[0]
      assert.equal(resource.uri, tabUri)
      cache.set(key, resource.text)
    }
    return cache.get(key)
  }
  const at = Date.now(), lines = ['在做什么：核对本地构建。', '有什么进展：定向验证通过。', '卡在哪里：等待最终校验。']
  const view = { signal: 'gray', summary: '当前记录重验中', identity: { threadId: 'cached-panel' },
    analysis: { state: 'unknown', checks: [] }, observation: { coverage: 'caught-up', recordRevalidating: true },
    recentModelActivity: { summary: { text: lines.join(' '), completedAt: at - 1000, expiresAt: at + 60000,
      blocking: { status: 'unblocked', reason: '定向验证通过', evidence: [{ text: '验证通过', reference: 'native:check' }] } } } }
  const previous = await connect(currentHtml)
  assert.match(await loadTab(previous, '0.4.6'), /查看判断依据<\/summary>/)

  const current = await connect(nextHtml)
  assert.match(await loadTab(current, '0.4.6'), /查看判断依据<\/summary>/)
  const updated = await loadTab(current, 'next-release')
  assert.match(updated, /查看原始判断依据<\/summary>/)
  const page = await panelHarness(t, { html: updated })
  await page.deliver(view)
  assert.deepEqual(page.element('#recent-summary-text').children.map(row => row.textContent.trim()), lines)
  assert.match(page.element('#recent-blocking-status').textContent, /^上次判断 · 未阻塞/)
  assert.match(page.element('#recent-blocking-evidence').textContent, /验证通过/)
  const [currentResource] = (await current.client.readResource({ uri: current.uri })).contents
  for (const uri of ['ui://ghost-sentinel/current-chat.html', `ui://ghost-sentinel/current-chat-${'a'.repeat(64)}.html`]) {
    const reopened = await current.client.readResource({ uri })
    assert.deepEqual(reopened.contents, [{ ...currentResource, uri }])
  }
  assert.deepEqual((await current.client.listResources()).resources.map(resource => resource.uri), [current.uri])
  await assert.rejects(current.client.readResource({ uri: 'ui://ghost-sentinel/current-chat-unknown.html' }), { code: -32602 })
})

test('GitHub normal and test panels have separate names through the public MCP resource and tool list', async t => {
  for (const [displayName, releasedAddress] of [
    ['GhostSentinel', 'e2dde0133a9f3247f746d169729c5afcb8a40110cf52b99a1a4203ae5c97e6c6'],
    ['GhostSentinel test', '5e95d866a9409c1c33aa3e4a6bb47a7b5bbda55aa0cf0634ee73c54e831766f4'],
  ]) {
    const page = await panel(t, { displayName })
    const tools = await page.rpc('tools/list')
    const monitor = tools.tools.find(tool => tool.name === 'sentinel_current_chat')
    assert.equal(monitor._meta.ui.resourceUri, `ui://ghost-sentinel/current-chat-${releasedAddress}.html`)
    assert.equal(monitor.title, displayName === 'GhostSentinel test' ? 'GhostSentinel 会话检查 test' : displayName + ' 会话检查')
    const resource = await page.rpc('resources/read', { uri: monitor._meta.ui.resourceUri })
    assert.ok(resource.contents[0].text.includes('<title>' + (displayName === 'GhostSentinel test' ? 'GhostSentinel 会话检查 test' : displayName + ' 会话检查') + '</title>'))
    assert.ok(resource.contents[0].text.includes('<h1>Sentinel · 当前会话</h1>'))
  }
})

test('legacy panel presentation remains unchanged and channel labels cannot inject arbitrary HTML', async t => {
  const page = await panel(t)
  const tools = await page.rpc('tools/list')
  const monitor = tools.tools.find(tool => tool.name === 'sentinel_current_chat')
  assert.equal(monitor.title, 'Sentinel 会话检查')
  const resource = await page.rpc('resources/read', { uri: monitor._meta.ui.resourceUri })
  assert.ok(resource.contents[0].text.includes('<h1>Sentinel · 当前会话</h1>'))
  for (const displayName of ['<script>alert(1)</script>', 'GhostSentinel TEST', '', null]) await assert.rejects(startPanelServer({ token, read: async () => ({ summary: '' }), displayName }), /display name/)
})

test('authenticated health preserves the optional release digest and rejects invalid or unauthenticated identity', async t => {
  let runtimeIdentity = { ...identity }
  const page = await panel(t, { runtimeStatus: () => runtimeIdentity })
  const health = authorization => fetch(page.server.url + '/health', { headers: { authorization } })
  assert.deepEqual(await (await health('Bearer ' + token)).json(), { protocol: 'ghost-sentinel-chatgpt-helper/1', ...identity })
  runtimeIdentity = { ...identity, releaseId: 'b'.repeat(64), secret: 'not-published' }
  assert.deepEqual(await (await health('Bearer ' + token)).json(), { protocol: 'ghost-sentinel-chatgpt-helper/1', ...identity, releaseId: 'b'.repeat(64) })
  assert.equal((await health('Bearer ' + 'c'.repeat(64))).status, 401)
  for (const releaseId of ['b'.repeat(63), 'B'.repeat(64), null, '<script>', 0]) {
    runtimeIdentity = { ...identity, releaseId }
    assert.equal((await health('Bearer ' + token)).status, 503)
  }
  runtimeIdentity = { ...identity, releaseId: 'b'.repeat(64), ownership: 'standby' }
  assert.equal((await health('Bearer ' + token)).status, 503)
})

test('two channel adapters share Core without losing their separate panel and source labels', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'sentinel-channel-presentation-'))
  const server = await startServer({ dataDir: join(dir, 'core') }), core = await connectCore({ dataDir: join(dir, 'core') }), adapters = []
  t.after(async () => { await Promise.all(adapters.map(adapter => adapter.close())); await core.close(); await server.close(); await rm(dir, { recursive: true, force: true }) })
  for (const [channel, displayName] of [['normal', 'GhostSentinel'], ['test', 'GhostSentinel test']]) {
    const native = { async sampleHost() { return { available: true, hostRunId: 'presentation-host', hostStartedAt: Date.now() - 1000, profile: supportedProfile } }, async readEnablement() { return { user: 'enabled', effective: 'enabled' } }, async listThreads() { return { data: [], nextCursor: null } }, async close() {} }
    const adapter = await startChatGPTAdapter({ core, native, sessionsRoot: dir, dataDir: join(dir, channel), installationId: 'presentation-' + channel, token, displayName, pollMs: 0, collectMaterials: true })
    adapters.push(adapter)
    await adapter.refresh()
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) })
    const tools = (await response.json()).result.tools
    assert.equal(tools.find(tool => tool.name === 'sentinel_current_chat').title, displayName === 'GhostSentinel test' ? 'GhostSentinel 会话检查 test' : displayName + ' 会话检查')
  }
  const sources = (await core.snapshot()).sources
  assert.equal(sources.find(source => source.descriptor.registrationId === 'chatgpt:presentation-normal').descriptor.name, 'GhostSentinel · ChatGPT · 本机执行域')
  assert.equal(sources.find(source => source.descriptor.registrationId === 'chatgpt:presentation-test').descriptor.name, 'GhostSentinel test · ChatGPT · 本机执行域')
})

test('test native submissions carry a visible channel label without changing business metadata or cancellation scope', async () => {
  const calls = [], metadata = { notificationId: 'notice', attemptId: 'attempt', sourceKey: 'source', hostRunId: 'host', mainId: 'main', lifecycleId: 'execution' }
  for (const testMode of [false, true]) {
    const installationId = testMode ? 'test-channel' : 'normal-channel', prefix = notificationPrefix(installationId)
    const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Presentation.app', installationId, testMode, run: async (file, args, input) => {
      const request = JSON.parse(input); calls.push(request)
      return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result: request.operation === 'cancel' ? { pendingIds: [], observedAt: 1000 } : { requestId: request.requestId, accepted: true, authorization: 'authorized', observedAt: 1000 } }) }
    } })
    try {
      const value = { requestId: prefix + 'attempt', title: 'GhostSentinel · 监控状态提醒', body: '当前来源采集明确失败。', metadata }
      await io.submit(value)
      assert.deepEqual(calls.at(-1), { schemaVersion: 1, operation: 'submit', ownerPrefix: prefix, ...value, title: testMode ? 'GhostSentinel test · 监控状态提醒' : value.title })
      await assert.rejects(io.cancel([notificationPrefix(testMode ? 'normal-channel' : 'test-channel') + 'attempt']), /another installation/)
      await io.cancel([prefix + 'attempt'])
      assert.deepEqual(calls.at(-1), { schemaVersion: 1, operation: 'cancel', ownerPrefix: prefix, requestIds: [prefix + 'attempt'] })
    } finally { io.close() }
  }
})

test('test titles stay within the Native budget without silently truncating business text', async () => {
  const calls = [], prefix = notificationPrefix('test-title-budget')
  const io = createNativeNotificationIO({ bundlePath: '/private/tmp/Presentation.app', installationId: 'test-title-budget', testMode: true, run: async (file, args, input) => {
    const request = JSON.parse(input); calls.push(request)
    return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result: { requestId: request.requestId, accepted: true, authorization: 'authorized', observedAt: 1000 } }) }
  } })
  try {
    const value = { requestId: prefix + 'attempt', body: 'b'.repeat(1200), metadata: { notificationId: 'notice', attemptId: 'attempt', sourceKey: 'source', hostRunId: 'host', mainId: null, lifecycleId: null } }
    for (const [title, expected] of [['业务提醒', 'GhostSentinel test · 业务提醒'], ['GhostSentinel test · 已标记', 'GhostSentinel test · 已标记'], ['GhostSentinel · ' + 'x'.repeat(179), 'GhostSentinel test · ' + 'x'.repeat(179)]]) {
      await io.submit({ ...value, title })
      assert.equal(calls.at(-1).title, expected)
      assert.ok(calls.at(-1).title.length <= 200)
      assert.equal(calls.at(-1).body, value.body)
    }
    const before = calls.length
    await assert.rejects(io.submit({ ...value, title: 'GhostSentinel · ' + 'x'.repeat(180) }), { code: 'native_protocol' })
    assert.equal(calls.length, before, 'over-budget relabeling never reaches Native')
  } finally { io.close() }
  assert.throws(() => createNativeNotificationIO({ bundlePath: '/private/tmp/Presentation.app', installationId: 'test-title-budget', testMode: 'true' }), /test mode/)
})
