import test from 'node:test'
import assert from 'node:assert/strict'
import { startPanelServer } from '../chatgpt/panel-server.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'

const state = { registrationId: 'chatgpt:installation-a', revision: 'settings-revision-a', config: { llm: { baseUrl: 'http://127.0.0.1:1234', model: 'summary-a' }, jev: { baseUrl: 'http://127.0.0.1:2345', model: 'judge-a' } } }

test('installation settings remain available through the real MCP when current-chat collection fails', async t => {
  const token = 'a'.repeat(64), saved = []
  const panel = await startPanelServer({ token, read: async () => { throw new Error('collection unavailable') }, modelSettings: {
    read: async () => state,
    save: async change => { saved.push(change); return { ...state, config: change.config, revision: 'settings-revision-b' } },
  } })
  t.after(() => panel.close())
  const rpc = async (method, params) => {
    const response = await fetch(panel.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const tools = (await rpc('tools/list', {})).tools
  assert.ok(tools.find(tool => tool.name === 'sentinel_model_settings'))
  const save = tools.find(tool => tool.name === 'sentinel_save_model_settings')
  assert.deepEqual(save._meta.ui.visibility, ['app'])
  assert.equal(save.annotations.readOnlyHint, false)
  assert.deepEqual((await rpc('tools/call', { name: 'sentinel_model_settings', arguments: {} })).structuredContent, state)
  const change = { revision: state.revision, config: state.config }
  assert.equal((await rpc('tools/call', { name: save.name, arguments: change })).structuredContent.revision, 'settings-revision-b')
  assert.deepEqual(saved, [change])
})

test('the fixed settings entry saves an installation configuration without needing a confirmed chat', async t => {
  const page = await panelHarness(t)
  await page.deliver({ signal: 'gray', summary: '当前会话未确认', identity: { threadId: null }, observation: { gap: 'observation-unavailable' }, analysis: { state: 'unknown' } })
  const open = page.element('#settings-toggle').onclick()
  await page.flush()
  let call = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(call.params.name, 'sentinel_model_settings')
  page.reply(call.id, { structuredContent: state })
  await open
  assert.equal(page.element('#model-settings').hidden, false)
  assert.equal(page.element('#settings-llm-model').value, 'summary-a')
  page.element('#settings-llm-model').value = 'summary-b'
  const saving = page.element('#settings-form').onsubmit({ preventDefault() {} })
  await page.flush()
  call = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(call.params.name, 'sentinel_save_model_settings')
  assert.equal(call.params.arguments.revision, state.revision)
  assert.equal(call.params.arguments.config.llm.model, 'summary-b')
  assert.equal(call.params.arguments.expectedThreadId, undefined)
  page.reply(call.id, { structuredContent: { ...state, revision: 'settings-revision-b', config: { ...state.config, llm: { ...state.config.llm, model: 'summary-b' } } } })
  await saving
  assert.match(page.element('#settings-status').textContent, /已保存.*等待重新分析/)
  assert.match(page.element('#summary').textContent, /未确认/)
})

test('production adapter settings bind the installation independently from collection and other installations', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-'))
  let server, core, adapter
  t.after(async () => { try { await adapter?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir })
  core = await connectCore({ dataDir })
  const token = 'b'.repeat(64)
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement(cwd) { assert.equal(cwd, null); return { user: 'enabled', effective: 'enabled' } },
  }
  adapter = await startChatGPTAdapter({ core, native, installationId: 'settings-installation', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const request = async (method, params = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    const envelope = await response.json()
    assert.ok(!envelope.error, envelope.error?.message)
    assert.ok(!envelope.result?.isError)
    return envelope.result
  }
  const rpc = async (name, args = {}) => (await request('tools/call', { name, arguments: args })).structuredContent
  const tools = (await request('tools/list')).tools
  const monitor = tools.find(tool => tool.name === 'sentinel_current_chat'), settings = tools.find(tool => tool.name === 'sentinel_model_settings')
  assert.deepEqual(monitor._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  assert.deepEqual(settings._meta['openai/ui'].entrypoints, [{ type: 'global' }])
  assert.notEqual(monitor._meta.ui.resourceUri, settings._meta.ui.resourceUri)
  const resources = (await request('resources/list')).resources
  assert.deepEqual(resources.map(resource => resource.uri).sort(), ['ui://ghost-sentinel/current-chat.html', 'ui://ghost-sentinel/settings.html'])
  const resource = await request('resources/read', { uri: settings._meta.ui.resourceUri })
  assert.equal(resource.contents[0].mimeType, 'text/html;profile=mcp-app')
  const first = await rpc('sentinel_model_settings')
  assert.equal(first.registrationId, 'chatgpt:settings-installation')
  const page = await panelHarness(t, { html: resource.contents[0].text })
  const reading = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(reading.params.name, 'sentinel_model_settings')
  page.reply(reading.id, { structuredContent: first }); await page.flush()
  for (const role of ['llm', 'jev']) {
    page.element('#settings-' + role + '-url').value = state.config[role].baseUrl
    page.element('#settings-' + role + '-model').value = state.config[role].model
  }
  const saving = page.element('#settings-form').onsubmit({ preventDefault() {} }); await page.flush()
  const change = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(change.params.name, 'sentinel_save_model_settings')
  assert.equal(change.params.arguments.expectedThreadId, undefined)
  page.reply(change.id, await request('tools/call', change.params)); await saving
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.deepEqual((await core.installationAnalysisConfig('chatgpt:another-installation')).config, {})
  assert.deepEqual((await rpc('sentinel_model_settings')).config, state.config)
  const monitorPage = await panelHarness(t)
  await monitorPage.deliver({ summary: '采集失败', identity: { threadId: null }, analysis: { state: 'unknown' } })
  page.element('#settings-llm-model').value = 'unsubmitted-draft'
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  await monitorPage.refresh(); await monitorPage.deliver({ summary: '另一条聊天', identity: { threadId: 'other-chat' }, analysis: { state: 'unknown' } })
  await monitorPage.destroy()
  assert.equal(page.element('#settings-llm-model').value, 'unsubmitted-draft')
  const pendingRead = page.element('#settings-reload').onclick(); await page.flush()
  const lateRead = page.sent.findLast(row => row.method === 'tools/call')
  page.document.hidden = true
  page.handlers.get('visibilitychange')()
  await pendingRead
  assert.equal(page.sent.findLast(row => row.method === 'notifications/cancelled').params.requestId, lateRead.id)
  page.reply(lateRead.id, { structuredContent: first }); await page.flush()
  page.document.hidden = false
  page.handlers.get('visibilitychange')()
  assert.equal(page.element('#settings-llm-model').value, 'unsubmitted-draft')
  const directory = page.element('#settings-llm-check').onclick(); await page.flush()
  const pendingDirectory = page.sent.findLast(row => row.method === 'tools/call')
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: 'settings-close', method: 'ui/resource-teardown', params: {} } })
  assert.equal(JSON.stringify(page.sent.find(row => row.id === 'settings-close')?.result), '{}')
  await directory
  assert.equal(page.sent.findLast(row => row.method === 'notifications/cancelled').params.requestId, pendingDirectory.id)
  assert.equal(page.timers.size, 0)
  assert.equal(adapter.diagnostics().closed, false)
  assert.equal(adapter.diagnostics().trackedThreads, 0)
})

test('global settings reject untrusted host or plugin qualification before accessing installation configuration', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-qualification-'))
  let server, core, adapter, enabled = true, available = true
  t.after(async () => { try { await adapter?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir })
  const token = 'c'.repeat(64), registrationId = 'chatgpt:qualified-settings'
  adapter = await startChatGPTAdapter({ core, native: {
    async sampleHost() { return { available, hostRunId: 'settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
  }, installationId: 'qualified-settings', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (name, args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const before = (await rpc('sentinel_model_settings')).structuredContent
  enabled = false
  assert.equal((await rpc('sentinel_model_settings')).isError, true)
  assert.equal((await rpc('sentinel_save_model_settings', { revision: before.revision, config: state.config })).isError, true)
  assert.equal((await rpc('sentinel_model_directory', { baseUrl: 'http://127.0.0.1:1' })).isError, true)
  assert.deepEqual(await core.installationAnalysisConfig(registrationId), before)
  enabled = true; available = false
  assert.equal((await rpc('sentinel_model_settings')).isError, true)
  assert.equal((await rpc('sentinel_save_model_settings', { revision: before.revision, config: state.config })).isError, true)
  assert.deepEqual(await core.installationAnalysisConfig(registrationId), before)
})

test('model dropdown checks only directory metadata and preserves the manual model choice', async t => {
  const page = await panelHarness(t)
  await page.deliver({ summary: '当前会话未确认', identity: { threadId: null }, analysis: { state: 'unknown' } })
  const opening = page.element('#settings-toggle').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.method === 'tools/call').id, { structuredContent: state }); await opening
  const checking = page.element('#settings-llm-check').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(call.params.name, 'sentinel_model_directory')
  assert.equal(call.params.arguments.baseUrl, state.config.llm.baseUrl)
  assert.equal(call.params.arguments.model, undefined)
  page.reply(call.id, { structuredContent: { state: 'online', online: true, inferenceVerified: false, models: [{ id: 'summary-a', role: 'summary' }, { id: '<manual>', role: 'unknown' }, { id: 'judge-a', role: 'jev' }] } })
  await checking
  assert.equal(page.element('#settings-llm-options').children.length, 2)
  assert.match(page.element('#settings-llm-status').textContent, /模型可见.*未验证/)
  assert.equal(page.element('#settings-llm-model').value, 'summary-a')
})

test('saving models rejects an already pending old-view response instead of restoring its summary', async t => {
  const page = await panelHarness(t)
  const view = { signal: 'gray', summary: '当前执行', identity: { threadId: 'main' }, analysis: { state: 'cooldown', resultDisposition: 'current', generated: { text: '旧模型总结。', evidence: [{ text: '旧依据', reference: 'old' }] } } }
  await page.deliver(view)
  const opening = page.element('#settings-toggle').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.method === 'tools/call').id, { structuredContent: state }); await opening
  const oldRead = await page.refresh()
  const saving = page.element('#settings-form').onsubmit({ preventDefault() {} }); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  page.reply(save.id, { structuredContent: { ...state, revision: 'settings-revision-b' } }); await saving
  page.reply(oldRead, { structuredContent: view }); await page.flush()
  assert.equal(page.element('#model-summary').hidden, true)
  assert.doesNotMatch(page.element('#model-summary-text').textContent, /旧模型总结/)
})
