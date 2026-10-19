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
  adapter = await startChatGPTAdapter({ core, native: {}, installationId: 'settings-installation', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (name, args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) })
    assert.equal(response.status, 200)
    const envelope = await response.json()
    assert.ok(!envelope.error, envelope.error?.message)
    assert.ok(!envelope.result?.isError)
    return envelope.result.structuredContent
  }
  const first = await rpc('sentinel_model_settings')
  assert.equal(first.registrationId, 'chatgpt:settings-installation')
  const saved = await rpc('sentinel_save_model_settings', { revision: first.revision, config: state.config })
  assert.deepEqual(saved.config, state.config)
  assert.deepEqual((await core.installationAnalysisConfig('chatgpt:another-installation')).config, {})
  assert.deepEqual((await rpc('sentinel_model_settings')).config, state.config)
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
