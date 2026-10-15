import test from 'node:test'
import assert from 'node:assert/strict'
import { startPanelServer } from '../chatgpt/panel-server.mjs'

test('the app-only pause tool passes host identity separately from checked control arguments', async t => {
  const calls = [], token = 'a'.repeat(64)
  const panel = await startPanelServer({ token, read: async () => ({ summary: 'observed' }), control: async (meta, change) => { calls.push({ meta, change }); return { summary: 'paused', monitoring: { paused: true } } } })
  t.after(() => panel.close())
  const call = async (method, params) => {
    const response = await fetch(panel.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    return response.json()
  }
  const tools = (await call('tools/list', {})).result.tools
  const tool = tools.find(tool => tool.name === 'sentinel_set_chat_analysis')
  assert.ok(tool, 'the real MCP endpoint advertises the scoped control')
  assert.equal(tool.annotations.readOnlyHint, false)
  assert.equal(tool.annotations.destructiveHint, false)
  assert.deepEqual(tool._meta.ui.visibility, ['app'])
  const change = { paused: true, expectedThreadId: 'main-a', revision: 'revision-a' }
  const response = await call('tools/call', { name: tool.name, arguments: change, _meta: { thread_id: 'main-a' } })
  assert.equal(response.result.structuredContent.monitoring.paused, true)
  assert.deepEqual(calls, [{ meta: { thread_id: 'main-a' }, change }])
  const invalid = await call('tools/call', { name: tool.name, arguments: { ...change, paused: 'true' }, _meta: { thread_id: 'main-a' } })
  assert.ok(invalid.error || invalid.result?.isError)
  assert.equal(calls.length, 1)
})

test('metadata-only panels do not expose a control they cannot perform', async t => {
  const token = 'b'.repeat(64), panel = await startPanelServer({ token, read: async () => ({ summary: 'observed' }) })
  t.after(() => panel.close())
  const response = await fetch(panel.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list', params: {} }) })
  const tools = (await response.json()).result.tools
  assert.equal(tools.some(tool => tool.name === 'sentinel_set_chat_analysis'), false)
})
