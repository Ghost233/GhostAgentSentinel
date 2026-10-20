import test from 'node:test'
import assert from 'node:assert/strict'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const view = () => ({ identity: { threadId: 'main-a' }, signal: 'gray', summary: '尚未分析', observation: { status: 'running' }, execution: { key: 'turn-a', status: 'running', historical: false }, analysis: { state: 'unknown', issues: [] } })

test('the monitor keeps four GS counts above two tabs and leaves the problem pane empty when analysis is unknown', async t => {
  const page = await panelHarness(t)
  await page.deliver(view())
  assert.equal(page.element('#monitor-tab-issues').getAttribute('aria-selected'), 'true')
  assert.equal(page.element('#monitor-details').hidden, true)
  assert.equal(page.element('#issues').hidden, false)
  assert.equal(page.element('#issues').textContent, '')
  assert.equal(page.element('#gs-gray').textContent, '⚪ 1')
  for (const [name, expected] of [['green', '🟢 0'], ['yellow', '🟡 0'], ['red', '🔴 0']]) assert.equal(page.element('#gs-' + name).textContent, expected)
  page.element('#monitor-tab-details').onclick()
  assert.equal(page.element('#monitor-details').hidden, false)
  assert.equal(page.element('#issues').hidden, true)
  assert.match(page.element('#summary').textContent, /尚未分析/)
  await page.refresh(); await page.deliver(view())
  assert.equal(page.element('#monitor-tab-details').getAttribute('aria-selected'), 'true', 'ordinary monitoring refresh keeps the chosen detail page')
  assert.match(page.html, /#gs-bar\s*\{[^}]*position:sticky[^}]*top:0/)
  assert.ok(page.html.indexOf('id="gs-bar"') < page.html.indexOf('id="monitor-tab-issues"'))
})

test('history details never count the selected old execution as the current main chat or hide an identical problem reason', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'red'; current.summary = '修改已偏离当前要求'
  current.analysis.issues = [{ checkId: 'request_drift', reason: current.summary, evidence: [{ text: '<literal> current action', reference: 'current-evidence' }] }]
  await page.deliver(current)
  assert.match(page.element('#issues').textContent, /修改已偏离当前要求/)
  assert.match(page.element('#issues').textContent, /<literal> current action/)
  assert.equal(page.element('#gs-red').textContent, '🔴 1')
  await page.refresh()
  await page.deliver({ ...current, execution: { key: 'prior', historical: true, status: 'ended' }, history: { currentKey: 'turn-a', selectedKey: 'prior', currentStatus: 'running', currentSignal: 'red', executions: [{ key: 'turn-a', status: 'running' }, { key: 'prior', status: 'ended' }] }, analysis: { ...current.analysis, issues: [{ checkId: 'request_drift', reason: '历史问题', evidence: [{ text: 'old action', reference: 'old-evidence' }] }] } })
  assert.equal(page.element('#gs-red').textContent, '🔴 1', 'the retained old execution does not replace the current main chat light')
  assert.equal(page.element('#issues').textContent, '', 'an old issue is not exposed as a current problem')
  assert.match(page.element('#detail-issues').textContent, /历史问题.*old-evidence/s)
  assert.match(page.element('#summary').textContent, /历史执行/)
})

test('host resource teardown cancels the outstanding monitor read and releases every refresh handle', async t => {
  const page = await panelHarness(t), activeRead = page.sent.findLast(row => row.params?.name === 'sentinel_current_chat')
  const receive = page.handlers.get('message')
  receive({ source: page.parent, data: { jsonrpc: '2.0', id: 501, method: 'ui/resource-teardown', params: {} } })
  await page.flush()
  assert.ok(page.sent.some(row => row.method === 'notifications/cancelled' && row.params.requestId === activeRead.id))
  assert.ok(page.sent.some(row => row.id === 501 && row.result))
  assert.equal(page.timers.size, 0)
  assert.equal(page.handlers.has('message'), false)
  receive({ source: page.parent, data: { jsonrpc: '2.0', id: activeRead.id, result: { structuredContent: view() } } })
  await page.flush()
  assert.equal(page.element('#issues').textContent, '')
  assert.equal(page.timers.size, 0)
})

test('a confirmed yellow observation exposes its short reason while ended and unbound chats do not gain running counts', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'yellow'; current.summary = '首次检查尚无可见执行活动'
  await page.deliver(current)
  assert.equal(page.element('#gs-yellow').textContent, '🟡 1')
  assert.match(page.element('#issues').textContent, /首次检查尚无可见执行活动/)
  assert.equal(page.sent.filter(row => /notification/.test(row.params?.name ?? '')).length, 0)
  await page.refresh(); await page.deliver({ ...current, execution: { key: 'turn-a', status: 'ended', historical: true }, history: { currentStatus: 'idle', currentSignal: 'yellow' } })
  for (const [name, expected] of [['green', '🟢 0'], ['yellow', '🟡 0'], ['red', '🔴 0'], ['gray', '⚪ 0']]) assert.equal(page.element('#gs-' + name).textContent, expected)
  await page.refresh(); await page.deliver({ ...view(), identity: { threadId: null } })
  assert.equal(page.element('#gs-gray').textContent, '⚪ 0', 'unknown identity is not an invented main agent')
  assert.equal(page.element('#issues').textContent, '')
})

test('a problem uses a short delivery fact without claiming that the user saw the notice', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'red'; current.analysis.issues = [{ checkId: 'request_drift', reason: '当前动作超出请求', notificationStatus: 'submitted', evidence: [{ text: 'observed action', reference: 'own-proof' }] }]
  await page.deliver(current)
  assert.match(page.element('#issues').textContent, /通知已提交/)
  assert.doesNotMatch(page.element('#issues').textContent, /已看到|你已看见|通知中心存在/)
})
