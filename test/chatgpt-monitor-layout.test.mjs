import test from 'node:test'
import assert from 'node:assert/strict'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const view = () => ({ identity: { threadId: 'main-a' }, signal: 'gray', summary: '尚未分析', observation: { status: 'running' }, execution: { key: 'turn-a', status: 'running', historical: false }, analysis: { state: 'unknown', issues: [] } })

test('the monitor puts the current light and JEV judgment beside the update time above the tabs', async t => {
  const page = await panelHarness(t)
  const completedAt = Date.now() - 1000
  const current = { ...view(), recentModelActivity: { summary: { text: '已核对日志', completedAt, expiresAt: completedAt + 60000,
    blocking: { status: 'unblocked', reason: '已取得工具结果', evidence: [{ text: '日志原文', reference: 'native:log' }] } } } }
  await page.deliver(current)
  assert.equal(page.element('#monitor-tab-issues').getAttribute('aria-selected'), 'true')
  assert.equal(page.element('#monitor-details').hidden, true)
  assert.equal(page.element('#issues').hidden, false)
  assert.equal(page.element('#issues').textContent, '')
  assert.equal(page.element('#session-light').textContent, '⚪')
  assert.match(page.element('#session-light').getAttribute('aria-label'), /不可判断/)
  assert.equal(page.element('#recent-blocking-status').textContent, '未阻塞')
  assert.equal(page.element('#recent-blocking-reason').textContent, '已取得工具结果')
  assert.equal(page.element('#recent-summary-time').dateTime, new Date(completedAt).toISOString())
  assert.equal(page.element('#recent-blocking-details').hidden, false)
  assert.equal(page.element('#recent-blocking-details').open, false)
  assert.match(page.element('#recent-blocking-evidence').textContent, /日志原文/)
  page.element('#monitor-tab-details').onclick()
  assert.equal(page.element('#monitor-details').hidden, false)
  assert.equal(page.element('#issues').hidden, true)
  assert.match(page.element('#summary').textContent, /尚未分析/)
  await page.refresh(); await page.deliver(current)
  assert.equal(page.element('#monitor-tab-details').getAttribute('aria-selected'), 'true', 'ordinary monitoring refresh keeps the chosen detail page')
  assert.match(page.html, /#gs-bar\s*\{[^}]*position:sticky[^}]*top:0/)
  const header = page.html.match(/<header id="gs-bar"[^>]*>[\s\S]*?<\/header>/)[0]
  assert.match(header, /id="session-light"[\s\S]*id="recent-blocking-status"[\s\S]*id="recent-summary-time"[\s\S]*<p id="recent-blocking-reason"/)
  assert.match(page.html, /#recent-blocking-reason\{[^}]*grid-column:1\/-1/)
  assert.doesNotMatch(header, /id="gs-(green|yellow|red|gray)"|>GS</)
  assert.ok(page.html.indexOf(header) < page.html.indexOf('id="monitor-tab-issues"'))
})

test('history details never count the selected old execution as the current main chat or hide an identical problem reason', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'red'; current.summary = '修改已偏离当前要求'
  current.analysis.issues = [{ checkId: 'request_drift', reason: current.summary, evidence: [{ text: '<literal> current action', reference: 'current-evidence' }] }]
  await page.deliver(current)
  assert.match(page.element('#issues').textContent, /修改已偏离当前要求/)
  assert.match(page.element('#issues').textContent, /<literal> current action/)
  assert.equal(page.element('#session-light').textContent, '🔴')
  await page.refresh()
  await page.deliver({ ...current, execution: { key: 'prior', historical: true, status: 'ended' }, history: { currentKey: 'turn-a', selectedKey: 'prior', currentStatus: 'running', currentSignal: 'red', executions: [{ key: 'turn-a', status: 'running' }, { key: 'prior', status: 'ended' }] }, analysis: { ...current.analysis, issues: [{ checkId: 'request_drift', reason: '历史问题', evidence: [{ text: 'old action', reference: 'old-evidence' }] }] } })
  assert.equal(page.element('#session-light').textContent, '🔴', 'the retained old execution does not replace the current main chat light')
  assert.equal(page.element('#issues').textContent, '', 'an old issue is not exposed as a current problem')
  assert.match(page.element('#detail-issues').textContent, /历史问题.*old-evidence/s)
  assert.match(page.element('#summary').textContent, /历史执行/)
})

test('the panel separates pending prefill release from its replacement summary and clears the status after release', async t => {
  const page = await panelHarness(t), current = view()
  current.recentModelActivity = { prefill: { state: 'cancelled' }, prefillRecovery: { summaryState: 'generating' } }
  await page.deliver(current)
  assert.equal(page.element('#recent-prefill-recovery').textContent, '旧预填待释放 · 新总结生成中')
  assert.equal(page.element('#recent-prefill-recovery').hidden, false)
  delete current.recentModelActivity.prefillRecovery
  await page.refresh(); await page.deliver(current)
  assert.equal(page.element('#recent-prefill-recovery').hidden, true)
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

test('a confirmed yellow observation exposes its short reason while ended and unbound chats have no active light', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'yellow'; current.summary = '首次检查尚无可见执行活动'
  await page.deliver(current)
  assert.equal(page.element('#session-light').textContent, '🟡')
  assert.match(page.element('#issues').textContent, /首次检查尚无可见执行活动/)
  assert.equal(page.sent.filter(row => /notification/.test(row.params?.name ?? '')).length, 0)
  await page.refresh(); await page.deliver({ ...current, execution: { key: 'turn-a', status: 'ended', historical: true }, history: { currentStatus: 'idle', currentSignal: 'yellow' } })
  assert.equal(page.element('#session-light').hidden, true)
  await page.refresh(); await page.deliver({ ...view(), identity: { threadId: null } })
  assert.equal(page.element('#session-light').hidden, true, 'unknown identity is not an invented main agent')
  assert.equal(page.element('#issues').textContent, '')
})

test('a problem uses a short delivery fact without claiming that the user saw the notice', async t => {
  const page = await panelHarness(t), current = view()
  current.signal = 'red'; current.analysis.issues = [{ checkId: 'request_drift', reason: '当前动作超出请求', notificationStatus: 'submitted', evidence: [{ text: 'observed action', reference: 'own-proof' }] }]
  await page.deliver(current)
  assert.match(page.element('#issues').textContent, /通知已提交/)
  assert.doesNotMatch(page.element('#issues').textContent, /已看到|你已看见|通知中心存在/)
})
