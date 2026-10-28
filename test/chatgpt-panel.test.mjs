import test from 'node:test'
import assert from 'node:assert/strict'
import { panelHarness as harness } from './helpers/chatgpt-panel.mjs'

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const result = id => ({ signal: 'gray', summary: id ? id + ' 执行开始' : '无法确认当前观察', identity: { threadId: id }, observation: { coverage: 'caught-up', gap: null }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } })

test('details stay directly readable across refresh, transport failure and chat changes without disclosure controls', async t => {
  const page = await harness(t)
  page.element('#monitor-tab-details').onclick()
  const sections = ['checks', 'diagnostics', 'history', 'last-summary']
  const expanded = () => {
    for (const id of sections) assert.equal(page.element('#' + id).tagName, 'SECTION', id + ' must not require expansion')
    assert.equal(page.element('#monitor-details').hidden, false)
  }
  await page.deliver(material('main-a'))
  expanded()
  assert.equal(page.element('#check-list').children.length, 6)
  await page.refresh(); await page.deliver(material('main-a'))
  expanded()
  const request = await page.refresh()
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: request, error: { code: -32603, message: 'Transport closed' } } }); await page.flush()
  expanded()
  assert.equal(page.element('#check-list').textContent, '', 'failed reads clear old checks without collapsing the details page')
  await page.refresh(); await page.deliver(material('main-b'))
  expanded()
  assert.match(page.element('#identity').textContent, /main-b/)
  assert.equal(page.element('#check-list').children.length, 6)
})
const material = (id = 'main-a', signal = 'red') => ({ ...result(id), signal, summary: '当前修改可能偏离请求', analysis: { connected: true, state: 'cooldown', resultDisposition: 'current', summary: '本轮六项检查已更新', waitMs: 0, lagMs: 2500, coverage: { complete: true, gaps: [] }, checks: ids.map(checkId => ({ id: checkId, status: checkId === 'request_drift' ? 'attention' : 'clear', reason: '已核对现有材料' })), issues: [{ checkId: 'request_drift', reason: '用户只要求改文字，代理却更改了数据层', evidence: [{ text: '<img src=x onerror="alert(1)"> 只改按钮文字', reference: 'record:private-evidence-id' }] }] }, discovery: { complete: false, directoryComplete: true, counts: { main: 3, fork: 1, child: 2, remote: 0, cloud: 0, unknown: 1 }, tracked: 3, connected: 1, currentIncluded: true, gaps: ['fork-coverage-pending'] } })

test('missing observation data stays visible and refreshes automatically or manually in the same page', async t => {
  const page = await harness(t)
  const waiting = { signal: 'gray', summary: '观察服务尚未就绪，等待数据', observation: { coverage: 'unavailable', gap: 'observer-unavailable' }, analysis: { state: 'unknown', checks: [] } }
  await page.deliver(waiting)
  assert.equal(page.element('#availability').hidden, false)
  assert.match(page.element('#availability').textContent, /等待数据/)
  await page.refresh(); await page.deliver(waiting)
  const before = page.sent.filter(row => row.params?.name === 'sentinel_current_chat').length
  const refreshing = page.element('#refresh').onclick(); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_current_chat').length, before + 1)
  await page.deliver(result('main-a')); await refreshing
  assert.equal(page.element('#availability').hidden, true)
  assert.match(page.element('#identity').textContent, /main-a/)
})

test('a completed model summary keeps literal evidence and disappears when the next result is pending or failed', async t => {
  const page = await harness(t), view = material('main-a', 'gray')
  view.analysis.generated = { text: '正在核对按钮文案。尚未得到验证结果。', evidence: [{ text: '<button> 已修改', reference: 'native:current-evidence' }] }
  await page.deliver(view)
  assert.equal(page.element('#model-summary').hidden, false)
  assert.match(page.element('#model-summary-text').textContent, /按钮文案/)
  assert.match(page.element('#model-summary-evidence').textContent, /<button> 已修改/)
  assert.match(page.element('#model-summary-evidence').textContent, /native:current-evidence/)
  assert.equal(page.element('#model-summary-evidence').children[0].children.length, 0)
  await page.refresh(); await page.deliver({ ...view, analysis: { ...view.analysis, state: 'idle', resultDisposition: 'reused' } })
  assert.equal(page.element('#model-summary').hidden, false, 'unchanged valid material keeps its successful model summary')
  assert.match(page.element('#model-status').textContent, /复用/)
  await page.refresh(); await page.deliver({ ...view, analysis: { ...view.analysis, resultDisposition: 'pending' } })
  assert.equal(page.element('#model-summary').hidden, true)
  await page.refresh(); await page.deliver({ ...view, analysis: { ...view.analysis, state: 'failed' } })
  assert.equal(page.element('#model-summary').hidden, true)
})

test('a failed new round keeps the prior successful summary only in directly readable history with its original time and evidence', async t => {
  const page = await harness(t), view = material('main-a', 'gray'), completedAt = Date.now() - 1000
  view.analysis.state = 'failed'; view.analysis.resultDisposition = 'invalid'
  view.analysis.lastSuccessfulSummary = { generated: { text: '上次已观察到按钮更新。', evidence: [{ text: '<button> 已修改', reference: 'native:prior-result' }] }, completedAt, expiresAt: completedAt + 86400000, configScope: { registrationId: 'chatgpt:installation-a', revision: 'prior-revision' } }
  view.history = { currentKey: 'current', selectedKey: 'current', executions: [{ key: 'current', status: 'running', observedAt: completedAt }], hasMore: false }
  await page.deliver(view)
  assert.equal(page.element('#model-summary').hidden, true)
  assert.equal(page.element('#last-summary').hidden, false)
  assert.equal(page.element('#last-summary').tagName, 'SECTION')
  assert.match(page.element('#last-summary-text').textContent, /上次已观察/)
  assert.match(page.element('#last-summary-time').textContent, /历史.*当前/)
  assert.match(page.element('#last-summary-evidence').textContent, /<button> 已修改.*native:prior-result/s)
  assert.doesNotMatch(page.element('#summary').textContent, /正常/)
  await page.refresh(); await page.deliver({ ...view, analysis: { ...view.analysis, lastSuccessfulSummary: { ...view.analysis.lastSuccessfulSummary, expiresAt: Date.now() - 1 } } })
  assert.equal(page.element('#last-summary').hidden, true)
  assert.equal(page.element('#last-summary-text').textContent, '')
  await page.refresh(); await page.deliver(result('main-b'))
  assert.equal(page.element('#last-summary').hidden, true)
  assert.equal(page.element('#last-summary-evidence').textContent, '')
})

test('an unconfirmed chat explains the actual collection boundary without inline configuration', async t => {
  const page = await harness(t)
  await page.deliver({ ...result(null), observation: { gap: 'observation-unavailable', failureStage: 'core-connection' }, health: { state: 'failed', failures: [{ domain: 'collection', error: { code: 'ECONNREFUSED', stage: 'core' } }] }, notifications: { permission: { authorization: 'denied' } } })
  assert.match(page.element('#observation').textContent, /共享核心连接/)
  assert.match(page.element('#health').textContent, /ECONNREFUSED/)
  assert.doesNotMatch(page.html, /id="(?:settings-toggle|model-settings|notification-permission|notification-log)"/)
  assert.equal(page.sent.filter(row => ['sentinel_model_settings', 'sentinel_request_notification_permission', 'sentinel_notification_records'].includes(row.params?.name)).length, 0)
})


test('current facts stay visible while inherited requirements remain pending and cannot appear fully green', async t => {
  const page = await harness(t), view = material('main-a', 'green')
  view.summary = '正常'
  view.analysis.issues = []
  view.analysis.checks = ids.map(id => ({ id, status: 'clear', reason: '当前片段未见问题' }))
  view.currentFacts = { request: { text: '只改 <button> 的文字', evidenceIds: ['input-current'] }, activity: { summary: '工具结果：文案已更新', evidenceIds: ['result-current'], observedAt: 1000 } }
  view.inheritance = { state: 'pending', gaps: ['fork_context_pending'] }
  await page.deliver(view)
  assert.match(page.element('#summary').textContent, /^⚪/)
  assert.doesNotMatch(page.element('#summary').textContent, /正常/)
  assert.match(page.element('#inheritance').textContent, /继承要求待补齐/)
  assert.equal(page.element('#current-facts').hidden, false)
  assert.match(page.element('#current-request').textContent, /只改 <button> 的文字/)
  assert.match(page.element('#current-activity').textContent, /文案已更新/)
  assert.equal(page.element('#current-request').children.length, 0, 'current facts remain literal text')
  assert.match(page.element('#diagnostics-title').textContent, /有缺口/)
})

test('current record revalidation is gray, retains verified facts and recovers without leaking into history', async t => {
  const page = await harness(t), view = material('main-a', 'green')
  view.summary = '正常'
  view.analysis.issues = []
  view.analysis.checks = ids.map(id => ({ id, status: 'clear', reason: '已核对现有材料' }))
  view.currentFacts = { request: { text: '只改文案', evidenceIds: ['input-current'] }, activity: { summary: '文案已更新', evidenceIds: ['result-current'], observedAt: 1000 } }
  view.observation.recordRevalidating = true
  await page.deliver(view)
  assert.equal(page.element('#summary').getAttribute('data-signal'), 'gray')
  assert.match(page.element('#summary').textContent, /当前记录重验中/)
  assert.match(page.element('#record-status').textContent, /已核实.*新活动尚未核实/)
  assert.match(page.element('#current-request').textContent, /已核实请求：只改文案/)
  assert.match(page.element('#current-activity').textContent, /已核实活动：文案已更新/)
  await page.refresh()
  await page.deliver({ ...view, observation: { ...view.observation, recordRevalidating: false } })
  assert.equal(page.element('#summary').getAttribute('data-signal'), 'green')
  assert.equal(page.element('#record-status').textContent, '')
  assert.match(page.element('#current-request').textContent, /^当前请求：/)
  await page.refresh()
  await page.deliver({ ...view, execution: { historical: true, key: 'older-execution', status: 'idle' } })
  assert.match(page.element('#summary').textContent, /历史执行/)
  assert.doesNotMatch(page.element('#summary').textContent, /重验/)
  assert.equal(page.element('#record-status').textContent, '')
  assert.equal(page.element('#current-facts').hidden, true)
})

test('pending inheritance preserves grounded findings and current facts clear when the displayed scope changes', async t => {
  const page = await harness(t), view = material()
  view.currentFacts = { request: { text: '只改文案', evidenceIds: ['input-current'] }, activity: { summary: '工具返回明确失败', evidenceIds: ['result-current'], observedAt: 1000 } }
  view.inheritance = { state: 'pending', gaps: ['fork_context_pending'] }
  await page.deliver(view)
  assert.match(page.element('#summary').textContent, /^🔴/)
  assert.match(page.element('#inheritance').textContent, /待补齐/)
  assert.equal(page.element('#issues').children.length, 1)
  await page.refresh()
  await page.deliver({ ...view, execution: { historical: true, key: 'older-execution', status: 'idle' } })
  assert.equal(page.element('#current-facts').hidden, true, 'a history container cannot present live current facts')
  assert.equal(page.element('#current-request').textContent, '')
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  assert.equal(page.element('#inheritance').textContent, '')
  assert.equal(page.element('#current-activity').textContent, '')
})

test('the real panel script accepts grounded material issues and places small literal evidence directly beneath each issue', async t => {
  const page = await harness(t)
  await page.deliver(material())
  assert.match(page.element('#summary').textContent, /🔴.*当前修改可能偏离请求/)
  assert.match(page.element('#analysis').textContent, /已更新/)
  assert.match(page.element('#analysis').textContent, /滞后.*[23].*秒/)
  const issues = page.element('#issues')
  assert.equal(issues.children.length, 1)
  assert.match(issues.textContent, /偏离当前请求/)
  assert.match(issues.textContent, /只要求改文字/)
  const evidence = issues.children[0].children.find(node => node.className === 'evidence')
  assert.ok(evidence)
  assert.match(evidence.textContent, /<img src=x/)
  assert.equal(evidence.tagName, 'P')
  assert.equal(evidence.children.length, 0, 'evidence is literal text, not injected markup')
  assert.equal(page.element('#check-list').children.length, 6)
  for (const label of ['活跃但推进不足', '偏离当前请求', '同类失败反复返工', '完整验证重复成本', '本地验证证据缺口', '长等待或活动中断']) assert.ok(page.element('#check-list').textContent.includes(label), label)
  assert.equal(page.element('#checks').tagName, 'SECTION')
  assert.equal(page.element('#diagnostics').tagName, 'SECTION')
  assert.match(page.element('#coverage').textContent, /主聊天.*3/)
  assert.match(page.element('#coverage').textContent, /分叉.*1/)
})

test('the problem tab keeps its conclusion and inline evidence while details avoid repeating the lead conclusion', async t => {
  const page = await harness(t), view = material()
  view.summary = view.analysis.issues[0].reason
  await page.deliver(view)
  assert.ok(page.element('#summary').textContent.includes(view.summary))
  assert.equal(page.element('#issues').textContent.includes(view.summary), true)
  assert.equal(page.element('#detail-issues').textContent.includes(view.summary), false)
  assert.match(page.element('#issues').textContent, /偏离当前请求/)
  assert.ok(page.element('#detail-issues').children[0].children.some(node => node.className === 'evidence'))
})

test('each issue keeps its own complete literal reference below the evidence rather than in diagnostics', async t => {
  const page = await harness(t), view = material(), excerpt = '<img src=x onerror="alert(1)"> same excerpt'
  const references = [('native:' + 'a'.repeat(180) + '<script>bad()</script>').padEnd(2000, 'x'), 'native:other-reference']
  view.analysis.issues = references.map((reference, index) => ({ checkId: index ? 'validation_gap' : 'request_drift', reason: 'Issue ' + index, evidence: [{ text: excerpt, reference }] }))
  await page.deliver(view)
  const issues = page.element('#issues').children
  assert.equal(issues.length, 2)
  for (let index = 0; index < 2; index++) {
    const evidence = issues[index].children.find(child => child.className === 'evidence')
    assert.ok(evidence.textContent.includes(excerpt))
    assert.ok(evidence.textContent.includes(references[index]), 'the complete bounded reference belongs to its own issue')
    assert.equal(evidence.children.length, 0, 'both excerpt and reference are literal text')
    assert.equal(issues[index].textContent.includes(references[1 - index]), false)
  }
  assert.equal(page.element('#technical').textContent.includes('native:'), false)
  assert.match(page.html, /overflow-wrap:anywhere/)
})

test('the current-chat pause button uses its displayed identity and revision and keeps collection visible', async t => {
  const page = await harness(t)
  const view = material('main-a')
  view.monitoring = { registrationId: 'chatgpt:installation', taskId: 'main-a', paused: false, revision: 'revision-a' }
  await page.deliver(view)
  const button = page.element('#pause')
  assert.equal(button.hidden, false)
  assert.equal(button.textContent, '暂停本聊天分析')
  const operation = button.onclick()
  await page.flush()
  assert.equal(button.disabled, true)
  const sent = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(sent.params.name, 'sentinel_set_chat_analysis')
  assert.equal(sent.params.arguments.expectedThreadId, 'main-a')
  assert.equal(sent.params.arguments.revision, 'revision-a')
  assert.equal(sent.params.arguments.paused, true)
  const paused = { ...view, monitoring: { ...view.monitoring, paused: true, revision: 'revision-b' }, analysis: { ...view.analysis, state: 'paused' } }
  page.reply(sent.id, { structuredContent: paused })
  await operation
  assert.equal(button.textContent, '恢复本聊天分析')
  assert.equal(button.disabled, false)
  assert.match(page.element('#summary').textContent, /本聊天分析已暂停/)
  assert.match(page.element('#analysis').textContent, /继续采集/)
  assert.match(page.element('#identity').textContent, /main-a/)
  assert.equal([...page.timers.values()].filter(value => value.delay === 1500).length, 1)
})

test('an unconfirmed pause is not replayed and a fresh read restores the explicit resume control', async t => {
  const page = await harness(t), view = material('main-a')
  view.monitoring = { registrationId: 'chatgpt:installation', taskId: 'main-a', paused: false, revision: 'before-pause' }
  await page.deliver(view)
  const operation = page.element('#pause').onclick()
  await page.flush()
  const sent = page.sent.findLast(row => row.params?.name === 'sentinel_set_chat_analysis')
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: sent.id, error: { code: -32603, message: 'Observation temporarily unconfirmed; request outcome may be unknown' } } })
  await operation
  assert.match(page.element('#summary').textContent, /暂停状态未确认/)
  assert.equal(page.element('#pause').hidden, true)
  await page.refresh()
  assert.equal(page.sent.findLast(row => row.method === 'tools/call').params.name, 'sentinel_current_chat')
  const paused = { ...view, monitoring: { ...view.monitoring, paused: true, revision: 'after-pause' }, analysis: { ...view.analysis, state: 'paused' } }
  await page.deliver(paused)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_set_chat_analysis').length, 1)
  assert.equal(page.element('#pause').hidden, false)
  assert.equal(page.element('#pause').textContent, '恢复本聊天分析')
  const resume = page.element('#pause').onclick()
  await page.flush()
  const change = page.sent.findLast(row => row.params?.name === 'sentinel_set_chat_analysis')
  assert.equal(change.params.arguments.paused, false)
  assert.equal(change.params.arguments.revision, 'after-pause')
  page.reply(change.id, { structuredContent: { ...view, monitoring: { ...view.monitoring, revision: 'after-resume' } } })
  await resume
  assert.equal(page.element('#pause').textContent, '暂停本聊天分析')
})

test('a pause response from the previous chat cannot overwrite the new chat or leave a control exposed', async t => {
  const page = await harness(t), view = material('main-a')
  view.monitoring = { registrationId: 'chatgpt:installation', taskId: 'main-a', paused: false, revision: 'revision-a' }
  await page.deliver(view)
  const operation = page.element('#pause').onclick()
  await page.flush()
  const sent = page.sent.findLast(row => row.method === 'tools/call')
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  assert.equal(page.element('#pause').hidden, true)
  page.reply(sent.id, { structuredContent: { ...view, monitoring: { ...view.monitoring, paused: true } } })
  await operation
  assert.equal(page.element('#identity').textContent, '')
  assert.equal(page.element('#pause').hidden, true)
  await page.refresh(); await page.deliver(result('main-b'))
  assert.match(page.element('#identity').textContent, /main-b/)
  assert.equal(page.element('#pause').hidden, true, 'a legacy observation-only view cannot write monitoring state')
})

test('the first settled monitoring failure is distinct from task findings and clears on chat switches', async t => {
  const page = await harness(t), view = material('main-a', 'gray')
  view.health = { state: 'failed', failures: [{ id: 'failure-a', domain: 'analysis', error: { code: 'invalid_json', stage: 'llm' } }] }
  await page.deliver(view)
  assert.match(page.element('#health').textContent, /监控故障.*分析.*invalid_json/)
  assert.match(page.element('#issues').textContent, /用户只要求/)
  assert.doesNotMatch(page.element('#health').textContent, /卡死|任务疑点/)
  await page.refresh()
  const priorGreen = { ...view, signal: 'green', analysis: { ...view.analysis, issues: [], checks: ids.map(id => ({ id, status: 'clear', reason: 'Previous result' })) } }
  await page.deliver(priorGreen)
  assert.match(page.element('#summary').textContent, /^⚪.*监控故障/)
  await page.refresh()
  await page.deliver({ ...priorGreen, health: { ...view.health, orderingGap: true } })
  assert.match(page.element('#summary').textContent, /^⚪.*恢复状态未确认/)
  assert.match(page.element('#health').textContent, /曾有失败.*恢复状态未确认/)
  assert.match(page.element('#gaps').textContent, /排序资料不完整/)
  await page.refresh()
  await page.deliver({ ...view, health: { state: 'recovered', failures: [] } })
  assert.match(page.element('#health').textContent, /监控已恢复/)
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  assert.equal(page.element('#health').textContent, '')
})

test('unknown or unconfigured results never masquerade as green while legitimate prior green can remain queued', async t => {
  const page = await harness(t)
  const unavailable = material('main-a', 'green')
  unavailable.summary = '正常'
  unavailable.analysis.state = 'unavailable'; unavailable.analysis.issues = []
  unavailable.analysis.coverage = { complete: false, gaps: ['llm_unconfigured'], dependencies: ['llm_unconfigured'] }
  unavailable.analysis.checks = ids.map(id => ({ id, status: 'unknown', reason: '依赖尚未配置' }))
  await page.deliver(unavailable)
  assert.match(page.element('#summary').textContent, /^⚪/)
  assert.doesNotMatch(page.element('#summary').textContent, /正常/)
  assert.match(page.element('#analysis').textContent, /未配置/)
  assert.equal(page.element('#issues').children.length, 0)
  assert.ok(page.element('#check-list').children.every(row => row.textContent.includes('不可判断')))
  await page.refresh()
  const unknown = { ...unavailable, analysis: { ...unavailable.analysis, state: 'unknown' } }
  await page.deliver(unknown)
  assert.match(page.element('#summary').textContent, /^⚪/)
  assert.match(page.element('#analysis').textContent, /不可判断/)
  await page.refresh()
  const queued = material('main-a', 'green')
  queued.summary = '旧正常结论等待本轮新材料'; queued.analysis.state = 'queued'; queued.analysis.resultDisposition = 'pending'; queued.analysis.waitMs = 12000; queued.analysis.lagMs = 9000
  queued.analysis.issues = []; queued.analysis.checks = ids.map(id => ({ id, status: 'clear', reason: '上轮结论仍在有效范围' }))
  await page.deliver(queued)
  assert.match(page.element('#summary').textContent, /^🟢.*上轮正常.*待分析/)
  assert.match(page.element('#analysis').textContent, /排队.*12.*秒/)
  assert.match(page.element('#analysis').textContent, /滞后.*9.*秒/)
  assert.equal(page.element('#error').textContent, '')
  await page.refresh()
  const first = { ...queued, summary: '正常', analysis: { ...queued.analysis, state: 'analyzing', resultDisposition: 'none' } }
  await page.deliver(first)
  assert.match(page.element('#summary').textContent, /^⚪/)
  assert.doesNotMatch(page.element('#summary').textContent, /正常/)
})

test('context changes clear issues and coverage immediately, reject late output and preserve legacy metadata rendering', async t => {
  const page = await harness(t)
  await page.deliver(material())
  const oldRequest = await page.refresh()
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  for (const id of ['identity', 'coverage', 'technical', 'issues', 'check-list', 'gaps']) assert.equal(page.element('#' + id).textContent, '', id)
  assert.equal(page.element('#checks').tagName, 'SECTION')
  assert.equal(page.element('#diagnostics').tagName, 'SECTION')
  page.reply(oldRequest, { structuredContent: material('main-a') }); await page.flush()
  assert.equal(page.element('#issues').children.length, 0)
  assert.doesNotMatch(page.element('#summary').textContent, /偏离请求/)
  await page.refresh(); await page.deliver(result('main-b'))
  assert.match(page.element('#identity').textContent, /main-b/)
  assert.match(page.element('#analysis').textContent, /尚未接入/)
  assert.equal(page.element('#coverage').textContent, '')
  assert.equal(page.element('#issues').children.length, 0)
  await page.refresh(); await page.deliver(result(null))
  assert.match(page.element('#summary').textContent, /会话未确认/)
  assert.equal(page.element('#identity').textContent, '')
  assert.equal(page.element('#check-list').children.length, 0)
})

test('a failed bridge response clears all prior material issues and technical coverage', async t => {
  const page = await harness(t)
  await page.deliver(material())
  const request = await page.refresh()
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: request, error: { message: 'controlled transport failure' } } })
  await page.flush()
  assert.match(page.element('#summary').textContent, /^⚪.*读取失败/)
  for (const id of ['identity', 'coverage', 'observation', 'issues', 'check-list', 'gaps']) assert.equal(page.element('#' + id).textContent, '', id)
  assert.match(page.element('#analysis').textContent, /不可判断/)
  assert.doesNotMatch(page.element('#technical').textContent, /private-evidence-id|fork-coverage-pending/)
  assert.match(page.element('#technical').textContent, /controlled transport failure/)
  await page.refresh(); await page.deliver(result('recovered-main'))
  assert.equal(page.element('#error').textContent, '')
  assert.doesNotMatch(page.element('#technical').textContent, /controlled transport failure/)
})

test('hidden pages stop scheduling, visibility restores one loop and pagehide releases an in-flight request', async t => {
  const page = await harness(t)
  await page.deliver(material())
  page.document.hidden = true; page.handlers.get('visibilitychange')(); await page.flush()
  assert.equal(page.timers.size, 0)
  const calls = page.sent.filter(value => value.method === 'tools/call').length
  page.document.hidden = false; page.handlers.get('visibilitychange')(); page.handlers.get('visibilitychange')(); await page.flush()
  assert.equal(page.sent.filter(value => value.method === 'tools/call').length, calls + 1)
  await page.deliver(result('main-b'))
  assert.equal([...page.timers.values()].filter(value => value.delay === 1500).length, 1)
  await page.refresh()
  assert.equal([...page.timers.values()].filter(value => value.delay === 5000).length, 1)
  await page.destroy()
  assert.equal(page.timers.size, 0)
  assert.equal(page.handlers.has('message'), false)
  assert.equal(page.handlers.has('visibilitychange'), false)
})

test('an unbound source failure keeps only its new diagnostic gaps after clearing the former chat', async t => {
  const page = await harness(t)
  await page.deliver(material('main-a'))
  assert.match(page.element('#issues').textContent, /只要求改文字/)
  await page.refresh()
  const failed = {
    ...result(null),
    observation: { coverage: 'gap', gap: 'execution-source-unavailable' },
    discovery: { ...material().discovery, gaps: ['directory-page-failed'] },
  }
  await page.deliver(failed)
  assert.match(page.element('#summary').textContent, /^⚪.*当前会话未确认/)
  assert.match(page.element('#gaps').textContent, /execution-source-unavailable/)
  assert.match(page.element('#gaps').textContent, /directory-page-failed/)
  assert.doesNotMatch(page.element('#gaps').textContent, /fork-coverage-pending/)
  for (const id of ['identity', 'coverage', 'technical', 'issues', 'check-list']) assert.equal(page.element('#' + id).textContent, '', id)
  assert.equal(page.element('#diagnostics').tagName, 'SECTION')
  assert.match(page.element('#diagnostics-title').textContent, /有缺口/)
  await page.refresh(); await page.deliver(result('main-b'))
  assert.equal(page.element('#gaps').textContent, '')
  assert.match(page.element('#identity').textContent, /main-b/)
})

test('the actual panel DOM omits excluded child-agent counts while retaining current-chat coverage and evidence', async t => {
  const page = await harness(t), view = material()
  view.discovery.counts.child = 987
  await page.deliver(view)
  const visible = ['summary', 'coverage', 'technical', 'gaps'].map(id => page.element('#' + id).textContent).join('\n')
  assert.doesNotMatch(visible, /子代理|child|987/)
  assert.match(page.element('#coverage').textContent, /本聊天：已接入/)
  assert.match(page.element('#coverage').textContent, /主聊天 3/)
  assert.match(page.element('#issues').textContent, /private-evidence-id/)
  assert.equal(page.element('#issues').children.length, 1)
})

test('the panel selects its own monitored history, keeps evidence inline and returns to the current execution', async t => {
  const page = await harness(t), view = material('main-a')
  view.monitoring = { registrationId: 'chatgpt:installation', taskId: 'main-a', paused: false, revision: 'revision-a' }
  view.execution = { key: 'current', lifecycleId: 'turn-current', historical: false, status: 'running', request: 'Current request' }
  view.history = { currentKey: 'current', selectedKey: 'current', executions: [{ key: 'current', lifecycleId: 'turn-current', status: 'running', observedAt: 1000 }, { key: 'prior', lifecycleId: 'turn-prior', status: 'ended', observedAt: 500 }], hasMore: false }
  await page.deliver(view)
  assert.equal(page.element('#history').tagName, 'SECTION')
  const prior = page.element('#history-list').children.find(node => node.getAttribute('data-execution-key') === 'prior')
  assert.ok(prior, 'actual history markup provides the prior execution selector')
  const selection = prior.onclick(); await page.flush()
  const request = page.sent.findLast(row => row.method === 'tools/call')
  assert.deepEqual(JSON.parse(JSON.stringify(request.params.arguments)), { executionKey: 'prior', expectedThreadId: 'main-a' })
  assert.equal(page.element('#issues').textContent, '', 'the former execution evidence clears while selection is pending')
  page.reply(request.id, { structuredContent: { ...view, execution: { key: 'prior', lifecycleId: 'turn-prior', historical: true, status: 'ended', request: 'Prior request' }, history: { ...view.history, selectedKey: 'prior' } } })
  await selection
  assert.match(page.element('#execution').textContent, /历史执行.*已结束/)
  assert.match(page.element('#execution').textContent, /Prior request/)
  assert.match(page.element('#summary').textContent, /^⚪.*历史/)
  assert.equal(page.element('#history').tagName, 'SECTION')
  assert.ok(page.element('#detail-issues').children[0].children.some(node => node.className === 'evidence'))
  const pause = page.element('#pause').onclick(); await page.flush()
  const control = page.sent.findLast(row => row.method === 'tools/call')
  page.reply(control.id, { structuredContent: { ...view, monitoring: { ...view.monitoring, paused: true, revision: 'revision-b' } } }); await page.flush()
  const retained = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(retained.params.name, 'sentinel_current_chat')
  assert.equal(retained.params.arguments.executionKey, 'prior')
  page.reply(retained.id, { structuredContent: { ...view, execution: { key: 'prior', historical: true, status: 'ended' }, history: { ...view.history, selectedKey: 'prior' }, monitoring: { ...view.monitoring, paused: true, revision: 'revision-b' } } }); await pause
  assert.match(page.element('#execution').textContent, /历史执行/)
  assert.match(page.element('#summary').textContent, /本聊天分析已暂停/)
  const current = page.element('#history-current').onclick(); await page.flush()
  const latest = page.sent.findLast(row => row.method === 'tools/call')
  assert.deepEqual(JSON.parse(JSON.stringify(latest.params.arguments)), {})
  page.reply(latest.id, { structuredContent: view }); await current
  assert.doesNotMatch(page.element('#execution').textContent, /历史执行/)
  assert.match(page.element('#summary').textContent, /^🔴/)
})

test('history pagination uses the displayed chat scope and a chat switch discards the pending older page', async t => {
  const page = await harness(t), view = material('main-a')
  view.history = { currentKey: 'current', selectedKey: 'current', executions: [{ key: 'current', status: 'running', observedAt: 1000 }], hasMore: true, nextCursor: 'page-cutoff' }
  await page.deliver(view)
  assert.equal(page.element('#history-more').hidden, false)
  const older = page.element('#history-more').onclick(); await page.flush()
  const request = page.sent.findLast(row => row.method === 'tools/call')
  assert.deepEqual(JSON.parse(JSON.stringify(request.params.arguments)), { historyCursor: 'page-cutoff', expectedThreadId: 'main-a' })
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  assert.equal(page.element('#history').hidden, true)
  page.reply(request.id, { structuredContent: view }); await older
  assert.equal(page.element('#history-list').textContent, '')
  await page.refresh()
  assert.deepEqual(JSON.parse(JSON.stringify(page.sent.findLast(row => row.method === 'tools/call').params.arguments)), {})
  await page.deliver(result('main-b'))
  assert.equal(page.element('#history').hidden, true)
  assert.equal([...page.timers.values()].filter(value => value.delay === 1500).length, 1)
})

test('retained history does not conceal an unconfirmed current execution or claim its old findings are current', async t => {
  const page = await harness(t), view = material('main-a', 'green')
  view.execution = { key: 'prior', historical: true, status: 'ended', request: 'Prior request' }
  view.history = { currentKey: 'prior', selectedKey: 'prior', currentGap: 'start-predates-host', executions: [{ key: 'prior', status: 'ended', observedAt: 1000 }] }
  await page.deliver(view)
  assert.match(page.element('#summary').textContent, /^⚪.*当前执行尚未确认/)
  assert.match(page.element('#analysis').textContent, /历史/)
  assert.match(page.element('#gaps').textContent, /start-predates-host/)
  assert.match(page.element('#execution').textContent, /历史执行/)
})
