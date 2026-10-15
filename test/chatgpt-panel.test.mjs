import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'

class Element {
  constructor(tag = 'div') { this.tagName = tag.toUpperCase(); this.children = []; this.attributes = new Map(); this.open = false; this.className = ''; this.value = '' }
  get textContent() { return this.value + this.children.map(child => typeof child === 'string' ? child : child.textContent).join('') }
  set textContent(value) { this.value = String(value); this.children = [] }
  set innerHTML(_value) { throw new Error('untrusted result must not be inserted as HTML') }
  append(...children) { this.children.push(...children) }
  replaceChildren(...children) { this.value = ''; this.children = children }
  setAttribute(name, value) { this.attributes.set(name, String(value)) }
  getAttribute(name) { return this.attributes.get(name) ?? null }
}
const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait']
const result = id => ({ signal: 'gray', summary: id ? id + ' 执行开始' : '无法确认当前观察', identity: { threadId: id }, observation: { coverage: 'caught-up', gap: null }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } })
const material = (id = 'main-a', signal = 'red') => ({ ...result(id), signal, summary: '当前修改可能偏离请求', analysis: { connected: true, state: 'cooldown', resultDisposition: 'current', summary: '本轮六项检查已更新', waitMs: 0, lagMs: 2500, coverage: { complete: true, gaps: [] }, checks: ids.map(checkId => ({ id: checkId, status: checkId === 'request_drift' ? 'attention' : 'clear', reason: '已核对现有材料' })), issues: [{ checkId: 'request_drift', reason: '用户只要求改文字，代理却更改了数据层', evidence: [{ text: '<img src=x onerror="alert(1)"> 只改按钮文字', reference: 'record:private-evidence-id' }] }] }, discovery: { complete: false, directoryComplete: true, counts: { main: 3, fork: 1, child: 2, remote: 0, cloud: 0, unknown: 1 }, tracked: 3, connected: 1, currentIncluded: true, gaps: ['fork-coverage-pending'] } })

async function harness(t) {
  const html = await readFile(new URL('../chatgpt/panel.html', import.meta.url), 'utf8')
  const handlers = new Map(), elements = new Map(), timers = new Map(), sent = []
  for (const match of html.matchAll(/<([a-z0-9]+)\b[^>]*\bid="([^"]+)"[^>]*>/g)) elements.set('#' + match[2], new Element(match[1]))
  const element = key => { assert.ok(elements.has(key), 'actual panel markup contains ' + key); return elements.get(key) }
  let timerId = 0
  const parent = { postMessage: value => sent.push(value) }
  const document = { hidden: false, querySelector: element, createElement: tag => new Element(tag) }
  const context = vm.createContext({ parent, document, addEventListener: (event, callback) => handlers.set(event, callback), removeEventListener: event => handlers.delete(event), setTimeout: (callback, delay) => { const id = ++timerId; timers.set(id, { callback, delay }); return id }, clearTimeout: id => timers.delete(id) })
  const flush = () => new Promise(resolve => setImmediate(resolve))
  const reply = (id, value) => handlers.get('message')({ source: parent, data: { jsonrpc: '2.0', id, result: value } })
  vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context)
  reply(sent[0].id, {}); await flush()
  const deliver = async value => { reply(sent.findLast(row => row.method === 'tools/call').id, { structuredContent: value }); await flush() }
  const refresh = async () => { const scheduled = [...timers].find(([, value]) => value.delay === 1500); assert.ok(scheduled); timers.delete(scheduled[0]); void scheduled[1].callback(); await flush(); return sent.findLast(row => row.method === 'tools/call').id }
  const destroy = async () => { handlers.get('pagehide')?.(); await flush() }
  t.after(destroy)
  return { html, parent, document, handlers, timers, sent, element, deliver, refresh, reply, flush, destroy }
}

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
  assert.equal(page.element('#checks').open, false)
  assert.equal(page.element('#diagnostics').open, false)
  assert.match(page.element('#coverage').textContent, /主聊天.*3/)
  assert.match(page.element('#coverage').textContent, /分叉.*1/)
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
  page.element('#checks').open = true; page.element('#diagnostics').open = true
  const oldRequest = await page.refresh()
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  for (const id of ['identity', 'coverage', 'technical', 'issues', 'check-list', 'gaps']) assert.equal(page.element('#' + id).textContent, '', id)
  assert.equal(page.element('#checks').open, false)
  assert.equal(page.element('#diagnostics').open, false)
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
  assert.equal(page.element('#diagnostics').open, false)
  assert.match(page.element('#diagnostics-title').textContent, /有缺口/)
  await page.refresh(); await page.deliver(result('main-b'))
  assert.equal(page.element('#gaps').textContent, '')
  assert.match(page.element('#identity').textContent, /main-b/)
})

test('the actual panel DOM omits excluded child-agent counts while retaining current-chat coverage and evidence', async t => {
  const page = await harness(t), view = material()
  view.discovery.counts.child = 987
  await page.deliver(view)
  page.element('#diagnostics').open = true
  const visible = ['summary', 'coverage', 'technical', 'gaps'].map(id => page.element('#' + id).textContent).join('\n')
  assert.doesNotMatch(visible, /子代理|child|987/)
  assert.match(page.element('#coverage').textContent, /本聊天：已接入/)
  assert.match(page.element('#coverage').textContent, /主聊天 3/)
  assert.match(page.element('#technical').textContent, /private-evidence-id/)
  assert.equal(page.element('#issues').children.length, 1)
})
