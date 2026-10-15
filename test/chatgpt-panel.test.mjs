import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'

test('a context switch clears the old chat immediately, rejects late output, and page teardown releases refresh resources', async () => {
  const html = await readFile(new URL('../chatgpt/panel.html', import.meta.url), 'utf8')
  const handlers = new Map(), elements = new Map(), timers = new Map(), sent = []
  let timerId = 0
  const parent = { postMessage: value => sent.push(value) }
  const element = key => { if (!elements.has(key)) elements.set(key, { textContent: '' }); return elements.get(key) }
  const document = { hidden: false, querySelector: element }
  const context = vm.createContext({ parent, document, addEventListener: (event, callback) => handlers.set(event, callback), removeEventListener: event => handlers.delete(event), setTimeout: (callback, delay) => { const id = ++timerId; timers.set(id, { callback, delay }); return id }, clearTimeout: id => timers.delete(id) })
  const flush = () => new Promise(resolve => setImmediate(resolve))
  const reply = (id, result) => handlers.get('message')({ source: parent, data: { jsonrpc: '2.0', id, result } })
  const result = id => ({ structuredContent: { signal: 'gray', summary: id ? id + ' 执行开始' : '无法确认当前观察', identity: { threadId: id }, observation: { coverage: 'caught-up' }, analysis: { state: 'not-connected', summary: '六项分析尚未接入' } } })
  const refresh = async () => { const scheduled = [...timers].find(([, value]) => value.delay === 1500); assert.ok(scheduled); timers.delete(scheduled[0]); void scheduled[1].callback(); await flush(); return sent.findLast(row => row.method === 'tools/call').id }
  try {
    vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context)
    reply(sent[0].id, {}); await flush()
    reply(sent.findLast(row => row.method === 'tools/call').id, result('main-a')); await flush()
    assert.match(element('#identity').textContent, /main-a/)
    const oldRequest = await refresh()
    handlers.get('message')({ source: parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
    assert.equal(element('#identity').textContent, '')
    reply(oldRequest, result('main-a')); await flush()
    assert.doesNotMatch(element('#summary').textContent, /main-a/)
    reply(await refresh(), result('main-b')); await flush()
    assert.match(element('#identity').textContent, /main-b/)
    reply(await refresh(), result(null)); await flush()
    assert.doesNotMatch(element('#identity').textContent, /main-b/)
    assert.match(element('#identity').textContent, /未确认/)
    document.hidden = true; handlers.get('visibilitychange')(); await flush()
    assert.equal(timers.size, 0)
    document.hidden = false; handlers.get('visibilitychange')(); await flush()
    reply(sent.findLast(row => row.method === 'tools/call').id, result('main-b')); await flush()
    assert.equal([...timers.values()].filter(value => value.delay === 1500).length, 1)
  } finally { handlers.get('pagehide')?.(); await flush() }
  assert.equal(timers.size, 0)
  assert.equal(handlers.has('message'), false)
  assert.equal(handlers.has('visibilitychange'), false)
})
