import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'

class Element {
  constructor(tag = 'div') { this.tagName = tag.toUpperCase(); this.children = []; this.attributes = new Map(); this.open = false; this.className = ''; this.value = ''; this.textWrites = 0 }
  get textContent() { return this.value + this.children.map(child => typeof child === 'string' ? child : child.textContent).join('') }
  set textContent(value) { this.textWrites++; this.value = String(value); this.children = [] }
  set innerHTML(_value) { throw new Error('untrusted result must not be inserted as HTML') }
  append(...children) { this.children.push(...children) }
  replaceChildren(...children) { this.value = ''; this.children = children }
  replaceChild(child, previous) { const index = this.children.indexOf(previous); assert.notEqual(index, -1); this.children[index] = child }
  removeChild(child) { const index = this.children.indexOf(child); assert.notEqual(index, -1); this.children.splice(index, 1) }
  isEqualNode(other) {
    return this.tagName === other.tagName && this.className === other.className && this.type === other.type && this.value === other.value
      && this.attributes.size === other.attributes.size && [...this.attributes].every(([key, value]) => other.getAttribute(key) === value)
      && this.children.length === other.children.length && this.children.every((child, index) =>
        typeof child === 'string' ? child === other.children[index] : typeof other.children[index] !== 'string' && child.isEqualNode(other.children[index]))
  }
  setAttribute(name, value) { this.attributes.set(name, String(value)) }
  getAttribute(name) { return this.attributes.get(name) ?? null }
}
export async function panelHarness(t, { html = null, now = Date.now, initialize = true, hidden = false } = {}) {
  html ??= await readFile(new URL('../../chatgpt/panel.html', import.meta.url), 'utf8')
  const handlers = new Map(), elements = new Map(), timers = new Map(), sent = []
  for (const match of html.matchAll(/<([a-z0-9]+)\b[^>]*\bid="([^"]+)"[^>]*>/g)) elements.set('#' + match[2], new Element(match[1]))
  const element = key => { assert.ok(elements.has(key), 'actual panel markup contains ' + key); return elements.get(key) }
  let timerId = 0
  const parent = { postMessage: value => sent.push(value) }
  const document = { hidden, querySelector: element, createElement: tag => new Element(tag) }
  const context = vm.createContext({ parent, document, URL, Date: class extends Date { static now() { return now() } }, addEventListener: (event, callback) => handlers.set(event, callback), removeEventListener: event => handlers.delete(event), setTimeout: (callback, delay) => { const id = ++timerId; timers.set(id, { callback, delay }); return id }, clearTimeout: id => timers.delete(id) })
  const flush = () => new Promise(resolve => setImmediate(resolve))
  const reply = (id, value) => handlers.get('message')({ source: parent, data: { jsonrpc: '2.0', id, result: value } })
  vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context)
  if (initialize) { reply(sent[0].id, {}); await flush() }
  const deliver = async value => { reply(sent.findLast(row => row.method === 'tools/call').id, { structuredContent: value }); await flush() }
  const refresh = async () => { const scheduled = [...timers].find(([, value]) => value.delay === 1500); assert.ok(scheduled); timers.delete(scheduled[0]); void scheduled[1].callback(); await flush(); return sent.findLast(row => row.method === 'tools/call').id }
  const destroy = async () => { handlers.get('pagehide')?.(); await flush() }
  t.after(destroy)
  return { html, parent, document, handlers, timers, sent, element, deliver, refresh, reply, flush, destroy }
}
