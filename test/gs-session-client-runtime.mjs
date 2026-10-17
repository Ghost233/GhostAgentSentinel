import { readFile } from 'node:fs/promises'
import { runInNewContext } from 'node:vm'
import { req, clientRequire, resolveGatewayBrowserClientPath } from './dsh-native-runtime.mjs'
import descriptors from '../dsh/remote.cjs'

const { Context, Service } = req('@deepseek-ai/cordis')
const { SlotCore } = req('@deepseek-ai/dsh-client-ui-slots')
const { createElement: h, act } = clientRequire('react')
const { createRoot } = clientRequire('react-dom/client')
const { JSDOM } = clientRequire('jsdom')
export async function mountGS(t, service, sessionId = 'root', surface) {
  const ctx = new Context(), slots = new SlotCore(), intervals = new Map()
  const dom = surface?.dom || new JSDOM('<html><head></head><body><div id="view"></div><input id="draft" value="keep draft"></body></html>', { pretendToBeVisual: true })
  const element = surface?.element || dom.window.document.getElementById('view'), existingStyles = new Set(dom.window.document.head.querySelectorAll('style'))
  const before = { window: globalThis.window, document: globalThis.document, act: globalThis.IS_REACT_ACT_ENVIRONMENT }
  globalThis.window = dom.window; globalThis.document = dom.window.document; globalThis.IS_REACT_ACT_ENVIRONMENT = true
  const root = createRoot(element), delivered = [], rpcActivity = { entered: 0, finished: 0, inFlight: 0, peak: 0, methods: {} }
  const commit = async action => { await act(action) }
  const settle = () => commit(async () => { await new Promise(resolve => setTimeout(resolve, surface ? 0 : 15)) })
  const until = async (predicate, label) => { const end = Date.now() + 1500; while (!predicate()) { if (Date.now() > end) throw new Error(label); await settle() } }
  let plugin, factory, timerId = 0, clientFiber, destroyed = false
  slots.register({ name: 'root', children: { 'conversation.composer.dock': { kind: 'list', scope: 'session' }, 'conversation.session.header.utilities': { kind: 'list', scope: 'session' }, 'sidebar.right.pane.tab': { kind: 'keyed', scope: 'root' } } }, () => null)
  slots.register({ name: 'conversation.composer.dock', id: 'activity', order: 0 }, () => h('span', { 'data-native-activity': true }, '1 轮 3 步'))
  class FixtureSlots extends Service {
    constructor(owner) { super(owner, 'slots') }
    inject(_name, install) { return this.ctx.effect(install) }
    register(options, component) { return this.ctx.effect(() => slots.register(options, component)) }
  }
  new FixtureSlots(ctx)
  ctx.provide('sidebarRightTabs', { register() { return () => {} } })
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  ctx.provide('connection', { registerGenerationSource: () => () => {}, start: () => ({ stop() {} }), rpc: {
    open: async function *() {}, call: async (_route, endpoint, payload, signal) => {
      const method = endpoint.split('/')[1], descriptor = descriptors().find(item => item.method === method)
      rpcActivity.entered++; rpcActivity.inFlight++; rpcActivity.peak = Math.max(rpcActivity.peak, rpcActivity.inFlight); rpcActivity.methods[method] = (rpcActivity.methods[method] || 0) + 1
      try { return { ok: true, value: await service[method](...descriptor.parameters.map(parameter => payload.args[parameter.wire]), signal) } }
      catch (error) { return { ok: false, error: { code: error.code || 'fixture', message: error.message } } }
      finally { rpcActivity.finished++; rpcActivity.inFlight-- }
    },
  } })
  let remoteFactory
  runInNewContext(await readFile(await resolveGatewayBrowserClientPath(), 'utf8'), { window: { crypto: globalThis.crypto, __ModuleLoader__: { load(value) { remoteFactory = value.factory } } }, crypto: globalThis.crypto, setTimeout, clearTimeout, AbortController, AbortSignal, console })
  const remote = remoteFactory(req); await ctx.plugin({ inject: remote.inject, apply: remote.apply })
  class Notification { static permission = 'granted'; constructor(title, options) { delivered.push({ title, options, instance: this }) } }
  runInNewContext(await readFile(new URL('../dsh/client.js', import.meta.url), 'utf8'), { window: { __ModuleLoader__: { load(value) { factory = value.factory } } }, document: dom.window.document, Notification,
    setInterval(callback) { const id = ++timerId; intervals.set(id, callback); return id }, clearInterval(id) { intervals.delete(id) }, setTimeout, clearTimeout, AbortController, Date, console })
  plugin = factory(name => name === 'react' ? clientRequire(name) : req(name))
  await commit(async () => { clientFiber = ctx.plugin({ inject: plugin.inject, apply: plugin.apply }); await clientFiber.await() })
  const ownedStyles = [...dom.window.document.head.querySelectorAll('style')].filter(style => !existingStyles.has(style))
  await until(() => plugin.diagnostics().backgroundTimer === 1, 'native Remote mounted')
  let currentSession = sessionId, footer = true, visible = true, footerDisplay = '', viewAbort = new AbortController()
  const layout = () => {
    const props = { sessionId: currentSession, visible, signal: viewAbort.signal }
    const entries = name => slots.entriesOfSlot(name).map(entry => h(entry.component, { ...props, key: entry.options.id || entry.options.key || name }))
    return h('div', null, h('header', null, ...entries('conversation.session.header.utilities')), footer && h('footer', { style: { display: footerDisplay || undefined } }, ...entries('conversation.composer.dock')))
  }
  const render = async (nextSession = currentSession, nextFooter = footer, nextVisible = visible, nextFooterDisplay = footerDisplay) => {
    currentSession = nextSession; footer = nextFooter; visible = nextVisible; footerDisplay = nextFooterDisplay
    await commit(async () => root.render(layout()))
    await settle()
  }
  const destroy = async () => {
    if (destroyed) return
    destroyed = true
    const failures = []
    for (const operation of [() => commit(async () => root.unmount()), () => ctx.fiber.dispose(), () => until(() => plugin.diagnostics().activeRequests === 0 && plugin.diagnostics().unfinishedRequests === 0 && rpcActivity.inFlight === 0, 'client requests truly finished'), () => {
      if (intervals.size) throw new Error('client timers leaked')
      for (const [name, count] of Object.entries(plugin.diagnostics())) if (count !== 0) throw new Error(`client resource leaked: ${name}=${count}`)
      if (ownedStyles.some(style => style.parentNode)) throw new Error('client style leaked')
    }]) { try { await operation() } catch (error) { failures.push(error) } }
    if (!surface) { globalThis.window = before.window; globalThis.document = before.document; globalThis.IS_REACT_ACT_ENVIRONMENT = before.act; dom.window.close() }
    if (failures.length) throw new AggregateError(failures, 'GS frontend cleanup failed')
  }
  t.after(destroy)
  await render()
  return { dom, element, slots, plugin, render, settle, until, destroy, delivered, rpcActivity, intervals, async renderOverview(nextVisible = true) { const entry = slots.entriesOfSlot('sidebar.right.pane.tab')[0]; await commit(async () => root.render(h(entry.component, { useTabInfo: () => ({ tab: { visible: nextVisible, signal: viewAbort.signal } }) }))); await settle() }, async click(element) { await commit(async () => element.dispatchEvent(new dom.window.MouseEvent('click', { bubbles: true }))); await settle() },
    async key(key) { await commit(async () => document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key, bubbles: true }))); await settle() },
    async pointer(element) { await commit(async () => element.dispatchEvent(new dom.window.Event('pointerdown', { bubbles: true }))); await settle() },
    async resize() { await commit(async () => dom.window.dispatchEvent(new dom.window.Event('resize'))); await settle() },
    async visibility(value) { await commit(async () => { Object.defineProperty(dom.window.document, 'visibilityState', { value, configurable: true }); dom.window.document.dispatchEvent(new dom.window.Event('visibilitychange')) }); await settle() },
    async abortView() { await commit(async () => viewAbort.abort()); await settle() },
    async reinstall() { await commit(async () => { await clientFiber.dispose(); root.render(layout()) }); await commit(async () => { clientFiber = ctx.plugin({ inject: plugin.inject, apply: plugin.apply }); await clientFiber.await(); root.render(layout()) }); await settle() },
    async renewView() { viewAbort = new AbortController(); await render() },
    fireTimers() { for (const callback of intervals.values()) callback() },
    async tick() { await commit(async () => { for (const callback of intervals.values()) callback(); await new Promise(resolve => setTimeout(resolve, 15)) }) } }
}
