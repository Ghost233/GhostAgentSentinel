import test from 'node:test'
import assert from 'node:assert/strict'
import { mountGS } from './gs-session-client-runtime.mjs'

import { projectSessionPanel } from '../src/panel-view.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'

const emptySnapshot = () => ({ protocol: 1, generatedAt: Date.now(), sources: [], tasks: [], adapter: { sourceKey: 'own' }, hierarchyContext: { schemaVersion: 1, groups: [], scopes: [] } })
test('GS appends beside native round steps, falls back to the header only without a footer and keeps all zero counts', async t => {
  const service = { snapshot: async () => emptySnapshot(), analysisConfig: async () => ({}), claimNotifications: async () => [] }
  const f = await mountGS(t, service)
  await f.until(() => f.dom.window.document.querySelector('[data-gs-entry]'), 'GS footer did not mount')
  const document = f.dom.window.document
  assert.equal(document.querySelectorAll('[data-gs-entry]').length, 1)
  assert.ok(document.querySelector('footer [data-native-activity]').textContent.includes('1 轮 3 步'))
  assert.ok(document.querySelector('footer [data-gs-entry]'))
  assert.deepEqual([...document.querySelectorAll('[data-gs-count]')].map(node => node.textContent), ['0', '0', '0', '0'])
  await f.render('root', false)
  assert.equal(document.querySelectorAll('[data-gs-entry]').length, 1)
  assert.ok(document.querySelector('header [data-gs-entry]'))
  assert.equal(document.getElementById('draft').value, 'keep draft')
})

test('GS keeps a readonly header entry when its mounted composer ancestor is hidden and switches without duplicates', async t => {
  const service = { snapshot: async () => emptySnapshot(), analysisConfig: async () => ({}), claimNotifications: async () => [] }
  const f = await mountGS(t, service), document = f.dom.window.document
  assert.ok(document.querySelector('footer [data-gs-entry]'))
  assert.equal(document.querySelector('header [data-gs-entry]'), null)
  await f.render('root', true, true, 'none')
  assert.ok(document.querySelector('footer'), 'Readonly retains the mounted composer ancestor')
  assert.equal(f.dom.window.getComputedStyle(document.querySelector('footer')).display, 'none')
  assert.ok(document.querySelector('header [data-gs-entry]'), 'A hidden mounted footer must not suppress the usable header')
  assert.equal(document.querySelectorAll('[data-gs-entry]').length, 1)
  await f.click(document.querySelector('header [data-gs-entry]'))
  assert.ok(document.querySelector('header [role=dialog]'))
  await f.key('Escape')
  assert.equal(document.querySelector('[role=dialog]'), null)
  await f.render('root', true, true, '')
  assert.ok(document.querySelector('footer [data-gs-entry]'))
  assert.equal(document.querySelector('header [data-gs-entry]'), null)
  assert.equal(document.querySelectorAll('[data-gs-entry]').length, 1)
  assert.equal(document.getElementById('draft').value, 'keep draft')
  await f.destroy()
  assert.ok(Object.values(f.plugin.diagnostics()).every(value => value === 0))
})

test('the actual GS popup uses the core current scope, selects details without rebinding and returns explicitly to the root tree', async t => {
  const nodes = [
    { taskKey: 'root-task', sessionId: 'root', depth: 0, state: 'ended', light: null },
    { taskKey: 'child-task', sessionId: 'child', depth: 1, state: 'running', light: 'gray' },
    { taskKey: 'deep-task', sessionId: 'deep', depth: 2, state: 'running', light: 'gray' },
    { taskKey: 'sibling-task', sessionId: 'sibling', depth: 1, state: 'running', light: 'gray' },
  ]
  const snapshot = { ...emptySnapshot(), tasks: nodes.map(node => ({ key: node.taskKey, title: node.sessionId, currentRequest: `task ${node.sessionId}`, status: node.state === 'ended' ? 'idle' : 'running', analysis: { state: 'unknown', checks: [] } })),
    hierarchyContext: { schemaVersion: 1, groups: [{ key: 'group', nodes, counts: { green: 0, yellow: 0, red: 0, gray: 3 } }], scopes: [
      { sourceKey: 'own', sessionId: 'child', groupKey: 'group', rootSessionId: 'root', start: 1, end: 3, path: [{ sessionId: 'root', title: 'root' }, { sessionId: 'child', title: 'child' }], counts: { green: 0, yellow: 0, red: 0, gray: 2 }, coverage: { complete: false, branchGaps: [{ sessionId: 'deep', reason: 'missing directory' }] } },
    ] } }
  const f = await mountGS(t, { snapshot: async () => snapshot, gsSummary: async sessionId => ({ protocol: 1, generatedAt: snapshot.generatedAt, sourceKey: 'own', sessionId, sourceStatus: 'connected', scopeKnown: sessionId === 'child', counts: sessionId === 'child' ? snapshot.hierarchyContext.scopes[0].counts : { green: 0, yellow: 0, red: 0, gray: 0 }, coverage: { complete: false, gaps: [] }, gsStatus: { supported: true } }), analysisConfig: async () => ({}), claimNotifications: async () => [] }, 'child')
  const document = f.dom.window.document
  await f.until(() => document.querySelector('[data-gs-count="gray"]')?.textContent === '2', 'core scope counts rendered')
  await f.click(document.querySelector('[data-gs-entry]')); await f.openTree()
  let dialog = document.querySelector('[role="dialog"]')
  assert.ok(dialog, 'GS must open a real mounted popup')
  assert.deepEqual([...dialog.querySelectorAll('[data-gs-node]')].map(node => node.dataset.gsNode), ['child-task', 'deep-task'])
  assert.ok(dialog.textContent.includes('root / child')); assert.ok(dialog.textContent.includes('missing directory'))
  await f.click(dialog.querySelector('[data-gs-node="deep-task"]'))
  assert.equal(document.querySelector('[data-gs-scope]').dataset.gsScope, 'child')
  assert.equal(document.querySelector('[data-gs-entry] [data-gs-count="gray"]').textContent, '2')
  const initial = await f.panelCall('ui/initialize', {}, '.sentinel-gs-node-panel iframe')
  assert.deepEqual(JSON.parse(JSON.stringify(initial.hostContext.sentinelExecution)), { threadId: 'deep', key: 'deep-task' })
  await f.click(document.querySelector('[data-gs-return-root]'))
  dialog = document.querySelector('[role="dialog"]')
  assert.deepEqual([...dialog.querySelectorAll('[data-gs-node]')].map(node => node.dataset.gsNode), ['root-task', 'child-task', 'deep-task', 'sibling-task'])
  assert.ok(dialog.querySelector('[data-gs-path="root-task"]'), 'The ended root remains the ownership path')
  assert.equal(document.querySelector('[data-gs-entry] [data-gs-count="gray"]').textContent, '2', 'entry stays bound to the current chat scope')
  await f.render('unobserved')
  assert.equal(document.querySelector('[role="dialog"]'), null)
  assert.equal(document.querySelector('[data-gs-entry]').dataset.gsEntry, 'unobserved')
  assert.equal(document.querySelector('[data-gs-count="gray"]').textContent, '0')
  assert.equal(document.getElementById('draft').value, 'keep draft')
})

test('the shared check view displays owned ancestor evidence and drops foreign or expired collisions', async t => {
  const at = Date.now()
  const ancestorRef = { taskId: 'root', lifecycleId: 'root-life', eventId: 'same-id' }
  const expiredRef = { ...ancestorRef, eventId: 'expired' }
  const snapshot = { ...emptySnapshot(), generatedAt: at, sources: [
    { key: 'own', status: 'connected', descriptor: { registrationId: 'own' } },
    { key: 'other', status: 'connected', descriptor: { registrationId: 'other' } },
  ], tasks: [
    { key: 'original', sourceKey: 'own', id: 'root', lifecycleId: 'root-life', sessionId: 'root', events: [
      { id: 'same-id', taskId: 'root', taskLifecycleId: 'root-life', capturedAt: at, text: 'Actual ancestor user restriction' },
      { id: 'expired', taskId: 'root', taskLifecycleId: 'root-life', capturedAt: at - 86_400_001, text: 'Expired restriction' },
    ] },
    { key: 'child-key', sourceKey: 'own', id: 'child', lifecycleId: 'child-life', sessionId: 'child', status: 'running', currentRequest: 'Own task',
      events: [{ id: 'same-id', taskId: 'child', taskLifecycleId: 'child-life', capturedAt: at, text: 'Wrong child collision' }],
      gs: { light: 'red', reason: 'Breach independently judged' },
      analysis: { state: 'cooldown', resultDisposition: 'current', checks: [{ id: 'request_drift', status: 'attention', reason: 'Breach independently judged' }] },
      alerts: [{ id: 'alert', checkId: 'request_drift', status: 'attention', reason: 'Breach independently judged',
        evidenceIds: ['same-id'], evidenceRefs: [ancestorRef, expiredRef, { ...ancestorRef, lifecycleId: 'foreign-life' }] }] },
    { key: 'other-root', sourceKey: 'other', id: 'root', lifecycleId: 'foreign-life', sessionId: 'root',
      events: [{ id: 'same-id', taskId: 'root', taskLifecycleId: 'foreign-life', capturedAt: at, text: 'Wrong other-source collision' }] },
  ] }
  const view = projectSessionPanel(snapshot, { sourceKey: 'own', sessionId: 'child' })
  assert.deepEqual(view.analysis.issues[0].evidence.map(item => item.text), ['Actual ancestor user restriction'])
  const panel = await panelHarness(t)
  await panel.deliver(view)
  assert.match(panel.element('#issues').textContent, /Actual ancestor user restriction/)
  assert.doesNotMatch(panel.element('#issues').textContent, /Expired restriction|Wrong/)
  assert.match(panel.element('#check-list').textContent, /Breach independently judged/)
})
