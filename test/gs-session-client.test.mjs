import test from 'node:test'
import assert from 'node:assert/strict'
import { mountGS } from './gs-session-client-runtime.mjs'

const showDetails = async f => {
  const tab = f.element.querySelector('[data-gs-tab=details]'); if (tab) await f.click(tab)
  const tree = f.element.querySelector('[data-gs-scope-tree]'); if (tree && !tree.open) await f.click(tree.querySelector('summary'))
}

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
  await f.click(document.querySelector('[data-gs-entry]')); await showDetails(f)
  let dialog = document.querySelector('[role="dialog"]')
  assert.ok(dialog, 'GS must open a real mounted popup')
  assert.deepEqual([...dialog.querySelectorAll('[data-gs-node]')].map(node => node.dataset.gsNode), ['child-task', 'deep-task'])
  assert.ok(dialog.textContent.includes('root / child')); assert.ok(dialog.textContent.includes('missing directory'))
  await f.click(dialog.querySelector('[data-gs-node="deep-task"]'))
  assert.equal(document.querySelector('[data-gs-scope]').dataset.gsScope, 'child')
  assert.equal(document.querySelector('[data-gs-entry] [data-gs-count="gray"]').textContent, '2')
  assert.ok(document.querySelector('[data-gs-detail]').textContent.includes('task deep'))
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

test('child checks and alert history show only source-scoped original ancestor citations and retain missing lifecycle gaps', async t => {
  const at = Date.now(), ancestorRef = { taskId: 'root-id', lifecycleId: 'root-life', eventId: 'shared-local-id' }
  const expiredRef = { ...ancestorRef, lifecycleId: 'expired-life' }, expiredTimeRef = { ...ancestorRef, eventId: 'expired-original' }
  const snapshot = { ...emptySnapshot(), monitoring: { state: 'active' }, sources: [{ key: 'own', status: 'connected', descriptor: { name: 'Own Host' } }, { key: 'other', status: 'connected', descriptor: { name: 'Other Host' } }],
    tasks: [
      { key: 'root-key', sourceKey: 'own', id: 'root-id', lifecycleId: 'root-life', title: 'Original ancestor', status: 'running', events: [{ id: 'shared-local-id', taskId: 'root-id', taskLifecycleId: 'root-life', capturedAt: at, text: 'Actual ancestor user restriction' }, { id: 'expired-original', taskId: 'root-id', taskLifecycleId: 'root-life', capturedAt: at - 86400001, text: 'Expired original restriction' }], analysis: { checks: [] } },
      { key: 'child-key', sourceKey: 'own', id: 'child-id', lifecycleId: 'child-life', title: 'Checked child', status: 'running', hierarchy: { parentSessionId: 'root' },
        events: [{ id: 'shared-local-id', taskId: 'child-id', taskLifecycleId: 'child-life', capturedAt: at, text: 'Wrong child collision' }],
        analysis: { state: 'completed', summary: { attention: 'Actual inherited restriction breach', evidenceRefs: [ancestorRef] }, checks: [{ id: 'request_drift', status: 'attention', reason: 'Breach independently judged', evidenceIds: ['shared-local-id'], evidenceRefs: [ancestorRef, expiredRef, expiredTimeRef] }] },
        alerts: [{ id: 'alert', checkId: 'request_drift', status: 'attention', reason: 'Original alert', firstAt: at, evidenceIds: ['shared-local-id'], evidenceRefs: [ancestorRef], recoveryEvidenceRefs: [expiredRef] }] },
      { key: 'other-root', sourceKey: 'other', id: 'root-id', lifecycleId: 'root-life', title: 'Other source ancestor', status: 'running', events: [{ id: 'shared-local-id', taskId: 'root-id', taskLifecycleId: 'root-life', capturedAt: at, text: 'Wrong other-source collision' }], analysis: { checks: [] } },
    ], hierarchyContext: { schemaVersion: 1, groups: [{ key: 'citation-group', nodes: [{ taskKey: 'root-key', sessionId: 'root', depth: 0 }, { taskKey: 'child-key', sessionId: 'child', depth: 1 }], counts: { gray: 2 }, coverage: { complete: true, branchGaps: [] } }], scopes: [{ sessionId: 'child', sourceKey: 'own', groupKey: 'citation-group', rootSessionId: 'root', start: 1, end: 2, path: [{ sessionId: 'root', title: 'Original ancestor' }, { sessionId: 'child', title: 'Checked child' }], counts: { gray: 1 }, coverage: { complete: true, branchGaps: [] } }] } }
  const f = await mountGS(t, { snapshot: async () => snapshot, analysisConfig: async () => ({}), claimNotifications: async () => [] }, 'child')
  await f.renderOverview(); await showDetails(f)
  await f.until(() => f.dom.window.document.body.textContent.includes('Checked child'), 'overview loaded')
  await f.click([...f.dom.window.document.querySelectorAll('nav button')].find(node => node.textContent.includes('Checked child')))
  const article = f.dom.window.document.querySelector('article')
  const citations = [...article.querySelectorAll('[data-sentinel-citation]')]
  assert.equal(citations.length, 3, 'summary, check and original alert each cite the actual ancestor')
  assert.ok(citations.every(node => node.textContent.includes('Original ancestor') && node.textContent.includes('Actual ancestor user restriction') && node.dataset.sourceKey === 'own' && node.dataset.taskId === 'root-id' && node.dataset.lifecycleId === 'root-life'))
  assert.ok(citations.every(node => !node.textContent.includes('Wrong child') && !node.textContent.includes('Wrong other-source')))
  assert.equal(article.querySelectorAll('[data-sentinel-citation-gap]').length, 3, 'missing lifecycle and expired original event refs must stay gaps')
  await f.render('child'); await f.click(f.dom.window.document.querySelector('[data-gs-entry]')); await showDetails(f); await f.click(f.dom.window.document.querySelector('[data-gs-node="child-key"]'))
  const popupDetail = f.dom.window.document.querySelector('[data-gs-detail]')
  assert.equal(popupDetail.querySelectorAll('[data-sentinel-citation]').length, 3)
  assert.equal(popupDetail.querySelectorAll('[data-sentinel-citation-gap]').length, 3)
  assert.ok(popupDetail.textContent.includes('Breach independently judged'))
})

test('a judged clear child displays its actual result while a missing-context child retains the actual gap', async t => {
  let snapshot = { ...emptySnapshot(), monitoring: { state: 'active' }, sources: [{ key: 'own', status: 'connected' }], tasks: [{ key: 'child-clear', sourceKey: 'own', id: 'child', lifecycleId: 'life', title: 'Clear child', status: 'running', hierarchy: { parentSessionId: 'parent' }, events: [], instructionContext: { gaps: [] }, analysis: { state: 'completed', checks: [{ id: 'request_drift', status: 'clear', reason: 'Delegation stays in scope', evidenceIds: [] }] } }] }
  snapshot.hierarchyContext = { schemaVersion: 1, groups: [{ key: 'clear-group', nodes: [{ taskKey: 'child-clear', sessionId: 'child', depth: 1 }], counts: { gray: 1 }, coverage: { complete: true, branchGaps: [] } }], scopes: [{ sessionId: 'child', sourceKey: 'own', groupKey: 'clear-group', rootSessionId: 'parent', start: 0, end: 1, path: [{ sessionId: 'parent', title: 'Parent' }, { sessionId: 'child', title: 'Clear child' }], counts: { gray: 1 }, coverage: { complete: true, branchGaps: [] } }] }
  const f = await mountGS(t, { snapshot: async () => snapshot, analysisConfig: async () => ({}), claimNotifications: async () => [] }, 'child')
  await f.renderOverview(); await showDetails(f); await f.until(() => f.dom.window.document.body.textContent.includes('Clear child'), 'actual child result')
  await f.click(f.dom.window.document.querySelector('[data-gs-node="child-clear"]'))
  let article = f.dom.window.document.querySelector('article')
  assert.ok(article.textContent.includes('本轮未发现疑点'))
  assert.ok(!article.textContent.includes('子代理判断尚未接入'))
  await f.render('child'); await f.click(f.dom.window.document.querySelector('[data-gs-entry]')); await showDetails(f); await f.click(f.dom.window.document.querySelector('[data-gs-node="child-clear"]'))
  assert.ok(f.dom.window.document.querySelector('[data-gs-detail]').textContent.includes('本轮未发现疑点'))
  await f.renderOverview(); await showDetails(f)
  await f.click(f.dom.window.document.querySelector('[data-gs-node="child-clear"]'))
  snapshot = { ...snapshot, tasks: [{ ...snapshot.tasks[0], instructionContext: { gaps: ['actual inherited permission provenance missing'] }, analysis: { state: 'unknown', checks: [{ id: 'request_drift', status: 'unknown', reason: 'actual inherited permission provenance missing', evidenceIds: [] }] } }] }
  await f.tick(); article = f.dom.window.document.querySelector('article')
  assert.ok(article.textContent.includes('actual inherited permission provenance missing'))
  assert.ok(article.textContent.includes('当前不可判断'))
  await f.render('child'); await f.click(f.dom.window.document.querySelector('[data-gs-entry]')); await showDetails(f); await f.click(f.dom.window.document.querySelector('[data-gs-node="child-clear"]'))
  assert.ok(f.dom.window.document.querySelector('[data-gs-detail]').textContent.includes('actual inherited permission provenance missing'))
})
