import test from 'node:test'
import assert from 'node:assert/strict'
import { startPanelServer } from '../chatgpt/panel-server.mjs'
import { panelHarness } from './helpers/chatgpt-panel.mjs'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { createLazyCore } from '../chatgpt/lazy-core.mjs'
import { supportedProfile } from '../chatgpt/storage-compatibility.mjs'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { withModelSummary } from './helpers/model-summary.mjs'

const state = { registrationId: 'chatgpt:installation-a', revision: 'settings-revision-a', config: { llm: { baseUrl: 'http://127.0.0.1:1234', model: 'summary-a' }, jev: { baseUrl: 'http://127.0.0.1:2345', model: 'judge-a' } } }
const diagnostics = { protocol: 1, registrationId: state.registrationId, generatedAt: 123, configRevision: state.revision, models: { llm: { configured: true }, jev: { configured: true } }, collection: { state: 'failed', sourceCount: 1, connectedSources: 0, lastSeenAt: null, error: { stage: 'native-collection', code: 'read-failed' } }, analysis: { counts: { scope: 'active-tasks', total: 1, states: { queued: 1 } }, latest: { state: 'failed', resultDisposition: 'historical', configRevision: 'prior-config', startedAt: 1, completedAt: 2, nextEligibleAt: null, error: { stage: 'jev', code: 'schema-invalid' } }, latestFailure: { stage: 'jev', code: 'schema-invalid', at: 2 } } }

async function settingsPage(t, initial = state) {
  const page = await panelHarness(t, { html: await readFile(new URL('../chatgpt/settings.html', import.meta.url), 'utf8') })
  page.reply(page.sent.findLast(row => row.method === 'tools/call').id, { structuredContent: initial })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences').id, { structuredContent: { registrationId: initial.registrationId, revision: 'notification-revision-a', preferences: { 'task-alert': true, 'monitoring-failure': true }, permission: null } })
  await page.flush()
  return page
}

function leaveModelGroup(page, role) { return page.element('#settings-' + role + '-group').onfocusout({ relatedTarget: null }) }

const installationRecords = { registrationId: state.registrationId, generatedAt: 123, records: [], hasMore: false, nextCursor: null, historyGap: false, retentionMs: 86400000, maximumRecords: 1000 }
const notificationRecord = { registrationId: state.registrationId, id: 'notice-a', sourceKey: 'source-a', mainId: 'chat-a', lifecycleId: 'run-a', chatTitle: '聊天甲', kind: 'task-alert', createdAt: 100, status: 'submitted', delivery: 'system-accepted', humanSeen: 'unknown', title: '任务疑点', body: '<img src=x onerror=alert(1)>\n原始通知正文', native: null }

test('the notification records tab reads the installation MCP only when opened and never requires a current chat', async t => {
  const page = await settingsPage(t)
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_installation_notification_records'), false)
  const opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  assert.ok(read, 'the actual records tab calls the installation records tool')
  assert.deepEqual(JSON.parse(JSON.stringify(read.params.arguments)), {})
  assert.equal(page.sent.some(row => ['sentinel_current_chat', 'sentinel_notification_records'].includes(row.params?.name)), false)
  page.reply(read.id, { structuredContent: installationRecords }); await opening
  assert.match(page.element('#settings-notifications-status').textContent, /暂无.*通知记录/)
  assert.equal(page.timers.size, 0)
})

test('notification rows retain chat and source scope and distinguish native delivery facts from human seeing', async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  page.reply(read.id, { structuredContent: { ...installationRecords, historyGap: true, records: [
    { ...notificationRecord, native: { authorization: { state: 'authorized' }, submission: { state: 'accepted' }, deliveredSnapshot: { present: true }, userAction: { action: 'default' } } },
    { ...notificationRecord, id: 'source-failure', mainId: null, lifecycleId: null, chatTitle: null, kind: 'monitoring-failure', status: 'pending', delivery: 'unknown', native: null },
  ] } }); await opening
  const rows = page.element('#settings-notifications-records').children
  assert.equal(rows.length, 2)
  assert.match(rows[0].textContent, /聊天甲.*任务告警/)
  assert.ok(rows[0].textContent.includes(notificationRecord.body), 'the notification body stays literal text')
  assert.match(rows[0].textContent, /系统已接受.*展示未确认/)
  assert.match(rows[0].textContent, /系统许可.*已允许.*通知中心快照.*存在.*用户动作.*点击.*用户实际看见.*未确认/)
  assert.doesNotMatch(rows[0].textContent, /用户已看见|用户已看到/)
  assert.match(rows[1].textContent, /来源级.*聊天身份未确认.*监控故障/)
  assert.match(rows[1].textContent, /待投递/)
  assert.doesNotMatch(rows[1].textContent, /系统许可|通知中心快照|用户动作/)
  assert.match(page.element('#settings-notifications-gap').textContent, /留存缺口/)
})

test('notification record pages use only the opaque cursor, replace the bounded page and never fetch ahead', async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  page.reply(first.id, { structuredContent: { ...installationRecords, records: [{ ...notificationRecord, body: '最近一页' }], hasMore: true, nextCursor: 'opaque:created-at/id?+==' } }); await opening
  assert.equal(page.sent.filter(row => row.params?.name === first.params.name).length, 1)
  assert.equal(page.element('#settings-notifications-older').hidden, false)
  const older = page.element('#settings-notifications-older').onclick(); await page.flush()
  const second = page.sent.findLast(row => row.params?.name === first.params.name)
  assert.deepEqual(JSON.parse(JSON.stringify(second.params.arguments)), { cursor: 'opaque:created-at/id?+==' })
  page.reply(second.id, { structuredContent: { ...installationRecords, records: [{ ...notificationRecord, id: 'older-notice', body: '较早一页' }] } }); await older
  assert.equal(page.element('#settings-notifications-records').children.length, 1)
  assert.match(page.element('#settings-notifications-records').textContent, /较早一页/)
  assert.doesNotMatch(page.element('#settings-notifications-records').textContent, /最近一页/)
  assert.equal(page.element('#settings-notifications-older').hidden, true)
  const refreshing = page.element('#settings-notifications-refresh').onclick(); await page.flush()
  const refresh = page.sent.findLast(row => row.params?.name === first.params.name)
  assert.deepEqual(JSON.parse(JSON.stringify(refresh.params.arguments)), {})
  page.reply(refresh.id, { structuredContent: installationRecords }); await refreshing
  assert.equal(page.sent.filter(row => row.params?.name === first.params.name).length, 3)
  assert.equal(page.timers.size, 0)
})

for (const boundary of ['foreign-installation', 'foreign-row', 'unbounded-page', 'wrong-retention', 'invented-human-seen']) test('notification records reject an unconfirmed public page without disturbing settings: ' + boundary, async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-url'); input.value = 'http://'; input.oninput()
  const permissionBefore = page.element('#settings-notification-permission-status').textContent
  const opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  const value = { ...installationRecords, records: [{ ...notificationRecord }] }
  if (boundary === 'foreign-installation') value.registrationId = 'chatgpt:foreign'
  if (boundary === 'foreign-row') value.records[0].registrationId = 'chatgpt:foreign'
  if (boundary === 'unbounded-page') value.records = Array.from({ length: 21 }, (_, index) => ({ ...notificationRecord, id: 'record-' + index }))
  if (boundary === 'wrong-retention') value.maximumRecords = 1024
  if (boundary === 'invented-human-seen') value.records[0].humanSeen = 'seen'
  page.reply(read.id, { structuredContent: value }); await opening
  assert.match(page.element('#settings-notifications-status').textContent, /读取未确认/)
  assert.equal(page.element('#settings-notifications-records').children.length, 0)
  assert.equal(page.element('#settings-notifications-older').hidden, true)
  page.element('#settings-tab-settings').onclick()
  assert.equal(input.value, 'http://')
  assert.equal(page.element('#settings-notification-task-alert').checked, true)
  assert.equal(page.element('#settings-notification-permission-status').textContent, permissionBefore)
})

test('a failed record refresh clears the prior page and leaves model drafts and notification preferences available', async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model'); input.value = 'unsubmitted-draft'; input.oninput()
  const opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records').id, { structuredContent: { ...installationRecords, records: [notificationRecord], hasMore: true, nextCursor: 'older' } }); await opening
  const refreshing = page.element('#settings-notifications-refresh').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records').id, { isError: true }); await refreshing
  assert.match(page.element('#settings-notifications-status').textContent, /读取未确认/)
  assert.equal(page.element('#settings-notifications-records').children.length, 0)
  assert.equal(page.element('#settings-notifications-older').hidden, true)
  page.element('#settings-tab-settings').onclick()
  assert.equal(input.value, 'unsubmitted-draft')
  const checkbox = page.element('#settings-notification-monitoring-failure'); checkbox.checked = false; const saving = checkbox.onchange(); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference')
  page.reply(save.id, { structuredContent: { registrationId: state.registrationId, revision: 'still-usable', preferences: { 'task-alert': true, 'monitoring-failure': false } } }); await saving
  assert.match(page.element('#settings-notification-monitoring-failure-status').textContent, /已保存/)
})

for (const transport of ['tool', 'rpc']) for (const unsupported of [true, false]) test('record read errors preserve the known unsupported reason without exposing other error details: ' + transport + '/' + unsupported, async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  const message = (unsupported ? '共享核心尚不支持本安装通知记录' : 'Unknown internal failure') + '\n at /private/internal-source.mjs:123'
  if (transport === 'tool') page.reply(read.id, { isError: true, content: [{ type: 'text', text: message }] })
  else page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: read.id, error: { code: -32603, message } } })
  await opening
  const status = page.element('#settings-notifications-status').textContent
  assert.match(status, unsupported ? /共享核心尚不支持本安装通知记录/ : /通知记录读取未确认/)
  assert.doesNotMatch(status, /private|internal-source|Unknown internal|重开|重试/)
  assert.equal(page.element('#settings-notifications-records').children.length, 0)
  page.element('#settings-tab-settings').onclick()
  assert.equal(page.element('#settings-llm-model').value, 'summary-a')
})

for (const boundary of ['tab', 'hidden', 'teardown']) test('notification reads cancel and ignore late responses after leaving their visible scope: ' + boundary, async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records'), listener = page.handlers.get('message')
  if (boundary === 'tab') page.element('#settings-tab-settings').onclick()
  else if (boundary === 'hidden') { page.document.hidden = true; page.handlers.get('visibilitychange')() }
  else await page.destroy()
  await opening
  const before = page.element('#settings-notifications-status').textContent, reads = page.sent.filter(row => row.params?.name === read.params.name).length
  assert.ok(page.sent.some(row => row.method === 'notifications/cancelled' && row.params.requestId === read.id))
  listener({ source: page.parent, data: { jsonrpc: '2.0', id: read.id, result: { structuredContent: { ...installationRecords, records: [notificationRecord] } } } }); await page.flush()
  assert.equal(page.element('#settings-notifications-records').children.length, 0)
  assert.equal(page.element('#settings-notifications-status').textContent, before)
  if (boundary === 'hidden') { page.element('#settings-notifications-refresh').onclick(); page.document.hidden = false; page.handlers.get('visibilitychange')(); await page.flush() }
  if (boundary !== 'teardown') {
    assert.equal(page.sent.filter(row => row.params?.name === read.params.name).length, reads, 'hidden or unrelated activity does not automatically fetch records')
    const reopening = page.element('#settings-tab-notifications').onclick(); await page.flush()
    const latest = page.sent.findLast(row => row.params?.name === read.params.name); assert.notEqual(latest.id, read.id)
    page.reply(latest.id, { structuredContent: installationRecords }); await reopening
  }
  assert.equal(page.timers.size, 0)
})

test('the first installation record read can bind identity while model and preference reads are unavailable', async t => {
  const page = await panelHarness(t, { html: await readFile(new URL('../chatgpt/settings.html', import.meta.url), 'utf8') }), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records').id, { structuredContent: { ...installationRecords, records: [notificationRecord] } }); await opening
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { isError: true })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences').id, { isError: true }); await page.flush()
  assert.equal(page.element('#settings-notifications-records').children.length, 1)
  assert.match(page.element('#settings-status').textContent, /读取失败/)
  const refreshing = page.element('#settings-notifications-refresh').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records').id, { structuredContent: { ...installationRecords, registrationId: 'chatgpt:foreign' } }); await refreshing
  assert.match(page.element('#settings-notifications-status').textContent, /读取未确认/)
})

async function installationNotificationPage(t, { installationId = 'notification-settings', authorization = 'denied' } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-settings-')), token = '7'.repeat(64)
  let server, core, adapter, authorizationCalls = 0, settingsCalls = 0, threadCalls = 0, authorizationError = false
  t.after(async () => { try { await adapter?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir })
  const permission = () => ({ authorization, alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: Date.now() })
  adapter = await startChatGPTAdapter({ core, native: {
    async sampleHost() { return { available: true, hostRunId: 'notification-settings-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread() { threadCalls++; throw new Error('installation settings must not read a chat') },
  }, notificationIO: {
    async settings() { settingsCalls++; return permission() },
    async requestAuthorization() { authorizationCalls++; if (authorizationError) throw Object.assign(new Error('controlled native failure'), { code: 'native_unavailable' }); return permission() },
    async snapshot() { return { pending: [], delivered: [], actions: [], observedAt: Date.now() } },
    async cancel() { return { pendingIds: [], observedAt: Date.now() } }, close() {},
  }, installationId, sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (method, params) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    const envelope = await response.json(); assert.equal(envelope.error, undefined, JSON.stringify(envelope)); return envelope.result
  }
  const resource = await rpc('resources/read', { uri: 'ui://ghost-sentinel/settings.html' })
  const deliver = async (page, name) => {
    const call = page.sent.findLast(row => row.params?.name === name); assert.ok(call, 'the actual settings page invokes ' + name)
    const result = await rpc('tools/call', call.params); page.reply(call.id, result); await page.flush(); return result
  }
  const open = async () => {
    const page = await panelHarness(t, { html: resource.contents[0].text })
    await deliver(page, 'sentinel_model_settings'); const notifications = await deliver(page, 'sentinel_installation_notification_preferences'); assert.ok(!notifications.isError, JSON.stringify(notifications)); return page
  }
  return { dataDir, server, core, registrationId: 'chatgpt:' + installationId, rpc, deliver, open, counts: () => ({ authorizationCalls, settingsCalls, threadCalls }), failAuthorization: () => { authorizationError = true } }
}

test('the actual global records tab reads one produced native notice and accepted receipt through Core HTTP and MCP', async t => {
  const fixture = await installationNotificationPage(t), sourceKey = await fixture.core.registerSource({ registrationId: fixture.registrationId, hostRunId: 'records-host', name: 'Own source', policy: 'chatgpt-main/1' })
  await fixture.core.ingest(sourceKey, { tasks: [{ id: 'named-chat', lifecycleId: 'own-run', title: '实际通知聊天', status: 'running' }] })
  await fixture.core.recordCollectionOutcome({ sourceKey, mainId: 'named-chat', lifecycleId: 'own-run', roundId: 'actual-read-failure', channel: 'directory', outcome: 'failed', settledAt: Date.now(), error: { code: 'EIO', stage: 'collection' } })
  await fixture.core.setNativeNotificationEligibility({ sourceKey, hostRunId: 'records-host', appRunning: true, pluginEnabled: true })
  const [notice] = await fixture.core.claimNotifications(sourceKey)
  assert.ok(notice, 'the public producer creates an actual claimable notification')
  const emit = fields => fixture.core.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: Date.now(), ...fields })
  await emit({ event: 'authorization', authorization: 'authorized' }); assert.equal((await emit({ event: 'submitting' })).maySubmit, true); await emit({ event: 'accepted' })
  const response = await fetch(fixture.server.url + '/v1/notifications/installations/records?' + new URLSearchParams({ registrationId: fixture.registrationId }), { headers: { authorization: 'Bearer ' + fixture.server.token } })
  assert.equal(response.status, 200)
  const publicPage = await response.json(), page = await fixture.open(), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  assert.deepEqual(JSON.parse(JSON.stringify(call.params.arguments)), {})
  const result = await fixture.deliver(page, call.params.name); await opening
  assert.ok(!result.isError, JSON.stringify(result))
  assert.deepEqual(result.structuredContent.records, publicPage.records)
  assert.equal(result.structuredContent.records[0].id, notice.id)
  assert.equal(result.structuredContent.records[0].humanSeen, 'unknown')
  assert.equal(page.element('#settings-notifications-records').children.length, 1)
  assert.match(page.element('#settings-notifications-records').textContent, /实际通知聊天.*监控故障.*系统已接受.*展示未确认/)
  assert.match(page.element('#settings-notifications-records').textContent, /系统许可.*已允许.*用户实际看见.*未确认/)
  assert.equal(page.sent.some(row => ['sentinel_current_chat', 'sentinel_notification_records'].includes(row.params?.name)), false)
  assert.deepEqual(fixture.counts(), { authorizationCalls: 0, settingsCalls: 0, threadCalls: 0 })
  assert.equal(page.timers.size, 0)
  t.diagnostic(JSON.stringify({ registrationId: fixture.registrationId, sourceKey, notificationId: notice.id, status: result.structuredContent.records[0].status, delivery: result.structuredContent.records[0].delivery }))
})

test('the actual installation records page spans chats and both kinds with an isolated opaque older-page cursor', async t => {
  const fixture = await installationNotificationPage(t, { installationId: 'records-multiple' }), model = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk
    const body = JSON.parse(raw); response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify(request.url === '/v1/chat/completions'
      ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) }))
  })
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve))
  t.after(async () => { model.closeAllConnections(); await new Promise(resolve => model.close(resolve)) })
  const baseUrl = 'http://127.0.0.1:' + model.address().port, config = { llm: { baseUrl, model: 'controlled-summary' }, jev: { baseUrl, model: 'controlled-judge' } }
  await fixture.core.configureInstallationAnalysis(fixture.registrationId, config, (await fixture.core.installationAnalysisConfig(fixture.registrationId)).revision)
  const sourceKey = await fixture.core.registerSource({ registrationId: fixture.registrationId, hostRunId: 'records-host', name: 'Own native source', policy: 'chatgpt-main/1' }), capturedAt = Date.now()
  const ref = eventId => ({ taskId: 'alert-chat', lifecycleId: 'native-run', eventId })
  const event = (id, kind, text, data = {}) => ({ id, taskId: 'alert-chat', taskLifecycleId: 'native-run', kind, text, data, capturedAt, occurredAt: null })
  await fixture.core.ingest(sourceKey, { tasks: [{ id: 'alert-chat', sessionId: 'alert-chat', lifecycleId: 'native-run', title: '原生任务告警聊天', status: 'running' }], events: [
    event('start', 'status', 'Execution started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'native-run', turnTrigger: 'composer', status: 'running' }),
    event('input', 'input', 'Only inspect the existing material', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'native-run', messageId: 'native-message', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }),
    event('output', 'output', 'Changing an unrelated directory'),
  ], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'alert-chat' }], activations: [{ taskId: 'alert-chat', lifecycleId: 'native-run', sessionId: 'alert-chat', runId: 'native-run', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'alert-chat', lifecycleId: 'native-run', inputRef: ref('input'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('start'), ref('input')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } })
  const until = async predicate => { const deadline = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < deadline, 'the actual producer must settle a task notification'); await delay(5) } }
  await until(async () => (await fixture.core.nativeNotificationRecords({ sourceKey })).records.some(record => record.kind === 'task-alert'))
  await fixture.core.ingest(sourceKey, { tasks: Array.from({ length: 22 }, (_, index) => ({ id: 'failure-chat-' + index, lifecycleId: 'failure-run', title: '故障聊天 ' + index, status: 'running' })) })
  for (let index = 0; index < 22; index++) await fixture.core.recordCollectionOutcome({ sourceKey, mainId: 'failure-chat-' + index, lifecycleId: 'failure-run', roundId: 'failure-' + index, channel: 'directory', outcome: 'failed', settledAt: Date.now(), error: { code: 'EIO', stage: 'collection' } })
  const foreign = await fixture.core.registerSource({ registrationId: 'chatgpt:records-multiple-test', hostRunId: 'foreign-host', name: 'Foreign test', policy: 'chatgpt-main/1' })
  await fixture.core.recordCollectionOutcome({ sourceKey: foreign, mainId: null, lifecycleId: null, roundId: 'foreign-failure', channel: 'directory', outcome: 'failed', settledAt: Date.now(), error: { code: 'EIO', stage: 'collection' } })
  const dshRegistration = 'dsh:records-multiple-other', dshSource = await fixture.core.registerSource({ registrationId: dshRegistration, hostRunId: 'dsh-host', name: 'Foreign DSH' })
  await fixture.core.configureInstallationAnalysis(dshRegistration, config, (await fixture.core.installationAnalysisConfig(dshRegistration)).revision)
  await fixture.core.ingest(dshSource, { tasks: [{ id: 'foreign-dsh-task', sessionId: 'foreign-dsh-chat', lifecycleId: 'dsh-run', title: '外部 DSH 聊天', status: 'running' }], events: ['input', 'output'].map(kind => ({ id: 'dsh-' + kind, taskId: 'foreign-dsh-task', taskLifecycleId: 'dsh-run', kind, text: kind === 'input' ? 'Inspect only' : 'Changing another directory', capturedAt: Date.now(), occurredAt: null })) })
  await until(async () => (await fixture.core.installationNotificationRecords(dshRegistration, {})).records.some(record => record.kind === 'task-alert' && record.native === null))
  const page = await fixture.open(), opening = page.element('#settings-tab-notifications').onclick(); await page.flush()
  const first = (await fixture.deliver(page, 'sentinel_installation_notification_records')).structuredContent; await opening
  assert.equal(first.records.length, 20)
  assert.equal(first.hasMore, true)
  assert.equal(page.element('#settings-notifications-records').children.length, 20)
  assert.ok(first.records.every(record => record.registrationId === fixture.registrationId && record.sourceKey === sourceKey))
  const older = page.element('#settings-notifications-older').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_records')
  assert.deepEqual(JSON.parse(JSON.stringify(call.params.arguments)), { cursor: first.nextCursor })
  const second = (await fixture.deliver(page, call.params.name)).structuredContent; await older
  assert.equal(second.records.length, 3)
  assert.equal(second.hasMore, false)
  assert.equal(page.element('#settings-notifications-records').children.length, 3)
  const records = [...first.records, ...second.records]
  assert.equal(new Set(records.map(record => record.id)).size, 23)
  assert.deepEqual([...new Set(records.map(record => record.kind))].sort(), ['monitoring-failure', 'task-alert'])
  assert.ok(records.every(record => record.mainId && record.chatTitle && record.humanSeen === 'unknown'))
  assert.match(page.element('#settings-notifications-records').textContent, /原生任务告警聊天.*任务告警/)
  assert.ok(!JSON.stringify(records).includes(foreign) && !JSON.stringify(records).includes(dshSource))
  assert.equal(page.sent.filter(row => row.params?.name === call.params.name).length, 2, 'the real page does not fetch beyond the requested older page')
  assert.deepEqual(fixture.counts(), { authorizationCalls: 0, settingsCalls: 0, threadCalls: 0 })
  assert.equal(page.timers.size, 0)
  t.diagnostic(JSON.stringify({ registrationId: fixture.registrationId, sourceKey, excludedSources: [foreign, dshSource], firstPageIds: first.records.map(record => record.id), cursor: first.nextCursor, secondPageIds: second.records.map(record => record.id) }))
})

test('the actual global settings page independently saves both notification kinds through MCP and Core and retains them on reopen', async t => {
  const fixture = await installationNotificationPage(t), page = await fixture.open()
  assert.equal(page.element('#settings-notification-task-alert').checked, true)
  assert.equal(page.element('#settings-notification-monitoring-failure').checked, true)
  for (const kind of ['task-alert', 'monitoring-failure']) {
    const checkbox = page.element('#settings-notification-' + kind); checkbox.checked = false
    const saving = checkbox.onchange(); await page.flush()
    assert.match(page.element('#settings-notification-' + kind + '-status').textContent, /正在保存/)
    const result = await fixture.deliver(page, 'sentinel_set_installation_notification_preference'); await saving
    assert.equal(result.structuredContent.preferences[kind], false)
    assert.match(page.element('#settings-notification-' + kind + '-status').textContent, /已保存.*关闭/)
  }
  await page.destroy()
  const reopened = await fixture.open()
  assert.equal(reopened.element('#settings-notification-task-alert').checked, false)
  assert.equal(reopened.element('#settings-notification-monitoring-failure').checked, false)
  assert.deepEqual(fixture.counts(), { authorizationCalls: 0, settingsCalls: 0, threadCalls: 0 })
  assert.equal(reopened.timers.size, 0)
})

test('a notification read started during a save cannot repaint the completed newer choice', async t => {
  const page = await settingsPage(t), checkbox = page.element('#settings-notification-task-alert')
  checkbox.checked = false; const saving = checkbox.onchange(); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference')
  const reading = page.element('#settings-notification-reload').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences')
  page.reply(save.id, { structuredContent: { registrationId: state.registrationId, revision: 'notification-disabled', preferences: { 'task-alert': false, 'monitoring-failure': true } } }); await saving
  page.reply(read.id, { structuredContent: { registrationId: state.registrationId, revision: 'notification-before-save', preferences: { 'task-alert': true, 'monitoring-failure': true }, permission: null } }); await reading
  assert.equal(checkbox.checked, false)
  assert.match(page.element('#settings-notification-task-alert-status').textContent, /已保存.*关闭/)
  assert.equal(page.element('#settings-notification-monitoring-failure').checked, true)
})

test('notification changes serialize each kind and keep the last intent across tabs and model refreshes', async t => {
  const fixture = await installationNotificationPage(t), page = await fixture.open(), task = page.element('#settings-notification-task-alert'), failure = page.element('#settings-notification-monitoring-failure')
  task.checked = false; const firstSaving = task.onchange(); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference'), firstResult = await fixture.rpc('tools/call', first.params)
  task.checked = true; task.onchange(); failure.checked = false; const failureSaving = failure.onchange(); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_set_installation_notification_preference').length, 2, 'another kind can save while the first kind waits')
  const failureCall = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference'), failureResult = await fixture.rpc('tools/call', failureCall.params)
  page.reply(failureCall.id, failureResult); await failureSaving
  const reading = page.element('#settings-notification-reload').onclick(); await page.flush()
  const oldRead = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences'), oldResult = await fixture.rpc('tools/call', oldRead.params)
  const modelReading = page.element('#settings-reload').onclick(); await page.flush()
  await fixture.deliver(page, 'sentinel_model_settings'); await modelReading
  page.element('#settings-tab-notifications').onclick(); page.element('#settings-tab-settings').onclick()
  page.reply(first.id, firstResult); await firstSaving; await page.flush()
  const last = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference')
  assert.notEqual(last.id, first.id)
  assert.deepEqual(JSON.parse(JSON.stringify(last.params.arguments)), { kind: 'task-alert', enabled: true })
  assert.equal(task.checked, true)
  assert.match(page.element('#settings-notification-task-alert-status').textContent, /正在保存/)
  await fixture.deliver(page, 'sentinel_set_installation_notification_preference')
  page.reply(oldRead.id, oldResult); await reading
  assert.equal(task.checked, true)
  assert.equal(failure.checked, false)
  assert.match(page.element('#settings-notification-task-alert-status').textContent, /已保存.*开启/)
  const persisted = await fixture.rpc('tools/call', { name: 'sentinel_installation_notification_preferences', arguments: {} })
  assert.deepEqual(persisted.structuredContent.preferences, { 'task-alert': true, 'monitoring-failure': false })
  assert.equal(page.timers.size, 0)
})

for (const outcome of ['rejected', 'unknown', 'timeout', 'wrong-installation', 'wrong-content']) test('notification choices survive an unacknowledged host response without reporting success: ' + outcome, async t => {
  const page = await settingsPage(t), checkbox = page.element('#settings-notification-task-alert')
  checkbox.checked = false; const saving = checkbox.onchange(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference')
  if (outcome === 'timeout') [...page.timers.values()].find(timer => timer.delay === 5000).callback()
  else page.reply(call.id, ['rejected', 'unknown'].includes(outcome) ? { isError: true, structuredContent: { saveOutcome: outcome } } : { structuredContent: { registrationId: outcome === 'wrong-installation' ? 'chatgpt:foreign' : state.registrationId, revision: 'not-confirmed', preferences: { 'task-alert': outcome === 'wrong-content', 'monitoring-failure': true } } })
  await saving
  assert.equal(checkbox.checked, false)
  assert.match(page.element('#settings-notification-task-alert-status').textContent, outcome === 'rejected' ? /保存失败.*拒绝/ : /保存结果未确认/)
  assert.doesNotMatch(page.element('#settings-notification-task-alert-status').textContent, /已保存/)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_set_installation_notification_preference').length, 1)
  if (outcome === 'timeout') { page.reply(call.id, { structuredContent: { registrationId: state.registrationId, revision: 'late', preferences: { 'task-alert': false, 'monitoring-failure': true } } }); await page.flush(); assert.match(page.element('#settings-notification-task-alert-status').textContent, /未确认/) }
})

test('the global permission action shows actual native denial and errors without reading a chat or authorizing on ordinary reads', async t => {
  const fixture = await installationNotificationPage(t), page = await fixture.open()
  assert.match(page.element('#settings-notification-permission-status').textContent, /未确认/)
  assert.equal(page.element('#settings-notification-task-alert').checked, true)
  assert.deepEqual(fixture.counts(), { authorizationCalls: 0, settingsCalls: 0, threadCalls: 0 })
  const staleReading = page.element('#settings-notification-reload').onclick(); await page.flush()
  const staleRead = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences'), staleResult = await fixture.rpc('tools/call', staleRead.params)
  for (const failed of [false, true]) {
    if (failed) fixture.failAuthorization()
    const requesting = page.element('#settings-notification-permission').onclick(); await page.flush()
    const call = page.sent.findLast(row => row.params?.name === 'sentinel_request_installation_notification_permission')
    assert.deepEqual(JSON.parse(JSON.stringify(call.params.arguments)), {})
    await fixture.deliver(page, call.params.name); await requesting
    if (!failed) { const current = page.element('#settings-notification-permission-status').textContent; page.reply(staleRead.id, staleResult); await staleReading; assert.equal(page.element('#settings-notification-permission-status').textContent, current, 'an older preference read cannot erase the explicit permission result') }
    const rereading = page.element('#settings-notification-reload').onclick(); await page.flush()
    await fixture.deliver(page, 'sentinel_installation_notification_preferences'); await rereading
    assert.match(page.element('#settings-notification-permission-status').textContent, failed ? /授权出错.*native_unavailable/ : /未允许.*系统设置/)
    assert.doesNotMatch(page.element('#settings-notification-permission-status').textContent, /已允许/)
  }
  assert.deepEqual(fixture.counts(), { authorizationCalls: 2, settingsCalls: 0, threadCalls: 0 })
  assert.equal(page.timers.size, 0)
})

test('failed notification reads leave model editing and an explicit global permission request available', async t => {
  const page = await panelHarness(t, { html: await readFile(new URL('../chatgpt/settings.html', import.meta.url), 'utf8') })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences').id, { isError: true })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { structuredContent: state }); await page.flush()
  assert.match(page.element('#settings-notification-status').textContent, /读取未确认/)
  assert.equal(page.element('#settings-notification-task-alert').disabled, true)
  assert.equal(page.element('#settings-llm-model').value, 'summary-a')
  const model = page.element('#settings-llm-model'); model.value = 'still-editable'; model.oninput(); const saving = leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  page.reply(save.id, { structuredContent: { ...state, revision: 'independent-save', config: save.params.arguments.config } }); await saving
  assert.match(page.element('#settings-status').textContent, /已保存/)
  const requesting = page.element('#settings-notification-permission').onclick(); await page.flush()
  const permission = page.sent.findLast(row => row.params?.name === 'sentinel_request_installation_notification_permission')
  page.reply(permission.id, { structuredContent: { registrationId: state.registrationId, permission: { authorization: 'denied', requestInFlight: false, errorCode: null } } }); await requesting
  assert.match(page.element('#settings-notification-permission-status').textContent, /未允许/)
  assert.equal(page.timers.size, 0)
})

test('foreign notification and permission results stay unconfirmed and disposed settings ignore late writes', async t => {
  const fixture = await installationNotificationPage(t), page = await fixture.open()
  const reading = page.element('#settings-notification-reload').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_notification_preferences'), value = (await fixture.rpc('tools/call', read.params)).structuredContent
  page.reply(read.id, { structuredContent: { ...value, registrationId: 'chatgpt:foreign', preferences: { 'task-alert': false, 'monitoring-failure': false } } }); await reading
  assert.match(page.element('#settings-notification-status').textContent, /未确认/)
  assert.equal(page.element('#settings-notification-task-alert').checked, true)
  const requesting = page.element('#settings-notification-permission').onclick(); await page.flush()
  const permission = page.sent.findLast(row => row.params?.name === 'sentinel_request_installation_notification_permission')
  page.reply(permission.id, { structuredContent: { registrationId: 'chatgpt:foreign', permission: { authorization: 'authorized' } } }); await requesting
  assert.match(page.element('#settings-notification-permission-status').textContent, /未确认/)
  const checkbox = page.element('#settings-notification-task-alert'); checkbox.checked = false; const saving = checkbox.onchange(); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_set_installation_notification_preference'), result = await fixture.rpc('tools/call', save.params), listener = page.handlers.get('message'), before = page.element('#settings-notification-task-alert-status').textContent
  await page.destroy()
  listener({ source: page.parent, data: { jsonrpc: '2.0', id: save.id, result } }); await saving
  assert.equal(page.element('#settings-notification-task-alert-status').textContent, before)
  assert.equal(checkbox.checked, false)
  assert.equal(page.timers.size, 0)
  assert.equal((await fixture.rpc('tools/call', { name: 'sentinel_installation_notification_preferences', arguments: {} })).structuredContent.preferences['task-alert'], false)
})

for (const loss of ['decode', 'transport']) test('notification settings distinguish an actual Core rejection from a committed response ' + loss + ' loss', async t => {
  const fixture = await installationNotificationPage(t), page = await fixture.open(), checkbox = page.element('#settings-notification-task-alert'), originalFetch = globalThis.fetch
  let truncate = false, truncated = 0
  t.after(() => { globalThis.fetch = originalFetch })
  globalThis.fetch = async (input, options) => {
    const response = await originalFetch(input, options)
    if (!truncate || input !== fixture.server.url + '/v1/notifications/installations/preferences' || options?.method !== 'POST' || !response.ok) return response
    truncate = false; truncated++
    if (loss === 'transport') { await response.text(); throw new TypeError('controlled committed notification response loss') }
    return new Response((await response.text()).slice(0, -1), { status: response.status, headers: response.headers })
  }
  const change = async enabled => {
    checkbox.checked = enabled; const saving = checkbox.onchange(); await page.flush()
    const result = await fixture.deliver(page, 'sentinel_set_installation_notification_preference'); await saving; return result
  }
  const { DatabaseSync } = await import('node:sqlite'), blocker = new DatabaseSync(join(fixture.dataDir, 'observations.sqlite'))
  try {
    blocker.exec('BEGIN IMMEDIATE')
    const result = await change(false)
    assert.equal(result.structuredContent.saveOutcome, 'rejected')
    assert.match(page.element('#settings-notification-task-alert-status').textContent, /保存失败/)
    assert.equal((await fixture.rpc('tools/call', { name: 'sentinel_installation_notification_preferences', arguments: {} })).structuredContent.preferences['task-alert'], true)
  } finally { blocker.exec('ROLLBACK'); blocker.close() }
  await change(true)
  truncate = true
  const unknown = await change(false)
  assert.equal(truncated, 1)
  assert.equal(unknown.structuredContent.saveOutcome, 'unknown')
  assert.match(page.element('#settings-notification-task-alert-status').textContent, /保存结果未确认/)
  assert.doesNotMatch(page.element('#settings-notification-task-alert-status').textContent, /已保存|保存失败/)
  const persisted = await fixture.rpc('tools/call', { name: 'sentinel_installation_notification_preferences', arguments: {} })
  assert.equal(persisted.structuredContent.preferences['task-alert'], false, 'the real public read proves the Core committed before its response was lost')
  assert.equal(checkbox.checked, false)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_set_installation_notification_preference').length, 3, 'an unconfirmed write is not retried automatically')
  const reading = page.element('#settings-notification-reload').onclick(); await page.flush()
  await fixture.deliver(page, 'sentinel_installation_notification_preferences'); await reading
  assert.match(page.element('#settings-notification-task-alert-status').textContent, /保存结果未确认/)
})

for (const role of ['llm', 'jev']) test('settings list and connection buttons keep separate results and never save the focused draft: ' + role, async t => {
  const page = await settingsPage(t), model = page.element('#settings-' + role + '-model'), group = page.element('#settings-' + role + '-group')
  model.value = 'manual-draft'; model.oninput()
  const list = page.element('#settings-' + role + '-list'), check = page.element('#settings-' + role + '-check')
  page.document.activeElement = list
  group.onfocusout({ relatedTarget: list })
  const listing = list.onclick(); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  assert.deepEqual(JSON.parse(JSON.stringify(first.params.arguments)), { baseUrl: state.config[role].baseUrl })
  page.reply(first.id, { structuredContent: { state: 'online', online: true, inferenceVerified: false, models: [{ id: 'candidate-a', role: role === 'llm' ? 'summary' : 'jev' }, { id: '<manual>', role: 'unknown' }] } }); await listing
  assert.equal(model.value, 'manual-draft')
  assert.equal(page.element('#settings-' + role + '-options').children.length, 3)
  assert.match(page.element('#settings-' + role + '-list-status').textContent, /候选/)
  assert.match(page.element('#settings-' + role + '-check-status').textContent, /尚未检查/)
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
  page.document.activeElement = check
  group.onfocusout({ relatedTarget: check })
  const checking = check.onclick(); await page.flush()
  const second = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  assert.notEqual(second.id, first.id)
  page.reply(second.id, { structuredContent: { state: 'online', online: true, inferenceVerified: false, models: [{ id: 'manual-draft', role: 'unknown' }] } }); await checking
  assert.match(page.element('#settings-' + role + '-check-status').textContent, /模型可见.*分析.*未验证/)
  assert.ok(page.element('#settings-' + role + '-check-status').textContent.includes(state.config[role].baseUrl))
  assert.equal(page.element('#settings-' + role + '-options').children.length, 3, 'checking does not replace the independently read candidates')
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
})

test('choosing a listed model or pressing Enter in a manual field completes the model group explicitly', async t => {
  const page = await settingsPage(t), model = page.element('#settings-llm-model'), options = page.element('#settings-llm-options')
  const listing = page.element('#settings-llm-list').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  page.reply(read.id, { structuredContent: { state: 'online', online: true, models: [{ id: 'chosen-summary', role: 'summary' }] } }); await listing
  assert.equal(model.value, 'summary-a')
  assert.equal(options.value, '')
  options.value = 'chosen-summary'; const choosing = options.onchange(); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(first.params.arguments.config.llm.model, 'chosen-summary')
  page.reply(first.id, { structuredContent: { ...state, revision: 'chosen-summary-saved', config: first.params.arguments.config } }); await choosing
  model.value = 'manual-enter'; model.oninput(); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1)
  let prevented = false
  const entering = page.element('#settings-llm-group').onkeydown({ key: 'Enter', target: model, preventDefault() { prevented = true } }); await page.flush()
  assert.equal(prevented, true)
  const second = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(second.params.arguments.config.llm.model, 'manual-enter')
  page.reply(second.id, { structuredContent: { ...state, revision: 'manual-enter-saved', config: second.params.arguments.config } }); await entering
})

test('settings tabs preserve the draft and position while diagnostics read only on demand and never select a tab on response', async t => {
  const page = await settingsPage(t)
  assert.equal(page.element('#settings-pane-settings').hidden, false)
  assert.equal(page.element('#settings-pane-diagnostics').hidden, true)
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_installation_diagnostics'), false)
  const url = page.element('#settings-llm-url'); url.value = 'http://'; url.oninput()
  page.document.scrollingElement = { scrollTop: 180 }
  const opening = page.element('#settings-tab-diagnostics').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics')
  assert.deepEqual(JSON.parse(JSON.stringify(read.params.arguments)), {})
  page.element('#settings-tab-settings').onclick()
  page.reply(read.id, { structuredContent: diagnostics }); await opening
  assert.equal(page.element('#settings-pane-settings').hidden, false)
  assert.equal(page.element('#settings-tab-settings').getAttribute('aria-selected'), 'true')
  assert.equal(url.value, 'http://')
  assert.equal(page.document.scrollingElement.scrollTop, 180)
  assert.match(page.element('#diagnostics-collection').textContent, /采集失败.*native-collection.*read-failed/)
  assert.match(page.element('#diagnostics-analysis').textContent, /queued.*最近保留.*failed.*historical/)
  assert.match(page.element('#diagnostics-failure').textContent, /jev.*schema-invalid/)
  assert.doesNotMatch(page.element('#diagnostics-analysis').textContent, /分析成功/)
  page.element('#settings-tab-notifications').onclick()
  assert.equal(page.element('#settings-pane-notifications').hidden, false)
  assert.match(page.element('#settings-notifications-status').textContent, /正在读取/)
  page.element('#settings-tab-settings').onclick()
  assert.equal(url.value, 'http://')
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_model_settings').length, 1)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_installation_diagnostics').length, 1)
  assert.equal(page.timers.size, 0)
})

test('a rejected diagnostic refresh clears the prior snapshot and never leaves old configured or analysis facts as current', async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-diagnostics').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics').id, { structuredContent: diagnostics }); await opening
  const refreshing = page.element('#diagnostics-refresh').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics').id, { isError: true }); await refreshing
  assert.match(page.element('#diagnostics-status').textContent, /未确认/)
  assert.doesNotMatch(page.element('#diagnostics-collection').textContent, /read-failed/)
  assert.doesNotMatch(page.element('#diagnostics-model-llm').textContent, /已配置/)
  assert.doesNotMatch(page.element('#diagnostics-analysis').textContent, /historical|queued/)
})

test('directory and connection responses stay independent and reject old service results after edits or disposal', async t => {
  const page = await settingsPage(t), model = page.element('#settings-llm-model')
  model.value = 'manual-not-listed'; model.oninput()
  const listing = page.element('#settings-llm-list').onclick(), checking = page.element('#settings-llm-check').onclick(); await page.flush()
  const [list, check] = page.sent.filter(row => row.params?.name === 'sentinel_model_directory')
  page.reply(check.id, { structuredContent: { online: true, state: 'online', models: [{ id: 'another-model', role: 'unknown' }] } }); await checking
  page.reply(list.id, { structuredContent: { online: true, state: 'online', models: [{ id: 'summary-candidate', role: 'summary' }] } }); await listing
  assert.equal(model.value, 'manual-not-listed')
  assert.match(page.element('#settings-llm-check-status').textContent, /未列出.*可手填/)
  assert.match(page.element('#settings-llm-list-status').textContent, /候选/)
  const oldCheck = page.element('#settings-llm-check').onclick(); await page.flush()
  const old = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  page.element('#settings-llm-url').value = 'http://localhost:1'; page.element('#settings-llm-url').oninput()
  page.reply(old.id, { structuredContent: { online: true, state: 'online', models: [{ id: 'manual-not-listed', role: 'unknown' }] } }); await oldCheck
  assert.match(page.element('#settings-llm-check-status').textContent, /尚未检查/)
  assert.doesNotMatch(page.element('#diagnostics-llm-check').textContent, /服务在线|模型可见/)
  assert.equal(page.element('#settings-llm-options').children.length, 0)
  const pendingList = page.element('#settings-llm-list').onclick(); await page.flush()
  const pending = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory'), listener = page.handlers.get('message')
  await page.destroy()
  listener({ source: page.parent, data: { jsonrpc: '2.0', id: pending.id, result: { structuredContent: { online: true, state: 'online', models: [{ id: 'late-model', role: 'summary' }] } } } }); await pendingList
  assert.equal(page.element('#settings-llm-options').children.length, 0)
  assert.equal(model.value, 'manual-not-listed')
  assert.equal(page.timers.size, 0)
})

test('editing a manual model during a list request keeps the draft and releases the obsolete loading status', async t => {
  const page = await settingsPage(t), listing = page.element('#settings-llm-list').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  const model = page.element('#settings-llm-model'); model.value = 'new-manual-draft'; model.oninput()
  page.reply(call.id, { structuredContent: { state: 'online', online: true, models: [{ id: 'old-candidate', role: 'summary' }] } }); await listing
  assert.equal(model.value, 'new-manual-draft')
  assert.doesNotMatch(page.element('#settings-llm-list-status').textContent, /正在读取/)
  assert.equal(page.element('#settings-llm-list').disabled, false)
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
})

for (const unsupported of [false, true]) test('unconfigured and unsupported diagnostics retain unknown actual analysis while settings remain usable: ' + unsupported, async t => {
  const page = await settingsPage(t, { ...state, config: {} }), opening = page.element('#settings-tab-diagnostics').onclick(); await page.flush()
  const value = { ...diagnostics, models: { llm: { configured: false }, jev: { configured: false } }, collection: { state: 'unconfirmed', sourceCount: 0, connectedSources: 0, lastSeenAt: null, error: unsupported ? { stage: 'host-verification', code: 'unsupported-host-version' } : null }, analysis: { counts: { scope: 'active-tasks', total: 0, states: {} }, latest: null, latestFailure: null } }
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics').id, { structuredContent: value }); await opening
  assert.match(page.element('#diagnostics-model-llm').textContent, /未配置/)
  assert.match(page.element('#diagnostics-analysis').textContent, /尚无可用轮次/)
  assert.match(page.element('#diagnostics-collection').textContent, unsupported ? /未确认.*unsupported-host-version/ : /未确认/)
  assert.doesNotMatch(page.element('#diagnostics-analysis').textContent, /分析成功/)
  page.element('#settings-tab-settings').onclick()
  page.element('#settings-llm-url').value = 'http://localhost:1'; page.element('#settings-llm-url').oninput()
  page.element('#settings-llm-model').value = 'manual-offline'; page.element('#settings-llm-model').oninput()
  const saving = leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  page.reply(save.id, { structuredContent: { ...state, revision: 'saved-with-unknown-collection', config: save.params.arguments.config } }); await saving
  assert.match(page.element('#settings-status').textContent, /已保存/)
})

test('diagnostics reject a foreign installation and ignore a late response after the settings page closes', async t => {
  const page = await settingsPage(t), opening = page.element('#settings-tab-diagnostics').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics').id, { structuredContent: { ...diagnostics, registrationId: 'chatgpt:foreign-installation' } }); await opening
  assert.match(page.element('#diagnostics-status').textContent, /未确认/)
  assert.equal(page.element('#diagnostics-collection').textContent, '')
  const refreshing = page.element('#diagnostics-refresh').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_installation_diagnostics'), listener = page.handlers.get('message'), before = page.element('#diagnostics-status').textContent
  await page.destroy()
  listener({ source: page.parent, data: { jsonrpc: '2.0', id: call.id, result: { structuredContent: diagnostics } } }); await refreshing
  assert.equal(page.element('#diagnostics-status').textContent, before)
  assert.equal(page.element('#diagnostics-collection').textContent, '')
  assert.equal(page.timers.size, 0)
})

test('settings autosave a completed offline model group without saving keystrokes or another incomplete draft', async t => {
  const page = await settingsPage(t)
  page.element('#settings-jev-url').value = 'http://'
  page.element('#settings-jev-url').oninput()
  leaveModelGroup(page, 'jev')
  page.element('#settings-llm-url').value = 'http://127.0.0.1:1'
  page.element('#settings-llm-url').oninput()
  page.element('#settings-llm-model').value = 'manual-offline-summary'
  page.element('#settings-llm-model').oninput()
  await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 0)
  leaveModelGroup(page, 'llm')
  await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.deepEqual(JSON.parse(JSON.stringify(save.params.arguments)), { revision: state.revision, config: { llm: { baseUrl: 'http://127.0.0.1:1', model: 'manual-offline-summary' }, jev: state.config.jev } })
  assert.match(page.element('#settings-status').textContent, /正在保存/)
  page.reply(save.id, { structuredContent: { ...state, revision: 'saved-offline', config: save.params.arguments.config } })
  await page.flush()
  assert.equal(page.element('#settings-jev-url').value, 'http://')
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_model_directory'), false)
})

test('settings retain the latest completed edit while an earlier save is pending and never repaint a newer draft', async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model')
  input.value = 'summary-b'; input.oninput(); leaveModelGroup(page, 'llm')
  await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  input.value = 'summary-c'; input.oninput(); leaveModelGroup(page, 'llm')
  page.element('#settings-llm-group').onfocusout({ relatedTarget: null })
  await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1)
  page.reply(first.id, { structuredContent: { ...state, revision: 'saved-b', config: first.params.arguments.config } })
  await page.flush()
  assert.equal(input.value, 'summary-c')
  const second = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.notEqual(second.id, first.id, 'the latest completed edit is saved after the active request')
  assert.equal(second.params.arguments.revision, 'saved-b')
  assert.equal(second.params.arguments.config.llm.model, 'summary-c')
  input.value = 'summary-draft'; input.oninput()
  page.reply(second.id, { structuredContent: { ...state, revision: 'saved-c', config: second.params.arguments.config } })
  await page.flush()
  assert.equal(input.value, 'summary-draft')
  assert.match(page.element('#settings-status').textContent, /已保存.*草稿/)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 2)
})

test('leaving an unchanged group does not save or requeue its already completed edit', async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model')
  page.element('#settings-llm-group').onfocusout({ relatedTarget: null }); await page.flush()
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
  assert.doesNotMatch(page.element('#settings-llm-status').textContent, /草稿/)
  input.value = 'summary-b'; input.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  page.element('#settings-llm-group').onfocusout({ relatedTarget: null })
  page.reply(save.id, { structuredContent: { ...state, revision: 'same-completed-model', config: save.params.arguments.config } }); await page.flush()
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.doesNotMatch(page.element('#settings-status').textContent, /草稿/)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1)
})

test('settings preserve edits and position across a late initial read, another read and host-context changes', async t => {
  const page = await panelHarness(t, { html: await readFile(new URL('../chatgpt/settings.html', import.meta.url), 'utf8') })
  const initialRead = page.sent.findLast(row => row.params?.name === 'sentinel_model_settings')
  page.document.scrollingElement = { scrollTop: 240 }
  const model = page.element('#settings-llm-model')
  page.element('#settings-llm-url').value = 'http://localhost:1'; page.element('#settings-llm-url').oninput()
  model.value = 'completed-before-read'; model.oninput(); leaveModelGroup(page, 'llm')
  page.reply(initialRead.id, { structuredContent: state }); await page.flush()
  assert.equal(model.value, 'completed-before-read')
  assert.equal(page.element('#settings-llm-url').value, 'http://localhost:1')
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(save.params.arguments.config.llm.model, 'completed-before-read')
  page.reply(save.id, { structuredContent: { ...state, revision: 'initial-edit-saved', config: save.params.arguments.config } }); await page.flush()
  const reading = page.element('#settings-reload').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === 'sentinel_model_settings')
  model.value = 'newer draft'; model.oninput()
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: { threadId: 'another-chat' } } })
  page.reply(read.id, { structuredContent: state }); await reading
  assert.equal(model.value, 'newer draft')
  assert.equal(page.document.scrollingElement.scrollTop, 240)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_model_settings').length, 2)
})

test('a model group cleared before the initial read completes stays cleared and saves its optional empty configuration', async t => {
  const page = await panelHarness(t, { html: await readFile(new URL('../chatgpt/settings.html', import.meta.url), 'utf8') })
  page.element('#settings-llm-url').value = ''; page.element('#settings-llm-url').oninput()
  page.element('#settings-llm-model').value = ''; page.element('#settings-llm-model').oninput()
  page.element('#settings-llm-group').onfocusout({ relatedTarget: null })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { structuredContent: state }); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.ok(save, 'the completed clearing edit is not mistaken for an untouched empty loading form')
  assert.deepEqual(JSON.parse(JSON.stringify(save.params.arguments.config)), { jev: state.config.jev })
  page.reply(save.id, { structuredContent: { ...state, revision: 'initial-empty-group', config: save.params.arguments.config } }); await page.flush()
  assert.equal(page.element('#settings-llm-url').value, '')
  assert.equal(page.element('#settings-llm-model').value, '')
})

for (const failure of ['rejected', 'unclassified', 'timeout', 'wrong-installation', 'wrong-content']) test('settings preserve a failed draft and distinguish unconfirmed writes: ' + failure, async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model')
  input.value = 'unsaved-summary'; input.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  if (failure === 'timeout') [...page.timers.values()].find(timer => timer.delay === 5000).callback()
  else page.reply(save.id, failure === 'rejected' ? { isError: true, structuredContent: { saveOutcome: 'rejected' } } : failure === 'unclassified' ? { isError: true } : { structuredContent: { ...state, registrationId: failure === 'wrong-installation' ? 'chatgpt:another-installation' : state.registrationId, revision: 'unexpected-save', config: failure === 'wrong-content' ? state.config : save.params.arguments.config } })
  await page.flush()
  assert.match(page.element('#settings-status').textContent, failure === 'rejected' ? /保存失败/ : /保存结果未确认/)
  assert.doesNotMatch(page.element('#settings-status').textContent, /已保存/)
  assert.equal(input.value, 'unsaved-summary')
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1, 'unconfirmed writes are not automatically retried')
  if (failure === 'timeout') {
    page.reply(save.id, { structuredContent: { ...state, revision: 'late-timeout-save', config: save.params.arguments.config } }); await page.flush()
    assert.match(page.element('#settings-status').textContent, /保存结果未确认/, 'a late response after cancellation cannot acknowledge the write')
  }
  input.value = 'next-completed-summary'; input.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const next = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.notEqual(next.id, save.id, 'a subsequent completed edit remains available without throwing away the draft')
  page.reply(next.id, { structuredContent: { ...state, revision: 'next-completed-save', config: next.params.arguments.config } }); await page.flush()
  assert.match(page.element('#settings-status').textContent, /已保存/)
})

test('settings save only after leaving a valid group and preserve clearing an optional model configuration', async t => {
  const page = await settingsPage(t), url = page.element('#settings-llm-url'), model = page.element('#settings-llm-model'), group = page.element('#settings-llm-group')
  for (const invalid of ['http://', 'https://example.com', 'http://user:secret@localhost', 'http://localhost/v1', 'http://localhost?key=x', 'file://localhost']) {
    url.value = invalid; url.oninput(); group.onfocusout({ relatedTarget: null }); await page.flush()
    assert.match(page.element('#settings-llm-status').textContent, /草稿未保存/)
  }
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
  url.value = 'http://localhost:1'; url.oninput()
  group.onfocusout({ relatedTarget: model }); await page.flush()
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false, 'moving between fields does not finish the group')
  group.onfocusout({ relatedTarget: null }); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(save.params.arguments.config.llm.baseUrl, 'http://localhost:1')
  page.reply(save.id, { structuredContent: { ...state, revision: 'group-left', config: save.params.arguments.config } }); await page.flush()
  group.onfocusout({ relatedTarget: null }); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1, 'the same completed group is not saved twice')
  url.value = ''; url.oninput(); model.value = ''; model.oninput(); group.onfocusout({ relatedTarget: null }); await page.flush()
  const clearing = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.deepEqual(JSON.parse(JSON.stringify(clearing.params.arguments.config)), { jev: state.config.jev })
  page.reply(clearing.id, { structuredContent: { ...state, revision: 'group-cleared', config: clearing.params.arguments.config } }); await page.flush()
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.equal(url.value, '')
  assert.equal(model.value, '')
})

test('settings reread an unconfirmed write without calling that lost save successful', async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model')
  input.value = 'lost-save'; input.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  ;[...page.timers.values()].find(timer => timer.delay === 5000).callback(); await page.flush()
  const reread = page.element('#settings-reload').onclick(); await page.flush()
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { structuredContent: { ...state, revision: 'read-after-loss', config: save.params.arguments.config } }); await reread
  assert.equal(input.value, 'lost-save')
  assert.doesNotMatch(page.element('#settings-status').textContent, /已保存/)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1)
})

test('settings preserve another completed group and ignore pending saves after disposal', async t => {
  const page = await settingsPage(t), input = page.element('#settings-llm-model')
  input.value = 'summary-b'; input.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  const judge = page.element('#settings-jev-model')
  judge.value = 'judge-b'; judge.oninput(); leaveModelGroup(page, 'jev')
  input.value = ''; input.oninput()
  page.reply(first.id, { structuredContent: { ...state, revision: 'saved-summary-b', config: first.params.arguments.config } }); await page.flush()
  assert.equal(input.value, '')
  assert.equal(judge.value, 'judge-b')
  const second = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(second.params.arguments.config.llm.model, 'summary-b')
  assert.equal(second.params.arguments.config.jev.model, 'judge-b')
  const listener = page.handlers.get('message'), status = page.element('#settings-status').textContent
  await page.destroy()
  listener({ source: page.parent, data: { jsonrpc: '2.0', id: second.id, result: { structuredContent: { ...state, revision: 'disposed-save', config: second.params.arguments.config } } } }); await page.flush()
  assert.equal(page.element('#settings-status').textContent, status)
  assert.equal(page.element('#settings-llm-model').value, '')
  assert.equal(page.timers.size, 0)
})

test('hiding settings cancels a pending write without losing a later completed group or writing while hidden', async t => {
  const page = await settingsPage(t), summary = page.element('#settings-llm-model'), judge = page.element('#settings-jev-model')
  summary.value = 'summary-pending'; summary.oninput(); leaveModelGroup(page, 'llm'); await page.flush()
  const first = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  judge.value = 'judge-completed'; judge.oninput(); leaveModelGroup(page, 'jev')
  page.document.hidden = true; page.handlers.get('visibilitychange')(); await page.flush()
  assert.equal(page.sent.findLast(row => row.method === 'notifications/cancelled').params.requestId, first.id)
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, 1)
  assert.match(page.element('#settings-status').textContent, /保存结果未确认/)
  page.document.hidden = false; page.handlers.get('visibilitychange')(); await page.flush()
  const resumed = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.notEqual(resumed.id, first.id)
  assert.equal(resumed.params.arguments.config.jev.model, 'judge-completed')
  page.reply(resumed.id, { structuredContent: { ...state, revision: 'resumed-completed-group', config: resumed.params.arguments.config } }); await page.flush()
  assert.equal(summary.value, 'summary-pending')
  assert.equal(judge.value, 'judge-completed')
  assert.match(page.element('#settings-status').textContent, /已保存.*草稿/)
})

test('installation settings remain available through the real MCP when current-chat collection fails', async t => {
  const token = 'a'.repeat(64), saved = []
  const panel = await startPanelServer({ token, read: async () => { throw new Error('collection unavailable') }, modelSettings: {
    read: async () => state,
    save: async change => { saved.push(change); return { ...state, config: change.config, revision: 'settings-revision-b' } },
  } })
  t.after(() => panel.close())
  const rpc = async (method, params) => {
    const response = await fetch(panel.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const tools = (await rpc('tools/list', {})).tools
  assert.ok(tools.find(tool => tool.name === 'sentinel_model_settings'))
  const save = tools.find(tool => tool.name === 'sentinel_save_model_settings')
  assert.deepEqual(save._meta.ui.visibility, ['app'])
  assert.equal(save.annotations.readOnlyHint, false)
  assert.deepEqual((await rpc('tools/call', { name: 'sentinel_model_settings', arguments: {} })).structuredContent, state)
  const change = { revision: state.revision, config: state.config }
  assert.equal((await rpc('tools/call', { name: save.name, arguments: change })).structuredContent.revision, 'settings-revision-b')
  assert.deepEqual(saved, [change])
})

test('production adapter settings bind the installation independently from collection and other installations', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-'))
  let server, core, lazy, adapter
  t.after(async () => { try { await adapter?.close(); await lazy?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir })
  core = await connectCore({ dataDir })
  lazy = createLazyCore({ dataDir })
  const token = 'b'.repeat(64)
  const native = {
    async sampleHost() { return { available: true, hostRunId: 'settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement(cwd) { assert.equal(cwd, null); return { user: 'enabled', effective: 'enabled' } },
  }
  adapter = await startChatGPTAdapter({ core: lazy, native, installationId: 'settings-installation', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const request = async (method, params = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    const envelope = await response.json()
    assert.ok(!envelope.error, envelope.error?.message)
    assert.ok(!envelope.result?.isError, JSON.stringify(envelope.result?.content))
    return envelope.result
  }
  const rpc = async (name, args = {}) => (await request('tools/call', { name, arguments: args })).structuredContent
  const tools = (await request('tools/list')).tools
  const monitor = tools.find(tool => tool.name === 'sentinel_current_chat'), settings = tools.find(tool => tool.name === 'sentinel_model_settings')
  assert.deepEqual(monitor._meta['openai/ui'].entrypoints, [{ type: 'thread' }])
  assert.deepEqual(settings._meta['openai/ui'].entrypoints, [{ type: 'global' }])
  assert.notEqual(monitor._meta.ui.resourceUri, settings._meta.ui.resourceUri)
  const resources = (await request('resources/list')).resources
  assert.deepEqual(resources.map(resource => resource.uri).sort(), ['ui://ghost-sentinel/current-chat.html', 'ui://ghost-sentinel/settings.html'])
  const resource = await request('resources/read', { uri: settings._meta.ui.resourceUri })
  assert.equal(resource.contents[0].mimeType, 'text/html;profile=mcp-app')
  const first = await rpc('sentinel_model_settings')
  assert.equal(first.registrationId, 'chatgpt:settings-installation')
  const page = await panelHarness(t, { html: resource.contents[0].text })
  const reading = page.sent.findLast(row => row.method === 'tools/call')
  assert.equal(reading.params.name, 'sentinel_model_settings')
  page.reply(reading.id, { structuredContent: first }); await page.flush()
  for (const role of ['llm', 'jev']) {
    page.element('#settings-' + role + '-url').value = state.config[role].baseUrl
    page.element('#settings-' + role + '-url').oninput()
    page.element('#settings-' + role + '-model').value = state.config[role].model
    page.element('#settings-' + role + '-model').oninput()
    const saving = leaveModelGroup(page, role); await page.flush()
    const change = page.sent.findLast(row => row.method === 'tools/call')
    assert.equal(change.params.name, 'sentinel_save_model_settings')
    assert.equal(change.params.arguments.expectedThreadId, undefined)
    page.reply(change.id, await request('tools/call', change.params)); await saving
  }
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.deepEqual((await core.installationAnalysisConfig('chatgpt:another-installation')).config, {})
  assert.deepEqual((await rpc('sentinel_model_settings')).config, state.config)
  const peer = await panelHarness(t, { html: resource.contents[0].text }), peerState = await rpc('sentinel_model_settings')
  peer.reply(peer.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { structuredContent: peerState }); await peer.flush()
  for (const [window, model] of [[page, 'earlier-window-summary'], [peer, 'last-window-summary']]) {
    const input = window.element('#settings-llm-model')
    input.value = model; input.oninput(); const saving = leaveModelGroup(window, 'llm'); await window.flush()
    const change = window.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
    if (window === peer) assert.equal(change.params.arguments.revision, peerState.revision, 'the later window may save the revision it originally read')
    window.reply(change.id, await request('tools/call', change.params)); await saving
    assert.match(window.element('#settings-status').textContent, /已保存/)
  }
  assert.equal((await rpc('sentinel_model_settings')).config.llm.model, 'last-window-summary')
  assert.equal(page.element('#settings-llm-model').value, 'earlier-window-summary')
  const beforeRestore = page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length
  const restored = page.element('#settings-llm-model')
  restored.value = 'temporary-summary'; restored.oninput()
  restored.value = 'earlier-window-summary'; restored.oninput()
  const restoring = leaveModelGroup(page, 'llm'); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, beforeRestore + 1, 'a new completed edit may intentionally restore this window value over the other window save')
  const restore = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  assert.equal(restore.params.arguments.config.llm.model, 'earlier-window-summary')
  page.reply(restore.id, await request('tools/call', restore.params)); await restoring
  assert.equal((await rpc('sentinel_model_settings')).config.llm.model, 'earlier-window-summary')
  page.element('#settings-llm-group').onfocusout({ relatedTarget: null }); await page.flush()
  assert.equal(page.sent.filter(row => row.params?.name === 'sentinel_save_model_settings').length, beforeRestore + 1, 'a later blur with no new input does not save twice')
  const monitorPage = await panelHarness(t)
  await monitorPage.deliver({ summary: '采集失败', identity: { threadId: null }, analysis: { state: 'unknown' } })
  page.element('#settings-llm-model').value = 'unsubmitted-draft'
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', method: 'ui/notifications/host-context-changed', params: {} } })
  await monitorPage.refresh(); await monitorPage.deliver({ summary: '另一条聊天', identity: { threadId: 'other-chat' }, analysis: { state: 'unknown' } })
  await monitorPage.destroy()
  assert.equal(page.element('#settings-llm-model').value, 'unsubmitted-draft')
  const pendingRead = page.element('#settings-reload').onclick(); await page.flush()
  const lateRead = page.sent.findLast(row => row.method === 'tools/call')
  page.document.hidden = true
  page.handlers.get('visibilitychange')()
  await pendingRead
  assert.equal(page.sent.findLast(row => row.method === 'notifications/cancelled').params.requestId, lateRead.id)
  page.reply(lateRead.id, { structuredContent: first }); await page.flush()
  page.document.hidden = false
  page.handlers.get('visibilitychange')()
  assert.equal(page.element('#settings-llm-model').value, 'unsubmitted-draft')
  const directory = page.element('#settings-llm-check').onclick(); await page.flush()
  const pendingDirectory = page.sent.findLast(row => row.method === 'tools/call')
  page.handlers.get('message')({ source: page.parent, data: { jsonrpc: '2.0', id: 'settings-close', method: 'ui/resource-teardown', params: {} } })
  assert.equal(JSON.stringify(page.sent.find(row => row.id === 'settings-close')?.result), '{}')
  await directory
  assert.equal(page.sent.findLast(row => row.method === 'notifications/cancelled').params.requestId, pendingDirectory.id)
  assert.equal(page.timers.size, 0)
  assert.equal(adapter.diagnostics().closed, false)
  assert.equal(adapter.diagnostics().trackedThreads, 0)
})

for (const loss of ['decode', 'transport']) test('Core response ' + loss + ' loss remains unconfirmed in the actual MCP settings page after a committed save', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-core-response-')), originalFetch = globalThis.fetch
  let server, core, lazy, adapter, blocker, truncate = false, decodeError, truncated = 0
  t.after(async () => {
    globalThis.fetch = originalFetch; blocker?.exec('ROLLBACK'); blocker?.close()
    try { await adapter?.close(); await lazy?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) }
  })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir }); lazy = createLazyCore({ dataDir })
  const registrationId = 'chatgpt:core-response-loss', token = 'b'.repeat(64)
  const before = await core.configureInstallationAnalysis(registrationId, state.config, (await core.installationAnalysisConfig(registrationId)).revision)
  globalThis.fetch = async (input, options) => {
    const response = await originalFetch(input, options)
    if (!truncate || input !== server.url + '/v1/analysis/installations/config' || options?.method !== 'POST' || !response.ok) return response
    truncate = false; truncated++
    const text = await response.text()
    if (loss === 'transport') { decodeError = new TypeError('controlled Core response transport loss'); throw decodeError }
    const broken = new Response(text.slice(0, -1), { status: response.status, headers: response.headers })
    const json = broken.json.bind(broken)
    broken.json = async () => { try { return await json() } catch (error) { decodeError = error; throw error } }
    return broken
  }
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'core-response-host', hostStartedAt: 1, profile: null } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
  }, installationId: 'core-response-loss', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (method, params) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const resource = await rpc('resources/read', { uri: 'ui://ghost-sentinel/settings.html' })
  const page = await panelHarness(t, { html: resource.contents[0].text })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, await rpc('tools/call', { name: 'sentinel_model_settings', arguments: {} })); await page.flush()
  const complete = async model => {
    const input = page.element('#settings-llm-model'); input.value = model; input.oninput(); const saving = leaveModelGroup(page, 'llm'); await page.flush()
    const call = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings'), result = await rpc('tools/call', call.params)
    page.reply(call.id, result); await saving; await page.flush()
    return result
  }
  const { DatabaseSync } = await import('node:sqlite')
  blocker = new DatabaseSync(join(dataDir, 'observations.sqlite')); blocker.exec('BEGIN IMMEDIATE')
  const rejected = await complete('explicitly-rejected')
  assert.equal(rejected.structuredContent?.saveOutcome, 'rejected')
  assert.match(page.element('#settings-status').textContent, /保存失败/)
  assert.deepEqual(await core.installationAnalysisConfig(registrationId), before, 'an explicit Core rejection does not commit')
  t.diagnostic('control: actual Core SQLite rejection retained the old public configuration and displayed 保存失败')
  blocker.exec('ROLLBACK'); blocker.close(); blocker = undefined
  truncate = true
  const result = await complete('committed-before-decode-loss')
  assert.equal(truncated, 1)
  assert.equal(decodeError?.coreRequestFailure?.kind, loss)
  assert.equal((await core.installationAnalysisConfig(registrationId)).config.llm.model, 'committed-before-decode-loss', 'normal public GET proves the successful POST committed before its response was truncated')
  assert.equal(result.structuredContent?.saveOutcome, 'unknown')
  t.diagnostic(JSON.stringify({ coreFailure: decodeError.coreRequestFailure.kind, coreStatus: decodeError.coreRequestFailure.status, mcpIsError: result.isError, saveOutcome: result.structuredContent?.saveOutcome, mcpContent: result.content, ui: page.element('#settings-status').textContent }))
  assert.match(page.element('#settings-status').textContent, /保存结果(?:未知|未确认)/)
  assert.doesNotMatch(page.element('#settings-status').textContent, /保存失败|已保存/)
})

test('global settings reject untrusted host or plugin qualification before accessing installation configuration', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-qualification-'))
  let server, core, lazy, adapter, enabled = false, available = true, connectionAttempts = 0
  t.after(async () => { try { await adapter?.close(); await lazy?.close(); await core?.close(); await server?.close() } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir })
  lazy = createLazyCore({ dataDir, connect: async options => { connectionAttempts++; return connectCore(options) } })
  const token = 'c'.repeat(64), registrationId = 'chatgpt:qualified-settings'
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available, hostRunId: 'settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
  }, installationId: 'qualified-settings', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (name, args = {}) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const before = await core.installationAnalysisConfig(registrationId)
  assert.equal((await rpc('sentinel_model_settings')).isError, true)
  assert.equal((await rpc('sentinel_save_model_settings', { revision: before.revision, config: state.config })).isError, true)
  assert.equal((await rpc('sentinel_model_directory', { baseUrl: 'http://127.0.0.1:1' })).isError, true)
  assert.deepEqual(await core.installationAnalysisConfig(registrationId), before)
  enabled = true; available = false
  assert.equal((await rpc('sentinel_model_settings')).isError, true)
  assert.equal((await rpc('sentinel_save_model_settings', { revision: before.revision, config: state.config })).isError, true)
  assert.deepEqual(await core.installationAnalysisConfig(registrationId), before)
  assert.equal(connectionAttempts, 0, 'unqualified installation settings never connect Core')
})

for (const change of ['none', 'disabled', 'closed']) test('cold settings save respects qualification during Core connection: ' + change, { timeout: 10000 }, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-cold-settings-'))
  let server, core, lazy, adapter, enabled = true, connected, releaseConnection
  const modelCalls = [], modelServer = createServer((req, res) => { modelCalls.push({ method: req.method, url: req.url }); res.writeHead(500).end() })
  await new Promise(resolve => modelServer.listen(0, '127.0.0.1', resolve))
  const config = { llm: { baseUrl: 'http://127.0.0.1:' + modelServer.address().port, model: 'summary-a' }, jev: { baseUrl: 'http://127.0.0.1:' + modelServer.address().port, model: 'judge-a' } }
  const entered = new Promise(resolve => { connected = resolve }), release = new Promise(resolve => { releaseConnection = resolve })
  t.after(async () => { releaseConnection(); try { await adapter?.close(); await lazy?.close(); await core?.close(); await server?.close() } finally { await new Promise(resolve => modelServer.close(resolve)); await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir })
  const registrationId = 'chatgpt:cold-settings', before = await core.installationAnalysisConfig(registrationId), token = 'd'.repeat(64)
  let connectionAttempts = 0
  lazy = createLazyCore({ dataDir, connect: async options => { connectionAttempts++; connected(); await release; return connectCore(options) } })
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'cold-settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
  }, installationId: 'cold-settings', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const saving = fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_save_model_settings', arguments: { revision: before.revision, config } } }) }).then(response => response.json()).then(result => ({ result }), error => ({ error }))
  await entered
  if (change === 'disabled') enabled = false
  if (change === 'closed') await adapter.close()
  releaseConnection()
  const response = await saving
  await lazy.health()
  const after = await core.installationAnalysisConfig(registrationId)
  if (change === 'none') {
    assert.equal(response.result?.result?.isError, undefined)
    assert.deepEqual(after.config, config)
    assert.equal(adapter.diagnostics().trackedThreads, 0)
    assert.equal(adapter.diagnostics().openReaders, 0)
  } else {
    if (change === 'disabled') assert.equal(response.result?.result?.isError, true)
    else assert.ok(response.error)
    assert.deepEqual(after, before, 'revoked settings cannot write after the Core connection completes')
  }
  assert.equal(connectionAttempts, 1)
  assert.deepEqual((await core.snapshot()).sources, [])
  assert.deepEqual(modelCalls, [], 'settings connection and save never invoke model IO')
})

test('actual settings list and check buttons leave cold Core in standby and only GET the external directory', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-directory-')), calls = []
  let available = true, adapter, connectionAttempts = 0
  const models = createServer((req, res) => { calls.push({ method: req.method, url: req.url }); res.writeHead(available ? 200 : 503, { 'content-type': 'application/json' }).end(JSON.stringify({ data: [{ id: 'summary-a' }] })) })
  const lazy = createLazyCore({ dataDir, connect: async () => { connectionAttempts++; throw new Error('model directory must not connect Core') } })
  t.after(async () => { try { await adapter?.close(); await lazy.close(); await new Promise(resolve => models.close(resolve)) } finally { await rm(dataDir, { recursive: true, force: true }) } })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const token = 'e'.repeat(64)
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'directory-settings-host', hostStartedAt: 1, profile: null } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
  }, installationId: 'directory-settings', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (method, params) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    return (await response.json()).result
  }
  const resource = await rpc('resources/read', { uri: 'ui://ghost-sentinel/settings.html' }), page = await panelHarness(t, { html: resource.contents[0].text })
  page.reply(page.sent.findLast(row => row.params?.name === 'sentinel_model_settings').id, { structuredContent: state }); await page.flush()
  const baseUrl = 'http://127.0.0.1:' + models.address().port
  for (const role of ['llm', 'jev']) {
    page.element('#settings-' + role + '-url').value = baseUrl; page.element('#settings-' + role + '-url').oninput()
    const model = page.element('#settings-' + role + '-model'); model.value = 'manual-not-listed'; model.oninput()
    for (const action of ['list', 'check']) {
      const button = page.element('#settings-' + role + '-' + action)
      page.document.activeElement = button
      page.element('#settings-' + role + '-group').onfocusout({ relatedTarget: button })
      const reading = button.onclick(); await page.flush()
      const call = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
      assert.deepEqual(JSON.parse(JSON.stringify(call.params.arguments)), { baseUrl })
      page.reply(call.id, await rpc('tools/call', call.params)); await reading
      assert.equal(model.value, 'manual-not-listed')
      if (action === 'list') {
        assert.equal(page.element('#settings-' + role + '-options').children.length, 2)
        assert.match(page.element('#settings-' + role + '-check-status').textContent, /尚未检查/)
      } else assert.match(page.element('#settings-' + role + '-check-status').textContent, /未列出.*实际分析未验证/)
    }
  }
  available = false
  const unavailable = page.element('#settings-llm-list').onclick(); await page.flush()
  const call = page.sent.findLast(row => row.params?.name === 'sentinel_model_directory')
  page.reply(call.id, await rpc('tools/call', call.params)); await unavailable
  assert.match(page.element('#settings-llm-list-status').textContent, /不可用.*手填/)
  assert.equal(page.element('#settings-llm-model').value, 'manual-not-listed')
  assert.deepEqual(calls, Array.from({ length: 5 }, () => ({ method: 'GET', url: '/v1/models' })))
  assert.equal(page.sent.some(row => row.params?.name === 'sentinel_save_model_settings'), false)
  assert.equal(connectionAttempts, 0)
  assert.equal(adapter.diagnostics().trackedThreads, 0)
  assert.equal(adapter.diagnostics().openReaders, 0)
})

test('the actual diagnostics tab reads own failed collection through MCP and Core while unconfigured settings stay usable', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-settings-diagnostics-')), modelCalls = [], token = 'f'.repeat(64)
  const models = createServer((req, res) => { modelCalls.push({ method: req.method, url: req.url }); res.writeHead(500).end() })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  let server, core, lazy, adapter, threadCalls = 0
  t.after(async () => { try { await adapter?.close(); await lazy?.close(); await core?.close(); await server?.close(); await new Promise(resolve => models.close(resolve)) } finally { await rm(dataDir, { recursive: true, force: true }) } })
  server = await startServer({ dataDir }); core = await connectCore({ dataDir }); lazy = createLazyCore({ dataDir })
  const registrationId = 'chatgpt:settings-diagnostics', own = await core.registerSource({ registrationId, hostRunId: 'own-host', name: 'own', policy: 'chatgpt-main/1' })
  const other = await core.registerSource({ registrationId: 'another-installation', hostRunId: 'other-host', name: 'other' })
  for (const [source, id] of [[own, 'own-task'], [other, 'other-task']]) await core.ingest(source, { tasks: [{ id, lifecycleId: 'life', title: id, status: 'running' }] })
  await core.recordCollectionOutcome({ sourceKey: own, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'metadata-failure', outcome: 'failed', settledAt: Date.now(), error: { stage: 'metadata', code: 'metadata-unavailable' } })
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'qualified-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
    async readThread() { threadCalls++; throw new Error('settings diagnostics do not read the current chat') },
  }, installationId: 'settings-diagnostics', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const rpc = async (method, params) => {
    const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
    assert.equal(response.status, 200)
    const result = (await response.json()).result; assert.ok(!result.isError, JSON.stringify(result)); return result
  }
  const tools = (await rpc('tools/list', {})).tools, diagnosticTool = tools.find(tool => tool.name === 'sentinel_installation_diagnostics')
  assert.deepEqual(diagnosticTool._meta.ui.visibility, ['app']); assert.equal(diagnosticTool.annotations.readOnlyHint, true)
  const resource = await rpc('resources/read', { uri: 'ui://ghost-sentinel/settings.html' }), page = await panelHarness(t, { html: resource.contents[0].text })
  const initialRead = page.sent.findLast(row => row.params?.name === 'sentinel_model_settings')
  page.reply(initialRead.id, await rpc('tools/call', initialRead.params)); await page.flush()
  const opening = page.element('#settings-tab-diagnostics').onclick(); await page.flush()
  const read = page.sent.findLast(row => row.params?.name === diagnosticTool.name), result = await rpc('tools/call', read.params)
  page.reply(read.id, result); await opening
  assert.equal(result.structuredContent.registrationId, registrationId)
  assert.deepEqual(result.structuredContent.analysis.counts, { scope: 'active-tasks', total: 1, states: { unknown: 1 } })
  assert.match(page.element('#diagnostics-collection').textContent, /采集失败.*metadata.*metadata-unavailable/)
  assert.match(page.element('#diagnostics-model-llm').textContent, /未配置/)
  assert.doesNotMatch(page.element('#diagnostics-analysis').textContent, /分析成功/)
  page.element('#settings-tab-settings').onclick()
  page.element('#settings-llm-url').value = 'http://127.0.0.1:' + models.address().port; page.element('#settings-llm-url').oninput()
  page.element('#settings-llm-model').value = 'manual-offline'; page.element('#settings-llm-model').oninput()
  const saving = leaveModelGroup(page, 'llm'); await page.flush()
  const save = page.sent.findLast(row => row.params?.name === 'sentinel_save_model_settings')
  page.reply(save.id, await rpc('tools/call', save.params)); await saving
  assert.match(page.element('#settings-status').textContent, /已保存/)
  assert.equal((await core.installationAnalysisConfig(registrationId)).config.llm.model, 'manual-offline')
  assert.equal(threadCalls, 0)
  assert.deepEqual(modelCalls, [])
  assert.equal((await core.snapshot()).sources.length, 2)
  assert.equal(page.sent.filter(row => row.params?.name === diagnosticTool.name).length, 1, 'saving and tab changes do not poll diagnostics')
})
