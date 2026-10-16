import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { decodeJevRequest } from './helpers/jev-wire.mjs'

async function fixture(t, options = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-notification-'))
  let clock = Date.now(), server = await startServer({ dataDir, now: () => clock, ...options }), core = await connectCore({ dataDir })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:native-notification', hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  t.after(async () => { await core.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const request = async (path, body) => {
    const response = await fetch(server.url + path, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: 'Bearer ' + server.token, 'content-type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) })
    return { status: response.status, body: await response.json() }
  }
  return { sourceKey, get core() { return core }, get server() { return server }, now: () => clock, advance: ms => { clock += ms }, request, async restart() { await core.close(); await server.close(); server = await startServer({ dataDir, now: () => clock }); core = await connectCore({ dataDir }) } }
}

test('a persisted first collection failure creates one linked native notice even when recovery precedes the first notification poll', async t => {
  const f = await fixture(t)
  const outcome = { sourceKey: f.sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'failed-collection', outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } }
  const first = await f.core.recordCollectionOutcome(outcome)
  assert.equal(first.created, true)
  f.advance(1)
  await f.core.recordCollectionOutcome({ ...outcome, roundId: 'recovered-collection', outcome: 'succeeded', settledAt: f.now(), error: undefined })
  assert.equal((await f.core.failureReceipts({ sourceKey: f.sourceKey })).receipts[0].state, 'recovered')
  const eligible = await f.request('/v1/notifications/native/eligibility', { sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  assert.equal(eligible.status, 200)
  const records = await f.request('/v1/notifications/native?' + new URLSearchParams({ sourceKey: f.sourceKey }))
  assert.equal(records.status, 200)
  assert.equal(records.body.records.length, 1)
  const notice = records.body.records[0]
  assert.equal(notice.kind, 'monitoring-failure')
  assert.deepEqual(notice.receiptIds, [first.receipt.id])
  assert.equal(notice.mainId, null)
  assert.equal(notice.lifecycleId, null)
  assert.equal(notice.hostRunId, 'native-host')
  assert.equal(notice.humanSeen, 'unknown')
  assert.equal((await f.core.claimNotifications(f.sourceKey)).length, 1)
})

async function firstNotice(f, roundId = 'first-failure') {
  await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId, outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } })
  await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  return (await f.core.claimNotifications(f.sourceKey))[0]
}

test('native authorization, acceptance, delivered snapshots and user action remain separate facts tied to one claim attempt', async t => {
  const f = await fixture(t), notice = await firstNotice(f)
  const base = { notificationId: notice.id, attemptId: notice.native?.attemptId ?? 'missing-attempt', nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.id, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now() }
  const emit = fields => f.request('/v1/notifications/native/result', { ...base, ...fields })
  const authorized = await emit({ event: 'authorization', authorization: 'authorized' })
  assert.equal(authorized.status, 200)
  assert.equal(authorized.body.record.native.authorization.state, 'authorized')
  assert.equal(authorized.body.record.delivery, 'unknown')
  assert.equal((await emit({ event: 'submitting' })).body.maySubmit, true)
  const accepted = await emit({ event: 'accepted' })
  assert.equal(accepted.body.record.delivery, 'system-accepted')
  assert.equal(accepted.body.record.native.submission.state, 'accepted')
  assert.equal(accepted.body.record.native.deliveredSnapshot, undefined)
  const delivered = await emit({ event: 'delivered-snapshot', present: true, deliveryDate: f.now() })
  assert.equal(delivered.body.record.native.deliveredSnapshot.present, true)
  const acted = await emit({ event: 'user-action', action: 'default' })
  assert.equal(acted.body.record.native.userAction.action, 'default')
  assert.equal(acted.body.record.humanSeen, 'unknown')
  assert.equal((await emit({ event: 'delivered-snapshot', present: false })).body.record.native.firstDeliveredAt, f.now(), 'later absence cannot erase a historical delivered fact')
  assert.equal((await emit({ event: 'accepted', attemptId: 'old-attempt' })).status, 400)
  assert.equal((await emit({ event: 'accepted', nativeRequestId: base.nativeRequestId + '-other' })).status, 400)
  assert.equal((await emit({ event: 'accepted', observedAt: f.now() + 1 })).status, 400)
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records[0].native.userAction.action, 'default')
})

test('qualification loss cancels a submitted attempt and late system acceptance cannot revive it', async t => {
  const f = await fixture(t), notice = await firstNotice(f)
  const base = { notificationId: notice.id, attemptId: notice.native?.attemptId ?? 'missing-attempt', nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.id, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now() }
  const emit = fields => f.request('/v1/notifications/native/result', { ...base, ...fields })
  assert.equal((await emit({ event: 'submitting' })).body.maySubmit, false, 'authorization is required independently of App eligibility')
  await emit({ event: 'authorization', authorization: 'authorized' })
  assert.equal((await emit({ event: 'submitting' })).body.maySubmit, true)
  await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: false, pluginEnabled: true })
  const late = await emit({ event: 'accepted' })
  assert.equal(late.status, 200)
  assert.equal(late.body.record.status, 'cancelled')
  assert.equal(late.body.record.delivery, 'system-accepted', 'a late native fact is retained without reactivating the notice')
  assert.ok(late.body.record.native.cancelRequestedAt)
  assert.equal((await emit({ event: 'pending-cancelled', pending: false })).body.record.native.pendingCancellation.pending, false)
  await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
})

test('durable failure dedup survives restart and recovery followed by a distinct failure creates a new notice', async t => {
  const f = await fixture(t), notice = await firstNotice(f)
  await f.restart()
  const records = await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })
  assert.equal(records.records.length, 1)
  assert.equal(records.records[0].id, notice.id)
  assert.equal(records.records[0].status, 'cancelled', 'a claim is sealed across core restart')
  assert.equal(records.qualification, 'unconfirmed', 'App eligibility is never restored from disk')
  f.advance(3001)
  await f.core.registerSource({ registrationId: 'chatgpt:native-notification', hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'recovery', outcome: 'succeeded', settledAt: f.now() })
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records.length, 1)
  const second = await firstNotice(f, 'second-episode')
  assert.notEqual(second.id, notice.id)
  assert.notDeepEqual(second.receiptIds, notice.receiptIds)
})

test('project suppression seals only its own attempt without creating a native failure or repeated claim', async t => {
  const f = await fixture(t)
  await f.core.ingest(f.sourceKey, { tasks: ['A', 'B'].map(id => ({ id, lifecycleId: 'one', title: id, status: 'running' })) })
  for (const mainId of ['A', 'B']) await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId, lifecycleId: 'one', channel: 'directory', roundId: 'collect-' + mainId, outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } })
  await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  const notices = await f.core.claimNotifications(f.sourceKey), a = notices.find(value => value.mainId === 'A'), b = notices.find(value => value.mainId === 'B')
  const result = await f.request('/v1/notifications/native/result', { notificationId: a.id, attemptId: a.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + a.id, bundleId: 'com.ghostsentinel.notifications', event: 'suppressed', reason: 'project-disabled', observedAt: f.now() })
  assert.equal(result.status, 200)
  assert.equal(result.body.record.status, 'cancelled')
  assert.equal(result.body.record.native.suppression.reason, 'project-disabled')
  assert.equal(result.body.record.native.errorCode, undefined)
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records.find(value => value.id === b.id).status, 'claimed')
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  assert.equal((await f.core.failureReceipts({ sourceKey: f.sourceKey })).receipts.length, 2, 'suppression does not manufacture a monitoring failure')
})

test('native history expires by the original evidence time and a recovered old receipt cannot renew it', async t => {
  const f = await fixture(t)
  await firstNotice(f)
  f.advance(86_400_001)
  assert.deepEqual((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records, [])
  await f.core.registerSource({ registrationId: 'chatgpt:native-notification', hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'late-recovery', outcome: 'succeeded', settledAt: f.now() })
  assert.deepEqual((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records, [])
})

const nativeBatch = at => {
  const ref = eventId => ({ taskId: 'main', lifecycleId: 'turn', eventId })
  const event = (id, kind, text, data = {}) => ({ id, taskId: 'main', taskLifecycleId: 'turn', kind, text, data, capturedAt: at, occurredAt: at })
  return { tasks: [{ id: 'main', sessionId: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }], events: [event('start', 'status', 'Started', { nativeType: 'task_started', sourceKind: 'chatgpt-native', sourceForm: 'event_msg', runId: 'turn', turnTrigger: 'composer', status: 'running' }), event('user', 'input', 'Inspect this log only', { nativeType: 'item_completed/UserMessage', sourceKind: 'chatgpt-native', sourceForm: 'event_msg/item_completed', runId: 'turn', messageId: 'typed-user', inputOrigin: 'user-entry', inputPhase: 'consumed', inherited: false }), event('output', 'output', 'Changing another unrelated directory')], hierarchyContext: { schemaVersion: 1, revision: 1, sessions: [{ sessionId: 'main' }], activations: [{ taskId: 'main', lifecycleId: 'turn', sessionId: 'main', runId: 'turn', state: 'running', stateRefs: [ref('start')] }], relations: [], instructionBindings: [{ taskId: 'main', lifecycleId: 'turn', inputRef: ref('user'), origin: 'user-entry', phase: 'consumed', originRefs: [ref('start'), ref('user')], ancestorLimitRefs: [] }], coverage: { complete: true, branchGaps: [] } } }
}
async function until(predicate, label) {
  const end = performance.now() + 3000
  while (!await predicate()) { assert.ok(performance.now() < end, label); await delay(3) }
}
async function provider(t, { failFirst = false, attention = false } = {}) {
  let calls = 0
  const server = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk
    response.setHeader('content-type', 'application/json')
    if (++calls === 1 && failFirst) { response.writeHead(503).end('{"error":{"code":"controlled"}}'); return }
    if (request.url === '/v1/chat/completions') response.end(JSON.stringify({ model: 'extractor', choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] }))
    else response.end(JSON.stringify({ model: 'judge', answers: Object.fromEntries(Object.keys(decodeJevRequest(JSON.parse(raw)).questions).map(id => [id, { type: 'choice', choice: attention && id === 'request_drift' ? 'attention' : 'clear' }])) }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  const baseUrl = 'http://127.0.0.1:' + server.address().port
  return { config: { llm: { baseUrl, model: 'extractor' }, jev: { baseUrl, model: 'judge' } }, calls: () => calls }
}

test('an actual analysis HTTP failure that recovers before polling still produces its original immediate monitoring notice', async t => {
  const model = await provider(t, { failFirst: true }), f = await fixture(t, { cooldownMs: 0, leaseMs: 1e9 })
  await f.core.configureAnalysis(model.config)
  await f.core.ingest(f.sourceKey, nativeBatch(f.now()))
  await until(async () => { const actor = (await f.core.snapshot()).tasks[0]; return actor.analysis.summary && !actor.analysis.error && model.calls() >= 3 }, 'actual failed and recovered provider calls settle')
  const receipt = (await f.core.failureReceipts({ sourceKey: f.sourceKey })).receipts[0]
  assert.equal(receipt.state, 'recovered')
  const page = await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })
  assert.equal(page.records.length, 1)
  assert.equal(page.records[0].kind, 'monitoring-failure')
  assert.deepEqual(page.records[0].receiptIds, [receipt.id])
  assert.deepEqual(page.records[0].firstRoundIds, [receipt.firstRoundId])
  assert.equal(page.records[0].mainId, 'main')
  assert.equal(page.records[0].lifecycleId, 'turn')
  await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  assert.equal((await f.core.claimNotifications(f.sourceKey)).length, 1, 'the first explicit failure has no extra notification delay')
})

test('ChatGPT task alerts retain the three-second window and ten-minute successful repeat while a late old attempt cannot overwrite the new one', async t => {
  const model = await provider(t, { attention: true }), f = await fixture(t, { leaseMs: 1e9 })
  await f.core.configureAnalysis(model.config)
  await f.core.ingest(f.sourceKey, nativeBatch(f.now()))
  await until(async () => (await f.core.snapshot()).tasks[0].gs.light === 'red', 'grounded concern updates the panel immediately')
  const qualify = () => f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
  await qualify()
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  f.advance(2999); assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  f.advance(1)
  const first = (await f.core.claimNotifications(f.sourceKey))[0]
  assert.equal(first.kind, 'task-alert')
  assert.deepEqual(first.checkIds, ['request_drift'])
  const emit = (notice, fields) => f.core.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields })
  await emit(first, { event: 'authorization', authorization: 'authorized' }); assert.equal((await emit(first, { event: 'submitting' })).maySubmit, true); await emit(first, { event: 'accepted' })
  f.advance(599999); await qualify(); assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  f.advance(1); await f.core.snapshot(); assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  f.advance(3000); await qualify()
  const second = (await f.core.claimNotifications(f.sourceKey))[0]
  assert.notEqual(second.id, first.id)
  assert.notEqual(second.native.attemptId, first.native.attemptId)
  await emit(second, { event: 'authorization', authorization: 'authorized' }); await emit(second, { event: 'submitting' }); await emit(second, { event: 'accepted' })
  await emit(first, { event: 'accepted' })
  const current = (await f.core.snapshot()).tasks[0].alerts[0].notification
  assert.equal(current.id, second.id)
  assert.equal(current.delivery, 'system-accepted')
  await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: 'turn', title: 'Main', status: 'idle' }] })
  f.advance(600000); await qualify()
  assert.deepEqual(await f.core.claimNotifications(f.sourceKey), [])
  assert.ok((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records.every(value => value.native.cancelRequestedAt), 'ended task requests are marked for pending cancellation')
})

test('native notification and failure-link retention remains capped and exposes omission instead of evicting an active notice silently', async t => {
  const f = await fixture(t, { leaseMs: 1e9 }), maximum = (await f.core.diagnostics()).nativeNotifications.maximumRecords
  await f.core.ingest(f.sourceKey, { tasks: Array.from({ length: maximum + 1 }, (_, index) => ({ id: 'main-' + index, lifecycleId: 'one', title: 'Main', status: 'running' })) })
  for (let index = 0; index <= maximum; index++) await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId: 'main-' + index, lifecycleId: 'one', channel: 'directory', roundId: 'fault-' + index, outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } })
  const retained = (await f.core.diagnostics()).nativeNotifications
  assert.equal(retained.records, maximum)
  assert.ok(retained.failureLinks <= retained.maximumFailureLinks)
  const firstPage = await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey, limit: 100 })
  assert.equal(firstPage.historyGap, true)
  assert.equal(firstPage.hasMore, true)
  assert.equal(firstPage.records.length, 100)
  assert.equal(firstPage.records[0].mainId, 'main-0', 'the active first attempt is retained at capacity')
  assert.equal(JSON.stringify(firstPage).includes('Bearer'), false)
})

test('an unknown or ended main stops its scoped failure notice while a trusted nullable source failure remains eligible', async t => {
  for (const status of ['unknown', 'idle']) await t.test(status, async t => {
    const f = await fixture(t)
    await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: 'turn', title: 'Main', status: 'running' }] })
    for (const mainId of ['main', null]) await f.core.recordCollectionOutcome({ sourceKey: f.sourceKey, mainId, lifecycleId: mainId === null ? null : 'turn', channel: 'directory', roundId: 'fault-' + mainId, outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } })
    await f.core.ingest(f.sourceKey, { tasks: [{ id: 'main', lifecycleId: 'turn', title: 'Main', status }] })
    await f.core.setNativeNotificationEligibility({ sourceKey: f.sourceKey, hostRunId: 'native-host', appRunning: true, pluginEnabled: true })
    const claims = await f.core.claimNotifications(f.sourceKey)
    assert.deepEqual(claims.map(value => value.mainId), [null])
    assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records.find(value => value.mainId === 'main').status, 'cancelled')
  })
})

test('a native result arriving after the evidence retention deadline cannot reinsert the expired attempt', async t => {
  const f = await fixture(t), notice = await firstNotice(f)
  const base = { notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.id, bundleId: 'com.ghostsentinel.notifications' }
  await f.core.recordNativeNotificationResult({ ...base, event: 'authorization', authorization: 'authorized', observedAt: f.now() })
  await f.core.recordNativeNotificationResult({ ...base, event: 'submitting', observedAt: f.now() })
  f.advance(86_400_001)
  await assert.rejects(f.core.recordNativeNotificationResult({ ...base, event: 'accepted', observedAt: f.now() }), error => error.status === 400)
  await f.restart()
  const currentSource = await f.core.registerSource({ registrationId: 'chatgpt:native-notification', hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  assert.deepEqual((await f.core.nativeNotificationRecords({ sourceKey: currentSource })).records, [])
})

test('native qualification leases have a fixed source cap and reclaim expired entries without renewing old attempts', async t => {
  const f = await fixture(t), first = await firstNotice(f), facts = { hostRunId: 'native-host', appRunning: true, pluginEnabled: true }
  for (let index = 1; index < 256; index++) {
    const sourceKey = await f.core.registerSource({ registrationId: 'chatgpt:lease-' + index, hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
    await f.core.setNativeNotificationEligibility({ ...facts, sourceKey })
  }
  const extra = await f.core.registerSource({ registrationId: 'chatgpt:lease-overflow', hostRunId: 'native-host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  await assert.rejects(f.core.setNativeNotificationEligibility({ ...facts, sourceKey: extra }), error => error.status === 400)
  f.advance(15001)
  await f.core.setNativeNotificationEligibility({ ...facts, sourceKey: extra })
  const diagnostics = (await f.core.diagnostics()).nativeNotifications
  assert.equal(diagnostics.eligibilitySources, 256)
  assert.equal(diagnostics.maximumEligibilitySources, 256)
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })).records.find(value => value.id === first.id).status, 'cancelled')
})

test('a burst of distinct recovered failure episodes has bounded per-notice identity lists without dropping the next first failure', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 101; index++) {
    const scope = { sourceKey: f.sourceKey, mainId: null, lifecycleId: null, channel: 'directory', settledAt: f.now() }
    await f.core.recordCollectionOutcome({ ...scope, roundId: 'burst-failure-' + index, outcome: 'failed', error: { code: 'EIO', stage: 'collection' } })
    await f.core.recordCollectionOutcome({ ...scope, roundId: 'burst-recovery-' + index, outcome: 'succeeded' })
  }
  const page = await f.core.nativeNotificationRecords({ sourceKey: f.sourceKey })
  assert.ok(page.records.every(value => value.receiptIds.length <= 100 && value.firstRoundIds.length <= 100))
  assert.equal(page.records.flatMap(value => value.receiptIds).length, 101)
  assert.equal(new Set(page.records.flatMap(value => value.receiptIds)).size, 101)
  assert.equal(page.records.length, 2)
})
