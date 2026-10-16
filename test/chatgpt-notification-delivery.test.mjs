import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { createNativeNotificationIO, notificationPrefix } from '../chatgpt/native-notifications.mjs'
import { createNotificationDelivery } from '../chatgpt/notifications.mjs'

async function fixture(t, { authorization = 'authorized', qualify } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-driver-'))
  let clock = Date.now(), permission = authorization
  const server = await startServer({ dataDir, now: () => clock }), core = await connectCore({ dataDir })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:driver', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true }, calls = [], pending = new Map(), delivered = new Map(), installationId = 'private-driver-installation'
  const native = createNativeNotificationIO({ bundlePath: '/private/tmp/GhostSentinel Notifications.app', installationId, async run(_executable, _args, input) {
    const request = JSON.parse(input); calls.push(request)
    let result = { observedAt: clock }
    if (['status', 'authorize'].includes(request.operation)) {
      if (request.operation === 'authorize') permission = 'authorized'
      result = { ...result, authorization: permission, alertSetting: 'enabled', notificationCenterSetting: 'enabled' }
    } else if (request.operation === 'submit') { pending.set(request.requestId, request.metadata); result = { ...result, requestId: request.requestId, accepted: true, authorization: permission } }
    else if (request.operation === 'cancel') { for (const id of request.requestIds) pending.delete(id); result.pendingIds = request.requestIds.filter(id => pending.has(id)) }
    else result = { ...result, pending: [...pending.keys()].slice(0, 100).map(requestId => ({ requestId })), delivered: [...delivered].map(([requestId, metadata]) => ({ requestId, metadata, deliveryDate: clock })), actions: [], historyGap: false, pendingHasMore: pending.size > 100, deliveredHasMore: false }
    return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result }) }
  } })
  const delivery = createNotificationDelivery({ core, native, installationId, now: () => clock, qualify: qualify ?? (async () => ({ ...facts, projectEnabled: true })) })
  t.after(async () => { await delivery.stop(); await core.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const fail = async (mainId = null) => core.recordCollectionOutcome({ sourceKey, mainId, lifecycleId: mainId === null ? null : 'one', channel: 'directory', roundId: 'collection-' + (mainId ?? 'source'), outcome: 'failed', settledAt: clock, error: { code: 'EIO', stage: 'collection' } })
  return { core, delivery, facts, calls, pending, delivered, fail, prefix: notificationPrefix(installationId), now: () => clock, advance: ms => { clock += ms } }
}

test('background refresh does not request authorization or consume a denied first-failure notice; explicit authorization enables the original attempt', async t => {
  const f = await fixture(t, { authorization: 'denied' })
  await f.fail()
  const before = f.calls.length
  assert.equal(f.delivery.authorization().authorization, 'unknown')
  assert.equal(f.calls.length, before, 'cached authorization reads have zero IO')
  await Promise.all([f.delivery.refresh(f.facts), f.delivery.refresh(f.facts)])
  assert.equal(f.calls.filter(value => value.operation === 'status').length, 1)
  assert.equal(f.calls.filter(value => value.operation === 'authorize').length, 0)
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 0)
  assert.equal((await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records[0].status, 'pending')
  assert.equal(f.delivery.authorization().authorization, 'denied')
  await Promise.all([f.delivery.requestAuthorization(), f.delivery.requestAuthorization()])
  assert.equal(f.calls.filter(value => value.operation === 'authorize').length, 1)
  await f.delivery.refresh(f.facts)
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 1)
  assert.equal(f.delivery.authorization().authorization, 'authorized')
  const record = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records[0]
  assert.equal(record.delivery, 'system-accepted')
  assert.equal(record.native.authorization.state, 'authorized')
  assert.equal(record.humanSeen, 'unknown')
  await f.delivery.refresh(f.facts)
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 1)
})

test('per-notice project recheck suppresses A without disabling B or source-level collection failure and stop drains only owned pending requests', async t => {
  let facts
  const f = await fixture(t, { qualify: async notice => ({ ...facts, projectEnabled: notice.mainId === 'A' ? false : true }) }); facts = f.facts
  await f.core.ingest(f.facts.sourceKey, { tasks: ['A', 'B'].map(id => ({ id, lifecycleId: 'one', title: id, status: 'running' })) })
  await f.fail('A'); await f.fail('B'); await f.fail()
  await f.delivery.refresh(f.facts)
  assert.deepEqual(f.calls.filter(value => value.operation === 'submit').map(value => value.metadata.mainId).sort(), ['B', null].sort())
  const records = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records
  assert.equal(records.find(value => value.mainId === 'A').native.suppression.reason, 'project-disabled')
  await f.delivery.refresh(f.facts)
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 2)
  const stopped = await f.delivery.stop()
  assert.equal(stopped.remainingPending, 0)
  assert.equal(f.pending.size, 0)
  assert.ok(f.calls.filter(value => value.operation === 'cancel').every(value => value.requestIds.every(id => id.startsWith(f.prefix))))
  assert.ok((await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records.filter(value => value.mainId !== 'A').every(value => value.native.pendingCancellation.pending === false))
  assert.equal((await f.delivery.refresh(f.facts)).stopped, true)
})

test('stop aborts only the owned in-flight native submission, seals its late result and releases the single native IO slot before cancellation', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-stop-')), server = await startServer({ dataDir }), core = await connectCore({ dataDir })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:stop', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true }, installationId = 'stop-installation'
  let entered, active = 0, maximumActive = 0, aborted = 0
  const submitted = new Promise(resolve => { entered = resolve })
  const native = createNativeNotificationIO({ bundlePath: '/private/tmp/GhostSentinel Notifications.app', installationId, async run(_file, _args, input, { signal }) {
    const request = JSON.parse(input); active++; maximumActive = Math.max(maximumActive, active)
    try {
      if (request.operation === 'submit') { entered(); await new Promise((_, reject) => signal.addEventListener('abort', () => { aborted++; reject(Object.assign(new Error('owned process aborted'), { code: 'native_unavailable' })) }, { once: true })) }
      const result = request.operation === 'status' ? { authorization: 'authorized', alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: Date.now() } : request.operation === 'cancel' ? { pendingIds: [], observedAt: Date.now() } : { pending: [], delivered: [], actions: [], historyGap: false, observedAt: Date.now() }
      return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result }) }
    } finally { active-- }
  } })
  const delivery = createNotificationDelivery({ core, native, installationId, qualify: async () => ({ ...facts, projectEnabled: true }) })
  t.after(async () => { await delivery.stop(); await core.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  await core.recordCollectionOutcome({ sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'failure', outcome: 'failed', settledAt: Date.now(), error: { code: 'EIO', stage: 'collection' } })
  const refresh = delivery.refresh(facts)
  await submitted
  const stopped = await delivery.stop(); await refresh
  assert.equal(stopped.remainingPending, 0)
  assert.equal(active, 0)
  assert.equal(maximumActive, 1)
  assert.equal(aborted, 1)
  const record = (await core.nativeNotificationRecords({ sourceKey })).records[0]
  assert.equal(record.native.submission.state, 'unknown')
  assert.notEqual(record.status, 'submitted')
  assert.equal(record.humanSeen, 'unknown')
})

test('stop drains an installation backlog larger than four native pages without touching another installation', async t => {
  const f = await fixture(t)
  for (let index = 0; index < 501; index++) f.pending.set(f.prefix + 'orphan-' + index, null)
  const result = await f.delivery.stop()
  assert.equal(result.remainingPending, 0)
  assert.equal(f.pending.size, 0)
  assert.equal(f.calls.filter(value => value.operation === 'cancel').length, 6)
  assert.ok(f.calls.filter(value => value.operation === 'cancel').every(value => value.requestIds.length <= 100 && value.requestIds.every(id => id.startsWith(f.prefix))))
})

test('confirmed cancellation history cannot hide a later pending request or fabricate its cancellation acknowledgement', async t => {
  const f = await fixture(t), tasks = Array.from({ length: 101 }, (_, index) => ({ id: 'cancel-main-' + index, lifecycleId: 'one', title: 'Owned cancellation main', status: 'running' }))
  await f.core.ingest(f.facts.sourceKey, { tasks })
  for (const task of tasks.slice(0, 100)) await f.fail(task.id)
  await f.delivery.refresh(f.facts)
  assert.equal(f.pending.size, 100)
  await f.core.pause(); await f.delivery.refresh(f.facts)
  assert.equal(f.pending.size, 0)
  let records = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey, limit: 100 })).records
  assert.equal(records.length, 100)
  assert.ok(records.every(record => record.native.pendingCancellation.pending === false))
  const previousCancellationCalls = f.calls.filter(value => value.operation === 'cancel').length
  await f.core.resume(); await f.fail(tasks[100].id); await f.delivery.refresh(f.facts)
  assert.equal(f.pending.size, 1)
  const currentId = [...f.pending.keys()][0]
  assert.equal(f.calls.filter(value => value.operation === 'cancel').length, previousCancellationCalls, 'confirmed history must not be cancelled again on ordinary refresh')
  await f.core.pause(); const result = await f.delivery.refresh(f.facts)
  assert.equal(f.pending.size, 0, 'the later pending request must be physically included in cancellation')
  assert.equal(result.cancelled, 1)
  const cancellation = f.calls.filter(value => value.operation === 'cancel').at(-1)
  assert.deepEqual(cancellation.requestIds, [currentId])
  const next = await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey, afterSequence: records.at(-1).sequence, limit: 100 })
  assert.equal(next.records.length, 1)
  assert.equal(next.records[0].native.nativeRequestId, currentId)
  assert.equal(next.records[0].native.pendingCancellation.pending, false)
})

test('coalesced qualification loss aborts an owned submission and processes the latest false facts without re-confirming the source', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-inflight-loss-')), server = await startServer({ dataDir }), core = await connectCore({ dataDir })
  const sourceKey = await core.registerSource({ registrationId: 'chatgpt:inflight', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' })
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true }
  let entered, aborted = 0, submits = 0
  const submitted = new Promise(resolve => { entered = resolve })
  const native = createNativeNotificationIO({ bundlePath: '/private/tmp/GhostSentinel Notifications.app', installationId: 'inflight-installation', async run(_file, _args, input, { signal }) {
    const request = JSON.parse(input)
    if (request.operation === 'submit') { submits++; entered(); await new Promise((_, reject) => signal.addEventListener('abort', () => { aborted++; reject(Object.assign(new Error('owned process aborted'), { code: 'native_unavailable' })) }, { once: true })) }
    const result = request.operation === 'status' ? { authorization: 'authorized', alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: Date.now() } : request.operation === 'cancel' ? { pendingIds: [], observedAt: Date.now() } : { pending: [], delivered: [], actions: [], historyGap: false, observedAt: Date.now() }
    return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result }) }
  } })
  const delivery = createNotificationDelivery({ core, native, installationId: 'inflight-installation', qualify: async () => ({ ...facts, projectEnabled: true }) })
  t.after(async () => { await delivery.stop(); await core.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  await core.recordCollectionOutcome({ sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'first', outcome: 'failed', settledAt: Date.now(), error: { code: 'EIO', stage: 'collection' } })
  const first = delivery.refresh(facts); await submitted
  const cancelled = delivery.refresh({ ...facts, appRunning: false })
  let settled, deadline
  try { settled = await Promise.race([cancelled.then(() => true), new Promise(resolve => { deadline = setTimeout(() => resolve(false), 250) })]) }
  finally { clearTimeout(deadline) }
  assert.equal(settled, true, 'latest disqualification frees the owned IO slot and drains cancellation without waiting for an old native reply')
  await first
  assert.equal(aborted, 1)
  assert.equal(submits, 1)
  const page = await core.nativeNotificationRecords({ sourceKey })
  assert.equal(page.qualification, 'unconfirmed')
  assert.notEqual(page.records[0].status, 'submitted')
  assert.equal(page.records[0].native.pendingCancellation.pending, false)
})

test('temporary per-main uncertainty defers the same unsubmitted receipt attempt until qualification recovers without a new failure episode', async t => {
  let qualified = false, facts
  const f = await fixture(t, { qualify: async () => ({ ...facts, projectEnabled: qualified ? true : null }) }); facts = f.facts
  await f.core.ingest(f.facts.sourceKey, { tasks: [{ id: 'A', lifecycleId: 'one', title: 'A', status: 'running' }] })
  await f.fail('A')
  await f.delivery.refresh(f.facts)
  const first = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records[0]
  assert.equal(first.status, 'pending')
  assert.equal(first.waitReason, 'project-unconfirmed')
  assert.equal(first.native.submission.state, 'not-attempted')
  for (let poll = 0; poll < 3; poll++) {
    await f.delivery.refresh(f.facts)
    const page = await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })
    assert.equal(page.records.length, 1)
    assert.equal(page.records[0].id, first.id)
    assert.equal(page.records[0].native.attemptId, first.native.attemptId)
    assert.equal(page.records[0].createdAt, first.createdAt)
    assert.equal(f.calls.filter(value => value.operation === 'submit').length, 0)
  }
  qualified = true
  await f.delivery.refresh(f.facts)
  await f.delivery.refresh(f.facts)
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 1)
  const page = await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })
  assert.equal(page.records.length, 1)
  assert.equal(page.records[0].native.attemptId, first.native.attemptId)
  assert.equal(page.records[0].delivery, 'system-accepted')
  assert.equal((await f.core.failureReceipts({ sourceKey: f.facts.sourceKey })).receipts.length, 1)
})

test('a transient qualification read failure retains the original first failure until the same main can be qualified', async t => {
  let unavailable = true, facts
  const f = await fixture(t, { qualify: async () => { if (unavailable) throw new Error('controlled metadata read unavailable'); return { ...facts, projectEnabled: true } } }); facts = f.facts
  await f.core.ingest(f.facts.sourceKey, { tasks: [{ id: 'A', lifecycleId: 'one', title: 'A', status: 'running' }] }); await f.fail('A')
  await f.delivery.refresh(f.facts)
  const first = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records[0]
  assert.equal(first.status, 'pending'); assert.equal(first.waitReason, 'qualification-unconfirmed')
  unavailable = false; await f.delivery.refresh(f.facts)
  const current = (await f.core.nativeNotificationRecords({ sourceKey: f.facts.sourceKey })).records[0]
  assert.equal(current.native.attemptId, first.native.attemptId)
  assert.equal(current.delivery, 'system-accepted')
  assert.equal(f.calls.filter(value => value.operation === 'submit').length, 1)
})
