import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { createNotificationDelivery } from '../chatgpt/notifications.mjs';
import { notificationPrefix } from '../chatgpt/native-notifications.mjs';
import { setTimeout as delay } from 'node:timers/promises';

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-notification-records-'));
  let clock = Date.now(), server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  return {
    get client() { return client; }, get server() { return server; }, now: () => clock, advance(ms) { clock += ms; },
    async restart() { await client.close(); await server.close(); server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }); client = await connectCore({ dataDir }); },
    async source(registrationId, hostRunId = 'host') { return client.registerSource({ registrationId, hostRunId, name: registrationId, policy: 'chatgpt-main/1' }); },
    async failure(sourceKey, roundId, mainId = null, lifecycleId = null) { return client.recordCollectionOutcome({ sourceKey, mainId, lifecycleId, roundId, channel: 'directory', outcome: 'failed', settledAt: clock, error: { code: 'EIO', stage: 'collection' } }); },
  };
}

test('one actual notification and native receipt are readable through the installation-only public HTTP and client', async t => {
  const f = await fixture(t), sourceKey = await f.source('chatgpt:records');
  await f.client.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: 'life', title: 'Own named chat', status: 'running' }] });
  await f.failure(sourceKey, 'failure', 'main', 'life');
  await f.client.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true });
  const [notice] = await f.client.claimNotifications(sourceKey);
  const emit = fields => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await emit({ event: 'authorization', authorization: 'authorized' });
  assert.equal((await emit({ event: 'submitting' })).maySubmit, true);
  await emit({ event: 'accepted' });
  const response = await fetch(f.server.url + '/v1/notifications/installations/records?registrationId=chatgpt%3Arecords', { headers: { authorization: `Bearer ${f.server.token}` } });
  assert.equal(response.status, 200);
  const page = await response.json();
  assert.equal(page.registrationId, 'chatgpt:records');
  assert.equal(page.maximumRecords, 1000); assert.equal(page.retentionMs, 86400000);
  assert.equal(page.hasMore, false); assert.equal(page.nextCursor, null); assert.equal(page.historyGap, false);
  assert.equal(page.records.length, 1);
  const record = page.records[0];
  assert.equal(record.id, notice.id); assert.equal(record.registrationId, 'chatgpt:records');
  assert.equal(record.sourceKey, sourceKey); assert.equal(record.mainId, 'main'); assert.equal(record.lifecycleId, 'life');
  assert.equal(record.chatTitle, 'Own named chat'); assert.equal(record.kind, 'monitoring-failure');
  assert.equal(record.status, 'submitted'); assert.equal(record.delivery, 'system-accepted'); assert.equal(record.humanSeen, 'unknown');
  assert.equal(record.native.submission.state, 'accepted');
  assert.deepEqual(await f.client.installationNotificationRecords('chatgpt:records', {}), page);
  assert.deepEqual((await f.client.installationNotificationRecords('chatgpt:other', {})).records, []);
});

async function allRecords(client, registrationId) {
  const records = []; let cursor;
  for (let page = 0; page < 12; page++) {
    const result = await client.installationNotificationRecords(registrationId, { ...(cursor ? { cursor } : {}), limit: 100 });
    records.push(...result.records);
    if (!result.hasMore) return { ...result, records };
    cursor = result.nextCursor;
  }
  assert.fail('the notification record page budget was exceeded');
}

test('999, 1000 and 1001 actual notifications apply a real per-installation cap without sharing another installation capacity or gaps', async t => {
  const f = await fixture(t), own = await f.source('chatgpt:own-cap'), other = await f.source('chatgpt:other-cap');
  for (let index = 0; index < 999; index++) { await f.failure(own, 'own-' + index, 'main-' + index); f.advance(1); }
  assert.equal((await allRecords(f.client, 'chatgpt:own-cap')).records.length, 999);
  await f.failure(own, 'own-999', 'main-999'); f.advance(1);
  assert.equal((await allRecords(f.client, 'chatgpt:own-cap')).records.length, 1000);
  const first = (await allRecords(f.client, 'chatgpt:own-cap')).records.at(-1).id;
  await f.failure(own, 'own-1000', 'main-1000');
  const capped = await allRecords(f.client, 'chatgpt:own-cap');
  assert.equal(capped.records.length, 1000);
  assert.equal(capped.historyGap, true);
  assert.ok(!capped.records.some(record => record.id === first));
  assert.equal(capped.records[0].mainId, 'main-1000');
  assert.equal((await f.client.installationNotificationRecords('chatgpt:other-cap', {})).historyGap, false);
  for (let index = 0; index < 1000; index++) { f.advance(1); await f.failure(other, 'other-' + index, 'other-main-' + index); }
  const independent = await allRecords(f.client, 'chatgpt:other-cap');
  assert.equal(independent.records.length, 1000); assert.equal(independent.historyGap, false);
  assert.equal((await allRecords(f.client, 'chatgpt:own-cap')).records.length, 1000);
});

test('another installation evicting a global auxiliary link cannot mark a complete one-record installation history as missing', async t => {
  const f = await fixture(t), a = await f.source('chatgpt:gap-a'), b = await f.source('chatgpt:gap-b');
  await f.failure(a, 'a-only');
  const original = await f.client.installationNotificationRecords('chatgpt:gap-a', {});
  assert.equal(original.records.length, 1); assert.equal(original.historyGap, false);
  await f.client.setNativeNotificationEligibility({ sourceKey: a, hostRunId: 'host', appRunning: true, pluginEnabled: true });
  const [notice] = await f.client.claimNotifications(a);
  const emit = fields => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: notificationPrefix('gap-a') + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await emit({ event: 'authorization', authorization: 'authorized' }); await emit({ event: 'submitting' });
  for (let index = 0; index < 1024; index++) { f.advance(1); await f.failure(b, 'b-' + index, 'b-main-' + index); }
  const retained = await f.client.installationNotificationRecords('chatgpt:gap-a', {});
  assert.equal(retained.records.length, 1); assert.equal(retained.records[0].id, original.records[0].id);
  assert.equal(retained.historyGap, false, 'B auxiliary-link churn does not remove any A notification row');
  const trimmed = await allRecords(f.client, 'chatgpt:gap-b');
  assert.equal(trimmed.records.length, 1000); assert.equal(trimmed.historyGap, true, 'B actual notification trimming still reports its own gap');
  const diagnostic = (await f.client.diagnostics()).nativeNotifications;
  assert.equal(diagnostic.failureLinks, diagnostic.maximumFailureLinks);
  assert.equal(diagnostic.historyGap, true, 'the original global auxiliary omission remains visible in its own diagnostic');
  await emit({ event: 'accepted' });
  const updated = await f.client.installationNotificationRecords('chatgpt:gap-a', {});
  assert.equal(updated.records[0].status, 'submitted'); assert.equal(updated.historyGap, false);
  await f.restart();
  const reopened = await f.client.installationNotificationRecords('chatgpt:gap-a', {});
  assert.equal(reopened.records.length, 1); assert.equal(reopened.historyGap, false);
  assert.equal((await f.client.diagnostics()).nativeNotifications.historyGap, true);
});

test('opaque installation cursors keep createdAt/id order across late facts and restart and reject tampering or another installation', async t => {
  const f = await fixture(t), source = await f.source('chatgpt:paged');
  for (let index = 0; index < 5; index++) await f.failure(source, 'page-' + index, 'main-' + index);
  const ordered = (await f.client.installationNotificationRecords('chatgpt:paged', { limit: 100 })).records.map(record => record.id);
  const first = await f.client.installationNotificationRecords('chatgpt:paged', { limit: 2 });
  assert.deepEqual(first.records.map(record => record.id), ordered.slice(0, 2));
  assert.ok(first.nextCursor && first.nextCursor.length <= 2000);
  await f.client.setNativeNotificationEligibility({ sourceKey: source, hostRunId: 'host', appRunning: true, pluginEnabled: true });
  const claims = await f.client.claimNotifications(source), old = claims.find(notice => notice.id === ordered.at(-1));
  await f.client.recordNativeNotificationResult({ notificationId: old.id, attemptId: old.native.attemptId, nativeRequestId: notificationPrefix('paged') + old.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), event: 'authorization', authorization: 'authorized' });
  const second = await f.client.installationNotificationRecords('chatgpt:paged', { limit: 2, cursor: first.nextCursor });
  assert.deepEqual(second.records.map(record => record.id), ordered.slice(2, 4));
  await f.restart();
  assert.deepEqual((await f.client.installationNotificationRecords('chatgpt:paged', { limit: 2, cursor: second.nextCursor })).records.map(record => record.id), ordered.slice(4));
  const tampered = first.nextCursor.slice(0, -1) + (first.nextCursor.at(-1) === '0' ? '1' : '0');
  await assert.rejects(f.client.installationNotificationRecords('chatgpt:paged', { cursor: tampered }), error => error.status === 400);
  await assert.rejects(f.client.installationNotificationRecords('chatgpt:another', { cursor: first.nextCursor }), error => error.status === 400);
  await assert.rejects(async () => f.client.installationNotificationRecords('chatgpt:paged', { registrationId: 'chatgpt:another' }), /unsupported/);
  for (const limit of [0, 101, 1.5]) {
    await assert.rejects(f.client.installationNotificationRecords('chatgpt:paged', { limit }), /limit must/);
    assert.equal((await fetch(f.server.url + '/v1/notifications/installations/records?registrationId=chatgpt%3Apaged&limit=' + limit, { headers: { authorization: `Bearer ${f.server.token}` } })).status, 400);
  }
});

test('24-hour query expiry hides old native history, rejects late facts and drains only its minimal cancellation fence through existing IO', async t => {
  const f = await fixture(t), sourceKey = await f.source('chatgpt:expired');
  await f.failure(sourceKey, 'expiry');
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true };
  await f.client.setNativeNotificationEligibility(facts);
  const [notice] = await f.client.claimNotifications(sourceKey), requestId = notificationPrefix('expired') + notice.native.attemptId;
  const emit = fields => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: requestId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await emit({ event: 'authorization', authorization: 'authorized' }); await emit({ event: 'submitting' });
  f.advance(86400000 - 1);
  assert.equal((await f.client.installationNotificationRecords('chatgpt:expired', {})).records.length, 1);
  f.advance(1);
  const expired = await f.client.installationNotificationRecords('chatgpt:expired', {});
  assert.deepEqual(expired.records, []); assert.equal(expired.historyGap, true);
  await assert.rejects(emit({ event: 'accepted' }), /pruned/);
  assert.equal((await f.client.diagnostics()).nativeNotifications.cancellationFences, 1);
  const pending = new Set([requestId, notificationPrefix('other') + 'keep']), cancelled = [];
  const delivery = createNotificationDelivery({ core: f.client, installationId: 'expired', now: f.now, qualify: async () => ({ ...facts, projectEnabled: true }), native: {
    async settings() { return { authorization: 'authorized', observedAt: f.now() }; },
    async snapshot() { return { pending: [...pending].map(requestId => ({ requestId })), delivered: [], actions: [], observedAt: f.now() }; },
    async cancel(ids) { cancelled.push(...ids); for (const id of ids) pending.delete(id); return { pendingIds: [], observedAt: f.now() }; },
    async submit() { assert.fail('reading or trimming history cannot submit a notification'); }, close() {},
  } });
  try { await delivery.refresh(facts); assert.deepEqual(cancelled, [requestId]); assert.equal((await f.client.diagnostics()).nativeNotifications.cancellationFences, 0); }
  finally { await delivery.stop(); }
  assert.equal(pending.size, 1, 'another installation native request is untouched');
  await assert.rejects(emit({ event: 'accepted' }), /match/);
  assert.deepEqual((await f.client.installationNotificationRecords('chatgpt:expired', {})).records, []);
  assert.equal((await f.client.installationNotificationRecords('chatgpt:other', {})).historyGap, false);
});

test('native monitoring facts and the existing legacy task queue share one installation cap', async t => {
  const f = await fixture(t), registrationId = 'combined-records', nativeSource = await f.source(registrationId);
  for (let index = 0; index < 499; index++) { await f.failure(nativeSource, 'before-' + index, 'before-main-' + index); f.advance(1); }
  const legacy = await f.client.registerSource({ registrationId, hostRunId: 'legacy-host', name: 'Legacy task producer' });
  const capturedAt = f.now() - 300001;
  await f.client.ingest(legacy, { tasks: [{ id: 'waiting-agent', sessionId: 'legacy-main', lifecycleId: 'life', title: 'Waiting DSH chat', status: 'running' }], events: [
    { id: 'input', kind: 'input', text: 'Observe this pending tool' }, { id: 'tool', kind: 'tool-start', text: 'Pending tool', data: { callId: 'own-tool', toolName: 'fixed_wait' } },
  ].map(event => ({ ...event, taskId: 'waiting-agent', taskLifecycleId: 'life', capturedAt, occurredAt: capturedAt })) });
  const deadline = Date.now() + 3000;
  while (!(await allRecords(f.client, registrationId)).records.some(record => record.kind === 'task-alert')) { assert.ok(Date.now() < deadline, 'the real unconfigured local wait produces a legacy task notice'); await delay(3); }
  for (let index = 0; index < 501; index++) { f.advance(1); await f.failure(nativeSource, 'after-' + index, 'after-main-' + index); }
  const result = await allRecords(f.client, registrationId);
  assert.equal(result.records.length, 1000); assert.equal(result.historyGap, true);
  assert.equal(result.records.filter(record => record.kind === 'task-alert').length, 1);
  assert.equal(result.records.filter(record => record.kind === 'monitoring-failure').length, 999);
  const task = result.records.find(record => record.kind === 'task-alert');
  assert.equal(task.mainId, 'legacy-main'); assert.equal(task.chatTitle, 'Waiting DSH chat'); assert.equal(task.native, null);
  f.advance(86400000);
  await assert.rejects(f.client.ackNotification(task.id, { status: 'submitted' }), /Unknown notification/);
  assert.deepEqual((await f.client.installationNotificationRecords(registrationId, {})).records, []);
});

test('cropped native attempts stay outside visible history, remain bounded and release their existing cancellation slots', async t => {
  const f = await fixture(t), sourceKey = await f.source('chatgpt:handles'), prefix = notificationPrefix('handles');
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true };
  await f.failure(sourceKey, 'first', 'first'); await f.client.setNativeNotificationEligibility(facts);
  const [first] = await f.client.claimNotifications(sourceKey);
  const emit = (notice, fields) => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: prefix + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await emit(first, { event: 'authorization', authorization: 'authorized' }); await emit(first, { event: 'submitting' });
  for (let index = 0; index < 1000; index++) { f.advance(1); await f.failure(sourceKey, 'next-' + index, 'next-main-' + index); }
  assert.equal((await allRecords(f.client, 'chatgpt:handles')).records.length, 1000);
  await assert.rejects(emit(first, { event: 'accepted' }), /pruned/);
  const claims = await f.client.claimNotifications(sourceKey); assert.equal(claims.length, 1000);
  let admitted = 0, held;
  for (const notice of claims) {
    await emit(notice, { event: 'authorization', authorization: 'authorized' });
    if ((await emit(notice, { event: 'submitting' })).maySubmit) admitted++; else held = notice;
  }
  assert.equal(admitted, 999, 'the one cropped unfinished attempt still occupies its cancellation slot');
  assert.ok(held); assert.equal((await f.client.diagnostics()).nativeNotifications.cancellationFences, 1);
  await emit(first, { event: 'pending-cancelled', pending: false });
  assert.equal((await emit(held, { event: 'submitting' })).maySubmit, true);
  const pending = new Set(claims.map(notice => prefix + notice.native.attemptId));
  const delivery = createNotificationDelivery({ core: f.client, installationId: 'handles', now: f.now, qualify: async () => ({ ...facts, projectEnabled: true }), native: {
    async settings() { return { authorization: 'authorized', observedAt: f.now() }; },
    async snapshot() { return { pending: [...pending].slice(0, 100).map(requestId => ({ requestId })), pendingHasMore: pending.size > 100, delivered: [], actions: [], observedAt: f.now() }; },
    async cancel(ids) { for (const id of ids) pending.delete(id); return { pendingIds: [], observedAt: f.now() }; }, close() {},
  } });
  try {
    await delivery.refresh({ ...facts, pluginEnabled: false });
    assert.equal(pending.size, 0);
    assert.equal((await f.client.diagnostics()).nativeNotifications.cancellationFences, 0);
  } finally { await delivery.stop(); }
});

test('stopping and purging one installation preserves another installation records and gap state', async t => {
  const f = await fixture(t), own = await f.source('chatgpt:stopped'), other = await f.source('chatgpt:other');
  await f.failure(own, 'own'); await f.failure(other, 'other');
  const before = await f.client.installationNotificationRecords('chatgpt:other', {});
  await f.client.setNativeNotificationEligibility({ sourceKey: own, hostRunId: 'host', appRunning: true, pluginEnabled: false });
  await f.client.unregisterSource(own, 'own installation disabled');
  const stopped = await f.client.installationNotificationRecords('chatgpt:stopped', {});
  assert.equal(stopped.records.length, 1); assert.equal(stopped.records[0].status, 'cancelled');
  assert.equal((await f.client.purgeChatGPTInstallation({ installationId: 'stopped' })).status, 'complete');
  const cleared = await f.client.installationNotificationRecords('chatgpt:stopped', {});
  assert.deepEqual(cleared.records, []); assert.equal(cleared.historyGap, false);
  assert.deepEqual(await f.client.installationNotificationRecords('chatgpt:other', {}), before);
});

test('a confirmed prior delivery never becomes an unfinished cancellation handle after its visible snapshot disappears', async t => {
  const f = await fixture(t), sourceKey = await f.source('chatgpt:delivered');
  await f.failure(sourceKey, 'delivered');
  await f.client.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true });
  const [notice] = await f.client.claimNotifications(sourceKey);
  const emit = fields => f.client.recordNativeNotificationResult({ notificationId: notice.id, attemptId: notice.native.attemptId, nativeRequestId: notificationPrefix('delivered') + notice.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await emit({ event: 'authorization', authorization: 'authorized' }); await emit({ event: 'submitting' }); await emit({ event: 'accepted' });
  await emit({ event: 'delivered-snapshot', present: true }); f.advance(1); await emit({ event: 'delivered-snapshot', present: false });
  f.advance(86400000);
  assert.deepEqual((await f.client.installationNotificationRecords('chatgpt:delivered', {})).records, []);
  assert.equal((await f.client.diagnostics()).nativeNotifications.cancellationFences, 0);
});
