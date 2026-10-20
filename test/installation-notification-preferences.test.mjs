import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { withModelSummary } from './helpers/model-summary.mjs';
import { DatabaseSync } from 'node:sqlite';
import { createNativeNotificationIO } from '../chatgpt/native-notifications.mjs';
import { createNotificationDelivery } from '../chatgpt/notifications.mjs';

async function until(predicate) { const end = Date.now() + 3000; while (!await predicate()) { assert.ok(Date.now() < end, 'expected public notification state'); await delay(3); } }
async function fixture(t) {
  let clock = Date.now();
  const model = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(request.url === '/v1/chat/completions'
      ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) }));
  });
  await new Promise(resolve => model.listen(0, '127.0.0.1', resolve));
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-preference-delivery-'));
  let server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); model.closeAllConnections(); await new Promise(resolve => model.close(resolve)); await rm(dataDir, { recursive: true, force: true }); });
  const baseUrl = `http://127.0.0.1:${model.address().port}`;
  const observe = async registrationId => {
    await client.configureInstallationAnalysis(registrationId, { llm: { baseUrl, model: 'summary' }, jev: { baseUrl, model: 'judge' } }, (await client.installationAnalysisConfig(registrationId)).revision);
    const source = await client.registerSource({ registrationId, hostRunId: 'host', name: registrationId });
    await client.ingest(source, { tasks: [{ id: registrationId, lifecycleId: 'life', title: registrationId, status: 'running' }], events: ['input', 'output'].map(kind => ({ id: kind, taskId: registrationId, taskLifecycleId: 'life', kind, text: kind === 'input' ? 'Inspect only' : 'Changing another directory', capturedAt: clock, occurredAt: null })) });
    await until(async () => (await client.snapshot()).tasks.find(task => task.id === registrationId).alerts.some(alert => alert.status === 'attention'));
    return source;
  };
  return { get client() { return client; }, get server() { return server; }, dataDir, observe, now: () => clock, advance(ms) { clock += ms; }, async restart() { await client.close(); await server.close(); server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }); client = await connectCore({ dataDir }); } };
}

test('notification preferences default on and persist single-category writes independently without a source', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notification-preferences-'));
  let server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const http = async (path, body) => {
    const response = await fetch(server.url + path, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
    return { status: response.status, body: await response.json() };
  };
  const path = '/v1/notifications/installations/preferences';
  const initial = await http(path + '?registrationId=own');
  assert.equal(initial.status, 200);
  assert.deepEqual(initial.body.preferences, { 'task-alert': true, 'monitoring-failure': true });
  const other = await client.installationNotificationPreferences('other');
  const off = await client.setInstallationNotificationPreference('own', 'task-alert', false);
  assert.deepEqual(off.preferences, { 'task-alert': false, 'monitoring-failure': true });
  assert.notEqual(off.revision, initial.body.revision);
  const both = await client.setInstallationNotificationPreference('own', 'monitoring-failure', false);
  assert.deepEqual(both.preferences, { 'task-alert': false, 'monitoring-failure': false });
  assert.deepEqual(await client.setInstallationNotificationPreference('own', 'monitoring-failure', false), both);
  assert.deepEqual(await client.installationNotificationPreferences('other'), other);
  for (const body of [{ registrationId: 'own', kind: 'unknown', enabled: false }, { registrationId: 'own', kind: 'task-alert', enabled: 'false' }, { registrationId: 'own', kind: 'task-alert', enabled: true, taskId: 'wrong-scope' }]) assert.equal((await http(path, body)).status, 400);
  assert.deepEqual(await client.installationNotificationPreferences('own'), both);
  await client.close(); await server.close();
  server = await startServer({ dataDir }); client = await connectCore({ dataDir });
  assert.deepEqual(await client.installationNotificationPreferences('own'), both);
  assert.deepEqual((await client.installationNotificationPreferences('other')).preferences, { 'task-alert': true, 'monitoring-failure': true });
});

test('OFF seals a claimed task alert, blocks its late begin and ACK, and ON does not replay its retained evidence', async t => {
  const f = await fixture(t), own = await f.observe('own'), other = await f.observe('other');
  f.advance(3000);
  const [old] = await f.client.claimNotifications(own), [kept] = await f.client.claimNotifications(other);
  await f.client.setInstallationNotificationPreference('own', 'task-alert', false);
  const response = await fetch(f.server.url + '/v1/notifications/submitting', { method: 'POST', headers: { authorization: `Bearer ${f.server.token}`, 'content-type': 'application/json' }, body: JSON.stringify({ sourceKey: own, id: old.id }) });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { id: old.id, maySubmit: false });
  assert.deepEqual(await f.client.beginNotificationSubmission(other, kept.id), { id: kept.id, maySubmit: true });
  assert.equal((await f.client.ackNotification(old.id, { status: 'submitted' })).status, 'cancelled');
  await f.client.setInstallationNotificationPreference('own', 'task-alert', true);
  f.advance(20 * 60000);
  assert.deepEqual(await f.client.claimNotifications(own), []);
  const task = (await f.client.snapshot()).tasks.find(task => task.id === 'own');
  assert.equal(task.alerts[0].status, 'attention', 'preferences never clear the panel concern');
  await f.client.ingest(own, { events: [{ id: 'new-output', taskId: 'own', taskLifecycleId: 'life', kind: 'output', text: 'New out-of-scope change', capturedAt: f.now(), occurredAt: null }] });
  await until(async () => (await f.client.snapshot()).tasks.find(task => task.id === 'own').analysis.completedAt === f.now());
  f.advance(3000);
  const [fresh] = await f.client.claimNotifications(own);
  assert.ok(fresh);
  assert.notEqual(fresh.id, old.id);
});

for (const [reopenMinute, dueMinute] of [[8, 10], [12, 20]]) test(`a successful alert reopened at minute ${reopenMinute} resumes at its original future minute ${dueMinute}`, async t => {
  const f = await fixture(t), source = await f.observe('repeat');
  f.advance(3000);
  const [first] = await f.client.claimNotifications(source);
  assert.equal((await f.client.beginNotificationSubmission(source, first.id)).maySubmit, true);
  assert.equal((await f.client.beginNotificationSubmission(source, first.id)).maySubmit, false, 'submission reservation is one-shot');
  await f.client.ackNotification(first.id, { status: 'submitted' });
  const base = f.now();
  f.advance(5 * 60000);
  await f.client.setInstallationNotificationPreference('repeat', 'task-alert', false);
  f.advance((reopenMinute - 5) * 60000);
  await f.client.setInstallationNotificationPreference('repeat', 'task-alert', true);
  assert.deepEqual(await f.client.claimNotifications(source), []);
  f.advance(base + dueMinute * 60000 - f.now() - 1);
  assert.deepEqual(await f.client.claimNotifications(source), []);
  f.advance(1);
  assert.deepEqual(await f.client.claimNotifications(source), [], 'the normal three-second merge window starts at the future due point');
  f.advance(3000);
  const [next] = await f.client.claimNotifications(source);
  assert.ok(next);
  assert.notEqual(next.id, first.id);
});

test('preference save rolls back configuration, task suppression and claimed delivery together on storage failure', async t => {
  const f = await fixture(t), source = await f.observe('atomic');
  f.advance(3000);
  const [notice] = await f.client.claimNotifications(source), before = await f.client.installationNotificationPreferences('atomic');
  const db = new DatabaseSync(join(f.dataDir, 'observations.sqlite'));
  try {
    db.exec("CREATE TRIGGER reject_notice_update BEFORE UPDATE ON records WHEN NEW.collection='notifications' BEGIN SELECT RAISE(FAIL,'own injected notification write failure'); END");
    await assert.rejects(f.client.setInstallationNotificationPreference('atomic', 'task-alert', false), /own injected notification write failure/);
  } finally { db.exec('DROP TRIGGER reject_notice_update'); db.close(); }
  assert.deepEqual(await f.client.installationNotificationPreferences('atomic'), before);
  assert.equal((await f.client.snapshot()).tasks[0].alerts[0].notification.status, 'claimed');
  assert.equal((await f.client.beginNotificationSubmission(source, notice.id)).maySubmit, true);
  await f.client.setInstallationNotificationPreference('atomic', 'task-alert', false);
  assert.equal((await f.client.ackNotification(notice.id, { status: 'submitted' })).status, 'cancelled', 'an earlier reservation cannot revive after OFF');
});

test('native monitoring OFF seals late acceptance and closed-period episodes across restart without touching another installation', async t => {
  const f = await fixture(t);
  const own = await f.client.registerSource({ registrationId: 'chatgpt:native-prefs', hostRunId: 'host', name: 'Own', policy: 'chatgpt-main/1' });
  const other = await f.client.registerSource({ registrationId: 'chatgpt:other', hostRunId: 'host', name: 'Other', policy: 'chatgpt-main/1' });
  const qualify = sourceKey => f.client.setNativeNotificationEligibility({ sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true });
  const outcome = (sourceKey, roundId, result = 'failed') => f.client.recordCollectionOutcome({ sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId, outcome: result, settledAt: f.now(), ...(result === 'failed' ? { error: { code: 'EIO', stage: 'collection' } } : {}) });
  await outcome(own, 'old'); await outcome(other, 'other');
  await qualify(own); await qualify(other);
  const [old] = await f.client.claimNotifications(own);
  const packet = fields => f.client.recordNativeNotificationResult({ notificationId: old.id, attemptId: old.native.attemptId, nativeRequestId: 'ghostsentinel:0123456789abcdef01234567:' + old.native.attemptId, bundleId: 'com.ghostsentinel.notifications', observedAt: f.now(), ...fields });
  await packet({ event: 'authorization', authorization: 'authorized' });
  assert.equal((await packet({ event: 'submitting' })).maySubmit, true);
  const before = await f.client.installationNotificationPreferences('chatgpt:native-prefs'), beforeSequence = (await f.client.nativeNotificationRecords({ sourceKey: own })).nextSequence;
  const db = new DatabaseSync(join(f.dataDir, 'observations.sqlite'));
  try {
    db.exec("CREATE TRIGGER reject_native_metadata BEFORE UPDATE ON records WHEN NEW.collection='settings' AND NEW.key='native-notification-metadata' BEGIN SELECT RAISE(FAIL,'own injected native metadata failure'); END");
    await assert.rejects(f.client.setInstallationNotificationPreference('chatgpt:native-prefs', 'monitoring-failure', false), /own injected native metadata failure/);
  } finally { db.exec('DROP TRIGGER reject_native_metadata'); db.close(); }
  assert.deepEqual(await f.client.installationNotificationPreferences('chatgpt:native-prefs'), before);
  assert.equal((await f.client.nativeNotificationRecords({ sourceKey: own })).records[0].status, 'claimed');
  await f.client.setInstallationNotificationPreference('chatgpt:native-prefs', 'monitoring-failure', false);
  const offRecords = await f.client.nativeNotificationRecords({ sourceKey: own, afterSequence: beforeSequence });
  assert.equal(offRecords.records[0].status, 'cancelled');
  assert.ok(offRecords.records[0].native.cancelRequestedAt);
  assert.equal((await f.client.claimNotifications(other)).length, 1);
  await outcome(own, 'recovery', 'succeeded'); f.advance(1); await outcome(own, 'muted-new-episode');
  await f.client.setInstallationNotificationPreference('chatgpt:native-prefs', 'monitoring-failure', true);
  const late = await packet({ event: 'accepted' });
  assert.equal(late.record.status, 'cancelled');
  assert.equal(late.record.delivery, 'system-accepted', 'late external facts remain facts without recovering eligibility');
  assert.deepEqual(await f.client.claimNotifications(own), []);
  await f.restart(); await qualify(own);
  assert.deepEqual(await f.client.claimNotifications(own), []);
  assert.equal((await f.client.failureReceipts({ sourceKey: own })).receipts.length, 2);
  await outcome(own, 'future-recovery', 'succeeded'); f.advance(1); await outcome(own, 'future-episode');
  const [future] = await f.client.claimNotifications(own);
  assert.ok(future);
  assert.notEqual(future.id, old.id);
});

test('full installation purge clears its durable notification preferences even before collection while leaving another installation intact', async t => {
  const f = await fixture(t);
  await f.client.setInstallationNotificationPreference('chatgpt:unused', 'task-alert', false);
  const other = await f.client.setInstallationNotificationPreference('chatgpt:other', 'monitoring-failure', false);
  assert.equal((await f.client.purgeChatGPTInstallation({ installationId: 'unused' })).status, 'complete');
  assert.deepEqual((await f.client.installationNotificationPreferences('chatgpt:unused')).preferences, { 'task-alert': true, 'monitoring-failure': true });
  assert.deepEqual(await f.client.installationNotificationPreferences('chatgpt:other'), other);
});

for (const disabledKind of ['monitoring-failure', 'task-alert']) test(`the native single IO slot cancels only an in-flight ${disabledKind} preference class`, { timeout: 5000 }, async t => {
  const f = await fixture(t), sourceKey = await f.client.registerSource({ registrationId: 'chatgpt:driver', hostRunId: 'host', name: 'Own', policy: 'chatgpt-main/1' });
  let entered, release, active = 0, maximumActive = 0, aborted = 0;
  const submitting = new Promise(resolve => { entered = resolve; });
  const native = createNativeNotificationIO({ bundlePath: '/private/tmp/Own Notification Fixture.app', installationId: 'driver', async run(_file, _args, input, { signal }) {
    const request = JSON.parse(input); active++; maximumActive = Math.max(maximumActive, active);
    try {
      if (request.operation === 'submit') {
        entered();
        await new Promise((resolve, reject) => { release = resolve; signal.addEventListener('abort', () => { aborted++; reject(Object.assign(new Error('own fixture cancelled'), { code: 'native_unavailable' })); }, { once: true }); });
      }
      const result = request.operation === 'status' ? { authorization: 'authorized', alertSetting: 'enabled', notificationCenterSetting: 'enabled', observedAt: f.now() }
        : request.operation === 'submit' ? { accepted: true, requestId: request.requestId, authorization: 'authorized', observedAt: f.now() }
          : request.operation === 'cancel' ? { pendingIds: [], observedAt: f.now() } : { pending: [], delivered: [], actions: [], historyGap: false, observedAt: f.now() };
      return { stdout: JSON.stringify({ schemaVersion: 1, bundleId: 'com.ghostsentinel.notifications', operation: request.operation, ok: true, result }) };
    } finally { active--; }
  } });
  const facts = { sourceKey, hostRunId: 'host', appRunning: true, pluginEnabled: true };
  const delivery = createNotificationDelivery({ core: f.client, native, installationId: 'driver', now: f.now, qualify: async () => ({ ...facts, projectEnabled: true }) });
  t.after(() => delivery.stop());
  await f.client.recordCollectionOutcome({ sourceKey, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'driver-failure', outcome: 'failed', settledAt: f.now(), error: { code: 'EIO', stage: 'collection' } });
  const refresh = delivery.refresh(facts);
  await submitting;
  await f.client.setInstallationNotificationPreference('chatgpt:driver', disabledKind, false);
  const changing = delivery.preferencesChanged(disabledKind);
  if (disabledKind === 'task-alert') { assert.equal(aborted, 0, 'changing another class leaves this active native submission alone'); release(); }
  await changing; await refresh;
  const record = (await f.client.nativeNotificationRecords({ sourceKey })).records[0];
  assert.equal(record.status, disabledKind === 'monitoring-failure' ? 'cancelled' : 'submitted');
  assert.equal(aborted, disabledKind === 'monitoring-failure' ? 1 : 0);
  assert.equal(active, 0); assert.equal(maximumActive, 1);
  if (disabledKind === 'monitoring-failure') assert.equal(record.native.pendingCancellation.pending, false);
  await delivery.stop();
});
