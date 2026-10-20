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
import { createCore } from '../src/core/index.mjs';

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
  const response = await fetch(f.server.url + '/v1/notifications/submitting', { method: 'POST', headers: { authorization: `Bearer ${f.server.token}`, 'content-type': 'application/json' }, body: JSON.stringify({ sourceKey: own, id: old.id, attemptToken: old.attemptToken }) });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { id: old.id, maySubmit: false });
  assert.deepEqual(await f.client.beginNotificationSubmission(other, kept.id, kept.attemptToken), { id: kept.id, maySubmit: true });
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
  assert.equal((await f.client.beginNotificationSubmission(source, first.id, first.attemptToken)).maySubmit, true);
  assert.equal((await f.client.beginNotificationSubmission(source, first.id, first.attemptToken)).maySubmit, false, 'submission reservation is one-shot');
  await f.client.ackNotification(first.id, { sourceKey: source, attemptToken: first.attemptToken, status: 'submitted' });
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
  assert.equal((await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken)).maySubmit, true);
  const closing = f.client.setInstallationNotificationPreference('atomic', 'task-alert', false);
  await until(async () => (await f.client.installationNotificationPreferences('atomic')).revocation?.['task-alert'] === 'unconfirmed');
  await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'invalidated' });
  await closing;
  assert.equal((await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'submitted' })).status, 'cancelled', 'an earlier reservation cannot revive after OFF');
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

test('repeated OFF and a later ON retain the same outstanding revocation until its exact attempt is invalidated', async t => {
  const f = await fixture(t), source = await f.observe('shared-revocation'), other = await f.observe('isolated-revocation');
  f.advance(3000);
  const [notice] = await f.client.claimNotifications(source), [kept] = await f.client.claimNotifications(other);
  await assert.rejects(f.client.beginNotificationSubmission(source, notice.id), /identifier/);
  await assert.rejects(f.client.beginNotificationSubmission(other, notice.id, notice.attemptToken), /Unknown legacy/);
  await assert.rejects(f.client.beginNotificationSubmission(source, notice.id, 'wrong'), /Unknown legacy/);
  assert.equal((await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken)).maySubmit, true);
  const watch = f.client.waitNotificationSubmissionRevocation(source, notice.id, notice.attemptToken);
  let finished = 0;
  const off = f.client.setInstallationNotificationPreference('shared-revocation', 'task-alert', false).then(value => { finished++; return value; });
  assert.deepEqual(await watch, { id: notice.id, revoked: true });
  const again = f.client.setInstallationNotificationPreference('shared-revocation', 'task-alert', false).then(value => { finished++; return value; });
  await until(async () => (await f.client.diagnostics()).temporary.submissionPreferenceWaits === 2);
  assert.equal(finished, 0, 'repeated false does not turn a stored boolean into confirmed revocation');
  const on = await f.client.setInstallationNotificationPreference('shared-revocation', 'task-alert', true);
  assert.equal(on.preferences['task-alert'], true); assert.equal(on.revocation['task-alert'], 'unconfirmed');
  assert.equal((await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken)).maySubmit, false);
  assert.deepEqual(await f.client.claimNotifications(source), []);
  assert.equal((await f.client.beginNotificationSubmission(other, kept.id, kept.attemptToken)).maySubmit, true);
  await assert.rejects(f.client.ackNotification(notice.id, { sourceKey: other, attemptToken: notice.attemptToken, status: 'invalidated' }), /Unknown legacy/);
  await assert.rejects(f.client.ackNotification(notice.id, { status: 'submitted' }), /Invalid notification submission/);
  assert.equal(finished, 0);
  await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'invalidated' });
  const completed = await Promise.all([off, again]);
  assert.equal(finished, 2); assert.ok(completed.every(value => value.preferences['task-alert'] === true && !value.revocation), 'an older OFF result reports the current later ON, rather than overwriting it');
  assert.equal((await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'submitted' })).status, 'cancelled');
  assert.equal((await f.client.installationNotificationRecords('shared-revocation')).records[0].status, 'cancelled');
  await f.client.ackNotification(kept.id, { sourceKey: other, attemptToken: kept.attemptToken, status: 'submitted' });
});

test('a disconnected or cancelled request cannot confirm OFF and restarting Core keeps the unresolved attempt', async t => {
  const f = await fixture(t), source = await f.observe('unconfirmed'); f.advance(3000);
  const [notice] = await f.client.claimNotifications(source);
  await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken);
  const abort = new AbortController();
  const off = f.client.setInstallationNotificationPreference('unconfirmed', 'task-alert', false, abort.signal);
  const rejected = assert.rejects(off);
  await until(async () => (await f.client.installationNotificationPreferences('unconfirmed')).revocation?.['task-alert'] === 'unconfirmed');
  abort.abort(); await rejected;
  await until(async () => (await f.client.diagnostics()).temporary.submissionPreferenceWaits === 0);
  await f.client.unregisterSource(source, 'controlled Host disconnected');
  assert.equal((await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken)).maySubmit, false);
  await f.restart();
  const read = await f.client.installationNotificationPreferences('unconfirmed');
  assert.equal(read.preferences['task-alert'], false); assert.equal(read.revocation['task-alert'], 'unconfirmed');
  const secondAbort = new AbortController(), again = f.client.setInstallationNotificationPreference('unconfirmed', 'task-alert', false, secondAbort.signal);
  const secondRejected = assert.rejects(again);
  await until(async () => (await f.client.diagnostics()).temporary.submissionPreferenceWaits === 1);
  secondAbort.abort(); await secondRejected;
  await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'invalidated' });
  assert.equal((await f.client.installationNotificationPreferences('unconfirmed')).revocation, undefined);
});

test('1000 unresolved permits survive retention and restart, admit no unsafe replacement, and leave another installation capacity independent', { timeout: 60000 }, async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-revocation-capacity-')); let time = Date.now(), core;
  const analyzer = { async analyze(input) { return { summary: { currentTask: input.task.currentRequest, currentState: '', recentProgress: '', attention: '' }, checks: ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'].map(id => ({ id, status: id === 'request_drift' ? 'attention' : 'unknown', reason: 'Observed out-of-scope action', evidenceIds: id === 'request_drift' ? ['output'] : [], applicability: { state: id === 'request_drift' ? 'triggered' : 'not-triggered', reason: 'Controlled evidence', evidenceIds: ['input'] } })), coverage: input.coverage }; } };
  const open = () => createCore({ dataDir, analyzer, now: () => time, cooldownMs: 1e9, leaseMs: 1e12 });
  t.after(async () => { await core?.close(); await rm(dataDir, { recursive: true, force: true }); }); core = await open();
  const source = await core.registerSource({ registrationId: 'full-permits', hostRunId: 'host', name: 'Own' });
  const ingest = async (key, ids) => {
    await core.ingest(key, { tasks: ids.map(id => ({ id, lifecycleId: 'life', title: id, status: 'running' })), events: ids.flatMap(id => ['input', 'output'].map(kind => ({ id: kind, taskId: id, taskLifecycleId: 'life', kind, text: id + kind, capturedAt: time, occurredAt: null }))) });
    for (let attempt = 0; attempt < 20; attempt++) {
      time += 3000;
      const claims = await core.claimNotifications(key);
      if (claims.length) return claims;
      await delay(0);
    }
    assert.fail('real Core producer did not yield a claim for ' + ids.join(','));
  };
  const notices = [];
  for (let i = 0; i < 1000; i++) {
    if (i) {
      await core.ingest(source, { tasks: [{ id: 'original-' + (i - 1), lifecycleId: 'life', status: 'idle' }] });
      time += 86400001; await core.heartbeat(source);
      assert.equal((await core.installationNotificationRecords('full-permits')).records.length, 0);
      assert.equal((await core.diagnostics()).retained.submissionPermits, i, 'retention must not settle any previously issued permit');
    }
    const [notice] = await ingest(source, ['original-' + i]); assert.ok(notice); notices.push(notice);
    assert.equal((await core.beginNotificationSubmission(source, notice.id, notice.attemptToken)).maySubmit, true);
    if ((i + 1) % 250 === 0) t.diagnostic('actual issued permits: ' + (i + 1));
  }
  assert.equal(notices.length, 1000);
  assert.equal((await core.diagnostics()).retained.submissionPermits, 1000, 'ending tasks does not settle their issued permits');
  time += 86400001;
  assert.equal((await core.installationNotificationRecords('full-permits')).records.length, 0);
  assert.equal((await core.diagnostics()).retained.submissionPermits, 1000);
  await core.close();
  assert.equal((await core.diagnostics()).temporary.submissionRevocationWaits, 0);
  assert.equal((await core.diagnostics()).temporary.submissionPreferenceWaits, 0);
  core = await open();
  assert.equal((await core.installationNotificationPreferences('full-permits')).revocation['task-alert'], 'unconfirmed');
  const activeSource = await core.registerSource({ registrationId: 'full-permits', hostRunId: 'host', name: 'Own' });
  const [next] = await ingest(activeSource, ['replacement']);
  await assert.rejects(core.beginNotificationSubmission(activeSource, next.id, next.attemptToken), /capacity/);
  const other = await core.registerSource({ registrationId: 'other-permits', hostRunId: 'other', name: 'Other' });
  const [separate] = await ingest(other, ['other-task']);
  assert.equal((await core.beginNotificationSubmission(other, separate.id, separate.attemptToken)).maySubmit, true);
  const abort = new AbortController(), watch = core.waitNotificationSubmissionRevocation(other, separate.id, separate.attemptToken, abort.signal), aborted = assert.rejects(watch); abort.abort(); await aborted;
  assert.equal((await core.diagnostics()).temporary.submissionRevocationWaits, 0);
  await core.ackNotification(notices[0].id, { sourceKey: source, attemptToken: notices[0].attemptToken, status: 'invalidated' });
  assert.equal((await core.beginNotificationSubmission(activeSource, next.id, next.attemptToken)).maySubmit, true, 'only an actual invalidation ACK frees capacity');
  await assert.rejects(core.ackNotification(notices[0].id, { sourceKey: source, attemptToken: notices[0].attemptToken, status: 'submitted' }), /Unknown notification/);
  const closeWait = core.waitNotificationSubmissionRevocation(activeSource, next.id, next.attemptToken), closeRejected = assert.rejects(closeWait);
  await core.close(); await closeRejected;
  assert.equal((await core.diagnostics()).temporary.submissionRevocationWaits, 0);
  assert.equal((await core.diagnostics()).temporary.submissionPreferenceWaits, 0);
});

test('the 1000-row history cap hides an unexpired issued legacy notice without forgetting its revocation fence', async t => {
  const f = await fixture(t), source = await f.observe('combined-revocation-cap'); f.advance(3000);
  const [notice] = await f.client.claimNotifications(source); await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken);
  const native = await f.client.registerSource({ registrationId: 'combined-revocation-cap', hostRunId: 'native', name: 'Native evidence', policy: 'chatgpt-main/1' });
  for (let i = 0; i < 1000; i++) {
    f.advance(1);
    await f.client.recordCollectionOutcome({ sourceKey: native, mainId: 'independent-' + i, lifecycleId: null, channel: 'directory', roundId: 'round-' + i, outcome: 'failed', settledAt: f.now(), error: { stage: 'collection', code: 'EIO' } });
  }
  let cursor, count = 0;
  do {
    const page = await f.client.installationNotificationRecords('combined-revocation-cap', { limit: 100, ...(cursor ? { cursor } : {}) });
    assert.ok(page.records.every(record => record.id !== notice.id)); count += page.records.length; cursor = page.nextCursor;
  } while (cursor);
  assert.equal(count, 1000);
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 1);
  assert.deepEqual(await f.client.waitNotificationSubmissionRevocation(source, notice.id, notice.attemptToken), { id: notice.id, revoked: true });
  await f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'invalidated' });
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 0);
  await assert.rejects(f.client.ackNotification(notice.id, { sourceKey: source, attemptToken: notice.attemptToken, status: 'submitted' }), /Unknown notification/);
});

test('server close releases outstanding submission waits and OFF without confirming an unanswered permit', async t => {
  const f = await fixture(t), own = await f.observe('closing-off'), other = await f.observe('closing-wait'); f.advance(3000);
  const [a] = await f.client.claimNotifications(own), [b] = await f.client.claimNotifications(other);
  await f.client.beginNotificationSubmission(own, a.id, a.attemptToken); await f.client.beginNotificationSubmission(other, b.id, b.attemptToken);
  const wait = f.client.waitNotificationSubmissionRevocation(other, b.id, b.attemptToken), waitRejected = assert.rejects(wait);
  const off = f.client.setInstallationNotificationPreference('closing-off', 'task-alert', false), offRejected = assert.rejects(off);
  await until(async () => { const value = await f.client.diagnostics(); return value.temporary.submissionRevocationWaits === 1 && value.temporary.submissionPreferenceWaits === 1; });
  await f.server.close(); await Promise.all([waitRejected, offRejected]);
  assert.deepEqual(f.client.transportDiagnostics(), { closed: false, pendingRequests: 0, deadlineTimers: 0 });
  await f.restart();
  assert.equal((await f.client.installationNotificationPreferences('closing-off')).revocation['task-alert'], 'unconfirmed');
  assert.equal((await f.client.diagnostics()).temporary.submissionRevocationWaits, 0);
  assert.equal((await f.client.diagnostics()).temporary.submissionPreferenceWaits, 0);
  assert.equal((await f.client.diagnostics()).temporary.timers.notificationSubmission, 0);
});

test('an expired issued fence has no alert members and does not prevent the next real producer', async t => {
  const f = await fixture(t), source = await f.observe('expired-fence-producer'); f.advance(3000);
  const [notice] = await f.client.claimNotifications(source); await f.client.beginNotificationSubmission(source, notice.id, notice.attemptToken);
  f.advance(86400001); assert.equal((await f.client.installationNotificationRecords('expired-fence-producer')).records.length, 0);
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 1);
  const activeSource = await f.observe('fresh-after-expired-fence');
  assert.notEqual(activeSource, source, 'an independent producer owns its own source');
  f.advance(3000); const [next] = await f.client.claimNotifications(activeSource);
  assert.ok(next); assert.notEqual(next.id, notice.id);
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 1, 'new producer activity does not settle an old issued permit');
});

test('a v0.1.1-shaped HTTP claim remains deliverable but OFF waits for its real consumed receipt rather than inventing invalidation', async t => {
  const f = await fixture(t), source = await f.observe('legacy-http-consumed'); f.advance(3000);
  const legacy = async (path, body) => {
    const response = await fetch(f.server.url + path, { method: 'POST', headers: { authorization: `Bearer ${f.server.token}`, 'content-type': 'application/json' }, body: JSON.stringify(body) });
    assert.equal(response.status, 200); return response.json();
  };
  const [notice] = await legacy('/v1/notifications/claim', { sourceKey: source });
  assert.ok(notice.body); assert.equal(notice.attemptToken, undefined);
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 1);
  let confirmed = false;
  const closing = f.client.setInstallationNotificationPreference('legacy-http-consumed', 'task-alert', false).then(value => { confirmed = true; return value; });
  await until(async () => (await f.client.installationNotificationPreferences('legacy-http-consumed')).revocation?.['task-alert'] === 'unconfirmed');
  assert.equal(confirmed, false);
  await assert.rejects(f.client.ackNotification(notice.id, { sourceKey: source, status: 'invalidated', attemptToken: 'not-a-legacy-proof' }), /Legacy notification/);
  let constructions = 0;
  class ControlledNotification { constructor() { constructions++; } }
  new ControlledNotification(notice.title, { body: notice.body });
  const receipt = await legacy('/v1/notifications/ack', { id: notice.id, result: { status: 'submitted' } });
  assert.equal(receipt.status, 'cancelled');
  assert.equal((await closing).preferences['task-alert'], false); assert.equal(constructions, 1);
  assert.equal((await f.client.installationNotificationRecords('legacy-http-consumed')).records[0].status, 'cancelled');
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 0);
});

for (const begun of [false, true]) test('stored legacy ' + (begun ? 'submissionStartedAt' : 'claim without begin') + ' remains unconfirmed across upgrade until an actual consumed receipt', async t => {
  const f = await fixture(t), source = await f.observe('stored-legacy'); f.advance(3000);
  const [notice] = await f.client.claimNotifications(source);
  await f.client.close(); await f.server.close();
  const db = new DatabaseSync(join(f.dataDir, 'observations.sqlite'));
  try {
    const row = db.prepare("SELECT value FROM records WHERE collection='notifications' AND key=?").get(notice.id), value = JSON.parse(row.value);
    delete value.submissionAttempt;
    if (begun) value.submissionStartedAt = f.now();
    db.prepare("UPDATE records SET value=? WHERE collection='notifications' AND key=?").run(JSON.stringify(value), notice.id);
  } finally { db.close(); }
  await f.restart();
  assert.equal((await f.client.installationNotificationPreferences('stored-legacy')).revocation['task-alert'], 'unconfirmed');
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 1);
  const controller = new AbortController(), closing = f.client.setInstallationNotificationPreference('stored-legacy', 'task-alert', false, controller.signal), rejected = assert.rejects(closing);
  await until(async () => (await f.client.diagnostics()).temporary.submissionPreferenceWaits === 1);
  controller.abort(); await rejected;
  assert.equal((await f.client.installationNotificationPreferences('stored-legacy')).revocation['task-alert'], 'unconfirmed');
  assert.equal((await f.client.ackNotification(notice.id, { status: 'submitted' })).status, 'cancelled');
  assert.equal((await f.client.installationNotificationPreferences('stored-legacy')).revocation, undefined);
});

test('unknown HTTP claim protocol values never downgrade into legacy issued authority', async t => {
  const f = await fixture(t), source = await f.observe('strict-claim'); f.advance(3000);
  for (const submissionProtocol of [null, 0, 2, '1', true, {}, []]) {
    const response = await fetch(f.server.url + '/v1/notifications/claim', { method: 'POST', headers: { authorization: `Bearer ${f.server.token}`, 'content-type': 'application/json' }, body: JSON.stringify({ sourceKey: source, submissionProtocol }) });
    assert.equal(response.status, 400);
  }
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 0);
  const [notice] = await f.client.claimNotifications(source);
  assert.equal(typeof notice.attemptToken, 'string');
  assert.equal((await f.client.diagnostics()).retained.submissionPermits, 0, 'modern claim is not itself a permit to construct');
});
