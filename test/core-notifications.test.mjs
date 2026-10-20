import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
const event = (id, capturedAt, kind = 'input') => ({ id, taskId: 'task', taskLifecycleId: 'life', kind, capturedAt, occurredAt: null, text: id });
const result = (input, status = 'attention', evidenceId = input.events[0].id) => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: id === 'request_drift' ? status : 'unknown', reason: id === 'request_drift' ? 'Action needs scope review' : 'Insufficient evidence', evidenceIds: id === 'request_drift' ? [evidenceId] : [], ...(id === 'request_drift' && status === 'clear' && input.events.some(event => event.id === evidenceId && event.kind === 'tool-result') && input.task.activeAlerts?.[0] ? { recoveryFor: input.task.activeAlerts[0].id, recoveryEvidenceIds: [evidenceId] } : {}) })), coverage: input.coverage, quality: 'unvalidated' });
async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await delay(2); }
  assert.fail('Expected core state did not become observable');
}

for (const longText of [false, true]) {
  test(`system-visible notices identify same-name tasks across sources${longText ? ' before long text is truncated' : ''}`, async t => {
    const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notify-identity-'));
    let core;
    t.after(async () => { try { await core?.close(); } finally { await rm(dataDir, { recursive: true, force: true }); } });
    let time = 1_000_000;
    const title = longText ? '同名任务'.repeat(200) : '合成自检任务';
    const name = longText ? '同名来源'.repeat(200) : 'DSH 同名来源';
    const reason = longText ? '最新动作需要核对'.repeat(200) : '最新动作需要核对';
    core = await createCore({ dataDir, now: () => time, analyzer: { async analyze(input) {
      const value = result(input); value.checks[1].reason = reason; return value;
    } } });
    const sources = await Promise.all(['profile-a', 'profile-b'].map(registrationId => core.registerSource({ registrationId, hostRunId: 'run', name })));
    for (const source of sources) await core.ingest(source, { tasks: [{ ...task, title }], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks.filter(task => task.alerts.length === 1).length === 2);
    time += 3_000;
    const snapshot = await core.snapshot(), notices = [];
    for (const source of sources) {
      const [notice] = await core.claimNotifications(source), observedTask = snapshot.tasks.find(task => task.sourceKey === source);
      assert.ok(notice);
      const visibleBody = notice.body.slice(0, 240); // The actual browser client uses this limit.
      assert.ok(visibleBody.includes(observedTask.key), 'the full overview observation identity must be system-visible');
      assert.ok(visibleBody.includes(source), 'the full source identity must precede any text truncation');
      assert.ok(visibleBody.includes(name.slice(0, 40)), 'the source display name remains readable');
      assert.ok(visibleBody.includes(reason.slice(0, 14)), 'the notification still explains its concern');
      assert.ok(notice.title.length < 240, 'a long task title does not dominate the notification');
      notices.push(notice);
    }
    assert.equal(notices[0].title, notices[1].title);
    assert.notEqual(notices[0].body.slice(0, 240), notices[1].body.slice(0, 240), 'same names/reasons cannot erase task/source attribution');
    assert.ok(!notices[0].body.includes(notices[1].taskKey));
    assert.ok(!notices[1].body.includes(notices[0].taskKey));
  });
}

test('first grounded alert notifies once, merges rounds, and repeats only after ten minutes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notify-'));
  let time = 1_000_000;
  let calls = 0;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) { calls++; return result(input); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    const other = await core.registerSource({ registrationId: 'other', hostRunId: 'run', name: 'Other' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].alerts.length === 1);
    assert.deepEqual(await core.claimNotifications(other), []);
    assert.deepEqual(await core.claimNotifications(source), []);
    time += 3_000;
    const first = await core.claimNotifications(source);
    assert.equal(first.length, 1);
    assert.deepEqual(first[0].checkIds, ['request_drift']);
    assert.deepEqual(await core.claimNotifications(source), []);
    const acknowledged = await core.ackNotification(first[0].id, { sourceKey: first[0].sourceKey, attemptToken: first[0].attemptToken, status: 'submitted' });
    assert.equal(acknowledged.delivery, 'system-accepted');
    assert.equal(acknowledged.seen, 'unknown');
    const alertId = (await core.snapshot()).tasks[0].alerts[0].id;
    time += 60_000;
    await core.ingest(source, { events: [event('fresh-diagnostic', time, 'status')] });
    await waitFor(() => calls === 2);
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    assert.deepEqual(await core.claimNotifications(source), []);
    assert.equal((await core.snapshot()).tasks[0].alerts[0].id, alertId);
    time += 540_000;
    await core.snapshot();
    assert.deepEqual(await core.claimNotifications(source), []);
    time += 3_000;
    const repeated = await core.claimNotifications(source);
    assert.equal(repeated.length, 1);
    await core.ackNotification(repeated[0].id, { sourceKey: repeated[0].sourceKey, attemptToken: repeated[0].attemptToken, status: 'submitted' });
    await core.ackNotification(first[0].id, { sourceKey: first[0].sourceKey, attemptToken: first[0].attemptToken, status: 'failed', error: 'Delayed error from the previous attempt' });
    const delivery = (await core.snapshot()).tasks[0].alerts[0].notification;
    assert.equal(delivery.id, repeated[0].id);
    assert.equal(delivery.status, 'submitted');
    await core.ingest(source, { tasks: [{ ...task, status: 'idle' }] });
    time += 600_000;
    assert.deepEqual(await core.claimNotifications(source), []);
    assert.equal((await core.snapshot()).tasks[0].alerts[0].status, 'ended');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('notification failure stays visible and does not cause immediate retry; recovery needs new evidence', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notify-fail-'));
  let time = 1_000_000;
  let calls = 0;
  let checkStatus = 'attention';
  let citation = 'request';
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) { calls++; return result(input, checkStatus, citation); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].alerts.length === 1);
    time += 3_000;
    const [notice] = await core.claimNotifications(source);
    await core.ackNotification(notice.id, { sourceKey: notice.sourceKey, attemptToken: notice.attemptToken, status: 'failed', error: 'permission denied' });
    let state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].notification.status, 'failed');
    assert.equal(state.tasks[0].alerts[0].notification.error, 'permission denied');
    time += 60_000;
    await core.snapshot();
    assert.equal(calls, 1, 'unchanged successful judgment is reused independently of notification failure');
    assert.deepEqual(await core.claimNotifications(source), []);
    checkStatus = 'clear';
    time += 60_000;
    await core.ingest(source, { events: [event('fresh-diagnostic', time, 'status')] });
    await waitFor(() => calls === 2);
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    assert.equal((await core.snapshot()).tasks[0].alerts[0].status, 'attention', 'a clear answer without new result evidence does not recover the retained alert');
    citation = 'actual-new-result';
    await core.ingest(source, { events: [event(citation, ++time, 'tool-result')] });
    time += 60_000;
    await core.snapshot();
    await waitFor(() => calls === 3);
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    assert.equal((await core.snapshot()).tasks[0].alerts[0].status, 'recovered');
    assert.deepEqual(await core.claimNotifications(source), []);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('cross-task citations fail analysis rather than create an alert', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-citation-'));
  const time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, analyzer: { async analyze(input) { return result(input, 'attention', 'other-task-evidence'); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'failed');
    const state = await core.snapshot();
    assert.match(state.tasks[0].analysis.error.message, /cross-task evidence/);
    assert.equal(state.tasks[0].alerts.length, 0);
    assert.deepEqual(await core.claimNotifications(source), []);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('a grounded wait fact can notify while unavailable semantic dependencies remain explicit', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-wait-fact-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, analyzer: { async analyze(input) {
    const value = result(input, 'unknown');
    value.coverage.dependencies = ['llm_unconfigured', 'jev_unconfigured'];
    value.checks[5] = { id: 'long_wait', status: 'attention', reason: 'Observed tool has no result after the development wait threshold', evidenceIds: ['tool-start'] };
    return value;
  } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('tool-start', time - 300_000, 'tool-start')] });
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'unavailable');
    const state = await core.snapshot();
    assert.deepEqual(state.tasks[0].analysis.coverage.dependencies, ['llm_unconfigured', 'jev_unconfigured']);
    time += 3_000;
    const notices = await core.claimNotifications(source);
    assert.equal(notices.length, 1);
    assert.deepEqual(notices[0].checkIds, ['long_wait']);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('an asynchronous notification error replaces system acceptance with visible failure without immediate retry', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notify-async-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) { return result(input); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].alerts.length === 1);
    time += 3_000;
    const [notice] = await core.claimNotifications(source);
    await core.ackNotification(notice.id, { sourceKey: notice.sourceKey, attemptToken: notice.attemptToken, status: 'submitted' });
    time++;
    const failed = await core.ackNotification(notice.id, { sourceKey: notice.sourceKey, attemptToken: notice.attemptToken, status: 'failed', error: 'Native notification failed asynchronously' });
    assert.equal(failed.status, 'failed');
    let state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].notification.status, 'failed');
    assert.equal(state.tasks[0].alerts[0].notification.error, 'Native notification failed asynchronously');
    assert.equal(state.tasks[0].alerts[0].notification.seen, 'unknown');
    time += 60_000;
    await core.snapshot();
    assert.deepEqual(await core.claimNotifications(source), []);
    await core.ingest(source, { tasks: [{ ...task, status: 'idle' }] });
    time += 600_000;
    assert.deepEqual(await core.claimNotifications(source), []);
    state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].status, 'ended');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('an abandoned claim expires without immediate resend and late acknowledgements cannot mutate the new attempt', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-notify-claim-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) { return result(input); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].alerts.length === 1);
    time += 3_000;
    const [abandoned] = await core.claimNotifications(source);
    time += 60_000;
    await core.heartbeat(source);
    let state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].notification.status, 'expired');
    assert.equal(state.tasks[0].alerts[0].notification.delivery, 'unknown');
    assert.equal(state.tasks[0].alerts[0].status, 'attention');
    assert.deepEqual(await core.claimNotifications(source), []);
    const oldAck = await core.ackNotification(abandoned.id, { sourceKey: abandoned.sourceKey, attemptToken: abandoned.attemptToken, status: 'submitted' });
    assert.equal(oldAck.status, 'expired');
    time += 540_000;
    await core.heartbeat(source);
    assert.deepEqual(await core.claimNotifications(source), []);
    time += 3_000;
    const [retried] = await core.claimNotifications(source);
    assert.ok(retried);
    assert.notEqual(retried.id, abandoned.id);
    await core.ackNotification(retried.id, { sourceKey: retried.sourceKey, attemptToken: retried.attemptToken, status: 'submitted' });
    await core.ackNotification(abandoned.id, { sourceKey: abandoned.sourceKey, attemptToken: abandoned.attemptToken, status: 'failed', error: 'late old client error' });
    state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].notification.id, retried.id);
    assert.equal(state.tasks[0].alerts[0].notification.status, 'submitted');
    assert.equal(state.tasks[0].alerts[0].notification.error, null);
    await core.unregisterSource(source);
    time += 600_000;
    state = await core.snapshot();
    assert.equal(state.tasks[0].alerts[0].status, 'source-disconnected');
    await assert.rejects(core.claimNotifications(source), /not registered or connected/);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});
