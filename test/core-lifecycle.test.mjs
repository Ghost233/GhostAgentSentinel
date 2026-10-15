import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const DAY = 24 * 60 * 60 * 1000;
const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const task = { id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' };
const event = (id, capturedAt, kind = 'input') => ({ id, taskId: 'task', taskLifecycleId: 'life', kind, capturedAt, occurredAt: null, text: id });
const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: 'unknown', reason: 'Insufficient evidence', evidenceIds: [] })), coverage: input.coverage, quality: 'unvalidated' });
async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await delay(2); }
  assert.fail('Expected core state did not become observable');
}

test('restored tasks stay unknown until their own baseline, and interrupted analysis is not resumed', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-restore-'));
  let time = 1_000_000;
  let signal;
  let core = await createCore({ dataDir, now: () => time, analyzer: { analyze(input, options) { signal = options.signal; return new Promise(() => {}); } } });
  try {
    const descriptor = { registrationId: 'profile', hostRunId: 'run', name: 'Source' };
    const source = await core.registerSource(descriptor);
    await core.ingest(source, { tasks: [task, { ...task, id: 'other' }], events: [event('request', time)] });
    await waitFor(() => !!signal);
    await core.close();
    assert.equal(signal.aborted, true);
    let calls = 0;
    core = await createCore({ dataDir, now: () => time, analyzer: { async analyze(input) { calls++; return result(input); } } });
    let state = await core.snapshot();
    assert.equal(state.tasks.every(value => value.status === 'unknown'), true);
    assert.equal(calls, 0);
    const restored = await core.registerSource(descriptor);
    assert.equal(restored, source);
    await core.snapshot();
    assert.equal(calls, 0);
    await core.ingest(source, { tasks: [task] });
    await waitFor(() => calls === 1);
    state = await core.snapshot();
    assert.equal(state.tasks.find(value => value.id === 'other').status, 'unknown');
    assert.equal(state.tasks.find(value => value.id === 'task').status, 'running');
    assert.equal(state.tasks.find(value => value.id === 'task').coverage.complete, false);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('the last source exit cancels only its analysis and fires idle after the grace period', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-idle-'));
  let idleCalls = 0;
  const signals = [];
  const core = await createCore({ dataDir, idleGraceMs: 30, onIdle: () => { idleCalls++; }, analyzer: { analyze(input, { signal }) { signals.push(signal); return new Promise(() => {}); } } });
  try {
    const a = await core.registerSource({ registrationId: 'a', hostRunId: 'run', name: 'A' });
    const b = await core.registerSource({ registrationId: 'b', hostRunId: 'run', name: 'B' });
    await core.ingest(a, { tasks: [task], events: [event('request', Date.now())] });
    await waitFor(() => signals.length === 1);
    await core.unregisterSource(b);
    assert.equal(signals[0].aborted, false);
    await delay(40);
    assert.equal(idleCalls, 0);
    await core.unregisterSource(a);
    assert.equal(signals[0].aborted, true);
    assert.equal(idleCalls, 0);
    await delay(40);
    assert.equal(idleCalls, 1);
    assert.equal((await core.snapshot()).overview.connectedSources, 0);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('a core with no sources exits after grace, while a reconnect cancels the pending idle callback', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-idle-initial-'));
  let idleCalls = 0;
  const core = await createCore({ dataDir, idleGraceMs: 30, onIdle: () => { idleCalls++; } });
  try {
    await delay(40);
    assert.equal(idleCalls, 1);
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.unregisterSource(source);
    await delay(10);
    await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await delay(40);
    assert.equal(idleCalls, 1);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('24 hours expires cited evidence, summaries and alerts, including across restart, with private files', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-retention-'));
  let time = 1_000_000;
  const analyzer = { async analyze(input) {
    const value = result(input);
    value.checks[1] = { id: 'request_drift', status: 'attention', reason: 'Scope review', evidenceIds: ['request'] };
    return value;
  } };
  let core = await createCore({ dataDir, now: () => time, leaseMs: DAY * 2, analyzer });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].alerts.length === 1);
    await core.pause();
    time += DAY + 1;
    let state = await core.snapshot();
    assert.deepEqual(state.tasks[0].events, []);
    assert.equal(state.tasks[0].currentRequest, '');
    assert.equal(state.tasks[0].analysis.summary, null);
    assert.equal(state.tasks[0].analysis.checks.every(check => check.status === 'unknown'), true);
    assert.deepEqual(state.tasks[0].alerts, []);
    await core.close();
    core = await createCore({ dataDir, now: () => time });
    state = await core.snapshot();
    assert.equal(state.tasks.every(value => value.events.length === 0 && value.analysis.summary === null), true);
    assert.equal((await stat(dataDir)).mode & 0o777, 0o700);
    assert.equal((await stat(join(dataDir, 'observations.sqlite'))).mode & 0o777, 0o600);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('historical collection gaps do not poison a later request, and a new request preserves completed cooldown', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-gap-scope-'));
  let time = 1_000_000;
  const calls = [];
  const core = await createCore({ dataDir, now: () => time, leaseMs: 1e9, analyzer: { async analyze(input) { calls.push(input); return result(input); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], gaps: ['Missing earlier observations'], events: [event('old-request', time)] });
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    assert.equal(calls[0].coverage.complete, false);
    const nextEligible = (await core.snapshot()).tasks[0].analysis.nextEligibleAt;
    await core.ingest(source, { events: [event('new-request', ++time)] });
    let state = await core.snapshot();
    assert.equal(state.tasks[0].coverage.complete, true);
    assert.equal(state.sources[0].gaps.includes('Missing earlier observations'), true);
    assert.equal(state.tasks[0].analysis.nextEligibleAt, nextEligible);
    await delay(5);
    assert.equal(calls.length, 1);
    time = nextEligible;
    await core.snapshot();
    await waitFor(() => calls.length === 2);
    assert.equal(calls[1].task.currentRequest, 'new-request');
    assert.equal(calls[1].coverage.complete, true);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('future timestamps are rejected and out-of-order or equal-time inputs do not change current scope', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-order-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('latest-request', time)] });
    await core.ingest(source, { events: [event('older-request', time - 100), event('same-time-backfill', time)] });
    let state = await core.snapshot();
    assert.equal(state.tasks[0].currentRequest, 'latest-request');
    assert.deepEqual(state.tasks[0].events.map(value => value.id), ['older-request', 'latest-request', 'same-time-backfill']);
    const rejected = await core.ingest(source, { events: [event('future-request', time + DAY)] });
    assert.equal(rejected.accepted, 0);
    assert.equal(rejected.expired, 1);
    state = await core.snapshot();
    assert.equal(state.tasks[0].events.length, 3);
    assert.equal(state.tasks[0].coverage.complete, false);
    assert.match(state.sources[0].gaps.join(' '), /future capture timestamp/);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('duplicate task baselines are not new execution activity, and a lease must be re-registered', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-baseline-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 500 });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time)] });
    const initial = (await core.snapshot()).tasks[0];
    time += 100;
    await core.ingest(source, { tasks: [task] });
    const repeated = (await core.snapshot()).tasks[0];
    assert.equal(repeated.updatedAt, initial.updatedAt);
    assert.equal(repeated.lastActivity, initial.lastActivity);
    time += 501;
    await assert.rejects(core.heartbeat(source), /not registered or connected/);
    assert.equal((await core.snapshot()).tasks[0].status, 'unknown');
    await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    assert.equal((await core.snapshot()).tasks[0].status, 'unknown');
    await core.ingest(source, { tasks: [task] });
    assert.equal((await core.snapshot()).tasks[0].status, 'running');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('privacy omissions reduce current request coverage without poisoning a later request', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-redacted-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task], events: [event('request', time), { ...event('omitted-output', ++time, 'output'), text: '[omitted]', data: { redacted: true } }] });
    let state = await core.snapshot();
    assert.equal(state.tasks[0].coverage.complete, false);
    assert.match(state.tasks[0].coverage.gaps.join(' '), /privacy/);
    await core.ingest(source, { events: [event('new-request', ++time)] });
    state = await core.snapshot();
    assert.equal(state.tasks[0].coverage.complete, true);
    assert.equal(state.tasks[0].events.find(value => value.id === 'omitted-output').data.redacted, true);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});
