import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const DAY = 24 * 60 * 60 * 1000;
const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const task = (id, status = 'running') => ({ id, lifecycleId: 'life', title: `Private task ${id}`, status });
const event = (id, time, text = 'Private request') => ({ id: `request-${id}`, taskId: id, taskLifecycleId: 'life', kind: 'input', capturedAt: time, occurredAt: null, text });
const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'late', recentProgress: '', attention: '' }, checks: checks.map(id => ({ id, status: 'unknown', reason: 'No evidence', evidenceIds: [] })), coverage: input.coverage, quality: 'unvalidated' });
async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await delay(2); }
  assert.fail('Expected resource state did not become observable');
}

test('finished queue entries are released, while cancelled external requests remain counted until they settle', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-core-resources-'));
  let finish;
  let input;
  let signal;
  const core = await createCore({ dataDir, analyzer: { analyze(value, options) {
    input = value;
    signal = options.signal;
    return new Promise(resolve => { finish = resolve; });
  } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'host', name: 'Private source' });
    await core.ingest(source, { tasks: [task('active')], events: [event('active', Date.now())] });
    await waitFor(() => !!finish);
    const queued = Array.from({ length: 100 }, (_, index) => task(String(index)));
    await core.ingest(source, { tasks: queued });
    assert.equal((await core.diagnostics()).temporary.queueKeys, 100);
    await core.ingest(source, { tasks: queued.map(value => ({ ...value, status: 'disposed' })) });
    let counts = await core.diagnostics();
    assert.equal(counts.temporary.queueKeys, 0);
    assert.equal(counts.temporary.queueMemberships, 0);
    assert.equal(counts.retained.tasks, 101);
    assert.equal(counts.runtime.activeAnalyses, 1);
    assert.equal(JSON.stringify(counts).includes('Private'), false);
    await core.pause();
    assert.equal(signal.aborted, true);
    counts = await core.diagnostics();
    assert.equal(counts.runtime.activeAnalyses, 0);
    assert.equal(counts.runtime.cancelledUnsettledRequests, 1);
    finish(result(input));
    await waitFor(async () => (await core.diagnostics()).runtime.cancelledUnsettledRequests === 0);
    assert.equal((await core.snapshot()).tasks.find(value => value.id === 'active').analysis.summary, null);
    await core.close();
    counts = await core.diagnostics();
    assert.equal(counts.pid, process.pid);
    assert.equal(counts.closed, true);
    assert.equal(counts.temporary.timers.total, 0);
    assert.equal(counts.temporary.queueKeys, 0);
    assert.equal(counts.temporary.queueMemberships, 0);
  } finally { if (finish) finish(result(input)); await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('repeated reconnect gaps stay bounded, apply to current scope, and do not renew old observations', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-core-reconnect-counts-'));
  let time = 1_000_000;
  let core = await createCore({ dataDir, now: () => time, leaseMs: DAY * 2 });
  try {
    await core.configureSources([{ registrationId: 'profile', name: 'Desktop', enabled: true }]);
    const descriptor = { registrationId: 'profile', hostRunId: 'host', name: 'Private source' };
    const source = await core.registerSource(descriptor);
    await core.ingest(source, { tasks: [task('active')], events: [event('active', time)], gaps: ['Observer missed some material'] });
    for (let index = 0; index < 100; index++) {
      time++;
      await core.unregisterSource(source, 'Observer detached');
      assert.equal(await core.registerSource(descriptor), source);
      await core.ingest(source, { tasks: [task('active')], gaps: ['Observer missed some material'] });
    }
    let counts = await core.diagnostics();
    assert.equal(counts.retained.sourceGapRecords, 2);
    assert.equal(counts.retained.events, 1);
    assert.equal(counts.temporary.queueKeys, 0);
    assert.equal(counts.temporary.timers.total, 1);
    assert.equal((await core.snapshot()).tasks[0].coverage.complete, false);
    await core.ingest(source, { events: [{ ...event('active', ++time), id: 'new-request' }] });
    assert.equal((await core.snapshot()).tasks[0].coverage.complete, true);
    await core.ingest(source, { gaps: ['Observer missed some material'] });
    assert.equal((await core.snapshot()).tasks[0].coverage.complete, false);
    assert.equal((await core.diagnostics()).retained.sourceGapRecords, 2);
    await core.close();
    time += DAY + 1;
    core = await createCore({ dataDir, now: () => time });
    counts = await core.diagnostics();
    assert.equal(counts.retained.events, 0);
    assert.equal(counts.retained.tasks, 0);
    assert.equal(counts.retained.sources, 0);
    assert.equal(counts.retained.sourceGapRecords, 0);
    assert.equal(counts.retained.taskGapRecords, 0);
    assert.equal(counts.configuration.sourcePlanEntries, 1);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('source lease loss releases active work and queued task indexes within the lease without ending other sources', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-core-lease-counts-'));
  let time = 1_000_000;
  let started;
  let signal;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 15_000, analyzer: { analyze(input, options) {
    signal = options.signal;
    started = true;
    return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(options.signal.reason), { once: true }));
  } } });
  try {
    const expired = await core.registerSource({ registrationId: 'expired', hostRunId: 'one', name: 'Expired' });
    const live = await core.registerSource({ registrationId: 'live', hostRunId: 'two', name: 'Live' });
    await core.ingest(expired, { tasks: [task('active'), task('queued')], events: [event('active', time)] });
    await core.ingest(live, { tasks: [] });
    await waitFor(() => started);
    assert.equal((await core.diagnostics()).temporary.queueKeys, 1);
    time += 10_000;
    await core.heartbeat(live);
    time += 5_001;
    let counts = await core.diagnostics();
    assert.equal(signal.aborted, true);
    assert.equal(counts.runtime.connectedSources, 1);
    assert.equal(counts.runtime.activeAnalyses, 0);
    assert.equal(counts.temporary.queueKeys, 0);
    assert.equal(counts.temporary.queueMemberships, 0);
    await waitFor(async () => (await core.diagnostics()).runtime.cancelledUnsettledRequests === 0);
    counts = await core.diagnostics();
    assert.equal(counts.temporary.timers.total, 1);
    assert.equal(counts.retained.tasks, 2);
    assert.equal((await core.snapshot()).tasks.every(value => value.status === 'unknown'), true);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('a long active request is unaffected by idle grace and clears its temporary state after normal completion', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-core-long-counts-'));
  let finish;
  let input;
  let signal;
  let idleCalls = 0;
  const core = await createCore({ dataDir, idleGraceMs: 10, onIdle: () => { idleCalls++; }, analyzer: { analyze(value, options) {
    input = value;
    signal = options.signal;
    return new Promise(resolve => { finish = resolve; });
  } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'host', name: 'Private source' });
    await core.ingest(source, { tasks: [task('active')], events: [event('active', Date.now())] });
    await waitFor(() => !!finish);
    await delay(25);
    assert.equal(signal.aborted, false);
    assert.equal(idleCalls, 0);
    assert.equal((await core.diagnostics()).runtime.activeAnalyses, 1);
    finish(result(input));
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    assert.equal((await core.diagnostics()).runtime.activeAnalyses, 0);
    assert.equal((await core.diagnostics()).temporary.timers.wakeup, 1);
    await core.ingest(source, { tasks: [task('active', 'disposed')] });
    const counts = await core.diagnostics();
    assert.equal(counts.temporary.timers.wakeup, 0);
    assert.equal(counts.temporary.timers.total, 1);
  } finally { if (finish) finish(result(input)); await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});
