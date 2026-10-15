import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: 'unknown', reason: 'test model has insufficient evidence', evidenceIds: [] })), coverage: input.coverage, quality: 'unvalidated' });
const task = id => ({ id, lifecycleId: 'life', title: id, status: 'running' });
const event = (id, taskId, capturedAt, text, kind = 'output') => ({ id, taskId, taskLifecycleId: 'life', kind, capturedAt, occurredAt: null, text });
async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt++) { if (await predicate()) return; await delay(2); }
  assert.fail('Expected core state did not become observable');
}

test('shared FIFO coalesces queued material while each completed task cools independently', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-schedule-'));
  let time = 1_000_000;
  const calls = [];
  const releases = [];
  const analyzer = { analyze(input) { calls.push(input); return new Promise(resolve => releases.push(() => resolve(result(input)))); } };
  const core = await createCore({ dataDir, analyzer, now: () => time, leaseMs: 1e9 });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task('a'), task('b')], events: [event('a-input', 'a', time, 'A request', 'input'), event('b-input', 'b', time, 'B request', 'input')] });
    await waitFor(() => calls.length === 1);
    assert.equal(calls[0].task.id, 'a');
    await core.ingest(source, { events: [event('b-new', 'b', ++time, 'B latest evidence'), event('a-new', 'a', ++time, 'A latest evidence')] });
    assert.equal((await core.snapshot()).monitoring.queueLength, 1);
    releases.shift()();
    await waitFor(() => calls.length === 2);
    assert.equal(calls[1].task.id, 'b');
    assert.deepEqual(calls[1].events.map(value => value.id), ['b-input', 'b-new']);
    assert.equal((await core.snapshot()).tasks.find(value => value.id === 'a').analysis.nextEligibleAt, time + 60_000);
    releases.shift()();
    await waitFor(async () => (await core.snapshot()).monitoring.activeTaskKey === null);
    time += 59_999;
    await core.snapshot();
    assert.equal(calls.length, 2);
    time++;
    await core.snapshot();
    await waitFor(() => calls.length === 3);
    assert.equal(calls[2].task.id, 'a');
    assert.deepEqual(calls[2].events.map(value => value.id), ['a-input', 'a-new']);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('a changed request seals its old round and late responses cannot replace current analysis', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-cancel-'));
  let time = 1_000_000;
  const calls = [];
  const releases = [];
  const analyzer = { analyze(input, { signal }) { calls.push({ input, signal }); return new Promise(resolve => releases.push(value => resolve(value ?? result(input)))); } };
  const core = await createCore({ dataDir, analyzer, now: () => time, leaseMs: 1e9 });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task('a')], events: [event('old', 'a', time, 'Old request', 'input')] });
    await waitFor(() => calls.length === 1);
    await core.ingest(source, { events: [event('new', 'a', ++time, 'New request', 'input')] });
    assert.equal(calls[0].signal.aborted, true);
    assert.equal((await core.snapshot()).monitoring.pendingCancelledRequests, 1);
    assert.equal(calls.length, 1);
    assert.equal((await core.snapshot()).tasks[0].analysis.state, 'queued');
    releases[0]({ ...result(calls[0].input), summary: { currentTask: 'INVALID LATE RESULT', currentState: '', recentProgress: '', attention: '' } });
    await waitFor(() => calls.length === 2);
    assert.equal((await core.snapshot()).tasks[0].analysis.completedAt, null);
    releases[1]();
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    await waitFor(async () => (await core.snapshot()).monitoring.pendingCancelledRequests === 0);
    const state = await core.snapshot();
    assert.equal(state.tasks[0].currentRequest, 'New request');
    assert.equal(state.tasks[0].analysis.summary.currentTask, 'New request');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('pause keeps collecting, configuration replacement aborts only the old analysis, and lease loss stops alerts', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-pause-'));
  let time = 1_000_000;
  let signal;
  let finish;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 500, analyzer: { analyze(input, options) { signal = options.signal; return new Promise(resolve => { finish = () => resolve(result(input)); }); } } });
  try {
    const source = await core.registerSource({ registrationId: 'profile', hostRunId: 'run', name: 'Source' });
    await core.ingest(source, { tasks: [task('a')], events: [event('old', 'a', time, 'Request', 'input')] });
    await waitFor(() => !!signal);
    await core.pause();
    assert.equal(signal.aborted, true);
    assert.equal((await core.ingest(source, { events: [event('while-paused', 'a', ++time, 'Work continues')] })).accepted, 1);
    assert.equal((await core.snapshot()).tasks[0].events.length, 2);
    await core.setAnalyzer({ async analyze(input) { return result(input); } });
    assert.equal((await core.snapshot()).tasks[0].analysis.state, 'paused');
    await core.resume();
    assert.equal((await core.diagnostics()).runtime.cancelledUnsettledRequests, 1);
    finish();
    await waitFor(async () => (await core.snapshot()).tasks[0].analysis.state === 'cooldown');
    time += 501;
    const state = await core.snapshot();
    assert.equal(state.sources[0].status, 'disconnected');
    assert.equal(state.tasks[0].status, 'unknown');
    assert.equal(state.tasks[0].analysis.state, 'stale');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});
