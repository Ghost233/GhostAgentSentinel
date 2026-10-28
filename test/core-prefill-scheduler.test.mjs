import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
async function until(predicate) {
  for (let attempt = 0; attempt < 50; attempt++) { if (await predicate()) return; await nextTurn(); }
  assert.fail('Expected controlled scheduler state');
}
async function fixture(t) {
  const dir = await mkdtemp(join(tmpdir(), 'core-prefill-schedule-'));
  let clock = 2_000_000;
  const calls = [];
  const pending = [];
  const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'Observed', recentProgress: '', attention: '' }, checks: checks.map(id => ({ id, status: 'unknown', reason: 'Test evidence', evidenceIds: [] })), coverage: input.coverage });
  const analyzer = {
    timeConditions() { return []; },
    async analyze(input) { calls.push({ kind: 'analysis', input }); return result(input); },
    prefill(input, options) {
      calls.push({ kind: 'prefill', input, signal: options.signal, onState: options.onState });
      return new Promise((resolve, reject) => pending.push({ resolve: () => resolve({ state: 'completed' }), reject }));
    },
  };
  const core = await createCore({ dataDir: dir, analyzer, now: () => clock, leaseMs: 1_000_000 });
  t.after(async () => { await core.close(); for (const value of pending.splice(0)) value.resolve(); await nextTurn(); await rm(dir, { recursive: true, force: true }); });
  const source = await core.registerSource({ registrationId: 'desktop', hostRunId: 'host', name: 'Desktop' });
  const observe = async (taskId, id, kind = 'output') => core.ingest(source, { events: [{ id, taskId, taskLifecycleId: 'life', kind, text: id, occurredAt: null, capturedAt: ++clock }] });
  const start = async ids => {
    await core.ingest(source, { tasks: ids.map(id => ({ id, lifecycleId: 'life', title: id, status: 'running' })), events: ids.map(taskId => ({ id: 'request', taskId, taskLifecycleId: 'life', kind: 'input', text: taskId, occurredAt: null, capturedAt: clock })) });
    await until(() => calls.filter(call => call.kind === 'analysis').length === ids.length);
    await until(async () => (await core.diagnostics()).runtime.activeAnalyses === 0);
  };
  return { core, calls, pending, start, observe, advance(ms) { clock += ms; }, get clock() { return clock; } };
}

test('ready complete analyses precede queued prefill without interrupting an active prefill', async t => {
  const f = await fixture(t);
  await f.start(['a', 'b']);
  assert.deepEqual(f.calls.map(call => call.kind), ['analysis', 'analysis']);
  const completed = (await f.core.snapshot()).tasks.map(task => task.analysis.nextEligibleAt);
  await f.observe('a', 'a-1');
  await until(() => f.pending.length === 1);
  await f.observe('b', 'b-1');
  f.advance(60_000);
  await f.core.snapshot();
  assert.deepEqual(f.calls.map(call => call.kind), ['analysis', 'analysis', 'prefill']);
  assert.equal(f.calls[2].signal.aborted, false);
  f.pending.shift().resolve();
  await until(() => f.calls.filter(call => call.kind === 'analysis').length === 4);
  assert.deepEqual(f.calls.slice(3).map(call => call.kind), ['analysis', 'analysis']);
  assert.deepEqual(f.calls.slice(3).map(call => call.input.task.id).sort(), ['a', 'b']);
  assert.ok((await f.core.snapshot()).tasks.every(task => task.analysis.nextEligibleAt > Math.max(...completed)));
});

test('new material is coalesced and the current prefill input stays frozen', async t => {
  const f = await fixture(t);
  await f.start(['a']);
  const eligibleAt = (await f.core.snapshot()).tasks[0].analysis.nextEligibleAt;
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  await f.observe('a', 'two');
  await f.observe('a', 'three');
  await f.core.diagnostics();
  assert.deepEqual(f.calls[1].input.events.map(event => event.id), ['request', 'one']);
  assert.equal(f.calls.length, 2);
  f.pending.shift().resolve();
  await until(() => f.calls.length === 3);
  assert.deepEqual(f.calls[2].input.events.map(event => event.id), ['request', 'one', 'two', 'three']);
  assert.equal((await f.core.snapshot()).tasks[0].analysis.nextEligibleAt, eligibleAt, 'prefill must not restart complete-analysis cooldown');
  f.pending.shift().resolve();
  await until(async () => (await f.core.diagnostics()).runtime.activePrefills === 0);
  await f.core.diagnostics();
  assert.equal(f.calls.length, 3, 'unchanged polling must not repeat prefill');
});

test('pause cancels its prefill but retains admission until the controlled call really settles', async t => {
  const f = await fixture(t);
  await f.start(['a']);
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  await f.core.pause();
  assert.equal(f.calls[1].signal.aborted, true);
  await f.observe('a', 'two');
  await f.core.resume();
  assert.equal((await f.core.diagnostics()).runtime.cancelledUnsettledRequests, 1);
  assert.equal(f.calls.length, 2);
  f.pending.shift().resolve();
  await until(() => f.calls.length === 3);
  assert.deepEqual(f.calls[2].input.events.map(event => event.id), ['request', 'one', 'two']);
  assert.equal(f.calls[2].kind, 'prefill');
});

test('settled prefill failure backs off without changing the successful analysis or spinning on new material', async t => {
  const f = await fixture(t);
  await f.start(['a']);
  const before = (await f.core.snapshot()).tasks[0].analysis;
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  f.pending.shift().reject(new Error('Controlled adapter failure after release'));
  await until(async () => (await f.core.diagnostics()).runtime.activePrefills === 0);
  await f.observe('a', 'two');
  await f.core.diagnostics();
  const after = (await f.core.snapshot()).tasks[0].analysis;
  assert.equal(f.calls.length, 2);
  assert.deepEqual(after.summary, before.summary);
  assert.equal(after.nextEligibleAt, before.nextEligibleAt);
  assert.equal(after.prefill.state, 'failed');
});

test('closing releases pending prefill versions and seals a late in-flight completion', async t => {
  const f = await fixture(t);
  await f.start(['a', 'b']);
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  await f.observe('b', 'two');
  assert.equal((await f.core.diagnostics()).temporary.prefillQueueKeys, 1);
  await f.core.close();
  const closed = await f.core.diagnostics();
  assert.equal(closed.temporary.prefillQueueKeys, 0);
  assert.equal(closed.runtime.activePrefills, 0);
  assert.equal(closed.temporary.timers.total, 0);
  f.pending.shift().resolve();
  await until(async () => (await f.core.diagnostics()).runtime.cancelledUnsettledRequests === 0);
  assert.equal(f.calls.length, 3);
});

test('an analysis becoming due behind its own prefill is visibly queued', async t => {
  const f = await fixture(t);
  await f.start(['a']);
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  f.advance(60_000);
  const snapshot = await f.core.snapshot();
  assert.equal(snapshot.monitoring.queueLength, 1);
  assert.equal(snapshot.tasks[0].analysis.state, 'queued');
  assert.equal(f.calls.length, 2, 'in-flight prefill is still not interrupted or overlapped');
});

test('unknown prefill is visible while collection and control reads continue without releasing admission', async t => {
  const f = await fixture(t);
  await f.start(['a']);
  await f.observe('a', 'one');
  await until(() => f.pending.length === 1);
  assert.equal(typeof f.calls[1].onState, 'function');
  f.calls[1].onState('unknown');
  await f.observe('a', 'two');
  const snapshot = await f.core.snapshot(), counts = await f.core.diagnostics();
  assert.equal(snapshot.tasks[0].analysis.prefill.state, 'unknown');
  assert.ok(snapshot.tasks[0].events.some(event => event.id === 'two'));
  assert.equal(counts.runtime.activePrefills, 1);
  assert.equal(counts.runtime.analysisAdmissionBlocked, true);
  assert.equal(f.calls.length, 2);
  await f.core.pause();
  f.calls[1].onState('unknown');
  assert.equal((await f.core.snapshot()).tasks[0].analysis.prefill.state, 'cancelled', 'late progress cannot undo a sealed round');
});
