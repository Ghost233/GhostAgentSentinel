import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const DAY = 86_400_000;
const checks = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];

test('unchanged heartbeat, notification polling and diagnostics do not walk retained event history', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'core-maintenance-'));
  let clock = DAY * 2;
  const core = await createCore({ dataDir, now: () => clock });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const source = await core.registerSource({ registrationId: 'desktop', hostRunId: 'host', name: 'Desktop' });
  const tasks = ['one', 'two', 'three'].map(id => ({ id, lifecycleId: 'life', title: id, status: 'running' }));
  await core.ingest(source, { tasks, events: tasks.flatMap(task => ['input', 'output', 'tool-result'].map((kind, index) => ({
    id: String(index), taskId: task.id, taskLifecycleId: task.lifecycleId, kind, text: kind,
    capturedAt: clock - 100 + index, occurredAt: null,
  }))) });
  let visits = 0;
  const values = Map.prototype.values;
  Map.prototype.values = function* () {
    for (const value of values.call(this)) {
      if (value?.taskKey && Number.isSafeInteger(value.sequence) && Number.isSafeInteger(value.capturedAt) && value.kind) visits++;
      yield value;
    }
  };
  try {
    clock++;
    await core.heartbeat(source);
    await core.claimNotifications(source);
    const diagnostic = await core.diagnostics();
    assert.equal(diagnostic.retained.events, 9);
    assert.equal(diagnostic.runtime.unsettledAnalysisCalls, 0);
    assert.equal(visits, 0, 'unchanged control requests must not traverse the retained event map');
  } finally { Map.prototype.values = values; }
});

test('completed unchanged analysis reuses its material while time conditions still advance', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'core-maintenance-reuse-'));
  let clock = DAY * 2, analyses = 0;
  const threshold = clock + 120_000;
  const core = await createCore({ dataDir, now: () => clock, leaseMs: DAY * 3, analyzer: {
    timeConditions(input, at) { return [at >= threshold, input.events.length]; },
    async analyze(input) {
      analyses++;
      return { summary: { currentTask: input.task.currentRequest, currentState: 'Observed', recentProgress: '', attention: '' }, checks: checks.map(id => ({ id, status: 'unknown', reason: 'Test evidence', evidenceIds: [] })), coverage: input.coverage };
    },
  } });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const source = await core.registerSource({ registrationId: 'desktop', hostRunId: 'host', name: 'Desktop' });
  await core.ingest(source, { tasks: [{ id: 'one', lifecycleId: 'life', title: 'One', status: 'running' }], events: [{
    id: 'request', taskId: 'one', taskLifecycleId: 'life', kind: 'input', text: 'Read only', capturedAt: clock, occurredAt: null,
  }] });
  for (let attempt = 0; attempt < 20 && (analyses !== 1 || (await core.diagnostics()).runtime.activeAnalyses); attempt++) await nextTurn();
  assert.equal(analyses, 1);
  let clones = 0;
  const clone = globalThis.structuredClone;
  globalThis.structuredClone = value => {
    if (value?.kind && Number.isSafeInteger(value.capturedAt)) clones++;
    return clone(value);
  };
  try {
    clock += 60_000;
    await core.heartbeat(source);
    await core.claimNotifications(source);
    await core.diagnostics();
    assert.equal(analyses, 1);
    assert.equal(clones, 0, 'unchanged checks must not clone the same event bodies on each control read');
  } finally { globalThis.structuredClone = clone; }
  clock = threshold;
  await core.heartbeat(source);
  for (let attempt = 0; attempt < 20 && analyses !== 2; attempt++) await nextTurn();
  assert.equal(analyses, 2, 'a time condition crossing its boundary must still schedule analysis');
});

test('event indexes preserve source isolation, late capture order, restart and exact expiry', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'core-maintenance-expiry-'));
  let clock = DAY * 2;
  let core = await createCore({ dataDir, now: () => clock, leaseMs: DAY * 3 });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const descriptors = ['a', 'b'].map(id => ({ registrationId: id, hostRunId: 'host', name: id }));
  const sources = await Promise.all(descriptors.map(descriptor => core.registerSource(descriptor)));
  const task = { id: 'same', lifecycleId: 'life', title: 'Same task ID', status: 'running' };
  const event = (id, capturedAt, text) => ({ id, capturedAt, text, taskId: task.id, taskLifecycleId: task.lifecycleId, kind: 'output', occurredAt: null });
  await core.ingest(sources[0], { tasks: [task], events: [event('newer', clock - 10, 'a new'), event('older', clock - DAY + 5, 'a old')] });
  await core.ingest(sources[1], { tasks: [task], events: [event('newer', clock - 10, 'b new')] });
  let snapshot = await core.snapshot();
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === sources[0]).events.map(value => value.id), ['older', 'newer']);
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === sources[1]).events.map(value => value.text), ['b new']);
  assert.equal((await core.diagnostics()).retention.nextExpiryAt, clock + 5);
  await core.close();
  core = await createCore({ dataDir, now: () => clock, leaseMs: DAY * 3 });
  for (const descriptor of descriptors) {
    const source = await core.registerSource(descriptor);
    await core.ingest(source, { tasks: [task] });
  }
  clock += 5;
  snapshot = await core.snapshot();
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === sources[0]).events.map(value => value.id), ['newer']);
  assert.equal((await core.diagnostics()).retained.events, 2);
  clock += DAY;
  assert.equal((await core.diagnostics()).retained.events, 0);
  await core.ingest(sources[0], { events: [event('fresh', clock, 'new observation')] });
  snapshot = await core.snapshot();
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === sources[0]).events.map(value => value.id), ['fresh']);
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === sources[1]).events, []);
});
