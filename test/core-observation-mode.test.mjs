import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { createCore } from '../src/core/index.mjs';
import { controlledModelService } from './helpers/controlled-model-service.mjs';
import { configureInstallationModels } from './helpers/installation-model-settings.mjs';

const descriptor = { registrationId: 'chatgpt', hostRunId: 'run', name: 'Current main', mode: 'observation-only' };
const task = { id: 'main', lifecycleId: 'turn', sessionId: 'main', title: 'Current main', status: 'running' };
const event = (id, time, kind = 'input') => ({ id, taskId: task.id, taskLifecycleId: task.lifecycleId,
  kind, capturedAt: time, occurredAt: null, text: id });

test('observation-only sources expose real lifecycle without analysis or silence alerts through HTTP', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-observation-only-'));
  let time = 1_000_000;
  const server = await startServer({ dataDir, now: () => time, leaseMs: 1e9 });
  let client;
  t.after(async () => { await client?.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  client = await connectCore({ dataDir });
  const sourceKey = await client.registerSource(descriptor);
  await client.ingest(sourceKey, { tasks: [task], events: [event('request', time), {
    ...event('tool-start', time, 'tool-start'), data: { callId: 'tool', toolName: 'wait' },
  }] });
  let state = await client.snapshot();
  assert.equal(state.tasks[0].analysis.state, 'observation-only');
  assert.equal(state.tasks[0].status, 'running');
  assert.equal(state.sources[0].descriptor.mode, 'observation-only');
  time += 301_000;
  state = await client.snapshot();
  assert.equal(state.tasks[0].analysis.completedAt, null);
  assert.equal(state.tasks[0].analysis.state, 'observation-only');
  assert.equal(state.tasks[0].analysis.checks.every(check => check.status === 'unknown'), true);
  assert.equal(state.tasks[0].gs.light, 'gray');
  assert.equal(state.tasks[0].gs.label, '只读观察');
  assert.equal(state.tasks[0].alerts.length, 0);
  assert.deepEqual(await client.claimNotifications(sourceKey), []);
  assert.equal((await client.diagnostics()).runtime.activeAnalyses, 0);
  assert.equal((await client.diagnostics()).temporary.queueKeys, 0);
  assert.ok((await client.health()).features.includes('source-observation/1'));
  await client.ingest(sourceKey, { tasks: [{ ...task, status: 'idle' }] });
  state = await client.snapshot();
  assert.equal(state.tasks[0].status, 'idle');
  assert.equal(state.tasks[0].gs.light, null);
});

test('switching to observation-only seals late analysis while the connected peer continues', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-observation-switch-'));
  const calls = [];
  const core = await createCore({ dataDir, leaseMs: 1e9, analyzer: { analyze(input, { signal }) {
    return new Promise(resolve => calls.push({ input, signal, resolve }));
  } } });
  const result = (input, attention = false) => ({
    summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' },
    checks: ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'].map(id => ({
      id, status: attention && id === 'request_drift' ? 'attention' : 'unknown', reason: 'Controlled result',
      evidenceIds: attention && id === 'request_drift' ? [input.events[0].id] : [],
    })), coverage: input.coverage, quality: 'unvalidated',
  });
  t.after(async () => { for (const call of calls) call.resolve(result(call.input)); await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  async function waitFor(predicate) {
    for (let i = 0; i < 200; i++) { if (await predicate()) return; await delay(5); }
    assert.fail('Expected observable core state');
  }
  const { mode, ...normal } = descriptor;
  const source = await core.registerSource(normal);
  const peer = await core.registerSource({ ...normal, registrationId: 'peer' });
  await core.ingest(source, { tasks: [task], events: [event('request', Date.now())] });
  await waitFor(() => calls.length === 1);
  await core.ingest(peer, { tasks: [task], events: [event('peer-request', Date.now())] });
  assert.equal(await core.registerSource(descriptor), source);
  assert.equal(calls[0].signal.aborted, true);
  calls[0].resolve(result(calls[0].input, true));
  await waitFor(() => calls.length === 2);
  assert.equal(calls[1].input.source.registrationId, 'peer');
  assert.equal(calls[1].signal.aborted, false);
  calls[1].resolve(result(calls[1].input));
  await waitFor(async () => (await core.diagnostics()).runtime.unsettledAnalysisCalls === 0);
  const observed = (await core.snapshot()).tasks.find(value => value.sourceKey === source);
  assert.equal(observed.analysis.state, 'observation-only');
  assert.equal(observed.analysis.completedAt, null);
  assert.equal(observed.analysis.summary, null);
  assert.deepEqual(observed.alerts.filter(alert => alert.status === 'attention'), []);
  assert.deepEqual(await core.claimNotifications(source), []);
  await core.registerSource(normal);
  await waitFor(() => calls.length === 3);
  calls[2].resolve(result(calls[2].input));
  await waitFor(async () => (await core.snapshot()).tasks.find(value => value.sourceKey === source).analysis.state === 'cooldown');
});

test('an initial ended or unknown task still confirms observation-only mode through HTTP', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-observation-baseline-'));
  const server = await startServer({ dataDir });
  let client;
  t.after(async () => { await client?.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  client = await connectCore({ dataDir });
  const source = await client.registerSource(descriptor);
  await client.ingest(source, { tasks: [{ ...task, status: 'idle' }, { ...task, id: 'unknown', status: 'unknown' }] });
  let state = await client.snapshot();
  assert.deepEqual(state.tasks.map(value => value.analysis.state), ['observation-only', 'observation-only']);
  assert.deepEqual(state.tasks.map(value => value.status), ['idle', 'unknown']);
  assert.equal(state.tasks.every(value => value.analysis.completedAt === null), true);
  await client.unregisterSource(source);
  state = await client.snapshot();
  assert.equal(state.sources[0].status, 'disconnected');
  assert.equal(state.tasks.every(value => value.status === 'unknown'), true);
  assert.equal(state.tasks.every(value => value.analysis.state === 'observation-only'), true);
});

test('invalid modes are atomic and default sources still analyze independently through HTTP', async t => {
  const models = await controlledModelService(t);
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-observation-compatibility-'));
  const server = await startServer({ dataDir });
  let client;
  t.after(async () => { await client?.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  client = await connectCore({ dataDir });
  const observed = await client.registerSource(descriptor);
  await assert.rejects(client.registerSource({ ...descriptor, mode: 'silent' }), error => error.status === 400);
  assert.equal((await client.snapshot()).sources[0].descriptor.mode, 'observation-only');
  const { mode, ...normal } = descriptor;
  await configureInstallationModels(client, 'legacy', models.config);
  const peer = await client.registerSource({ ...normal, registrationId: 'legacy' });
  const time = Date.now();
  await client.ingest(observed, { tasks: [task, { ...task, id: 'second' }], events: [event('request', time)] });
  await client.ingest(peer, { tasks: [task], events: [event('legacy-request', time), event('legacy-output', time, 'output')] });
  let state;
  for (let i = 0; i < 100; i++) {
    state = await client.snapshot();
    if (state.tasks.find(value => value.sourceKey === peer).analysis.completedAt !== null) break;
    await delay(5);
  }
  const legacy = state.tasks.find(value => value.sourceKey === peer);
  assert.notEqual(legacy.analysis.completedAt, null);
  assert.equal(legacy.analysis.error, null);
  assert.notEqual(legacy.analysis.state, 'observation-only');
  assert.equal(Object.hasOwn(state.sources.find(value => value.key === peer).descriptor, 'mode'), false);
  assert.equal(state.tasks.find(value => value.sourceKey === observed && value.id === task.id).analysis.completedAt, null);
  await client.ingest(observed, { tasks: [{ ...task, status: 'unknown', gaps: ['Project qualification revoked'] }] });
  state = await client.snapshot();
  assert.equal(state.tasks.find(value => value.sourceKey === observed && value.id === task.id).status, 'unknown');
  assert.equal(state.tasks.find(value => value.sourceKey === observed && value.id === 'second').status, 'running');
  assert.equal(state.sources.find(value => value.key === observed).status, 'connected');
});

test('only observation-only unknown descriptors leave the task baseline unconfirmed through HTTP', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-observation-progress-'));
  const server = await startServer({ dataDir });
  const client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const observed = await client.registerSource(descriptor);
  const { mode, ...normal } = descriptor;
  const legacy = await client.registerSource({ ...normal, registrationId: 'legacy-progress' });
  await client.ingest(observed, { tasks: [{ ...task, status: 'unknown' }] });
  await client.ingest(legacy, { tasks: [{ ...task, status: 'unknown' }] });
  let snapshot = await client.snapshot();
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === observed).coverage, { complete: false, gaps: ['Task baseline is unconfirmed'] });
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === legacy).coverage, { complete: true, gaps: [] });
  for (const status of ['running', 'idle']) {
    await client.ingest(observed, { tasks: [{ ...task, status }] });
    snapshot = await client.snapshot();
    assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === observed).coverage, { complete: true, gaps: [] });
    assert.equal(snapshot.tasks.find(value => value.sourceKey === observed).analysis.state, 'observation-only');
  }
  await client.ingest(observed, { tasks: [{ ...task, status: 'unknown', gaps: ['Fixed genuine capture loss'] }] });
  await client.ingest(observed, { tasks: [{ ...task, status: 'running', gaps: [] }] });
  snapshot = await client.snapshot();
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === observed).coverage, { complete: false, gaps: ['Fixed genuine capture loss'] });
  assert.deepEqual(snapshot.tasks.find(value => value.sourceKey === legacy).coverage, { complete: true, gaps: [] });
});
