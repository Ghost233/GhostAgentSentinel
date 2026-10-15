import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCore } from '../src/core/index.mjs';
import { normalizeEnrollment } from '../src/enrollment-config.mjs';

const DAY = 24 * 60 * 60 * 1000;
const task = { id: 'same-task', lifecycleId: 'life', title: 'Task', status: 'running' };
const event = (text, time) => ({ id: 'request', taskId: task.id, taskLifecycleId: task.lifecycleId, kind: 'input', occurredAt: null, capturedAt: time, text });

test('configured sources show missing enrollment while self-registration keeps machine discovery unknown', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-'));
  const core = await createCore({ dataDir });
  try {
    const unlisted = await core.registerSource({ registrationId: 'unlisted', hostRunId: 'one', name: 'Unlisted' });
    await core.ingest(unlisted, { tasks: [] });
    let state = await core.snapshot();
    assert.equal(state.enrollment.coverage.complete, false);
    const configured = await core.configureSources([
      { registrationId: 'desktop', name: 'Desktop', profile: '/profiles/desktop', enabled: true },
      { registrationId: 'cli', name: 'CLI', enabled: true },
    ]);
    assert.equal(configured.configured, 2);
    assert.equal(configured.missing, 2);
    assert.deepEqual(configured.entries.map(entry => entry.status), ['pending', 'pending']);
    const desktop = await core.registerSource({ registrationId: 'desktop', hostRunId: 'host', name: 'Runtime Desktop' });
    state = await core.snapshot();
    assert.equal(state.enrollment.entries[0].status, 'connecting');
    await core.ingest(desktop, { tasks: [] });
    state = await core.snapshot();
    assert.equal(state.enrollment.entries[0].status, 'connected');
    assert.equal(state.enrollment.connected, 1);
    assert.equal(state.enrollment.missing, 1);
    assert.equal(state.enrollment.coverage.complete, false);
    assert.deepEqual(state.enrollment.unlisted.map(source => source.sourceKey), [unlisted]);
    assert.equal(state.enrollment.discovery.complete, false);
    assert.equal(state.overview.coverage.complete, false);
    const cli = await core.registerSource({ registrationId: 'cli', hostRunId: 'cli-host', name: 'CLI' });
    await core.ingest(cli, { tasks: [] });
    state = await core.snapshot();
    assert.equal(state.enrollment.coverage.complete, true);
    assert.equal(state.enrollment.unlisted.length, 1);
    assert.equal(state.enrollment.discovery.complete, false);
    assert.equal(state.overview.coverage.complete, false);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('expired source leases remain visibly disconnected while another configuration continues', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-lease-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: 500 });
  try {
    await core.configureSources([
      { registrationId: 'desktop', name: 'Desktop', enabled: true },
      { registrationId: 'cli', name: 'CLI', enabled: true },
    ]);
    const desktop = await core.registerSource({ registrationId: 'desktop', hostRunId: 'desktop', name: 'Desktop' });
    const cli = await core.registerSource({ registrationId: 'cli', hostRunId: 'cli', name: 'CLI' });
    await core.ingest(desktop, { tasks: [task] });
    await core.ingest(cli, { tasks: [task] });
    time += 300;
    await core.heartbeat(cli);
    time += 201;
    const state = await core.snapshot();
    assert.equal(state.enrollment.entries[0].status, 'disconnected');
    assert.equal(state.enrollment.entries[0].lastSeenAt, 1_000_000);
    assert.equal(state.enrollment.entries[1].status, 'connected');
    assert.equal(state.enrollment.entries[1].lastSeenAt, 1_000_300);
    assert.equal(state.enrollment.missing, 1);
    assert.equal(state.enrollment.coverage.complete, false);
    assert.equal(state.tasks.find(value => value.sourceKey === desktop).status, 'unknown');
    assert.equal(state.tasks.find(value => value.sourceKey === cli).status, 'running');
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('multiple hosts in one configuration remain independent and one disconnect does not hide unknown coverage', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-hosts-'));
  let time = 1_000_000;
  const core = await createCore({ dataDir, now: () => time, leaseMs: DAY * 2 });
  try {
    await core.configureSources([{ registrationId: 'desktop', name: 'Desktop', enabled: true }]);
    const a = await core.registerSource({ registrationId: 'desktop', hostRunId: 'a', name: 'Desktop A' });
    const b = await core.registerSource({ registrationId: 'desktop', hostRunId: 'b', name: 'Desktop B' });
    await core.ingest(a, { tasks: [task], events: [event('A request', time)] });
    await core.ingest(b, { tasks: [task], events: [event('B request', ++time)] });
    let state = await core.snapshot();
    assert.equal(state.enrollment.connected, 1);
    assert.equal(state.enrollment.entries[0].connectedSources, 2);
    assert.equal(state.enrollment.coverage.complete, true);
    assert.equal(state.enrollment.discovery.complete, false);
    assert.deepEqual(state.tasks.map(value => value.currentRequest).sort(), ['A request', 'B request']);
    await core.unregisterSource(a, 'Adapter connection lost');
    state = await core.snapshot();
    assert.equal(state.enrollment.entries[0].connectedSources, 1);
    assert.equal(state.enrollment.entries[0].disconnectedSources, 1);
    assert.equal(state.enrollment.coverage.complete, false);
    assert.equal(state.tasks.find(value => value.sourceKey === a).status, 'unknown');
    assert.equal(state.tasks.find(value => value.sourceKey === b).status, 'running');
    await core.configureSources([{ registrationId: 'desktop', name: 'Desktop', enabled: false }]);
    state = await core.snapshot();
    assert.equal(state.enrollment.entries[0].status, 'disabled');
    assert.equal(state.tasks.find(value => value.sourceKey === b).status, 'running');
    assert.equal(state.overview.connectedSources, 1);
    assert.equal(state.enrollment.coverage.complete, false);
    await core.configureSources([]);
    assert.equal((await core.snapshot()).enrollment.unlisted.length, 2);
    assert.equal((await core.snapshot()).tasks.length, 2);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('installation plans survive 24-hour observation expiry and restart without restoring runtime claims', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-restore-'));
  let time = 1_000_000;
  let core = await createCore({ dataDir, now: () => time, leaseMs: DAY * 2 });
  try {
    const source = await core.registerSource({ registrationId: 'desktop', hostRunId: 'a', name: 'Desktop' });
    await core.ingest(source, { tasks: [task], events: [event('private observation', time)] });
    await core.configureSources([
      { registrationId: 'desktop', name: 'Desktop', enabled: true },
      { registrationId: 'future', name: 'Next CLI', profile: '/profiles/cli', enabled: true },
    ]);
    await core.pause();
    await core.close();
    core = await createCore({ dataDir, now: () => time });
    let state = await core.snapshot();
    assert.equal(state.monitoring.paused, true);
    assert.equal(state.enrollment.entries[0].status, 'disconnected');
    assert.equal(state.enrollment.entries[1].status, 'pending');
    assert.equal(state.enrollment.coverage.complete, false);
    time += DAY + 1;
    state = await core.snapshot();
    assert.equal(state.sources.length, 0);
    assert.equal(state.tasks.length, 0);
    assert.equal(state.enrollment.entries.length, 2);
    assert.equal(state.enrollment.entries[0].lastSeenAt, 1_000_000);
    assert.equal(state.enrollment.entries[0].status, 'disconnected');
    assert.deepEqual(state.enrollment.entries[0].sourceKeys, []);
    await core.close();
    core = await createCore({ dataDir, now: () => time });
    const plan = await core.sourcePlan();
    assert.equal(plan.entries[0].status, 'disconnected');
    assert.equal(plan.entries[1].status, 'pending');
    const text = JSON.stringify(plan);
    assert.equal(text.includes('private observation'), false);
    const restarted = await core.registerSource({ registrationId: 'desktop', hostRunId: 'b', name: 'Desktop' });
    assert.notEqual(restarted, source);
    assert.equal((await core.sourcePlan()).entries[0].status, 'connecting');
    await core.ingest(restarted, { tasks: [] });
    assert.equal((await core.sourcePlan()).entries[0].status, 'connected');
    assert.equal((await core.sourcePlan()).entries[0].lastSeenAt, time);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('enrollment input rejects credentials and duplicate identities without changing the current plan', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-schema-'));
  const core = await createCore({ dataDir });
  try {
    const entries = [{ registrationId: 'desktop', name: 'Desktop', enabled: true }];
    await core.configureSources(entries);
    for (const invalid of [
      [{ ...entries[0], token: 'secret' }],
      [{ ...entries[0], name: 'authorization: Bearer private' }],
      [{ ...entries[0], profile: 'http://localhost/?token=secret' }],
      [entries[0], entries[0]],
      [{ ...entries[0], enabled: 'true' }],
    ]) await assert.rejects(core.configureSources(invalid));
    assert.equal((await core.sourcePlan()).entries[0].name, 'Desktop');
    assert.equal((await core.sourcePlan()).entries.length, 1);
    assert.throws(() => normalizeEnrollment(Array.from({ length: 101 }, (_, index) => ({ ...entries[0], registrationId: String(index) }))), /bounded/);
    assert.deepEqual(normalizeEnrollment(entries), entries);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('enabled adapters enroll stable configurations once across independent hosts and restart', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-automatic-'));
  let time = 1_000_000;
  let core = await createCore({ dataDir, now: () => time, leaseMs: DAY * 2 });
  try {
    const descriptor = { registrationId: 'desktop-profile', hostRunId: 'a', name: 'Desktop', profile: '/profiles/desktop', enroll: true };
    const a = await core.registerSource(descriptor);
    let plan = await core.sourcePlan();
    assert.equal(plan.entries.length, 1);
    assert.equal(plan.entries[0].enabled, true);
    assert.equal(plan.entries[0].status, 'connecting');
    assert.equal(plan.entries[0].profile, '/profiles/desktop');
    const b = await core.registerSource({ ...descriptor, hostRunId: 'b', name: 'Another runtime name' });
    await core.ingest(a, { tasks: [task], events: [event('A request', time)] });
    await core.ingest(b, { tasks: [task], events: [event('B request', ++time)] });
    plan = await core.sourcePlan();
    assert.equal(plan.entries.length, 1);
    assert.equal(plan.entries[0].name, 'Desktop');
    assert.equal(plan.entries[0].connectedSources, 2);
    assert.equal(plan.coverage.complete, true);
    assert.equal(plan.discovery.complete, false);
    await core.unregisterSource(a, 'Adapter disconnected');
    plan = await core.sourcePlan();
    assert.equal(plan.entries[0].disconnectedSources, 1);
    assert.equal(plan.coverage.complete, false);
    await core.close();
    core = await createCore({ dataDir, now: () => time });
    plan = await core.sourcePlan();
    assert.equal(plan.entries.length, 1);
    assert.equal(plan.entries[0].status, 'disconnected');
    assert.equal(await core.registerSource({ ...descriptor, hostRunId: 'b' }), b);
    assert.equal((await core.sourcePlan()).entries.length, 1);
    await core.registerSource({ registrationId: 'cli-profile', hostRunId: 'cli', name: 'CLI', enroll: true });
    assert.equal((await core.sourcePlan()).entries.length, 2);
    assert.equal((await core.snapshot()).tasks.length, 2);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});

test('explicit user plans suppress automatic re-enrollment, including disable, removal and restart', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-enrollment-managed-'));
  let core = await createCore({ dataDir });
  try {
    const descriptor = { registrationId: 'desktop-profile', hostRunId: 'a', name: 'Desktop', enroll: true };
    const a = await core.registerSource(descriptor);
    await core.ingest(a, { tasks: [task] });
    assert.equal((await core.sourcePlan()).entries.length, 1);
    await core.configureSources([{ registrationId: descriptor.registrationId, name: 'User name', enabled: false }]);
    await core.registerSource({ ...descriptor, hostRunId: 'b' });
    const cli = await core.registerSource({ registrationId: 'cli-profile', hostRunId: 'cli', name: 'CLI', enroll: true });
    let state = await core.snapshot();
    assert.equal(state.enrollment.entries.length, 1);
    assert.equal(state.enrollment.entries[0].enabled, false);
    assert.equal(state.enrollment.entries[0].name, 'User name');
    assert.equal(state.enrollment.entries[0].status, 'disabled');
    assert.equal(state.enrollment.unlisted[0].sourceKey, cli);
    assert.equal(state.tasks[0].status, 'running');
    await core.configureSources([]);
    state = await core.snapshot();
    assert.equal(state.enrollment.entries.length, 0);
    assert.equal(state.enrollment.unlisted.length, 3);
    assert.equal(state.overview.connectedSources, 3);
    await core.close();
    core = await createCore({ dataDir });
    await core.registerSource({ ...descriptor, hostRunId: 'new-host' });
    assert.equal((await core.sourcePlan()).entries.length, 0);
    assert.equal((await core.sourcePlan()).coverage.complete, false);
    assert.equal((await core.sourcePlan()).discovery.complete, false);
  } finally { await core.close(); await rm(dataDir, { recursive: true, force: true }); }
});
