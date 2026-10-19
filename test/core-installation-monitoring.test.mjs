import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createCore } from '../src/core/index.mjs';

const ids = ['stalled_progress', 'request_drift', 'failure_loop', 'validation_cost', 'validation_gap', 'long_wait'];
const result = input => ({ summary: { currentTask: input.task.currentRequest, currentState: 'observed', recentProgress: '', attention: '' }, checks: ids.map(id => ({ id, status: 'unknown', reason: 'Controlled external analysis has insufficient evidence', evidenceIds: [] })), coverage: input.coverage });
async function until(predicate) { for (let n = 0; n < 100; n++) { if (await predicate()) return; await delay(2); } assert.fail('expected public analysis state'); }

test('pausing an installation seals only its round and keeps collection and other installation work eligible', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-monitoring-')), calls = [], releases = [];
  const core = await createCore({ dataDir, analyzer: { analyze(input, { signal }) { calls.push({ input, signal }); return new Promise(resolve => releases.push(() => resolve(result(input)))); } } });
  t.after(async () => { for (const release of releases) release(); await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const a = await core.registerSource({ registrationId: 'a', hostRunId: 'host', name: 'A' }), b = await core.registerSource({ registrationId: 'b', hostRunId: 'host', name: 'B' });
  const observe = (source, id) => core.ingest(source, { tasks: [{ id, lifecycleId: 'life', status: 'running', title: id }], events: [{ id: id + '-input', taskId: id, taskLifecycleId: 'life', kind: 'input', text: id + ' request', capturedAt: Date.now(), occurredAt: null }] });
  await observe(a, 'a-first'); await until(() => calls.length === 1); await observe(b, 'b-first');
  const initial = await core.installationMonitoring('a'), other = await core.installationMonitoring('b');
  const changed = await core.setInstallationMonitoring('a', true, initial.revision);
  assert.equal(changed.paused, true); assert.equal(calls[0].signal.aborted, true);
  assert.deepEqual(await core.installationMonitoring('b'), other);
  assert.equal((await observe(a, 'a-future')).accepted, 1, 'a persisted installation pause does not stop new collection');
  releases[0](); await until(() => calls.length === 2);
  assert.equal(calls[1].input.task.id, 'b-first');
  const snapshot = await core.snapshot();
  assert.ok(snapshot.tasks.filter(task => task.sourceKey === a).every(task => task.analysis.state === 'paused'));
  assert.equal(snapshot.tasks.find(task => task.sourceKey === b).analysis.state, 'analyzing');
  assert.equal(snapshot.monitoring.paused, false);
  await assert.rejects(core.setInstallationMonitoring('a', false, initial.revision), error => error.code === 'installation_monitoring_conflict');
});

test('installation pause survives reconnect and reopening while installation resume preserves manual chat pause', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-monitoring-reopen-'));
  let core = await createCore({ dataDir });
  t.after(async () => { await core.close(); await rm(dataDir, { recursive: true, force: true }); });
  const paused = await core.setInstallationMonitoring('chatgpt:owned', true, (await core.installationMonitoring('chatgpt:owned')).revision);
  await core.close(); core = await createCore({ dataDir });
  assert.deepEqual(await core.installationMonitoring('chatgpt:owned'), paused);
  const source = await core.registerSource({ registrationId: 'chatgpt:owned', hostRunId: 'new-host', name: 'Owned', policy: 'chatgpt-main/1' });
  await core.ingest(source, { tasks: [{ id: 'main', lifecycleId: 'life', status: 'running', title: 'Main' }] });
  assert.equal((await core.snapshot()).tasks[0].analysis.state, 'paused');
  const chat = await core.conversationMonitoring({ registrationId: 'chatgpt:owned', taskId: 'main' });
  await core.setConversationMonitoring({ ...chat, paused: true });
  await core.setInstallationMonitoring('chatgpt:owned', false, paused.revision);
  assert.equal((await core.snapshot()).tasks[0].analysis.state, 'paused');
  assert.equal((await core.conversationMonitoring({ registrationId: 'chatgpt:owned', taskId: 'main' })).paused, true);
  await core.setInstallationMonitoring('chatgpt:owned', true, (await core.installationMonitoring('chatgpt:owned')).revision);
  await core.unregisterSource(source);
  await core.purgeChatGPTInstallation({ installationId: 'owned' });
  assert.equal((await core.installationMonitoring('chatgpt:owned')).paused, false, 'full uninstall clears only its durable installation pause');
});
