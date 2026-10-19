import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { configureInstallationModels } from './helpers/installation-model-settings.mjs';
import { createCore } from '../src/core/index.mjs';

test('unconfigured installations retain local wait facts and notifications without inference, generated summaries, or global fallback', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-unconfigured-wait-'));
  let clock = Date.now(), requests = 0;
  t.mock.method(Date, 'now', () => clock);
  const models = createServer((request, response) => { requests++; request.resume(); response.writeHead(503).end('{"error":"unexpected inference"}'); });
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve));
  const server = await startServer({ dataDir, now: () => clock, leaseMs: 1e9 }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }); });
  const baseUrl = 'http://127.0.0.1:' + models.address().port;
  await client.configureAnalysis({ llm: { baseUrl, model: 'old-global-summary' }, jev: { baseUrl, model: 'old-global-judge' } });
  await configureInstallationModels(client, 'summary-only', { llm: { baseUrl, model: 'partial-summary' } });
  await configureInstallationModels(client, 'judge-only', { jev: { baseUrl, model: 'partial-judge' } });
  const sources = {};
  for (const registrationId of ['empty', 'summary-only', 'judge-only']) {
    const sourceKey = await client.registerSource({ registrationId, hostRunId: 'host', name: registrationId });
    sources[registrationId] = sourceKey;
    const capturedAt = registrationId === 'summary-only' ? clock - 300_001 : clock;
    await client.ingest(sourceKey, { tasks: [{ id: 'actor', lifecycleId: 'life', title: 'Observe only', status: 'running' }], events: [
      { id: 'request', kind: 'input', text: 'Observe the current read-only task' },
      registrationId === 'summary-only'
        ? { id: 'work', kind: 'tool-start', text: 'Controlled tool wait', data: { callId: 'own-wait', toolName: 'fixed_wait' } }
        : { id: 'work', kind: 'output', text: 'Recent observed output' },
    ].map(event => ({ ...event, taskId: 'actor', taskLifecycleId: 'life', occurredAt: capturedAt, capturedAt })) });
  }
  const deadline = performance.now() + 2000;
  let snapshot;
  do {
    snapshot = await client.snapshot();
    if (snapshot.monitoring.activeTaskKey === null && snapshot.monitoring.queueLength === 0) break;
    assert.ok(performance.now() < deadline, 'bounded local checks settle without inference'); await delay(2);
  } while (true);
  for (const [registrationId, sourceKey] of Object.entries(sources)) {
    const task = snapshot.tasks.find(task => task.sourceKey === sourceKey), analysis = task.analysis;
    assert.notEqual(analysis.completedAt, null, registrationId + ' local checks completed');
    assert.equal(analysis.error, null);
    assert.equal(analysis.state, 'unavailable');
    assert.equal(analysis.summary.generated, undefined);
    assert.equal(analysis.configScope.registrationId, registrationId);
    assert.deepEqual(analysis.coverage.dependencies, registrationId === 'empty' ? ['llm_unconfigured', 'jev_unconfigured'] : [registrationId === 'summary-only' ? 'jev_unconfigured' : 'llm_unconfigured']);
    assert.ok(analysis.checks.filter(check => check.id !== 'long_wait').every(check => check.status === 'unknown'));
    const wait = analysis.checks.find(check => check.id === 'long_wait');
    assert.equal(wait.status, registrationId === 'summary-only' ? 'attention' : 'clear');
    assert.deepEqual(wait.evidenceIds, ['work']);
  }
  clock += 3000;
  assert.deepEqual(await client.claimNotifications(sources.empty), []);
  const notices = await client.claimNotifications(sources['summary-only']);
  assert.equal(notices.length, 1);
  assert.equal(notices[0].sourceKey, sources['summary-only']);
  assert.equal(notices[0].taskKey, snapshot.tasks.find(task => task.sourceKey === sources['summary-only']).key);
  assert.deepEqual(await client.claimNotifications(sources['judge-only']), []);
  assert.equal(requests, 0, 'neither incomplete installation nor the old global configuration may send inference');
});

test('raw cores require an explicit analyzer while the production server uses its injected clock for local wait facts', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-analysis-ownership-')), at = 1_000_000;
  const raw = await createCore({ dataDir: join(dataDir, 'raw'), now: () => at, leaseMs: 1e9 });
  let server, client;
  t.after(async () => { await raw.close(); await client?.close(); await server?.close(); await rm(dataDir, { recursive: true, force: true }); });
  const batch = { tasks: [{ id: 'task', lifecycleId: 'life', title: 'Task', status: 'running' }], events: [
    { id: 'request', kind: 'input', text: 'Observe the read-only task' }, { id: 'output', kind: 'output', text: 'Recent observed work' },
  ].map(event => ({ ...event, taskId: 'task', taskLifecycleId: 'life', occurredAt: at, capturedAt: at })) };
  const descriptor = { registrationId: 'clock', hostRunId: 'host', name: 'Clock' };
  const rawSource = await raw.registerSource(descriptor);
  await raw.ingest(rawSource, batch); await delay(0);
  assert.equal((await raw.snapshot()).tasks[0].analysis.completedAt, null, 'a raw core has no implicit analysis provider');
  assert.equal((await raw.diagnostics()).runtime.activeAnalyses, 0);
  server = await startServer({ dataDir: join(dataDir, 'server'), now: () => at, leaseMs: 1e9 });
  client = await connectCore({ dataDir: join(dataDir, 'server') });
  const source = await client.registerSource(descriptor);
  await client.ingest(source, batch);
  const deadline = performance.now() + 2000;
  let analysis;
  do { analysis = (await client.snapshot()).tasks[0].analysis; if (analysis.completedAt !== null) break; assert.ok(performance.now() < deadline); await delay(2); } while (true);
  assert.equal(analysis.checks.find(check => check.id === 'long_wait').status, 'clear');
  assert.deepEqual(analysis.coverage.gaps, []);
  assert.deepEqual(analysis.coverage.dependencies, ['llm_unconfigured', 'jev_unconfigured']);
  assert.equal(analysis.summary.generated, undefined);
  assert.equal(analysis.error, null);
});
