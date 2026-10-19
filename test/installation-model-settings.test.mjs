import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';

async function until(predicate, label) {
  const deadline = Date.now() + 3000;
  while (!await predicate()) { assert.ok(Date.now() < deadline, label); await delay(5); }
}

async function modelFixture(t, { holdModels = [] } = {}) {
  const calls = [], held = [];
  const models = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw); calls.push({ path: request.url, body });
    response.setHeader('content-type', 'application/json');
    if (request.url === '/v1/chat/completions') {
      const reply = () => response.end(JSON.stringify({ model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }));
      if (holdModels.includes(body.model)) held.push({ reply, response });
      else reply();
    }
    else response.end(JSON.stringify({ model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }));
  });
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve));
  t.after(async () => { models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); });
  const baseUrl = `http://127.0.0.1:${models.address().port}`;
  return { calls, held, config: name => ({ llm: { baseUrl, model: `${name}-summary` }, jev: { baseUrl, model: `${name}-judge` } }) };
}

async function observe(client, registrationId, taskId, hostRunId = 'host') {
  const sourceKey = await client.registerSource({ registrationId, hostRunId, name: registrationId });
  await client.ingest(sourceKey, { tasks: [{ id: taskId, lifecycleId: 'life', title: taskId, status: 'running' }], events: [
    { id: `${taskId}-request`, kind: 'input', text: `Inspect ${taskId}` },
    { id: `${taskId}-output`, kind: 'output', text: `Inspecting ${taskId} evidence` },
  ].map(event => ({ ...event, taskId, taskLifecycleId: 'life', capturedAt: Date.now() - 1000, occurredAt: null })) });
  return sourceKey;
}

test('installation configuration is independent of collection, uses compare-save, and survives reopening', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-models-'));
  let server, client;
  t.after(async () => { await client?.close(); await server?.close(); await rm(dataDir, { recursive: true, force: true }); });
  await writeFile(join(dataDir, 'analysis-config.json'), JSON.stringify({ llm: { model: 'old-global' } }));
  server = await startServer({ dataDir }); client = await connectCore({ dataDir });
  const first = await client.installationAnalysisConfig('chatgpt:formal');
  const other = await client.installationAnalysisConfig('chatgpt:test');
  assert.deepEqual(first.config, {});
  assert.deepEqual(other.config, {});
  assert.notEqual(first.revision, other.revision);
  const config = { llm: { baseUrl: 'http://127.0.0.1:1234', model: 'summary' }, jev: { baseUrl: 'http://localhost:5678', model: 'judge' } };
  const racing = await Promise.allSettled([
    client.configureInstallationAnalysis('chatgpt:formal', config, first.revision),
    client.configureInstallationAnalysis('chatgpt:formal', { llm: { model: 'concurrent' } }, first.revision),
  ]);
  const saved = racing.find(value => value.status === 'fulfilled').value;
  assert.equal(racing.filter(value => value.status === 'fulfilled').length, 1);
  assert.equal(racing.find(value => value.status === 'rejected').reason.code, 'analysis_config_conflict');
  assert.equal(saved.registrationId, 'chatgpt:formal');
  assert.notEqual(saved.revision, first.revision);
  assert.deepEqual(await client.installationAnalysisConfig('chatgpt:test'), other);
  await assert.rejects(client.configureInstallationAnalysis('chatgpt:formal', {}, first.revision), error => error.status === 409 && error.code === 'analysis_config_conflict');
  await client.close(); await server.close();
  server = await startServer({ dataDir }); client = await connectCore({ dataDir });
  assert.deepEqual(await client.installationAnalysisConfig('chatgpt:formal'), saved);
  const source = await client.registerSource({ registrationId: 'chatgpt:formal', hostRunId: 'new-host', name: 'reconnected installation' });
  await client.unregisterSource(source);
  assert.deepEqual(await client.installationAnalysisConfig('chatgpt:formal'), saved);
});

test('saving one installation seals only its round and preserves another installation queue and cooldown', async t => {
  const models = await modelFixture(t, { holdModels: ['old-a-summary'] }), dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-switch-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const save = async (id, config) => client.configureInstallationAnalysis(id, config, (await client.installationAnalysisConfig(id)).revision);
  const oldA = await save('a', models.config('old-a')), originalB = await save('b', models.config('b'));
  await observe(client, 'b', 'b-cooling');
  await until(async () => (await client.snapshot()).tasks.find(task => task.id === 'b-cooling').analysis.completedAt !== null, 'the other installation has an existing completed result');
  await observe(client, 'a', 'a-active');
  await until(() => models.held.length === 1, 'the old actual model request is held');
  await observe(client, 'b', 'b-queued');
  const before = await client.snapshot(), beforeB = before.tasks.filter(task => task.id.startsWith('b-'));
  const newA = await client.configureInstallationAnalysis('a', models.config('new-a'), oldA.revision);
  const switched = await client.snapshot();
  assert.equal(switched.monitoring.pendingCancelledRequests, 1);
  assert.equal(switched.tasks.find(task => task.id === 'a-active').analysis.resultDisposition, 'none');
  assert.deepEqual(switched.tasks.filter(task => task.id.startsWith('b-')).map(task => task.analysis), beforeB.map(task => ({ ...task.analysis, waitMs: switched.tasks.find(current => current.id === task.id).analysis.waitMs })));
  assert.equal(models.held[0].response.destroyed, false);
  assert.equal(models.calls.filter(call => call.path === '/v1/chat/completions').length, 2);
  models.held[0].reply();
  await until(async () => (await client.snapshot()).tasks.filter(task => task.id !== 'b-cooling').every(task => task.analysis.completedAt !== null), 'the remaining FIFO work uses the new installation configuration');
  const final = await client.snapshot();
  assert.deepEqual(final.tasks.find(task => task.id === 'a-active').analysis.configScope, { registrationId: 'a', revision: newA.revision });
  assert.deepEqual(final.tasks.find(task => task.id === 'b-queued').analysis.configScope, { registrationId: 'b', revision: originalB.revision });
  assert.equal(final.tasks.find(task => task.id === 'b-cooling').analysis.nextEligibleAt, beforeB.find(task => task.id === 'b-cooling').analysis.nextEligibleAt);
  assert.deepEqual(models.calls.filter(call => call.path === '/v1/chat/completions').map(call => call.body.model), ['b-summary', 'old-a-summary', 'b-summary', 'new-a-summary']);
});

test('normal model HTTP requests use only their installation configuration and expose its version', async t => {
  const models = await modelFixture(t), dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-http-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  await client.pause();
  await client.configureAnalysis(models.config('old-global'));
  const scopes = {};
  for (const id of ['chatgpt:formal', 'dsh:stable']) {
    const current = await client.installationAnalysisConfig(id);
    scopes[id] = await client.configureInstallationAnalysis(id, models.config(id), current.revision);
  }
  await observe(client, 'chatgpt:formal', 'a');
  await observe(client, 'chatgpt:formal', 'b', 'reconnected-host');
  await observe(client, 'dsh:stable', 'c');
  await observe(client, 'chatgpt:test', 'unconfigured');
  await client.resume();
  await until(async () => (await client.snapshot()).tasks.filter(task => task.id !== 'unconfigured').every(task => task.analysis.completedAt !== null), 'configured installations complete actual model calls');
  const snapshot = await client.snapshot();
  for (const task of snapshot.tasks.filter(task => task.id !== 'unconfigured')) {
    const registrationId = snapshot.sources.find(source => source.key === task.sourceKey).descriptor.registrationId;
    assert.deepEqual(task.analysis.configScope, { registrationId, revision: scopes[registrationId].revision });
    assert.equal(task.analysis.error, null);
  }
  assert.equal(snapshot.tasks.find(task => task.id === 'unconfigured').analysis.state, 'unavailable');
  assert.deepEqual(models.calls.filter(call => call.path === '/v1/chat/completions').map(call => call.body.model).sort(), ['chatgpt:formal-summary', 'chatgpt:formal-summary', 'dsh:stable-summary']);
  assert.deepEqual(models.calls.filter(call => call.path === '/v1/systemone').map(call => call.body.model).sort(), ['chatgpt:formal-judge', 'chatgpt:formal-judge', 'dsh:stable-judge']);
});

test('purging an installation clears only its model selection and leaves the other installation configuration intact', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-purge-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const owned = await client.installationAnalysisConfig('chatgpt:owned'), other = await client.installationAnalysisConfig('chatgpt:other');
  await client.configureInstallationAnalysis('chatgpt:owned', { llm: { model: 'owned' } }, owned.revision);
  const kept = await client.configureInstallationAnalysis('chatgpt:other', { llm: { model: 'other' } }, other.revision);
  assert.equal((await client.purgeChatGPTInstallation({ installationId: 'owned' })).status, 'complete');
  assert.deepEqual((await client.installationAnalysisConfig('chatgpt:owned')).config, {});
  assert.deepEqual(await client.installationAnalysisConfig('chatgpt:other'), kept);
});

test('a configuration edit leaves another installation actual in-flight request intact and invalidates only its own prior result', async t => {
  const models = await modelFixture(t, { holdModels: ['b-summary'] }), dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-other-active-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  for (const id of ['a', 'b']) await client.configureInstallationAnalysis(id, models.config(id), (await client.installationAnalysisConfig(id)).revision);
  await observe(client, 'a', 'a-finished');
  await until(async () => (await client.snapshot()).tasks[0].analysis.completedAt !== null, 'the edited installation has a prior result');
  await observe(client, 'b', 'b-active');
  await until(() => models.held.length === 1, 'another installation has a real model request in flight');
  const before = await client.snapshot(), prior = before.tasks.find(task => task.id === 'a-finished').analysis;
  const changed = await client.configureInstallationAnalysis('a', models.config('new-a'), (await client.installationAnalysisConfig('a')).revision);
  await client.configureAnalysis(models.config('unused-global'));
  const after = await client.snapshot(), stale = after.tasks.find(task => task.id === 'a-finished').analysis;
  assert.equal(after.monitoring.activeTaskKey, before.monitoring.activeTaskKey);
  assert.equal(after.monitoring.pendingCancelledRequests, 0);
  assert.deepEqual(after.tasks.find(task => task.id === 'b-active').analysis, before.tasks.find(task => task.id === 'b-active').analysis);
  assert.equal(models.held[0].response.destroyed, false);
  assert.equal(stale.resultDisposition, 'invalid');
  assert.deepEqual(stale.summary, prior.summary);
  assert.equal(stale.nextEligibleAt, prior.nextEligibleAt);
  assert.deepEqual(stale.currentConfigScope, { registrationId: 'a', revision: changed.revision });
  assert.deepEqual(stale.configScope, prior.configScope);
  models.held[0].reply();
  await until(async () => (await client.snapshot()).tasks.find(task => task.id === 'b-active').analysis.completedAt !== null, 'the unaffected in-flight round completes');
  assert.deepEqual(models.calls.filter(call => call.path === '/v1/chat/completions').map(call => call.body.model), ['a-summary', 'b-summary']);
});

test('a failed task invalidation cannot partially publish an installation configuration save', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-save-failure-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  await client.pause();
  const initial = await client.installationAnalysisConfig('storage-failure');
  const source = await client.registerSource({ registrationId: 'storage-failure', hostRunId: 'host', name: 'Own storage fixture' });
  await client.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'life', title: 'Own task', status: 'idle' }] });
  // External storage fault injection only; acceptance uses the public state.
  const db = new DatabaseSync(join(dataDir, 'observations.sqlite'));
  try {
    db.exec("CREATE TRIGGER reject_task_invalidation BEFORE UPDATE ON records WHEN NEW.collection='tasks' BEGIN SELECT RAISE(FAIL,'injected task invalidation failure'); END");
    await assert.rejects(client.configureInstallationAnalysis('storage-failure', { llm: { model: 'must-not-publish' } }, initial.revision), /injected task invalidation failure/);
  } finally { db.exec('DROP TRIGGER reject_task_invalidation'); db.close(); }
  assert.deepEqual(await client.installationAnalysisConfig('storage-failure'), initial);
  const retried = await client.configureInstallationAnalysis('storage-failure', { llm: { model: 'retry' } }, initial.revision);
  assert.equal(retried.config.llm.model, 'retry');
});
