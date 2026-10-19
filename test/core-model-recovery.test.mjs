import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';
import { configureInstallationModels } from './helpers/installation-model-settings.mjs';

async function until(predicate, label) { const end = performance.now() + 3000; while (!await predicate()) { assert.ok(performance.now() < end, label); await delay(2); } }

test('offline analysis backs off per object and resumes the same models without releasing a manual installation pause', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-model-recovery-')), calls = [];
  let clock = Date.now(), offline = true;
  const models = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw); calls.push({ path: request.url, model: body.model });
    response.setHeader('content-type', 'application/json');
    if (offline && body.model.startsWith('a-')) { response.writeHead(503).end('{"error":{"code":"offline"}}'); return; }
    response.end(JSON.stringify(request.url === '/v1/chat/completions'
      ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }
      : { model: body.model, answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }));
  });
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve));
  const server = await startServer({ dataDir, now: () => clock, cooldownMs: 10 }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }); });
  const baseUrl = 'http://127.0.0.1:' + models.address().port;
  for (const id of ['a', 'b']) {
    await configureInstallationModels(client, id, { llm: { baseUrl, model: id + '-summary' }, jev: { baseUrl, model: id + '-judge' } });
    const source = await client.registerSource({ registrationId: id, hostRunId: 'host', name: id });
    await client.ingest(source, { tasks: [{ id, lifecycleId: 'life', title: id, status: 'running' }], events: [
      { id: 'request', kind: 'input', text: 'Inspect ' + id }, { id: 'output', kind: 'output', text: 'Inspecting ' + id },
    ].map(event => ({ ...event, taskId: id, taskLifecycleId: 'life', occurredAt: null, capturedAt: clock })) });
  }
  const task = async id => (await client.snapshot()).tasks.find(task => task.id === id);
  await until(async () => (await task('a')).analysis.state === 'failed' && (await task('b')).analysis.completedAt !== null, 'offline A fails while B completes normally');
  const first = (await task('a')).analysis, b = (await task('b')).analysis;
  assert.equal(first.nextEligibleAt, clock + 10);
  clock += 10; await client.snapshot();
  await until(async () => (await task('a')).analysis.completedAt === clock, 'the first retry fails at the same service');
  const second = (await task('a')).analysis;
  assert.equal(second.nextEligibleAt, clock + 20);
  assert.equal(second.retryAttempt, 2);
  const paused = await client.setInstallationMonitoring('a', true, (await client.installationMonitoring('a')).revision);
  offline = false; clock += 20;
  await client.snapshot();
  assert.equal(calls.filter(call => call.model === 'a-summary').length, 2);
  assert.equal((await task('a')).analysis.state, 'paused');
  await client.setInstallationMonitoring('a', false, paused.revision);
  await until(async () => (await task('a')).analysis.summary && !(await task('a')).analysis.error, 'the selected models recover after explicit resume');
  const recovered = (await task('a')).analysis;
  assert.equal(recovered.retryAttempt, 0);
  assert.equal(recovered.nextEligibleAt, clock + 10);
  assert.equal((await task('b')).analysis.nextEligibleAt, b.nextEligibleAt);
  assert.deepEqual(calls.filter(call => call.model.startsWith('a-')).map(call => call.model), ['a-summary', 'a-summary', 'a-summary', 'a-judge']);
});
