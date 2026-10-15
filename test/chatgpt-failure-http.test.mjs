import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-failure-http-'));
  const server = await startServer({ dataDir }), client = await connectCore({ dataDir });
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const sourceKey = await client.registerSource({ registrationId: 'chatgpt:failure-http', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' });
  await client.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: 'run', title: 'Main', status: 'running' }] });
  const scope = { sourceKey, mainId: 'main', lifecycleId: 'run' };
  const failure = { ...scope, roundId: 'capture-1', outcome: 'failed', settledAt: Date.now(), error: { code: 'EACCES', stage: 'collection', message: 'private-body', headers: { authorization: 'private-token' } } };
  return { client, scope, failure, async request(path, body, authorized = true) {
    const response = await fetch(server.url + path, { method: body === undefined ? 'GET' : 'POST', headers: { ...(authorized ? { authorization: 'Bearer ' + server.token } : {}), 'content-type': 'application/json' }, ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
    return { status: response.status, body: await response.json() };
  } };
}

test('private HTTP exposes settled collection failures and exact current task/source health', async t => {
  const f = await fixture(t);
  assert.ok((await f.client.health()).features.includes('monitoring-failures/1'));
  const first = await f.client.recordCollectionOutcome(f.failure);
  assert.equal(first.created, true);
  assert.equal(first.receipt.error.code, 'EACCES');
  const page = await f.client.failureReceipts(f.scope);
  assert.equal(page.receipts.length, 1);
  assert.deepEqual(await f.client.failureReceipts(f.scope), page);
  assert.deepEqual((await f.client.failureReceipts({ ...f.scope, afterSequence: page.nextSequence })).receipts, []);
  let snapshot = await f.client.snapshot();
  assert.equal(snapshot.tasks[0].monitoringHealth.state, 'failed');
  assert.equal(snapshot.sources[0].monitoringHealth.state, 'unknown');
  await f.client.recordCollectionOutcome({ ...f.failure, roundId: 'capture-2', outcome: 'succeeded', settledAt: Date.now() });
  snapshot = await f.client.snapshot();
  assert.equal(snapshot.tasks[0].monitoringHealth.state, 'recovered');
  const unbound = { ...f.failure, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'directory-1' };
  await f.client.recordCollectionOutcome(unbound);
  snapshot = await f.client.snapshot();
  assert.equal(snapshot.sources[0].monitoringHealth.state, 'failed');
  assert.equal(snapshot.tasks[0].monitoringHealth.state, 'recovered');
  assert.equal((await f.client.failureReceipts({ sourceKey: f.scope.sourceKey, mainId: null, lifecycleId: null })).receipts.length, 1);
  const publicValue = JSON.stringify({ snapshot, history: await f.client.failureReceipts({ sourceKey: f.scope.sourceKey }) });
  assert.equal(publicValue.includes('private-body'), false);
  assert.equal(publicValue.includes('private-token'), false);
  assert.equal((await f.client.diagnostics()).failureReceipts.receipts, 2);
});

test('retained disconnected source accepts historical FIFO settlement without restoring execution', async t => {
  const f = await fixture(t);
  await f.client.unregisterSource(f.scope.sourceKey, 'controlled source exit');
  const before = await f.client.snapshot();
  const recorded = await f.client.recordCollectionOutcome(f.failure);
  assert.equal(recorded.created, true);
  await f.client.recordCollectionOutcome({ ...f.failure, roundId: 'offline-recovery', outcome: 'succeeded', settledAt: Date.now() });
  const after = await f.client.snapshot();
  assert.equal(after.sources[0].status, 'disconnected');
  assert.equal(after.tasks[0].status, before.tasks[0].status);
  assert.deepEqual(after.tasks[0].alerts, before.tasks[0].alerts);
  assert.equal((await f.client.failureReceipts(f.scope)).receipts[0].state, 'recovered');
});

test('failure HTTP requires private auth, a known ChatGPT source, matching task and exact bounded scope', async t => {
  const f = await fixture(t), path = '/v1/monitoring/failures?' + new URLSearchParams(f.scope);
  assert.equal((await f.request(path, undefined, false)).status, 401);
  assert.equal((await f.request('/v1/monitoring/collection-outcome', f.failure, false)).status, 401);
  const dsh = await f.client.registerSource({ registrationId: 'dsh:failures', hostRunId: 'host', name: 'DSH' });
  await f.client.ingest(dsh, { tasks: [{ id: 'main', lifecycleId: 'run', title: 'DSH', status: 'running' }] });
  for (const body of [
    { ...f.failure, sourceKey: 'unknown-source' }, { ...f.failure, sourceKey: dsh },
    { ...f.failure, mainId: 'unverified-main' }, { ...f.failure, lifecycleId: 'unverified-execution' },
    { ...f.failure, mainId: null, lifecycleId: 'run' }, { ...f.failure, domain: 'analysis' },
    { ...f.failure, channel: 'private body' }, { ...f.failure, settledAt: Date.now() + 100_000 },
  ]) assert.equal((await f.request('/v1/monitoring/collection-outcome', body)).status, 400);
  for (const query of [path + '&sourceKey=duplicate', path + '&limit=101', path + '&afterSequence=-1', path + '&limit=1e2', path + '&extra=secret', '/v1/monitoring/failures', '/v1/monitoring/failures?sourceKey=' + dsh]) assert.equal((await f.request(query)).status, 400);
  assert.deepEqual((await f.client.failureReceipts(f.scope)).receipts, []);
  await f.client.recordCollectionOutcome(f.failure);
  assert.equal((await f.client.failureReceipts(f.scope)).receipts.length, 1);
});
