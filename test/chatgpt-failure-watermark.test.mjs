import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.mjs';
import { connectCore } from '../src/core-client.mjs';
import { DatabaseSync } from 'node:sqlite';
import { createFailureReceipts } from '../src/core/failure-receipts.mjs';

async function fixture(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-recovery-watermark-'));
  let time = 1_800_000_000_000, server, client;
  const start = async () => { server = await startServer({ dataDir, now: () => time }); client = await connectCore({ dataDir }); };
  await start();
  t.after(async () => { await client.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }); });
  const sourceKey = await client.registerSource({ registrationId: 'watermark', hostRunId: 'host', name: 'ChatGPT', policy: 'chatgpt-main/1' });
  await client.ingest(sourceKey, { tasks: [{ id: 'main', lifecycleId: 'run', title: 'Main', status: 'running' }] });
  const scope = { sourceKey, mainId: 'main', lifecycleId: 'run' };
  return { scope, get client() { return client }, async report(roundId, offset, code, channel = 'default') { return client.recordCollectionOutcome({ ...scope, channel, roundId, settledAt: time + offset, outcome: code ? 'failed' : 'succeeded', ...(code ? { error: { code, stage: 'collection' } } : {}) }); }, async health() { return (await client.snapshot()).tasks.find(task => task.id === 'main').monitoringHealth; }, async restart() { await client.close(); await server.close(); await start(); }, advance(value) { time += value; } };
}

test('a newer recovery fences a late different cause while retaining its original historical first failure', async t => {
  const f = await fixture(t);
  await f.report('failure-A', -3000, 'EACCES');
  await f.report('recovery', -1000);
  assert.equal((await f.health()).state, 'recovered');
  await f.restart();
  const late = await f.report('late-failure-B', -2000, 'ENOENT');
  assert.equal(late.created, true);
  assert.equal(late.receipt.state, 'recovered');
  assert.equal(late.receipt.firstFailedAt, 1_799_999_998_000);
  assert.equal(late.receipt.recoveredAt, 1_799_999_999_000);
  assert.equal((await f.health()).state, 'recovered');
  assert.equal((await f.client.failureReceipts(f.scope)).receipts.length, 2);
  await f.report('genuinely-new', 0, 'EIO');
  assert.deepEqual((await f.health()).failures.map(value => value.error.code), ['EIO']);
  await f.report('other-channel', -2000, 'ENOENT', 'directory');
  assert.deepEqual(new Set((await f.health()).failures.map(value => value.channel)), new Set(['default', 'directory']));
});

test('success before any failure stores ordering proof without inventing a recovery notice', async t => {
  const f = await fixture(t);
  await f.report('success-only', -1000);
  assert.equal((await f.health()).state, 'unknown');
  assert.deepEqual((await f.client.failureReceipts(f.scope)).receipts, []);
  await f.restart();
  const late = await f.report('late-first-failure', -2000, 'EACCES');
  assert.equal(late.receipt.state, 'recovered');
  assert.equal((await f.health()).state, 'recovered');
  await f.report('new-first-failure', 0, 'ENOENT');
  assert.equal((await f.health()).state, 'failed');
});

test('recovery proof shares the bounded state budget and obeys restart, expiry, source cleanup and close', () => {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE records (collection TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(collection,key))');
  let time = 1_000_000;
  const options = { db, now: () => time };
  let store = createFailureReceipts(options);
  try {
    const success = { sourceKey: 'source-a', mainId: 'main', lifecycleId: 'run', domain: 'collection', roundId: 'success', outcome: 'succeeded', attempted: true, settled: true };
    for (let index = 0; index < 1_100; index++) store.settle({ ...success, mainId: 'main-' + index });
    assert.equal(store.diagnostics().states, 1024);
    assert.equal(store.diagnostics().recoveryWatermarks, 1024);
    assert.equal(store.diagnostics().recoveryWatermarkEvictions, 76);
    assert.equal(store.list().historyGap, true);
    const lostScope = { sourceKey: 'source-a', mainId: 'main-0', lifecycleId: 'run' };
    const late = store.settle({ ...success, ...lostScope, roundId: 'late-after-capacity', outcome: 'failed', settledAt: time - 1, error: { code: 'ENOENT' } });
    assert.equal(late.receipt.firstFailedAt, 999_999);
    assert.equal(store.health(lostScope).orderingGap, true, 'current health must disclose missing recovery-order proof after capacity eviction');
    assert.equal(store.list(lostScope).orderingGap, true);
    store.close();
    assert.equal(store.diagnostics().recoveryWatermarks, 0);
    store = createFailureReceipts(options);
    assert.equal(store.diagnostics().recoveryWatermarks, 1023);
    store.clearSource('source-a');
    assert.equal(store.diagnostics().states, 0);
    store.settle({ ...success, sourceKey: 'source-b' });
    store.clearSource('source-a');
    assert.equal(store.diagnostics().recoveryWatermarks, 1);
    time += 86_400_000;
    store.prune();
    assert.equal(store.diagnostics().recoveryWatermarks, 0);
    assert.equal(store.diagnostics().states, 0);
  } finally { store.close(); db.close(); }
});
