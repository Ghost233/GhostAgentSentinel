import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { createFailureReceipts, failureErrorInfo } from '../src/core/failure-receipts.mjs';

const scope = { sourceKey: 'source-a', mainId: 'main-a', lifecycleId: 'run-a' };
const failure = { ...scope, domain: 'analysis', roundId: 'round-a', outcome: 'failed', attempted: true, settled: true, error: { code: 'deadline', stage: 'llm' } };
function fixture(t) {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE records (collection TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(collection,key))');
  let time = 1_000_000;
  const store = createFailureReceipts({ db, now: () => time });
  t.after(() => db.close());
  return { db, store, now: () => time, advance: value => { time += value; } };
}

test('the first settled failure remains readable after duplicate failure and reader reconnect', t => {
  const f = fixture(t), first = f.store.settle(failure);
  assert.equal(first.created, true);
  assert.equal(first.receipt.firstRoundId, 'round-a');
  assert.equal(first.receipt.firstFailedAt, 1_000_000);
  assert.deepEqual(first.receipt.error, { code: 'deadline', stage: 'llm', category: 'timeout' });
  f.advance(100);
  const repeated = f.store.settle({ ...failure, roundId: 'round-b' });
  assert.equal(repeated.created, false);
  const reconnected = createFailureReceipts({ db: f.db, now: f.now });
  const page = reconnected.list({ ...scope, afterSequence: 0, limit: 10 });
  assert.equal(page.receipts.length, 1);
  assert.equal(page.receipts[0].id, first.receipt.id);
  assert.equal(page.receipts[0].firstRoundId, 'round-a');
  assert.equal(page.receipts[0].firstFailedAt, 1_000_000);
  assert.equal(reconnected.list({ ...scope, afterSequence: page.nextSequence }).receipts.length, 0);
});

test('queueing, slow calls, valid unknown, cancellation and uncalled dependencies produce no receipt', t => {
  const f = fixture(t);
  for (const input of [
    { outcome: 'queued' }, { outcome: 'running' }, { outcome: 'unknown' },
    { outcome: 'cancelled' }, { settled: false }, { attempted: false },
    { error: { code: 'cancelled' } }, { error: { code: 'analyzer_unconfigured' } },
  ]) {
    assert.equal(f.store.settle({ ...failure, ...input }).created, false);
  }
  f.advance(31_000);
  assert.deepEqual(f.store.list().receipts, []);
});

test('recovery stays visible and recurrence forms a new episode without crossing executions or domains', t => {
  const f = fixture(t), old = f.store.settle(failure).receipt;
  const current = { ...failure, lifecycleId: 'run-b', roundId: 'round-b' };
  const fresh = f.store.settle(current).receipt;
  const collection = f.store.settle({ ...current, domain: 'collection' }).receipt;
  f.advance(20);
  f.store.settle({ ...current, outcome: 'succeeded' });
  const history = f.store.list({ sourceKey: scope.sourceKey }).receipts;
  assert.equal(history.find(r => r.id === fresh.id).state, 'recovered');
  assert.equal(history.find(r => r.id === fresh.id).recoveredAt, 1_000_020);
  assert.equal(history.find(r => r.id === old.id).state, 'active');
  assert.equal(history.find(r => r.id === collection.id).state, 'active');
  f.advance(1);
  const recurrence = f.store.settle({ ...current, roundId: 'round-c' });
  assert.equal(recurrence.created, true);
  assert.notEqual(recurrence.receipt.id, fresh.id);
  assert.equal(recurrence.receipt.firstRoundId, 'round-c');
  const restart = createFailureReceipts({ db: f.db, now: f.now });
  assert.equal(restart.list({ lifecycleId: 'run-b' }).receipts.length, 3);
});

test('settled errors retain their original safe classification without response bodies or credentials', t => {
  const f = fixture(t);
  for (const [code, category] of [['network', 'transport'], ['http_error', 'transport'], ['invalid_json', 'protocol'], ['invalid_evidence', 'evidence'], ['evidence_expired', 'expired'], ['configuration', 'configuration']]) {
    const result = f.store.settle({ ...failure, error: { code, stage: 'jev', message: 'Bearer private-token', details: { status: 503, remoteCode: 'private-token', body: 'private text' } } });
    assert.equal(result.receipt.error.code, code);
    assert.equal(result.receipt.error.category, category);
    assert.equal(result.receipt.error.stage, 'jev');
    assert.equal(result.receipt.error.status, 503);
  }
  const unknown = f.store.settle({ ...failure, error: { code: 'private-token', stage: 'https://private.invalid', message: 'private text', headers: { authorization: 'private-token' } } });
  assert.equal(unknown.receipt.error.code, 'unrecognized_error');
  assert.match(unknown.receipt.error.codeHash, /^[a-f0-9]{64}$/);
  assert.equal(unknown.receipt.error.stage, null);
  const output = JSON.stringify(createFailureReceipts({ db: f.db, now: f.now }).list());
  for (const secret of ['private-token', 'private text', 'private.invalid', 'authorization', 'headers', 'message', 'remoteCode']) assert.equal(output.includes(secret), false);
});

test('24 hour retention does not renew on replay, while a continuing cause stays deduplicated', t => {
  const f = fixture(t), first = f.store.settle(failure).receipt;
  f.advance(86_400_000 - 1);
  f.store.settle({ ...failure, roundId: 'next-round' });
  f.advance(1);
  assert.deepEqual(f.store.list().receipts, []);
  const restart = createFailureReceipts({ db: f.db, now: f.now });
  assert.equal(restart.settle({ ...failure, roundId: 'continuing-round' }).created, false);
  assert.equal(restart.settle({ ...failure, settledAt: first.firstFailedAt }).created, false);
  restart.settle({ ...failure, roundId: 'recovery', outcome: 'succeeded' });
  const fresh = restart.settle({ ...failure, roundId: 'recurrence' });
  assert.equal(fresh.created, true);
  assert.ok(fresh.receipt.sequence > first.sequence);
});

test('the outbox and core use the same idempotent safe error contract', () => {
  for (const input of [
    { code: 'network', stage: 'llm', details: { operation: 'fetch', name: 'TypeError', code: 'ECONNREFUSED', cause: { name: 'SocketError', code: 'ECONNRESET', private: 'secret' }, body: 'secret' } },
    { code: 'secret', stage: 'secret', message: 'secret' },
  ]) {
    const safe = failureErrorInfo(input);
    assert.deepEqual(failureErrorInfo(safe), safe);
    assert.equal(JSON.stringify(safe).includes('secret'), false);
  }
});

test('source-level collection failure stays unbound while analysis requires verified execution identity', t => {
  const f = fixture(t), input = { ...failure, domain: 'collection', mainId: null, lifecycleId: null, error: { code: 'metadata-unavailable', stage: 'metadata' } };
  const receipt = f.store.settle(input).receipt;
  assert.equal(receipt.mainId, null);
  assert.equal(receipt.lifecycleId, null);
  assert.deepEqual(f.store.list(scope).receipts, []);
  assert.equal(f.store.list({ sourceKey: scope.sourceKey, mainId: null }).receipts.length, 1);
  assert.throws(() => f.store.settle({ ...input, domain: 'analysis' }), /identity/);
  assert.throws(() => f.store.settle({ ...failure, roundId: null }), /identity/);
  assert.throws(() => f.store.settle({ ...failure, settledAt: f.now() + 1 }), /time/);
  assert.throws(() => f.store.list({ limit: 101 }), /limit/);
  assert.throws(() => f.store.list({ afterSequence: -1 }), /cursor/);
});

test('bounded pages expose history loss and source cleanup leaves other sources intact', t => {
  const f = fixture(t);
  for (let index = 0; index < 1_100; index++) f.store.settle({ ...failure, mainId: `main-${index}`, roundId: `round-${index}` });
  assert.equal(f.store.diagnostics().receipts, 1024);
  assert.equal(f.store.diagnostics().states, 1024);
  const ids = new Set(); let cursor = 0, page;
  do {
    page = f.store.list({ afterSequence: cursor, limit: 100 });
    assert.ok(page.receipts.length <= 100);
    assert.equal(page.historyGap, cursor < 76);
    for (const receipt of page.receipts) { assert.equal(ids.has(receipt.id), false); ids.add(receipt.id); }
    cursor = page.nextSequence;
  } while (page.hasMore);
  assert.equal(ids.size, 1024);
  const other = f.store.settle({ ...failure, sourceKey: 'source-b' }).receipt;
  f.store.clearSource('source-a');
  assert.deepEqual(f.store.list().receipts.map(r => r.id), [other.id]);
  assert.equal(f.store.diagnostics().states, 1);
  f.advance(86_400_000);
  f.store.prune();
  assert.equal(f.store.diagnostics().receipts, 0);
  assert.equal(f.store.diagnostics().states, 0);
  const restart = createFailureReceipts({ db: f.db, now: f.now });
  assert.ok(restart.settle(failure).receipt.sequence > other.sequence);
});

test('collection IO retains safe native permission and filesystem error codes', () => {
  for (const code of ['EACCES', 'EPERM', 'ENOENT', 'ESTALE', 'EIO', 'ENOTDIR', 'EISDIR', 'EMFILE', 'ENFILE', 'ELOOP', 'ENOSPC', 'EROFS']) {
    assert.deepEqual(failureErrorInfo({ code, stage: 'collection', path: '/private/secret', message: 'private text' }), { code, stage: 'collection', category: 'collection' });
  }
});

test('FIFO retries keep original settlement times and cannot revive a recovered episode', t => {
  const f = fixture(t), first = f.store.settle(failure).receipt;
  f.advance(1);
  const repeatedAt = f.now();
  f.store.settle({ ...failure, roundId: 'round-b', settledAt: repeatedAt });
  f.advance(1);
  const recoveredAt = f.now();
  f.store.settle({ ...failure, roundId: 'round-c', outcome: 'succeeded', settledAt: recoveredAt });
  assert.equal(f.store.settle({ ...failure, settledAt: first.firstFailedAt }).created, false);
  assert.equal(f.store.settle({ ...failure, roundId: 'round-b', settledAt: repeatedAt }).created, false);
  f.advance(1);
  const current = f.store.settle({ ...failure, roundId: 'round-d' }).receipt;
  f.store.settle({ ...failure, roundId: 'round-c', outcome: 'succeeded', settledAt: recoveredAt });
  const values = f.store.list(scope).receipts;
  assert.equal(values.length, 2);
  assert.equal(values.find(r => r.id === first.id).state, 'recovered');
  assert.equal(values.find(r => r.id === current.id).state, 'active');
});

test('a SQLite write failure leaves the first failure retryable and does not fake recovery', t => {
  const f = fixture(t);
  f.db.exec('PRAGMA query_only=ON');
  assert.throws(() => f.store.settle(failure), /readonly/);
  f.db.exec('PRAGMA query_only=OFF');
  const first = f.store.settle(failure);
  assert.equal(first.created, true);
  assert.equal(first.receipt.sequence, 1);
  f.db.exec('PRAGMA query_only=ON');
  assert.throws(() => f.store.settle({ ...failure, outcome: 'succeeded', roundId: 'recovery' }), /readonly/);
  f.db.exec('PRAGMA query_only=OFF');
  assert.equal(f.store.list(scope).receipts[0].state, 'active');
  f.store.settle({ ...failure, outcome: 'succeeded', roundId: 'recovery' });
  assert.equal(f.store.list(scope).receipts[0].state, 'recovered');
});

test('closing releases failure indexes without deleting durable receipts or owning the core database', t => {
  const f = fixture(t), first = f.store.settle(failure).receipt;
  f.store.close(); f.store.close();
  assert.equal(f.store.diagnostics().closed, true);
  assert.equal(f.store.diagnostics().receipts, 0);
  assert.equal(f.store.diagnostics().states, 0);
  assert.throws(() => f.store.list(), /closed/);
  assert.throws(() => f.store.settle(failure), /closed/);
  const restart = createFailureReceipts({ db: f.db, now: f.now });
  assert.equal(restart.list().receipts[0].id, first.id);
});

test('definite reader failures retain fixed codes without dynamic type text', () => {
  for (const code of ['record-identity-changed', 'cursor-invalid', 'cursor-retains-gap']) assert.equal(failureErrorInfo({ code }).code, code);
  for (const code of ['invalid-lifecycle-record', 'unsupported-record-type', 'unsupported-event-type']) assert.equal(failureErrorInfo({ code }).category, 'protocol');
  const safe = failureErrorInfo({ code: 'unsupported-record-type:private type text' });
  assert.equal(safe.code, 'unsupported-record-type');
  assert.equal(JSON.stringify(safe).includes('private'), false);
});

test('current health reads active state independently of history pages and expired first receipts', t => {
  const f = fixture(t);
  for (let index = 0; index < 110; index++) {
    const input = { ...failure, domain: 'collection', roundId: `collection-${index}` };
    f.store.settle(input); f.store.settle({ ...input, roundId: `recovery-${index}`, outcome: 'succeeded' });
  }
  const current = f.store.settle(failure).receipt;
  f.store.settle({ ...failure, domain: 'collection', mainId: null, lifecycleId: null });
  assert.equal(f.store.list(scope).receipts.some(r => r.id === current.id), false);
  const health = f.store.health(scope);
  assert.equal(health.state, 'failed');
  assert.deepEqual(health.failures.map(value => value.id), [current.id]);
  assert.equal(f.store.health({ ...scope, mainId: null, lifecycleId: null }).failures.length, 1);
  f.advance(86_400_000 - 1);
  f.store.settle({ ...failure, roundId: 'still-failed' });
  f.advance(1);
  const expired = f.store.health(scope);
  assert.equal(expired.state, 'failed');
  assert.equal(expired.failures[0].firstFailedAt, null);
  assert.equal(expired.failures[0].firstRoundId, null);
  assert.equal(expired.failures[0].roundId, 'still-failed');
  assert.equal(expired.failures[0].updatedAt, 87_399_999);
  f.store.settle({ ...failure, outcome: 'succeeded', roundId: 'valid-unknown-result' });
  assert.equal(f.store.health(scope).state, 'recovered');
  assert.equal(f.store.health({ ...scope, lifecycleId: 'new-run' }).state, 'unknown');
});

test('source-level collection recovery only clears its directory or target channel', t => {
  const f = fixture(t), unbound = { ...failure, domain: 'collection', mainId: null, lifecycleId: null };
  const directory = f.store.settle({ ...unbound, channel: 'directory' }).receipt;
  const channel = 'main:' + 'a'.repeat(64);
  const target = f.store.settle({ ...unbound, channel }).receipt;
  assert.notEqual(directory.id, target.id);
  f.store.settle({ ...unbound, channel, outcome: 'succeeded', roundId: 'target-ok' });
  const health = f.store.health({ sourceKey: scope.sourceKey, mainId: null, lifecycleId: null });
  assert.equal(health.state, 'failed');
  assert.deepEqual(health.failures.map(value => value.channel), ['directory']);
  f.store.settle({ ...unbound, channel: 'directory', outcome: 'succeeded', roundId: 'directory-ok' });
  assert.equal(f.store.health({ sourceKey: scope.sourceKey, mainId: null, lifecycleId: null }).state, 'recovered');
  assert.throws(() => f.store.settle({ ...unbound, channel: 'private body text' }), /channel/);
  assert.throws(() => f.store.settle({ ...failure, channel: 'directory' }), /channel/);
});

test('current health is bounded and callers cannot mutate retained failure state', t => {
  const f = fixture(t);
  for (let status = 500; status < 507; status++) f.store.settle({ ...failure, error: { code: 'http_error', stage: 'llm', status } });
  const health = f.store.health(scope);
  assert.equal(health.failures.length, 6);
  assert.equal(health.hasMore, true);
  health.failures[0].error.code = 'caller-mutation';
  assert.equal(f.store.health(scope).failures.some(value => value.error.code === 'caller-mutation'), false);
});

test('outbox capacity failure keeps its fixed persistence classification without raw details', () => {
  assert.deepEqual(failureErrorInfo({ code: 'monitoring_outbox_capacity', stage: 'persistence', message: 'private pending outcome' }), { code: 'monitoring_outbox_capacity', stage: 'persistence', category: 'persistence' });
});

test('empty failure maintenance neither samples time nor writes and preserves durable sequence gaps', t => {
  const f = fixture(t), receipt = f.store.settle(failure).receipt;
  f.store.clearSource(scope.sourceKey);
  let calls = 0;
  f.db.exec('PRAGMA query_only=ON');
  const empty = createFailureReceipts({ db: f.db, now: () => { calls++; return f.now(); } });
  assert.equal(empty.prune().latestSequence, receipt.sequence);
  assert.equal(empty.diagnostics().prunedThroughSequence, receipt.sequence);
  assert.equal(empty.list().historyGap, true);
  assert.equal(empty.health(scope).state, 'unknown');
  assert.equal(calls, 0);
});
