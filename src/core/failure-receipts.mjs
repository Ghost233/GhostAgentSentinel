import { createHash, randomUUID } from 'node:crypto';
import { inferenceFailureInfo } from '../analysis/failure-info.mjs';

const COLLECTION = 'failure-receipts';
const STATES = 'failure-states', DAY = 86_400_000, LIMIT = 1024;
const copy = value => structuredClone(value);
const publicReceipt = ({ causeKey, ...value }) => copy(value);
const identifier = value => typeof value === 'string' && value.trim().length > 0 && value.length <= 256 && !/[\x00-\x1f\x7f]/.test(value);
const CATEGORIES = new Map([
  ['timeout', ['deadline', 'timeout', 'ETIMEDOUT']],
  ['transport', ['network', 'http_error', 'metadata-unavailable', 'ECONNREFUSED', 'ECONNRESET', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']],
  ['protocol', ['invalid_json', 'invalid_response', 'invalid_model', 'unexpected_stream', 'incomplete_response', 'invalid_extraction', 'invalid_decision', 'invalid-lifecycle-record', 'unsupported-record-type', 'unsupported-event-type']],
  ['evidence', ['invalid_evidence']], ['expired', ['evidence_expired', 'analysis_expired']],
  ['configuration', ['configuration', 'analysis_config_conflict']],
  ['persistence', ['monitoring_outbox_capacity']],
  ['collection', ['EACCES', 'EPERM', 'ENOENT', 'ESTALE', 'EIO', 'ENOTDIR', 'EISDIR', 'EMFILE', 'ENFILE', 'ELOOP', 'ENOSPC', 'EROFS', 'record-source-unavailable', 'record-identity-changed', 'record-file-replaced', 'record-file-truncated', 'record-rewrite-suspected', 'cursor-anchor-changed', 'record-malformed-or-over-limit', 'cursor-invalid', 'cursor-retains-gap', 'observation-unavailable', 'cursor-unavailable', 'record-path-unverified']],
  ['other', ['analysis_failed', 'collection_failed', 'unrecognized_error']],
].flatMap(([category, codes]) => codes.map(code => [code, category])));
const STAGES = new Set(['llm', 'jev', 'configuration', 'collection', 'metadata', 'projection', 'persistence', 'core']);
export function failureErrorInfo(error = {}) {
  error ??= {};
  if (typeof error.code === 'string' && /^(unsupported-record-type|unsupported-event-type):/.test(error.code)) error = { ...error, code: error.code.split(':', 1)[0] };
  const known = CATEGORIES.has(error.code), code = known ? error.code : error.code == null ? 'analysis_failed' : 'unrecognized_error';
  const value = { code, stage: STAGES.has(error.stage) ? error.stage : null, category: CATEGORIES.get(code) ?? 'other' };
  if (!known && typeof error.code === 'string') value.codeHash = createHash('sha256').update(error.code).digest('hex');
  else if (code === 'unrecognized_error' && /^[a-f0-9]{64}$/.test(error.codeHash ?? '')) value.codeHash = error.codeHash;
  const status = error.status ?? error.details?.status;
  if (Number.isInteger(status) && status >= 100 && status <= 599) value.status = status;
  if (code === 'network') { const details = inferenceFailureInfo(error.details); if (details) value.details = details; }
  return value;
}

// Uses the core-owned records table and connection; it does not own the DB.
export function createFailureReceipts({ db, now = Date.now }) {
  const rows = db.prepare('SELECT value FROM records WHERE collection=?');
  const write = db.prepare('INSERT INTO records(collection,key,value) VALUES(?,?,?) ON CONFLICT(collection,key) DO UPDATE SET value=excluded.value');
  const remove = db.prepare('DELETE FROM records WHERE collection=? AND key=?');
  const load = collection => new Map(rows.all(collection).map(row => { const value = JSON.parse(row.value); return [value.key ?? value.id, value]; }));
  let receipts = load(COLLECTION), states = load(STATES), closed = false;
  const stored = db.prepare('SELECT value FROM records WHERE collection=? AND key=?').get('settings', COLLECTION);
  const metadata = stored ? JSON.parse(stored.value) : {};
  let sequence = metadata.sequence ?? Math.max(0, ...[...receipts.values()].map(value => value.sequence)), prunedThrough = metadata.prunedThrough ?? 0;
  const save = (collection, key, value) => write.run(collection, key, JSON.stringify(value));
  function eraseReceipt(receipt) { remove.run(COLLECTION, receipt.id); receipts.delete(receipt.id); prunedThrough = Math.max(prunedThrough, receipt.sequence); }
  function pruneRows(current) {
    const cutoff = current - DAY;
    for (const receipt of receipts.values()) if (receipt.firstFailedAt <= cutoff) eraseReceipt(receipt);
    for (const state of states.values()) if (state.updatedAt <= cutoff) { remove.run(STATES, state.key); states.delete(state.key); }
    for (const receipt of [...receipts.values()].sort((a, b) => a.sequence - b.sequence).slice(0, Math.max(0, receipts.size - LIMIT))) eraseReceipt(receipt);
    for (const state of [...states.values()].sort((a, b) => Number(a.active) - Number(b.active) || a.updatedAt - b.updatedAt).slice(0, Math.max(0, states.size - LIMIT))) { remove.run(STATES, state.key); states.delete(state.key); }
  }
  // SQLite and the in-memory index advance together, including on write errors.
  function change(operation, current = now()) {
    if (closed) throw new Error('Failure receipt store is closed');
    const previous = { receipts, states, sequence, prunedThrough };
    db.exec('SAVEPOINT failure_receipt_change');
    receipts = new Map(receipts); states = new Map(states);
    try {
      pruneRows(current);
      const result = operation();
      pruneRows(current);
      if (sequence !== previous.sequence || prunedThrough !== previous.prunedThrough) save('settings', COLLECTION, { sequence, prunedThrough });
      db.exec('RELEASE failure_receipt_change');
      return result;
    } catch (error) {
      db.exec('ROLLBACK TO failure_receipt_change'); db.exec('RELEASE failure_receipt_change');
      ({ receipts, states, sequence, prunedThrough } = previous);
      throw error;
    }
  }
  function maintain(current) {
    if (closed) throw new Error('Failure receipt store is closed');
    if (!receipts.size && !states.size) return;
    current ??= now();
    const cutoff = current - DAY;
    if (receipts.size > LIMIT || states.size > LIMIT || [...receipts.values()].some(value => value.firstFailedAt <= cutoff) || [...states.values()].some(value => value.updatedAt <= cutoff)) change(() => {}, current);
  }
  const matches = (value, scope) => ['sourceKey', 'mainId', 'lifecycleId'].every(field => scope[field] === undefined || value[field] === scope[field]);
  const sameScope = (value, input) => matches(value, input) && value.domain === input.domain && (value.channel ?? 'default') === (input.channel ?? 'default');
  const stats = () => ({ closed, receipts: receipts.size, states: states.size, activeStates: [...states.values()].filter(value => value.active).length, maximumReceipts: LIMIT, maximumStates: LIMIT, retentionMs: DAY, latestSequence: sequence, prunedThroughSequence: prunedThrough });
  maintain();
  return {
    settle(input) {
      if (closed) throw new Error('Failure receipt store is closed');
      if (input.attempted !== true || input.settled !== true || !['failed', 'succeeded'].includes(input.outcome) || ['cancelled', 'analyzer_unconfigured'].includes(input.error?.code)) return { created: false, receipt: null };
      if (!['analysis', 'collection'].includes(input.domain) || !identifier(input.sourceKey) || !identifier(input.roundId) || !identifier(input.mainId) && !(input.domain === 'collection' && input.mainId === null) || !identifier(input.lifecycleId) && !(input.domain === 'collection' && input.lifecycleId === null) || input.mainId === null && input.lifecycleId !== null) throw new Error('Failure receipt identity is incomplete');
      const channel = input.channel ?? 'default';
      if (!/^(default|directory|main:[a-f0-9]{64})$/.test(channel) || input.domain === 'analysis' && channel !== 'default') throw new Error('Failure receipt channel is invalid');
      const current = now();
      if (input.settledAt !== undefined && (!Number.isSafeInteger(input.settledAt) || input.settledAt < 0 || input.settledAt > current)) throw new Error('Failure settlement time is invalid');
      return change(() => {
        const { sourceKey, mainId, lifecycleId, roundId, domain } = input;
        const at = input.settledAt ?? current;
        if (at <= current - DAY) return { created: false, receipt: null };
        if (input.outcome === 'succeeded') {
          const recovered = [];
          for (const state of states.values()) if (sameScope(state, input) && state.active && at >= state.updatedAt) {
            const next = { ...state, active: false, updatedAt: at, lastRoundId: roundId };
            save(STATES, next.key, next); states.set(next.key, next);
            const receipt = receipts.get(state.episodeId);
            if (receipt) {
              const value = { ...receipt, state: 'recovered', recoveredAt: at };
              save(COLLECTION, value.id, value); receipts.set(value.id, value); recovered.push(value.id);
            }
          }
          return { created: false, receipt: null, recovered };
        }
        const error = failureErrorInfo(input.error);
        const causeKey = JSON.stringify([sourceKey, mainId, lifecycleId, domain, channel, error]);
        const historical = [...receipts.values()].find(value => value.causeKey === causeKey && value.firstRoundId === roundId);
        if (historical) return { created: false, receipt: publicReceipt(historical) };
        const state = states.get(causeKey);
        if (state && (at < state.updatedAt || !state.active && state.lastRoundId === roundId)) return { created: false, receipt: null };
        if (state?.active) {
          const next = { ...state, updatedAt: at, lastRoundId: roundId };
          save(STATES, causeKey, next); states.set(causeKey, next);
          const receipt = receipts.get(state.episodeId);
          return { created: false, receipt: receipt ? publicReceipt(receipt) : null };
        }
        const receipt = { id: randomUUID(), sequence: ++sequence, sourceKey, mainId, lifecycleId, domain, channel, firstRoundId: roundId, firstFailedAt: at, recoveredAt: null, state: 'active', error, causeKey };
        const next = { key: causeKey, sourceKey, mainId, lifecycleId, domain, channel, episodeId: receipt.id, updatedAt: at, active: true, lastRoundId: roundId, error };
        save(COLLECTION, receipt.id, receipt); save(STATES, causeKey, next);
        receipts.set(receipt.id, receipt); states.set(causeKey, next);
        return { created: true, receipt: publicReceipt(receipt) };
      }, current);
    },
    list({ sourceKey, mainId, lifecycleId, afterSequence = 0, limit = 100 } = {}) {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error('Failure receipt page limit is invalid');
      if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) throw new Error('Failure receipt cursor is invalid');
      maintain();
      const values = [...receipts.values()].filter(value => matches(value, { sourceKey, mainId, lifecycleId }) && value.sequence > afterSequence).sort((a, b) => a.sequence - b.sequence);
      const page = values.slice(0, limit);
      return { receipts: page.map(publicReceipt), nextSequence: page.at(-1)?.sequence ?? afterSequence, hasMore: values.length > page.length, historyGap: afterSequence < prunedThrough };
    },
    health(scope) {
      if (!identifier(scope?.sourceKey) || !identifier(scope.mainId) && scope.mainId !== null || !identifier(scope.lifecycleId) && scope.lifecycleId !== null || scope.mainId === null && scope.lifecycleId !== null) throw new Error('Failure health scope identity is incomplete');
      maintain();
      const current = [...states.values()].filter(value => matches(value, scope));
      const active = current.filter(value => value.active).sort((a, b) => b.updatedAt - a.updatedAt);
      return { state: active.length ? 'failed' : current.length ? 'recovered' : 'unknown', failures: active.slice(0, 6).map(value => {
        const receipt = receipts.get(value.episodeId);
        return { id: value.episodeId, sourceKey: value.sourceKey, mainId: value.mainId, lifecycleId: value.lifecycleId, domain: value.domain, channel: value.channel ?? 'default', error: copy(value.error), roundId: value.lastRoundId, firstRoundId: receipt?.firstRoundId ?? null, firstFailedAt: receipt?.firstFailedAt ?? null, updatedAt: value.updatedAt };
      }), hasMore: active.length > 6 };
    },
    prune(current) { maintain(current); return stats(); },
    clearSource(sourceKey, current) {
      if (!identifier(sourceKey)) throw new Error('Failure receipt source identity is invalid');
      if (closed) throw new Error('Failure receipt store is closed');
      if (![...receipts.values(), ...states.values()].some(value => value.sourceKey === sourceKey)) return stats();
      return change(() => {
        for (const receipt of receipts.values()) if (receipt.sourceKey === sourceKey) eraseReceipt(receipt);
        for (const state of states.values()) if (state.sourceKey === sourceKey) { remove.run(STATES, state.key); states.delete(state.key); }
        return stats();
      }, current);
    },
    diagnostics() { if (!closed) maintain(); return stats(); },
    close() { closed = true; receipts.clear(); states.clear(); },
  };
}
