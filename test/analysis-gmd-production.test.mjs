import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { createAnalyzer } from '../src/analysis/index.mjs';
import { withModelSummary } from './helpers/model-summary.mjs';
import { createCore } from '../src/core/index.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';

const now = 1_800_000_000_000;
const hash = value => createHash('sha256').update(value).digest('hex');
const observation = {
  task: { key: 'stable-task-key', id: 'task', lifecycleId: 'run', status: 'running', currentRequestId: 'request' },
  source: { registrationId: 'codex-stable-install', name: 'Controlled source', gaps: [] },
  events: [
    { id: 'request', taskId: 'task', taskLifecycleId: 'run', kind: 'input', text: 'Read the requested log.', capturedAt: now - 1000, occurredAt: now - 1000 },
    { id: 'action', taskId: 'task', taskLifecycleId: 'run', kind: 'output', text: 'Read the requested log.', capturedAt: now, occurredAt: now },
  ], coverage: { complete: true, gaps: [] },
};

async function fixture(t, { supported = true, spoofReceipt = false, invalidJudgment = false, holdSummary = false, invalidConsumeReceipt = false, holdJudgment = false, rejectFirstSummary = false } = {}) {
  const requests = [];
  const operations = new Map();
  let released = !holdSummary, nativeSequence = 0;
  let releaseJudge;
  const judgeGate = new Promise(resolve => { releaseJudge = resolve; });
  const lifetime = new AbortController();
  const deadline = setTimeout(() => lifetime.abort(new Error('Controlled production-chain deadline')), 5000);
  const generation = model => `gmd-boot.${model}.native-1`;
  const server = createServer(async (req, res) => {
    const send = value => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(value)); };
    if (req.url === '/api/v1/models') return send({ source: 'ghost-model-deck', models: ['summary', 'judge'].map(model => ({
      key: model, type: model === 'summary' ? 'llm' : 'decision', source: 'ghost-model-deck', loaded_instances: [{ id: model }],
      capabilities: { prefill: { protocol: 'gmd-prefill/1', supported, mode: model === 'summary' ? 'chat_max_tokens_zero' : 'systemone_state', runtime_generation: generation(model),
        operation_policy: { id_format: 'gmd1', time_basis_ms: now, past_window_ms: 600_000, future_tolerance_ms: 60_000 } } },
    })) });
    if (req.url.startsWith('/api/v1/inference/requests/')) {
      const entry = operations.get(req.url.split('/')[5]);
      if (!entry) { res.statusCode = 404; return send({ error: { code: 'operation_not_found' } }); }
      if (!released && entry.held) return send({ ...entry.metadata, state: 'unknown', terminal: false, lease_released: false, children: [] });
      return send(entry.metadata);
    }
    let raw = ''; for await (const chunk of req) raw += chunk;
    const body = JSON.parse(raw);
    requests.push({ path: req.url, body, id: req.headers['idempotency-key'] });
    if (rejectFirstSummary && body.model === 'summary' && body.max_tokens > 0 && requests.filter(request => request.body.model === 'summary' && request.body.max_tokens > 0).length === 1) {
      res.statusCode = 503;
      return send({ request_id: req.headers['idempotency-key'], runtime_generation: generation(body.model), state: 'rejected', terminal: true, lease_released: true,
        native_dispatch: 'not_started', proof_scope: 'identity_only', error: { code: 'capacity_unavailable', stage: 'admission' } });
    }
    const existing = operations.get(req.headers['idempotency-key']);
    if (existing) return send(existing.result);
    if (holdJudgment && body.model === 'judge') await judgeGate;
    const prefill = body.max_tokens === 0 || body.prefill_only === true;
    const metadata = req.headers['idempotency-key'] ? { protocol: 'gmd-prefill/1', request_id: req.headers['idempotency-key'], model: body.model,
      runtime_generation: generation(body.model), cache_salt_sha256: hash(body.cache_salt), state: 'completed', terminal: true, lease_released: true,
      children: [{ native_request_id: ++nativeSequence, runtime_generation: 1, question_id: null, state: prefill ? 'length' : 'stop', prompt_tokens: 24,
        completion_tokens: prefill || body.model === 'judge' ? 0 : 8, matched_prompt_tokens: 8, deduplicated_tokens: 0, lease_released: true }] } : null;
    const result = prefill ? { model: body.model, choices: [], prefill: { ...metadata, generated_tokens: 0 } }
      : req.url === '/v1/chat/completions' ? { model: body.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }], ...(metadata ? { cache: invalidConsumeReceipt ? { ...metadata, request_id: 'foreign-operation' } : metadata } : {}), ...(spoofReceipt ? { gmdOperation: { lease_released: true, secret: 'unverified-provider-field' } } : {}) }
        : { model: body.model, answers: invalidJudgment ? {} : Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])), ...(metadata ? { cache: metadata } : {}) };
    const held = !released && body.model === 'summary' && !prefill;
    if (metadata) operations.set(metadata.request_id, { metadata, result, held });
    if (held) { res.statusCode = 202; return send({ ...metadata, state: 'running', terminal: false, lease_released: false, children: [] }); }
    return send(result);
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { clearTimeout(deadline); lifetime.abort(new Error('Controlled fixture closed')); return new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }); });
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const config = { llm: { baseUrl, model: 'summary' }, jev: { baseUrl, model: 'judge' }, now: () => now + 100 };
  return { requests, config, analyzer: createAnalyzer(config), options: { lifetimeSignal: lifetime.signal }, release() { released = true; },
    releaseJudge, stopLifetime() { lifetime.abort(new Error('Observer lifetime closed')); } };
}

test('a warmed observation consumes the newer frozen input and publishes only after JEV succeeds', async t => {
  const f = await fixture(t);
  const warmed = await f.analyzer.prefill(observation, f.options);
  assert.equal(warmed.state, 'completed');
  const newer = { ...observation, events: [...observation.events, { ...observation.events[1], id: 'latest', text: 'Found the newer requested log entry.' }] };
  const result = await f.analyzer.analyze(newer, f.options);
  assert.ok(result.summary.generated);
  assert.equal(result.checks.length, 6);
  assert.deepEqual(f.requests.map(request => [request.path, request.body.max_tokens ?? null]), [
    ['/v1/chat/completions', 0], ['/v1/chat/completions', 1024], ['/v1/systemone', null],
  ], 'consume handles the new suffix without an extra zero-budget request');
  assert.ok(JSON.stringify(f.requests[1].body).includes('Found the newer requested log entry.'));
  assert.equal(f.requests[0].body.cache_salt, f.requests[1].body.cache_salt);
  assert.notEqual(f.requests[1].body.cache_salt, f.requests[2].body.cache_salt);
  assert.notEqual(f.requests[0].id, f.requests[1].id);
  assert.ok(result.usage.gmd.llm.lease_released);
});

test('an undeclared prefill provider retains the existing two-model analysis without zero-budget calls', async t => {
  const f = await fixture(t, { supported: false });
  assert.equal((await f.analyzer.prefill(observation, f.options)).state, 'unsupported');
  assert.equal(f.requests.length, 0);
  const result = await f.analyzer.analyze(observation, f.options);
  assert.ok(result.summary.generated);
  assert.deepEqual(f.requests.map(request => request.path), ['/v1/chat/completions', '/v1/systemone']);
  assert.ok(f.requests.every(request => request.id === undefined && request.body.cache_salt === undefined));
});

test('an ordinary provider cannot inject a certified GMD receipt into analysis history', async t => {
  const f = await fixture(t, { supported: false, spoofReceipt: true });
  const result = await f.analyzer.analyze(observation, f.options);
  assert.ok(result.summary.generated);
  assert.equal(result.usage.gmd, undefined);
});

test('managed model success does not bypass the final judgment validation', async t => {
  const f = await fixture(t, { invalidJudgment: true });
  await assert.rejects(f.analyzer.analyze(observation, f.options), error => error.code === 'invalid_decision' && error.stage === 'jev');
});

test('two installation identities keep their model cache namespaces separate', async t => {
  const f = await fixture(t);
  await f.analyzer.analyze(observation, f.options);
  const other = createAnalyzer(f.config);
  await other.analyze({ ...observation, source: { ...observation.source, registrationId: 'codex-test-install' } }, f.options);
  assert.equal(new Set(f.requests.map(request => request.body.cache_salt)).size, 4);
  assert.ok(f.requests.every(request => !request.body.cache_salt.includes('install')));
});

test('a managed-operation fault never starts an untracked replacement generation', async t => {
  const f = await fixture(t, { invalidConsumeReceipt: true });
  await assert.rejects(f.analyzer.analyze(observation, f.options), error => error.code === 'gmd_binding_mismatch');
  assert.equal(f.requests.length, 1);
  assert.ok(f.requests[0].id);
  assert.equal(f.requests[0].body.max_tokens, 1024);
});

test('an explicitly unconfirmed request cannot borrow a historical input to prefill inherited context', async t => {
  const f = await fixture(t);
  const input = { ...observation, task: { ...observation.task, currentRequestId: null }, instructionContext: {
    binding: null, gaps: ['current_request_unconfirmed'], inheritedEvents: [{ id: 'limit', taskId: 'parent', taskLifecycleId: 'parent-run',
      kind: 'input', text: 'Read only.', capturedAt: now, contextRole: 'user-limit', authorizationEligible: true }],
  } };
  assert.equal((await f.analyzer.prefill(input, f.options)).state, 'skipped');
  assert.equal((await f.analyzer.analyze(input, f.options)).summary.generated, undefined);
  assert.equal(f.requests.length, 0);
});

test('the actual Core and analyzer remain responsive and freeze their input while GMD status is unknown', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'gmd-production-core-'));
  let core;
  t.after(async () => { await core?.close(); await rm(dir, { recursive: true, force: true }); });
  const f = await fixture(t, { holdSummary: true });
  core = await createCore({ dataDir: dir, analyzer: f.analyzer, now: () => now + 100, leaseMs: 1_000_000 });
  const source = await core.registerSource({ registrationId: observation.source.registrationId, hostRunId: 'host', name: 'Controlled source' });
  await core.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'run', status: 'running', title: 'Read the log' }], events: observation.events.map(event => ({ ...event, taskLifecycleId: 'run' })) });
  async function until(predicate) {
    const deadline = Date.now() + 1500;
    while (!await predicate()) { assert.ok(Date.now() < deadline, 'expected observable production model state'); await delay(5); }
  }
  await until(async () => (await core.snapshot()).tasks[0].analysis.modelState === 'unknown');
  await core.ingest(source, { events: [{ ...observation.events[1], taskLifecycleId: 'run', id: 'new-while-pending', text: 'Collected after the frozen analysis started.', capturedAt: now + 1 }] });
  const pending = await core.snapshot();
  assert.ok(pending.tasks[0].events.some(event => event.id === 'new-while-pending'));
  assert.equal((await core.diagnostics()).runtime.analysisAdmissionBlocked, true);
  f.release();
  await until(async () => Boolean((await core.snapshot()).tasks[0].analysis.summary?.generated));
  const consume = f.requests.find(request => request.body.max_tokens > 0);
  assert.equal(JSON.stringify(consume.body).includes('Collected after the frozen analysis started.'), false);
});

test('production cancellation keeps admission until release and resumes using the latest collected material', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'gmd-production-cancel-'));
  let core;
  t.after(async () => { await core?.close(); await rm(dir, { recursive: true, force: true }); });
  const f = await fixture(t, { holdSummary: true });
  core = await createCore({ dataDir: dir, analyzer: f.analyzer, now: () => now + 100, leaseMs: 1_000_000 });
  const source = await core.registerSource({ registrationId: observation.source.registrationId, hostRunId: 'host', name: 'Controlled source' });
  const until = async predicate => { const deadline = Date.now() + 1500; while (!await predicate()) { assert.ok(Date.now() < deadline, 'expected controlled cancellation state'); await delay(5); } };
  await core.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'run', status: 'running', title: 'Read the log' }], events: observation.events.map(event => ({ ...event, taskLifecycleId: 'run' })) });
  await until(async () => (await core.snapshot()).tasks[0].analysis.modelState === 'unknown');
  await core.pause();
  await core.ingest(source, { events: [{ ...observation.events[1], taskLifecycleId: 'run', id: 'paused-material', text: 'Material collected during pause.', capturedAt: now + 1 }] });
  await core.resume();
  assert.equal((await core.diagnostics()).runtime.cancelledUnsettledRequests, 1);
  assert.equal(f.requests.filter(request => request.body.max_tokens > 0).length, 1);
  assert.equal((await core.snapshot()).tasks[0].analysis.summary, null);
  f.release();
  await until(async () => Boolean((await core.snapshot()).tasks[0].analysis.summary?.generated));
  const consumes = f.requests.filter(request => request.body.max_tokens > 0);
  assert.equal(consumes.length, 2);
  assert.ok(JSON.stringify(consumes[1].body).includes('Material collected during pause.'));
  assert.notEqual(consumes[0].id, consumes[1].id);
});

test('ending the observer lifetime cannot publish a late legacy-provider judgment', async t => {
  const f = await fixture(t, { supported: false, holdJudgment: true });
  const round = f.analyzer.analyze(observation, f.options);
  const deadline = Date.now() + 1500;
  while (!f.requests.some(request => request.body.model === 'judge')) { assert.ok(Date.now() < deadline); await delay(5); }
  f.stopLifetime();
  f.releaseJudge();
  await assert.rejects(round, /Observer lifetime closed/);
});

test('a production background prefill exposes its verified operation evidence without publishing another summary', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'gmd-production-prefill-'));
  let core;
  t.after(async () => { await core?.close(); await rm(dir, { recursive: true, force: true }); });
  const f = await fixture(t);
  core = await createCore({ dataDir: dir, analyzer: f.analyzer, now: () => now + 100, leaseMs: 1_000_000 });
  const source = await core.registerSource({ registrationId: observation.source.registrationId, hostRunId: 'host', name: 'Controlled source' });
  const until = async predicate => { const deadline = Date.now() + 1500; while (!await predicate()) { assert.ok(Date.now() < deadline); await delay(5); } };
  await core.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'run', status: 'running', title: 'Read the log' }], events: observation.events.map(event => ({ ...event, taskLifecycleId: 'run' })) });
  await until(async () => Boolean((await core.snapshot()).tasks[0].analysis.summary?.generated));
  const previous = (await core.snapshot()).tasks[0].analysis;
  await core.ingest(source, { events: [{ ...observation.events[1], taskLifecycleId: 'run', id: 'warm-update', text: 'New material to warm.', capturedAt: now + 1 }] });
  await until(async () => (await core.snapshot()).tasks[0].analysis.prefill?.state === 'completed');
  const current = (await core.snapshot()).tasks[0].analysis;
  assert.equal(current.prefill.receipt.lease_released, true);
  assert.equal(current.prefill.receipt.children[0].completion_tokens, 0);
  assert.ok(current.prefill.receipt.request_id.startsWith('gmd1.'));
  assert.deepEqual(current.summary, previous.summary);
  assert.equal(current.nextEligibleAt, previous.nextEligibleAt);
});

test('a verified no-admission rejection records failure and lets the next queued task analyze normally', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'gmd-production-rejected-'));
  let core;
  t.after(async () => { await core?.close(); await rm(dir, { recursive: true, force: true }); });
  const f = await fixture(t, { rejectFirstSummary: true });
  core = await createCore({ dataDir: dir, analyzer: f.analyzer, now: () => now + 100, leaseMs: 1_000_000 });
  const source = await core.registerSource({ registrationId: observation.source.registrationId, hostRunId: 'host', name: 'Controlled source' });
  await core.ingest(source, { tasks: ['task', 'peer'].map(id => ({ id, lifecycleId: 'run', status: 'running', title: 'Read the log' })),
    events: ['task', 'peer'].flatMap(taskId => observation.events.map(event => ({ ...event, taskId }))) });
  const deadline = Date.now() + 1500;
  while (!(await core.snapshot()).tasks.find(task => task.id === 'peer').analysis.summary?.generated) { assert.ok(Date.now() < deadline); await delay(5); }
  const tasks = (await core.snapshot()).tasks;
  assert.equal(tasks.find(task => task.id === 'task').analysis.error.code, 'gmd_not_admitted');
  assert.equal(tasks.find(task => task.id === 'task').analysis.summary, null);
  assert.ok(tasks.find(task => task.id === 'peer').analysis.summary.generated);
  assert.equal(f.requests.filter(request => request.body.max_tokens > 0).length, 2, 'each task receives one normal analysis; the rejection is not retried as a replacement');
  assert.equal((await core.diagnostics()).runtime.cancelledUnsettledRequests, 0);
});
