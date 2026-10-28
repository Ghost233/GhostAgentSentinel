import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { discoverGMDPrefill, createGMDOperation, gmdOperationNonce } from '../src/analysis/gmd-prefill-client.mjs';
import { failureErrorInfo } from '../src/core/failure-receipts.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
async function fixture(t, options = {}) {
  const posts = [], operations = new Map(), rejections = new Map();
  const lifetime = new AbortController();
  const cleanupDeadline = setTimeout(() => lifetime.abort(new Error('Controlled client test deadline')), 5000);
  let executions = 0, queries = 0, cancels = 0;
  const model = 'test-model', generation = 'gmd-boot.native-service.12.1';
  const send = (res, status, value) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(value)); };
  function view(id, state = 'completed') {
    const operation = operations.get(id);
    const terminal = ['completed', 'cancelled', 'failed'].includes(state);
    return { protocol: 'gmd-prefill/1', request_id: id, model, runtime_generation: generation,
      cache_salt_sha256: hash(operation.body.cache_salt), state, terminal, lease_released: terminal,
      children: terminal ? [{ question_id: null, native_request_id: 1, runtime_generation: 1, state: operation.prefill ? 'length' : 'stop', prompt_tokens: 12, completion_tokens: operation.prefill ? 0 : 3, matched_prompt_tokens: 4, deduplicated_tokens: 0, lease_released: true }] : [] };
  }
  function rejection(id, body, recovered = false) {
    const value = { request_id: id, state: 'rejected', terminal: true, lease_released: true, runtime_generation: generation,
      native_dispatch: 'not_started', proof_scope: options.noAdmission.scope,
      ...(options.noAdmission.scope === 'bound' ? { model, cache_salt_sha256: hash(body.cache_salt) } : {}),
      error: { code: recovered && options.noAdmission.scope === 'identity_only' ? 'not_admitted' : options.noAdmission.code ?? 'capacity_unavailable', stage: 'admission' } };
    options.rejection?.(value);
    return value;
  }
  const unregistered = (id, body) => ({ request_id: id, model, runtime_generation: generation, cache_salt_sha256: hash(body.cache_salt), state: 'queued', terminal: false, lease_released: false });
  const server = createServer(async (req, res) => {
    if (req.url === '/api/v1/models') {
      const directory = { source: 'ghost-model-deck', models: [{ key: model, type: options.jev ? 'decision' : 'llm', source: 'ghost-model-deck', loaded_instances: [{ id: model }], capabilities: { prefill: {
      protocol: 'gmd-prefill/1', supported: options.supported !== false, mode: options.jev ? 'systemone_state' : 'chat_max_tokens_zero', runtime_generation: generation,
      operation_policy: { id_format: 'gmd1', time_basis_ms: 1_800_000_000_000, past_window_ms: 600_000, future_tolerance_ms: 60_000 },
      } } }] };
      options.metadata?.(directory);
      return send(res, 200, directory);
    }
    if (req.url.startsWith('/api/v1/inference/requests/')) {
      const id = req.url.split('/')[5];
      const rejected = rejections.get(id);
      if (rejected && rejected.token === req.headers['x-gmd-operation-token']) {
        if (req.url.endsWith('/cancel')) cancels++; else { queries++; options.onQuery?.(queries); }
        if (!req.url.endsWith('/cancel') && options.holdQuery) return options.holdQuery(() => send(res, 503, { error: { code: 'status_unavailable' } }));
        if (options.noAdmission.pending && !cancels) return send(res, 200, unregistered(id, rejected.body));
        return send(res, 200, rejection(id, rejected.body, true));
      }
      if (!operations.has(id) || operations.get(id).token !== req.headers['x-gmd-operation-token'] || options.missing) return send(res, 404, { error: { code: 'operation_not_found' } });
      if (req.url.endsWith('/cancel')) { cancels++; if (options.holdCancel) return; return send(res, options.cancelStatus ?? 200, view(id, options.cancelState ?? 'cancel_requested')); }
      queries++;
      options.onQuery?.(queries);
      const status = view(id, cancels ? 'cancelled' : options.state?.(queries) ?? 'completed');
      if (options.omitPendingChildren && !status.terminal) delete status.children;
      options.status?.(status);
      return send(res, 200, status);
    }
    let raw = ''; for await (const chunk of req) raw += chunk;
    const body = JSON.parse(raw), id = req.headers['idempotency-key'];
    posts.push({ raw, body, id, headers: req.headers });
    if (id?.split('.').at(-1) !== gmdOperationNonce(req.headers['x-gmd-operation-token'])) return send(res, 403, { error: { code: 'identity_mismatch' } });
    if (options.noAdmission) {
      rejections.set(id, { body, token: req.headers['x-gmd-operation-token'] });
      if (options.noAdmission.dropResponse) return res.destroy();
      if (options.noAdmission.pending) return send(res, 202, unregistered(id, body));
      return send(res, options.noAdmission.status ?? 503, rejection(id, body));
    }
    if (operations.has(id) && (operations.get(id).token !== req.headers['x-gmd-operation-token'] || operations.get(id).raw !== raw)) return send(res, 409, { error: { code: 'binding_conflict' } });
    if (options.rejectBeforeRegistration) return send(res, 503, { error: { code: 'prefill_registration_capacity' } });
    if (!operations.has(id)) {
      executions++;
      operations.set(id, { body, raw, token: req.headers['x-gmd-operation-token'], prefill: body.max_tokens === 0 || body.prefill_only === true });
      options.onPost?.();
      if (options.holdPost) await options.holdPost;
      if (options.dropFirst) return res.destroy();
      if (options.truncateFirst) { res.writeHead(200, { 'content-type': 'application/json' }); return res.end('{"model":'); }
      if (options.failFirstDelivery) return send(res, 500, { error: { code: 'response_delivery_failed' } });
      if (options.pending) return send(res, 202, view(id, 'running'));
    }
    const operation = operations.get(id);
    let metadata = view(id);
    if (options.invalidBinding) metadata = { ...metadata, request_id: 'foreign-operation' };
    if (options.emptyChildren) metadata.children = [];
    if (options.nativeState) metadata.children[0].state = options.nativeState;
    if (operation.prefill) return send(res, 200, { model, ...(options.jev ? { object: 'systemone.prefill' } : { choices: [] }), prefill: { ...metadata, generated_tokens: 0 } });
    return send(res, 200, { model, choices: [{ finish_reason: options.finishReason ?? 'stop', message: { content: options.content ?? '完整结果' } }], cache: metadata });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => { clearTimeout(cleanupDeadline); lifetime.abort(new Error('Controlled fixture closed')); return new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }); });
  const config = { baseUrl: `http://127.0.0.1:${server.address().port}`, model };
  const mode = options.jev ? 'systemone_state' : 'chat_max_tokens_zero';
  return { config, mode, posts, generation, get executions() { return executions; }, get queries() { return queries; }, get cancels() { return cancels; },
    async operation(body) {
      const operation = createGMDOperation(config, { mode, capability: await discoverGMDPrefill(config, { mode }), body });
      const execute = operation.execute;
      operation.execute = (settings = {}) => execute({ ...settings, lifetimeSignal: settings.lifetimeSignal ? AbortSignal.any([settings.lifetimeSignal, lifetime.signal]) : lifetime.signal });
      return operation;
    } };
}
const body = () => ({ model: 'test-model', stream: false, max_tokens: 0, cache_salt: 'opaque-install-task-role', messages: [{ role: 'user', content: '整理中文执行材料 😀' }] });

test('operation identity authentication matches the shared domain-separated byte vectors', () => {
  assert.equal(gmdOperationNonce('aa'.repeat(32)), 'faa591af733fea97bd8c09da5bdd24b5');
  assert.equal(gmdOperationNonce('bb'.repeat(32)), '4146148b74e124330f8b80e7a7fd192c');
  assert.throws(() => gmdOperationNonce('aa'), error => error.code === 'configuration');
});

test('unsupported capability never submits an inference request', async t => {
  const f = await fixture(t, { supported: false });
  const capability = await discoverGMDPrefill(f.config, { mode: f.mode });
  assert.equal(capability.supported, false);
  assert.throws(() => createGMDOperation(f.config, { mode: f.mode, capability, body: body() }), error => error.code === 'prefill_unsupported');
  assert.equal(f.posts.length, 0);
});

test('prefill freezes UTF-8 bytes, issues a generation-bound gmd1 ID and keeps credentials out of diagnostics', async t => {
  const f = await fixture(t), input = body(), operation = await f.operation(input);
  input.messages[0].content = 'Changed after preparation';
  const result = await operation.execute();
  assert.equal(result.prefill.generated_tokens, 0);
  assert.equal(f.posts[0].body.messages[0].content, '整理中文执行材料 😀');
  assert.ok(f.posts[0].id.startsWith('gmd1.' + hash(f.generation) + '.'));
  assert.ok(/^gmd1\.[a-f0-9]{64}\.[0-9]+\.[a-f0-9]{32}$/.test(f.posts[0].id));
  assert.equal(Number(f.posts[0].headers['content-length']), Buffer.byteLength(f.posts[0].raw));
  assert.ok(/^[a-f0-9]{64}$/.test(f.posts[0].headers['x-gmd-operation-token']));
  const expectedTag = createHash('sha256').update('gmd-prefill/1:operation-token', 'utf8').update(Buffer.from([0]))
    .update(Buffer.from(f.posts[0].headers['x-gmd-operation-token'], 'hex')).digest().subarray(0, 16).toString('hex');
  assert.equal(f.posts[0].id.split('.').at(-1), expectedTag, 'unregistered identity must authenticate the operation token');
  assert.equal(JSON.stringify(operation.snapshot()).includes(f.posts[0].headers['x-gmd-operation-token']), false);
  assert.equal(JSON.stringify(operation.snapshot()).includes('中文'), false);
});

test('202 and unknown reuse one operation and replay the original response after confirmed release', async t => {
  const states = [];
  const f = await fixture(t, { pending: true, state: index => index === 1 ? 'unknown' : 'completed' });
  const operation = await f.operation(body());
  await operation.execute({ onState: state => states.push(state) });
  assert.ok(states.includes('unknown'));
  assert.equal(f.executions, 1);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[0].id, f.posts[1].id);
  assert.equal(f.posts[0].raw, f.posts[1].raw);
  assert.equal(f.posts[0].headers['x-gmd-operation-token'], f.posts[1].headers['x-gmd-operation-token']);
});

test('a lost POST response is recovered by query and same-ID replay, not a second execution', async t => {
  const f = await fixture(t, { dropFirst: true });
  await (await f.operation(body())).execute();
  assert.equal(f.executions, 1);
  assert.equal(f.posts.length, 2);
  assert.equal(f.posts[0].id, f.posts[1].id);
});

test('cancellation waits for confirmed release before rejecting the sealed operation', async t => {
  const controller = new AbortController();
  const f = await fixture(t, { pending: true, state: () => 'running', onQuery: () => controller.abort(new Error('Logical cancellation')) });
  const operation = await f.operation(body());
  await assert.rejects(operation.execute({ signal: controller.signal }), /Logical cancellation/);
  assert.equal(f.cancels, 1);
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.executions, 1);
});

test('an HTTP 202 cancellation acknowledgement remains pending until native release', async t => {
  let release, observed;
  const holdPost = new Promise(resolve => { release = resolve; });
  const posted = new Promise(resolve => { observed = resolve; });
  const f = await fixture(t, { holdPost, onPost: observed, cancelStatus: 202 });
  t.after(release);
  const operation = await f.operation(body());
  let settled = false;
  const finished = assert.rejects(operation.execute(), error => error.code === 'cancelled').finally(() => { settled = true; });
  await posted;
  try {
    const acknowledgement = await operation.cancel();
    assert.equal(acknowledgement.state, 'cancel_requested');
    assert.equal(acknowledgement.lease_released, false);
    assert.equal(settled, false);
  } finally {
    release();
    await finished;
  }
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.executions, 1);
});

test('404 stays unknown and a closed lifetime stops probes without claiming native release', async t => {
  const lifetime = new AbortController();
  const f = await fixture(t, { pending: true, missing: true });
  const operation = await f.operation(body());
  await assert.rejects(operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('Owner closed')); } }), /Owner closed/);
  assert.equal(operation.snapshot().lease_released, false);
  assert.equal(f.posts.length, 1);
});

test('foreign binding and empty native prefill evidence cannot publish a successful result', async t => {
  for (const options of [{ invalidBinding: true }, { emptyChildren: true }, { nativeState: 'cancelled' }]) {
    const f = await fixture(t, options);
    await assert.rejects((await f.operation(body())).execute(), error => ['gmd_binding_mismatch', 'gmd_invalid_evidence'].includes(error.code));
    assert.equal(f.executions, 1);
  }
});

test('normal generation retains strict finish and nonempty text validation', async t => {
  for (const options of [{ finishReason: 'length' }, { content: '' }]) {
    const f = await fixture(t, options);
    await assert.rejects((await f.operation({ ...body(), max_tokens: 32 })).execute(), error => error.code === 'incomplete_response');
  }
});

test('supported JEV state prefill is separate from answers and requires native evidence', async t => {
  const f = await fixture(t, { jev: true });
  const result = await (await f.operation({ model: 'test-model', stream: false, cache_salt: 'separate-jev-role', state: { context: '状态材料' }, prefill_only: true })).execute();
  assert.equal(result.object, 'systemone.prefill');
  assert.equal(result.answers, undefined);
  assert.equal(result.prefill.children.length, 1);
});

test('an already stopped transport never submits or starts cancellation probes', async t => {
  const f = await fixture(t), operation = await f.operation(body());
  const transport = new AbortController(), lifetime = new AbortController();
  transport.abort(new Error('Transport already stopped'));
  await assert.rejects(operation.execute({ transportSignal: transport.signal, lifetimeSignal: lifetime.signal, onState() { lifetime.abort(new Error('Unexpected probe')); } }), error => error === transport.signal.reason);
  assert.equal(f.posts.length, 0);
  assert.equal(f.cancels, 0);
});

test('a stalled cancellation acknowledgement does not erase independently confirmed release', async t => {
  const controller = new AbortController(), states = [];
  const f = await fixture(t, { pending: true, holdCancel: true, state: () => 'running', onQuery: () => controller.abort(new Error('Cancel while running')) });
  const operation = await f.operation(body());
  await assert.rejects(operation.execute({ signal: controller.signal, onState: state => states.push(state) }), /Cancel while running/);
  assert.equal(operation.snapshot().lease_released, true);
  assert.notEqual(states.at(-1), 'unknown');
});

test('a late successful response cannot restore a directly cancelled operation', async t => {
  let release, observed;
  const holdPost = new Promise(resolve => { release = resolve; });
  const posted = new Promise(resolve => { observed = resolve; });
  const f = await fixture(t, { holdPost, onPost: observed, cancelState: 'cancelled' });
  const operation = await f.operation(body());
  const finished = assert.rejects(operation.execute(), error => error.code === 'cancelled');
  await posted;
  await operation.cancel();
  release();
  await finished;
  assert.equal(operation.snapshot().state, 'cancelled');
  assert.equal(operation.snapshot().lease_released, true);
});

test('changing caller configuration cannot reroute or relabel an already prepared operation', async t => {
  const f = await fixture(t), config = { ...f.config };
  const capability = await discoverGMDPrefill(config, { mode: f.mode });
  const operation = createGMDOperation(config, { mode: f.mode, capability, body: body() });
  config.baseUrl = 'http://127.0.0.1:1'; config.model = 'replacement-model';
  capability.runtime_generation = 'replacement-generation';
  const lifetime = new AbortController();
  const result = await operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('Original binding was not preserved')); } });
  assert.equal(result.model, f.config.model);
  assert.equal(operation.snapshot().runtime_generation, f.generation);
  assert.equal(f.posts.length, 1);
});

test('old and incomplete declarations stay disabled without inference', async t => {
  for (const metadata of [directory => { delete directory.models[0].capabilities; }, directory => { delete directory.models[0].capabilities.prefill.operation_policy; },
    directory => { directory.models[0].capabilities.prefill.runtime_generation = '中文代次'; }, directory => { directory.models[0].capabilities.prefill.runtime_generation = ' padded-generation'; }]) {
    const f = await fixture(t, { metadata });
    const capability = await discoverGMDPrefill(f.config, { mode: f.mode });
    assert.equal(capability.supported, false);
    assert.equal(f.posts.length, 0);
  }
});

test('a damaged delivery recovers the stored result and retains the original communication failure', async t => {
  for (const options of [{ truncateFirst: true }, { failFirstDelivery: true }]) {
    const f = await fixture(t, options), operation = await f.operation(body());
    await operation.execute();
    assert.equal(f.executions, 1);
    assert.equal(f.posts.length, 2);
    assert.ok(['invalid_json', 'gmd_http'].includes(operation.snapshot().communicationFailure.code));
  }
});

test('a failed released operation preserves the remote result-loss cause without another submission', async t => {
  const f = await fixture(t, { pending: true, state: () => 'failed', status: value => { value.error = { code: 'result_unavailable', stage: 'response', message: 'private provider details must not be copied' }; } });
  const operation = await f.operation({ ...body(), max_tokens: 32 });
  await assert.rejects(operation.execute(), error => error.code === 'gmd_operation_failed' && error.details?.remoteCode === 'result_unavailable' && error.message.includes('result_unavailable'));
  assert.deepEqual(operation.snapshot().error, { code: 'result_unavailable', stage: 'response' });
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.posts.length, 1);
  assert.equal(f.executions, 1);
});

test('JEV question shape is checked locally before registering any native operation', async t => {
  const f = await fixture(t, { jev: true });
  const capability = await discoverGMDPrefill(f.config, { mode: f.mode });
  for (const questions of [null, [], { unexpected: {} }]) assert.throws(() => createGMDOperation(f.config, { mode: f.mode, capability,
    body: { model: 'test-model', stream: false, cache_salt: 'jev', state: 'State', prefill_only: true, questions },
  }), error => error.code === 'configuration' && error.stage === 'jev');
  assert.equal(f.posts.length, 0);
});

test('public failure normalization retains a specific client protocol fault', async t => {
  const f = await fixture(t, { nativeState: 'cancelled' });
  const operation = await f.operation(body());
  let failure;
  try { await operation.execute(); } catch (error) { failure = failureErrorInfo(error); }
  assert.equal(failure?.code, 'gmd_invalid_evidence');
  assert.equal(failure.stage, 'llm');
  assert.equal(failure.category, 'protocol');
  assert.equal(JSON.stringify(failure).includes(f.posts[0].headers['x-gmd-operation-token']), false);
});

test('unimplemented release-without-terminal recovery is not silently accepted', async t => {
  const lifetime = new AbortController();
  const f = await fixture(t, { pending: true, state: () => 'unknown', status: view => { view.lease_released = true; } });
  const operation = await f.operation(body());
  await assert.rejects(operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('Recovery requires a confirmed protocol')); } }), /Recovery requires a confirmed protocol/);
  assert.equal(operation.snapshot().lease_released, false);
  assert.equal(f.executions, 1);
});

test('capacity rejection without declared no-admission proof remains unknown rather than falsely releasing', async t => {
  const lifetime = new AbortController();
  const f = await fixture(t, { rejectBeforeRegistration: true }), operation = await f.operation(body());
  await assert.rejects(operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('No admission proof')); } }), /No admission proof/);
  assert.equal(f.executions, 0);
  assert.equal(f.posts.length, 1);
  assert.equal(operation.snapshot().communicationFailure.status, 503);
  assert.equal(operation.snapshot().lease_released, false, 'this is a known protocol gap, not successful capacity recovery');
});

test('a verified bound or identity-only rejection releases only this operation without a replacement call', async t => {
  for (const scope of ['bound', 'identity_only']) {
    const f = await fixture(t, { noAdmission: { scope } }), operation = await f.operation(body());
    const lifetime = new AbortController();
    await assert.rejects(operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('Expected trusted no-admission proof')); } }), error => error.code === 'gmd_not_admitted' && error.details.remoteCode === 'capacity_unavailable' && failureErrorInfo(error).code === 'gmd_not_admitted');
    const receipt = operation.snapshot();
    assert.equal(receipt.state, 'rejected');
    assert.equal(receipt.lease_released, true);
    assert.equal(receipt.proof_scope, scope);
    assert.equal(receipt.communicationFailure, undefined, 'a valid refusal is not a transport failure');
    assert.equal(Object.hasOwn(receipt, 'model'), scope === 'bound');
    assert.equal(Object.hasOwn(receipt, 'cache_salt_sha256'), scope === 'bound');
    assert.equal(f.executions, 0);
    assert.equal(f.posts.length, 1);
    assert.equal(f.queries, 0);
  }
});

test('invalid-input and busy rejections preserve their cause without changing into a positive-budget request', async t => {
  for (const [status, code] of [[400, 'invalid_prefill_request'], [409, 'prefill_context_busy']]) {
    const f = await fixture(t, { noAdmission: { scope: 'identity_only', status, code } }), operation = await f.operation(body());
    await assert.rejects(operation.execute(), error => error.code === 'gmd_not_admitted' && error.details.status === status && error.details.remoteCode === code);
    assert.equal(operation.snapshot().lease_released, true);
    assert.equal(f.posts.length, 1);
    assert.equal(f.posts[0].body.max_tokens, 0);
    assert.equal(f.executions, 0);
  }
});

test('a lost rejection response recovers identity-only proof without inventing the original cause', async t => {
  const f = await fixture(t, { noAdmission: { scope: 'identity_only', dropResponse: true } }), operation = await f.operation(body());
  await assert.rejects(operation.execute({ lifetimeSignal: AbortSignal.timeout(1500) }), error => error.code === 'gmd_not_admitted' && error.details.remoteCode === 'not_admitted');
  assert.equal(operation.snapshot().error.code, 'not_admitted');
  assert.equal(operation.snapshot().communicationFailure.code, 'network');
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.posts.length, 1);
  assert.equal(f.executions, 0);
});

test('pre-registration cancellation accepts only its own rejection proof and seals output', async t => {
  const controller = new AbortController();
  const f = await fixture(t, { noAdmission: { scope: 'identity_only', pending: true }, onQuery: () => controller.abort(new Error('Cancel before registration')) });
  const operation = await f.operation(body());
  await assert.rejects(operation.execute({ signal: controller.signal }), /Cancel before registration/);
  assert.equal(operation.snapshot().proof_scope, 'identity_only');
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.executions, 0);
  assert.equal(f.posts.length, 1);
  assert.equal(f.cancels, 1);
});

test('a late failed status query cannot erase an independently verified cancellation proof', async t => {
  const controller = new AbortController(), lifetime = new AbortController();
  let releaseQuery, verified = false;
  const f = await fixture(t, { noAdmission: { scope: 'identity_only', pending: true },
    onQuery: () => controller.abort(new Error('Cancel before registration')), holdQuery: release => { releaseQuery = release; } });
  const operation = await f.operation(body());
  const finished = assert.rejects(operation.execute({ signal: controller.signal, lifetimeSignal: lifetime.signal,
    onState(state) { if (verified && state === 'unknown') lifetime.abort(new Error('Late status erased proof')); } }), /Cancel before registration/);
  try {
    const deadline = Date.now() + 1500;
    while (!operation.snapshot().lease_released) { assert.ok(Date.now() < deadline); await new Promise(resolve => setTimeout(resolve, 2)); }
    verified = true;
  } finally {
    releaseQuery?.();
    await finished;
  }
  assert.equal(operation.snapshot().state, 'rejected');
  assert.equal(operation.snapshot().lease_released, true);
  assert.equal(f.executions, 0);
});

test('incomplete, contradictory and foreign rejection proofs cannot release admission', async t => {
  const cases = [
    ['bound', value => { value.request_id = 'foreign'; }], ['bound', value => { value.runtime_generation = 'foreign'; }],
    ['bound', value => { value.model = 'foreign'; }], ['bound', value => { value.cache_salt_sha256 = 'foreign'; }],
    ['bound', value => { value.terminal = false; }], ['bound', value => { value.lease_released = false; }],
    ['bound', value => { delete value.native_dispatch; }], ['bound', value => { value.proof_scope = 'unknown'; }],
    ['bound', value => { value.children = [{ native_request_id: 1 }]; }], ['bound', value => { value.error.stage = 'native'; }],
    ['identity_only', value => { value.model = 'test-model'; }], ['identity_only', value => { value.cache_salt_sha256 = null; }],
  ];
  for (const [scope, rejection] of cases) {
    const lifetime = new AbortController();
    const f = await fixture(t, { noAdmission: { scope }, rejection }), operation = await f.operation(body());
    await assert.rejects(operation.execute({ lifetimeSignal: lifetime.signal, onState(state) { if (state === 'unknown') lifetime.abort(new Error('Proof is not trusted')); } }), /Proof is not trusted/);
    assert.equal(operation.snapshot().lease_released, false);
    assert.equal(f.posts.length, 1);
    assert.equal(f.executions, 0);
  }
});

test('pending native association can omit children without becoming a transport failure', async t => {
  const states = [];
  const f = await fixture(t, { pending: true, omitPendingChildren: true, state: index => index === 1 ? 'running' : 'completed' });
  await (await f.operation(body())).execute({ onState: state => states.push(state) });
  assert.equal(states.includes('unknown'), false);
  assert.equal(f.executions, 1);
});
