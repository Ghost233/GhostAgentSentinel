import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { requestInference } from '../src/analysis/http.mjs';
import { AnalysisError } from '../src/analysis/index.mjs';
import { createCore } from '../src/core/index.mjs';

async function localServer(t, handler) {
  const server = createServer(handler);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  return { server, config: { baseUrl: `http://127.0.0.1:${server.address().port}`, model: 'boundary' } };
}

const request = (config, options = {}) => requestInference(config, '/inference', { model: 'boundary', private: 'PRIVATE_REQUEST_BODY' }, { stage: 'llm', timeoutMs: 1000, ...options });

test('network failures identify dispatch, incomplete response body, and refused connection without private error text', async t => {
  let mode = 'disconnect';
  const { server, config } = await localServer(t, async (req, res) => {
    for await (const _chunk of req) {}
    if (mode === 'disconnect') { req.socket.destroy(); return; }
    res.writeHead(200, { 'content-type': 'application/json', 'content-length': '1000' });
    res.flushHeaders();
    res.once('finish', () => req.socket.end());
    res.end('{"model":"boundary"}');
  });
  const dispatch = await request(config).catch(error => error);
  assert.equal(dispatch.code, 'network'); assert.equal(dispatch.stage, 'llm');
  assert.deepEqual(dispatch.details, { operation: 'fetch', name: 'TypeError', code: null, cause: { name: 'SocketError', code: 'UND_ERR_SOCKET' } });
  mode = 'truncated';
  const truncated = await request(config).catch(error => error);
  assert.equal(truncated.code, 'network'); assert.equal(truncated.stage, 'llm');
  assert.deepEqual(truncated.details, { operation: 'response-body', name: 'TypeError', code: null, cause: { name: 'SocketError', code: 'UND_ERR_SOCKET' } });
  await new Promise(resolve => { server.closeAllConnections(); server.close(resolve); });
  const refused = await request(config).catch(error => error);
  assert.equal(refused.code, 'network'); assert.equal(refused.stage, 'llm');
  assert.deepEqual(refused.details, { operation: 'fetch', name: 'TypeError', code: null, cause: { name: 'Error', code: 'ECONNREFUSED' } });
  for (const failure of [dispatch, truncated, refused]) {
    const serialized = JSON.stringify(failure);
    for (const secret of ['PRIVATE_REQUEST_BODY', config.baseUrl, 'stack', 'message', 'socket', 'address', 'port']) assert.equal(serialized.includes(secret), false);
  }
});

test('public Core failures persist safe network evidence and discard private or unrelated analyzer details', async t => {
  const { config } = await localServer(t, req => req.socket.destroy());
  const secret = 'PRIVATE_TOKEN_COOKIE_URL_BODY_STACK';
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-inference-failure-'));
  let core;
  t.after(async () => { await core?.close(); await rm(dataDir, { recursive: true, force: true }); });
  core = await createCore({ dataDir, now: () => 1_000_000, leaseMs: 1e9, analyzer: { async analyze(input) {
    if (input.task.id === 'actual') {
      const error = await request(config).catch(error => error);
      error.details ??= {};
      Object.assign(error.details, { message: secret, stack: secret, url: secret, token: secret, body: secret, cookie: secret, extra: secret });
      if (error.details.cause) Object.assign(error.details.cause, { message: secret, stack: secret, cause: { name: secret, code: secret } });
      throw error;
    }
    const details = { operation: 'fetch', name: secret, code: secret, cause: { name: secret, code: secret, message: secret }, url: secret, cookie: secret };
    if (input.task.id === 'protocol') throw new AnalysisError('invalid_json', 'llm', 'Fixed invalid JSON', details);
    if (input.task.id === 'invalid-operation') details.operation = secret;
    throw new AnalysisError('network', 'jev', 'Fixed network failure', details);
  } } });
  const source = await core.registerSource({ registrationId: 'failure', hostRunId: 'run', name: 'Source' });
  const ids = ['actual', 'unrecognized', 'invalid-operation', 'protocol'];
  await core.ingest(source, { tasks: ids.map(id => ({ id, lifecycleId: 'life', title: id, status: 'running' })),
    events: ids.map(id => ({ id: `${id}-request`, taskId: id, taskLifecycleId: 'life', kind: 'input', capturedAt: 1_000_000, occurredAt: null, text: 'Inspect the fixed scope.' })) });
  let state;
  for (let attempt = 0; attempt < 100; attempt++) {
    state = await core.snapshot();
    if (state.tasks.every(task => task.analysis.state === 'failed')) break;
    await delay(2);
  }
  assert.ok(state.tasks.every(task => task.analysis.state === 'failed'), 'all analyzer failures become publicly observable');
  const expectedActual = { code: 'network', stage: 'llm', message: '无法连接本机推理服务', details: { operation: 'fetch', name: 'TypeError', code: null, cause: { name: 'SocketError', code: 'UND_ERR_SOCKET' } } };
  const expectedUnknown = { code: 'network', stage: 'jev', message: 'Fixed network failure', details: { operation: 'fetch', name: null, code: null, cause: { name: null, code: null } } };
  const assertPublic = snapshot => {
    const failure = id => snapshot.tasks.find(task => task.id === id).analysis.error;
    assert.deepEqual(failure('actual'), expectedActual);
    assert.deepEqual(failure('unrecognized'), expectedUnknown);
    assert.equal(failure('invalid-operation').details, undefined);
    assert.equal(failure('protocol').details, undefined);
    assert.equal(JSON.stringify(snapshot).includes(secret), false);
  };
  assertPublic(state);
  state.tasks.find(task => task.id === 'actual').analysis.error.details.cause.code = secret;
  assertPublic(await core.snapshot());
  await core.close();
  core = await createCore({ dataDir, now: () => 1_000_000 });
  assertPublic(await core.snapshot());
});

test('adding network evidence preserves caller cancellation values, client deadlines, and protocol failures', async t => {
  let mode = 'held', received;
  const { config } = await localServer(t, async (req, res) => {
    for await (const _chunk of req) {}
    received?.();
    if (mode === 'held') return;
    if (mode === 'sse') { res.writeHead(200, { 'content-type': 'text/event-stream' }); res.end('data: {}\n\n'); return; }
    res.setHeader('content-type', 'application/json');
    if (mode === 'invalid-json') { res.end('incomplete JSON'); return; }
    res.writeHead(503);
    res.end(JSON.stringify({ error: { code: 'provider-refusal' } }));
  });
  for (const reason of [0, false, '', null, new Error('Fixed caller cancellation'), undefined]) {
    const alreadyCancelled = new AbortController();
    alreadyCancelled.abort(reason);
    if (reason === undefined) { assert.ok(alreadyCancelled.signal.reason instanceof DOMException); assert.equal(alreadyCancelled.signal.reason.name, 'AbortError'); }
    else assert.equal(alreadyCancelled.signal.reason, reason);
    assert.equal(await request(config, { signal: alreadyCancelled.signal }).catch(error => error), alreadyCancelled.signal.reason);
    const controller = new AbortController();
    const submitted = new Promise(resolve => { received = resolve; });
    const pending = request(config, { signal: controller.signal }).catch(error => error);
    await submitted;
    controller.abort(reason);
    assert.equal(await pending, controller.signal.reason, 'submitted cancellation retains exactly the original thrown value');
  }
  received = null;
  const deadline = await request(config, { timeoutMs: 15 }).catch(error => error);
  assert.equal(deadline.code, 'deadline'); assert.equal(deadline.stage, 'llm'); assert.equal(deadline.details, undefined);
  for (const [fault, code] of [['http', 'http_error'], ['invalid-json', 'invalid_json'], ['sse', 'unexpected_stream']]) {
    mode = fault;
    const error = await request(config).catch(error => error);
    assert.equal(error.code, code); assert.equal(error.stage, 'llm');
    assert.deepEqual(error.details, fault === 'http' ? { status: 503, remoteCode: 'provider-refusal' } : undefined);
  }
  const originalFetch = globalThis.fetch, secret = 'PRIVATE_MESSAGE_STACK_URL_TOKEN_BODY_COOKIE_NAME_CODE';
  try {
    const unsafe = Object.assign(new Error(secret), { code: secret, url: secret, token: secret, body: secret, cookie: secret,
      cause: { name: secret, code: secret, message: secret, stack: secret, cause: { name: 'Error', code: 'ECONNRESET' } } });
    Object.defineProperty(unsafe, 'name', { get() { throw new Error(secret); } });
    Object.defineProperty(unsafe.cause, 'code', { get() { throw new Error(secret); } });
    globalThis.fetch = async () => { throw unsafe; };
    const network = await request(config).catch(error => error);
    assert.equal(network.code, 'network'); assert.equal(network.stage, 'llm');
    assert.deepEqual(network.details, { operation: 'fetch', name: null, code: null, cause: { name: null, code: null } });
    assert.equal(JSON.stringify(network).includes(secret), false);
    for (const code of ['EPERM', 'EACCES']) {
      globalThis.fetch = async () => { throw Object.assign(new TypeError(secret), { cause: Object.assign(new Error(secret), { code }) }); };
      const denied = await request(config).catch(error => error);
      assert.equal(denied.code, 'network');
      assert.deepEqual(denied.details, { operation: 'fetch', name: 'TypeError', code: null, cause: { name: 'Error', code } });
      assert.equal(JSON.stringify(denied).includes(secret), false);
    }
    const protocol = new AnalysisError('invalid_response', 'jev', 'Fixed protocol error');
    globalThis.fetch = async () => { throw protocol; };
    assert.equal(await request(config).catch(error => error), protocol, 'an existing AnalysisError is rethrown by identity');
  } finally { globalThis.fetch = originalFetch; }
});
