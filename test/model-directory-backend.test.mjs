import test from 'node:test'
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { supportedProfile } from '../chatgpt/storage-compatibility.mjs'

const { Context } = req('@deepseek-ai/cordis')
async function until(predicate, label) {
  const end = Date.now() + 5000
  while (!await predicate()) { assert.ok(Date.now() < end, label); await delay(5) }
}
async function nativeHost(t, connect) {
  const ctx = new Context()
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  const { Peer } = req('@deepseek-ai/dsh-client-connection')
  const peer = new Peer(ctx, { principal: { kind: 'owner' }, interactions: 'none' })
  const adapter = createAdapter(ctx, { registrationId: 'directory-owned' }, { connect, flushMs: 10000 })
  t.after(async () => { await adapter.close(); await ctx.fiber.dispose() })
  await until(() => !adapter.diagnostics().adapter.tickInFlight, 'native initial Core attempt settles')
  return { adapter, rpc: (method, args = {}, signal) => ctx.typertGateway.dispatchRpc(`sentinel/${method}`, { args }, signal, peer) }
}
async function directory(t, reply) {
  const calls = [], server = createServer((request, response) => { calls.push([request.method, request.url]); reply(response, request) })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  return { calls, baseUrl: `http://127.0.0.1:${server.address().port}` }
}
async function codex(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-directory-backend-')), token = 'd'.repeat(64)
  let enabled = true, coreCalls = 0
  const adapter = await startChatGPTAdapter({ core: { async health() { coreCalls++; throw new Error('directory must not use Core') }, async installationAnalysisConfig() { coreCalls++; throw new Error('directory must not read config') } }, native: {
    async sampleHost() { return { available: true, hostRunId: 'directory-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
  }, installationId: 'directory', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  t.after(async () => { await adapter.close(); await rm(dataDir, { recursive: true, force: true }) })
  const rpc = async baseUrl => {
    try {
      const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'sentinel_model_directory', arguments: { baseUrl } } }) })
      return (await response.json()).result
    } catch (error) { return { transportError: error.name } }
  }
  return { adapter, rpc, setEnabled(value) { enabled = value }, coreCalls: () => coreCalls }
}

test('Native Remote reads a bounded GET-only model directory without connecting Core or saving models', async t => {
  let coreAttempts = 0
  const host = await nativeHost(t, async () => { coreAttempts++; throw new Error('Own Core unavailable') })
  const models = await directory(t, response => response.end(JSON.stringify({ data: Array.from({ length: 257 }, (_, index) => ({ id: `model-${index}` })) })))
  const before = coreAttempts, result = await host.rpc('modelDirectory', { directorySelection: { baseUrl: models.baseUrl } })
  assert.equal(result.ok, true, JSON.stringify(result.error))
  assert.equal(result.value.inferenceVerified, false)
  assert.equal(result.value.models.length, 256)
  assert.equal(result.value.complete, false)
  assert.equal(result.value.state, 'online')
  assert.equal(coreAttempts, before)
  assert.deepEqual(models.calls, [['GET', '/v1/models']])
  const invalid = await host.rpc('modelDirectory', { directorySelection: { baseUrl: 'https://example.com' } })
  assert.equal(invalid.ok, false)
  assert.deepEqual(models.calls, [['GET', '/v1/models']])
})

test('Codex and native DSH return the same explicit GMD dual capabilities without Core or inference', async t => {
  const native = await nativeHost(t, async () => { throw new Error('Own Core unavailable') }), app = await codex(t)
  const models = await directory(t, (response, request) => response.end(JSON.stringify(request.url === '/v1/models'
    ? { data: [{ id: 'dual', source: 'native', owned_by: 'ghostmodeldeck' }] }
    : { source: 'ghost-model-deck', models: [
      { key: 'dual', type: 'llm', source: 'ghost-model-deck', loaded_instances: [{ id: 'dual' }] },
      { key: 'dual', type: 'decision', source: 'ghost-model-deck', loaded_instances: [{ id: 'dual' }] },
    ] })))
  const dsh = (await native.rpc('modelDirectory', { directorySelection: { baseUrl: models.baseUrl } })).value
  const chatgpt = (await app.rpc(models.baseUrl)).structuredContent
  for (const result of [dsh, chatgpt]) {
    assert.equal(result.state, 'online'); assert.equal(result.inferenceVerified, false)
    assert.deepEqual(result.models, [{ id: 'dual', role: 'multiple', roles: ['summary', 'jev'] }])
  }
  assert.equal(app.coreCalls(), 0)
  assert.deepEqual(models.calls, [['GET', '/v1/models'], ['GET', '/api/v1/models'], ['GET', '/v1/models'], ['GET', '/api/v1/models']])
})

test('Native directory caller cancellation and Host close release requests and seal late model metadata', async t => {
  const host = await nativeHost(t, async () => { throw new Error('Own Core unavailable') }), responses = []
  const models = await directory(t, response => responses.push(response)), abort = new AbortController()
  const first = host.rpc('modelDirectory', { directorySelection: { baseUrl: models.baseUrl } }, abort.signal)
  await until(() => responses.length === 1, 'the actual directory GET is held')
  abort.abort()
  assert.equal((await first).ok, false)
  await until(() => host.adapter.diagnostics().adapter.activeRequests === 0, 'caller cancellation releases directory request')
  const second = host.rpc('modelDirectory', { directorySelection: { baseUrl: models.baseUrl } })
  await until(() => responses.length === 2, 'the next directory GET is held')
  const closed = await host.adapter.close()
  assert.equal(closed.status, 'closed')
  assert.equal(closed.pendingRequests, 0)
  for (const response of responses) response.end('{"data":[{"id":"late"}]}')
  assert.equal((await second).ok, false)
  assert.deepEqual(models.calls, [['GET', '/v1/models'], ['GET', '/v1/models']])
})

test('Codex directory rechecks plugin qualification and close cancels its held GET without using Core', async t => {
  const host = await codex(t), responses = [], models = await directory(t, response => responses.push(response))
  const first = host.rpc(models.baseUrl)
  await until(() => responses.length === 1, 'the qualified app-only GET is held')
  host.setEnabled(false)
  responses[0].end('{"data":[{"id":"late-disabled"}]}')
  const disabled = await first
  assert.equal(disabled.isError, true)
  assert.ok(!JSON.stringify(disabled).includes('late-disabled'))
  host.setEnabled(true)
  const second = host.rpc(models.baseUrl)
  await until(() => responses.length === 2, 'the requalified GET is held')
  await host.adapter.close()
  responses[1].end('{"data":[{"id":"late-closed"}]}')
  const closed = await second
  assert.ok(closed.transportError || closed.isError)
  assert.ok(!JSON.stringify(closed).includes('late-closed'))
  assert.equal(host.adapter.diagnostics().pendingOperations, 0)
  assert.equal(host.adapter.diagnostics().activeRequests, 0)
  assert.equal(host.coreCalls(), 0)
  assert.deepEqual(models.calls, [['GET', '/v1/models'], ['GET', '/v1/models']])
})

test('public directory reads retain the byte budget, invalid-result states and five-second deadline', { timeout: 12000 }, async t => {
  const native = await nativeHost(t, async () => { throw new Error('Own Core unavailable') }), app = await codex(t)
  let mode = 'large'
  const models = await directory(t, response => {
    if (mode === 'large') response.end(JSON.stringify({ data: [], padding: 'x'.repeat(262144) }))
    else if (mode === 'invalid') response.end('{"models":[]}')
    else if (mode === 'http') response.writeHead(503).end('{}')
  })
  for (const read of [async () => (await native.rpc('modelDirectory', { directorySelection: { baseUrl: models.baseUrl } })).value, async () => (await app.rpc(models.baseUrl)).structuredContent]) {
    mode = 'large'
    const large = await read()
    assert.equal(large.state, 'unconfirmed')
    assert.equal(large.error.code, 'model_directory_limit')
    assert.equal(large.inferenceVerified, false)
    mode = 'invalid'
    assert.equal((await read()).error.code, 'model_directory_invalid')
    mode = 'http'
    assert.deepEqual((await read()).error, { code: 'model_directory_http', status: 503 })
  }
  mode = 'held'
  const started = Date.now(), expired = (await app.rpc(models.baseUrl)).structuredContent
  assert.equal(expired.error.code, 'model_directory_timeout')
  assert.equal(expired.inferenceVerified, false)
  assert.ok(Date.now() - started < 8000, 'directory metadata has a five-second deadline')
  assert.ok(models.calls.every(([method, path]) => method === 'GET' && path === '/v1/models'))
  assert.equal(app.coreCalls(), 0)
})
