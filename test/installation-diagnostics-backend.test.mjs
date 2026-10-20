import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { startChatGPTAdapter } from '../chatgpt/adapter.mjs'
import { createLazyCore } from '../chatgpt/lazy-core.mjs'
import { supportedProfile } from '../chatgpt/storage-compatibility.mjs'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { req } from './dsh-native-runtime.mjs'
import { createAdapter } from '../dsh/host.mjs'

async function until(predicate, label) {
  const end = Date.now() + 5000
  while (!await predicate()) { assert.ok(Date.now() < end, label); await delay(5) }
}

async function mcp(adapter, token, method, params = {}) {
  const response = await fetch(adapter.url + '/mcp', { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) })
  assert.equal(response.status, 200)
  return (await response.json()).result
}

test('global installation diagnostics expose only own configuration and actual collection facts without a current chat', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-installation-diagnostics-')), token = 'c'.repeat(64)
  const server = await startServer({ dataDir }), control = await connectCore({ dataDir }), lazy = createLazyCore({ dataDir })
  let adapter
  t.after(async () => { await adapter?.close(); await lazy.close(); await control.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const own = await control.registerSource({ registrationId: 'chatgpt:own-diagnostics', hostRunId: 'own-host', name: 'PRIVATE OWN SOURCE', policy: 'chatgpt-main/1' })
  const other = await control.registerSource({ registrationId: 'PRIVATE OTHER INSTALLATION', hostRunId: 'other-host', name: 'PRIVATE OTHER SOURCE' })
  await control.ingest(own, { tasks: [{ id: 'PRIVATE OWN TASK', lifecycleId: 'own-life', title: 'PRIVATE OWN TITLE', status: 'running' }] })
  await control.ingest(other, { tasks: [{ id: 'PRIVATE OTHER TASK', lifecycleId: 'other-life', title: 'PRIVATE OTHER TITLE', status: 'running' }] })
  const initial = await control.installationAnalysisConfig('chatgpt:own-diagnostics')
  const saved = await control.configureInstallationAnalysis('chatgpt:own-diagnostics', { llm: { baseUrl: 'http://127.0.0.1:1', model: 'PRIVATE MODEL' } }, initial.revision)
  await control.recordCollectionOutcome({ sourceKey: own, mainId: null, lifecycleId: null, channel: 'directory', roundId: 'own-failure', outcome: 'failed', settledAt: Date.now(), error: { code: 'metadata-unavailable', stage: 'metadata' } })
  let threadCalls = 0, profile = supportedProfile, enabled = true
  adapter = await startChatGPTAdapter({ core: lazy, native: {
    async sampleHost() { return { available: true, hostRunId: 'qualified-host', hostStartedAt: 1, profile } },
    async readEnablement(cwd) { assert.equal(cwd, null); return { user: enabled ? 'enabled' : 'disabled', effective: enabled ? 'enabled' : 'disabled' } },
    async readThread() { threadCalls++; throw new Error('diagnostics must not read a chat') },
  }, installationId: 'own-diagnostics', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  const tools = (await mcp(adapter, token, 'tools/list')).tools
  const tool = tools.find(tool => tool.name === 'sentinel_installation_diagnostics')
  assert.ok(tool, 'the installation-only app tool exists')
  assert.deepEqual(tool._meta.ui.visibility, ['app'])
  assert.equal(tool.annotations.readOnlyHint, true)
  const result = await mcp(adapter, token, 'tools/call', { name: tool.name, arguments: {} })
  assert.ok(!result.isError, JSON.stringify(result))
  const view = result.structuredContent
  assert.equal(view.protocol, 1)
  assert.equal(view.registrationId, 'chatgpt:own-diagnostics')
  assert.equal(view.configRevision, saved.revision)
  assert.deepEqual(view.models, { llm: { configured: true }, jev: { configured: false } })
  assert.equal(view.collection.sourceCount, 1)
  assert.equal(view.collection.connectedSources, 1)
  assert.equal(view.collection.state, 'failed')
  assert.deepEqual(view.collection.error, { stage: 'metadata', code: 'metadata-unavailable' })
  assert.deepEqual(view.analysis.counts, { scope: 'active-tasks', total: 1, states: { unknown: 1 } })
  assert.equal(view.analysis.latest.state, 'unknown')
  assert.equal(view.analysis.latest.completedAt, null)
  assert.equal(view.analysis.latestFailure, null)
  assert.equal(threadCalls, 0)
  assert.ok(!JSON.stringify(view).includes('PRIVATE'))
  assert.ok(!JSON.stringify(view).includes(token))
  assert.ok(!JSON.stringify(view).includes(dataDir))
  assert.equal((await control.snapshot()).sources.length, 2, 'diagnostics do not register a collection source')
  profile = null
  const unsupported = (await mcp(adapter, token, 'tools/call', { name: tool.name, arguments: {} })).structuredContent
  assert.equal(unsupported.collection.state, 'unconfirmed', 'old connected sources cannot hide an unsupported current host')
  assert.equal(unsupported.collection.connectedSources, 1)
  assert.deepEqual(unsupported.collection.error, { stage: 'host-verification', code: 'unsupported-host-version' })
  enabled = false
  assert.equal((await mcp(adapter, token, 'tools/call', { name: tool.name, arguments: {} })).isError, true)
  profile = supportedProfile; enabled = true
  assert.equal((await mcp(adapter, token, 'tools/call', { name: tool.name, arguments: { registrationId: 'PRIVATE OTHER INSTALLATION' } })).isError, true)
})

test('an unconfigured installation with no source reports unknown collection and no retained analysis', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-empty-diagnostics-')), token = 'f'.repeat(64)
  const server = await startServer({ dataDir }), core = await connectCore({ dataDir })
  const adapter = await startChatGPTAdapter({ core, native: {
    async sampleHost() { return { available: true, hostRunId: 'empty-host', hostStartedAt: 1, profile: supportedProfile } },
    async readEnablement() { return { user: 'enabled', effective: 'enabled' } },
  }, installationId: 'empty', sessionsRoot: dataDir, dataDir: join(dataDir, 'adapter'), collectMaterials: true, token, pollMs: 0 })
  t.after(async () => { await adapter.close(); await core.close(); await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const result = (await mcp(adapter, token, 'tools/call', { name: 'sentinel_installation_diagnostics', arguments: {} })).structuredContent
  assert.deepEqual(result.models, { llm: { configured: false }, jev: { configured: false } })
  assert.deepEqual(result.collection, { state: 'unconfirmed', sourceCount: 0, connectedSources: 0, lastSeenAt: null, error: null })
  assert.deepEqual(result.analysis, { counts: { scope: 'active-tasks', total: 0, states: {} }, latest: null, latestFailure: null })
})

test('Native Remote installation diagnostics retain the real failed model stage and exclude other installations', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-diagnostics-')), modelCalls = []
  const models = createServer((request, response) => { modelCalls.push([request.method, request.url]); response.writeHead(503, { 'content-type': 'application/json' }).end('{"error":{"code":"own-service-offline"}}') })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const server = await startServer({ dataDir }), control = await connectCore({ dataDir }), ctx = new (req('@deepseek-ai/cordis').Context)()
  let adapter
  t.after(async () => { await adapter?.close(); await ctx.fiber.dispose(); await control.close(); await server.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }) })
  await ctx.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await ctx.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  await ctx.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await ctx.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  const peer = new (req('@deepseek-ai/dsh-client-connection').Peer)(ctx, { principal: { kind: 'owner' }, interactions: 'none' })
  adapter = createAdapter(ctx, { dataDir, registrationId: 'dsh:diagnostics' }, { flushMs: 5, heartbeatMs: 1000 })
  await until(() => adapter.connected, 'the production native adapter is connected')
  const initial = await control.installationAnalysisConfig('dsh:diagnostics'), baseUrl = `http://127.0.0.1:${models.address().port}`
  const saved = await control.configureInstallationAnalysis('dsh:diagnostics', { llm: { baseUrl, model: 'PRIVATE SUMMARY' }, jev: { baseUrl, model: 'PRIVATE JUDGE' } }, initial.revision)
  const own = await control.registerSource({ registrationId: 'dsh:diagnostics', hostRunId: 'seed-host', name: 'PRIVATE OWN' })
  const other = await control.registerSource({ registrationId: 'PRIVATE OTHER', hostRunId: 'other-host', name: 'PRIVATE OTHER' })
  for (const [source, id] of [[own, 'own'], [other, 'PRIVATE OTHER TASK']]) await control.ingest(source, { tasks: [{ id, lifecycleId: 'life', title: 'PRIVATE TITLE', status: 'running' }], events: [
    { id: 'input', taskId: id, taskLifecycleId: 'life', kind: 'input', text: 'PRIVATE REQUEST', capturedAt: Date.now(), occurredAt: null },
    { id: 'output', taskId: id, taskLifecycleId: 'life', kind: 'output', text: 'PRIVATE EVIDENCE', capturedAt: Date.now(), occurredAt: null },
  ] })
  await until(async () => (await control.snapshot()).tasks.find(task => task.id === 'own').analysis.state === 'failed', 'the actual summary HTTP failure is recorded')
  await control.ingest(own, { tasks: [{ id: 'PRIVATE ENDED', lifecycleId: 'ended-life', title: 'PRIVATE HISTORY', status: 'idle' }] })
  const result = await ctx.typertGateway.dispatchRpc('sentinel/installationDiagnostics', { args: {} }, undefined, peer)
  assert.equal(result.ok, true, JSON.stringify(result.error))
  const view = result.value
  assert.equal(view.registrationId, 'dsh:diagnostics')
  assert.equal(view.configRevision, saved.revision)
  assert.deepEqual(view.models, { llm: { configured: true }, jev: { configured: true } })
  assert.deepEqual(view.analysis.counts, { scope: 'active-tasks', total: 1, states: { failed: 1 } })
  assert.equal(view.analysis.latest.state, 'failed')
  assert.equal(view.analysis.latest.resultDisposition, 'invalid')
  assert.deepEqual(view.analysis.latest.error, { stage: 'llm', code: 'http_error' })
  assert.equal(view.analysis.latestFailure.stage, 'llm')
  assert.equal(view.analysis.latestFailure.code, 'http_error')
  assert.ok(Number.isSafeInteger(view.analysis.latestFailure.at))
  assert.ok(!JSON.stringify(view).includes('PRIVATE'))
  assert.deepEqual(modelCalls, [['POST', '/v1/chat/completions']], 'reading diagnostics adds no directory or inference request')
  const changed = await control.configureInstallationAnalysis('dsh:diagnostics', { llm: { baseUrl, model: 'new-summary' }, jev: { baseUrl, model: 'new-judge' } }, saved.revision)
  const retained = (await ctx.typertGateway.dispatchRpc('sentinel/installationDiagnostics', { args: {} }, undefined, peer)).value
  assert.equal(retained.configRevision, changed.revision)
  assert.equal(retained.analysis.latest.configRevision, saved.revision, 'the retained failure remains bound to its older actual round')
  assert.equal(retained.analysis.latest.resultDisposition, 'invalid')
  const adapterSource = (await control.snapshot()).sources.find(source => source.descriptor.registrationId === 'dsh:diagnostics' && source.key !== own)
  await control.unregisterSource(adapterSource.key, 'own diagnostics disconnect')
  await until(() => adapter.diagnostics().adapter.communicationFailure?.active, 'the adapter records its actual failed heartbeat')
  const disconnected = (await ctx.typertGateway.dispatchRpc('sentinel/installationDiagnostics', { args: {} }, undefined, peer)).value
  assert.equal(disconnected.collection.state, 'failed')
  assert.deepEqual(disconnected.collection.error, { stage: 'core', code: null })
  assert.ok(disconnected.collection.connectedSources > 0, 'a different retained source connection cannot hide the adapter failure')
  assert.deepEqual(modelCalls, [['POST', '/v1/chat/completions']])
})
