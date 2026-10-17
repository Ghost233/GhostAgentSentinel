import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, stat, readFile, mkdir, rename } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { startServer } from '../src/server.mjs'
import { connectCore } from '../src/core-client.mjs'
import { setTimeout as delay } from 'node:timers/promises'
import { createServer } from 'node:http'

test('private loopback transport stores capture, hides secrets, supports pause and real server restart', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-server-'))
  let server = await startServer({ dataDir })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  let unexpected
  try { await assert.rejects(async () => { unexpected = await startServer({ dataDir }) }, /already running/) }
  finally { await unexpected?.close() }
  const unauthorized = await fetch(`${server.url}/v1/snapshot`)
  assert.equal(unauthorized.status, 401)
  const client = await connectCore({ dataDir })
  const key = await client.registerSource({ registrationId: 'p', hostRunId: 'host', name: 'test' })
  const result = await client.ingest(key, { tasks: [{ id: 'a', lifecycleId: 'life', title: 'Make a log', status: 'running' }],
    events: [{ id: 'e1', taskId: 'a', taskLifecycleId: 'life', kind: 'input', text: 'Make a log', occurredAt: null, capturedAt: Date.now() }] })
  assert.equal(result.accepted, 1)
  await client.pause()
  const first = await client.snapshot()
  assert.equal(first.tasks[0].currentRequest, 'Make a log')
  assert.equal(JSON.stringify(first).includes(server.token), false)
  assert.equal((await stat(join(dataDir, 'runtime.json'))).mode & 0o777, 0o600)
  await assert.rejects(client.configureAnalysis({ llm: { baseUrl: 'https://example.org', model: 'x' } }), /loopback/)
  await client.configureAnalysis({ llm: { baseUrl: 'http://localhost:1234', model: 'x' } })
  assert.equal((await client.analysisConfig()).llm.model, 'x')
  await client.close()
  await server.close()
  server = await startServer({ dataDir })
  const second = await connectCore({ dataDir })
  assert.equal((await second.snapshot()).tasks[0].status, 'unknown')
  assert.equal((await second.analysisConfig()).llm.model, 'x')
  await second.close()
})

test('two independent clients share one real daemon; last source exit cleans runtime and stops its own process', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-process-'))
  let runtime
  t.after(async () => {
    if (runtime) {
      try {
        const current = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
        if (current.instanceId === runtime.instanceId) {
          const health = await fetch(`${runtime.url}/v1/health`, { headers: { authorization: `Bearer ${runtime.token}` }, signal: AbortSignal.timeout(500) })
          if ((await health.json()).instanceId === runtime.instanceId) process.kill(runtime.pid, 'SIGTERM')
        }
      } catch {}
    }
    await rm(dataDir, { recursive: true, force: true })
  })
  const [one, two] = await Promise.all([connectCore({ dataDir }), connectCore({ dataDir })])
  runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
  const first = await one.registerSource({ registrationId: 'p', hostRunId: 'one', name: 'one' })
  const second = await two.registerSource({ registrationId: 'p', hostRunId: 'two', name: 'two' })
  assert.notEqual(first, second)
  assert.equal((await one.snapshot()).overview.connectedSources, 2)
  await one.unregisterSource(first, 'test done')
  assert.equal((await two.snapshot()).overview.connectedSources, 1)
  await two.unregisterSource(second, 'test done')
  await one.close(); await two.close()
  const deadline = Date.now() + 14_000
  let missing = false
  while (Date.now() < deadline) {
    try { await stat(join(dataDir, 'runtime.json')) } catch (error) { if (error.code === 'ENOENT') { missing = true; break } throw error }
    await delay(100)
  }
  assert.equal(missing, true, 'own daemon must exit after last source grace')
  let alive = true
  while (Date.now() < deadline) {
    try { process.kill(runtime.pid, 0) } catch (error) { if (error.code === 'ESRCH') { alive = false; break } throw error }
    await delay(50)
  }
  assert.equal(alive, false, 'runtime cleanup alone is not process exit')
})

test('production HTTP path extracts cited events, judges drift and creates a source-specific notification', async t => {
  const models = createServer(async (request, response) => {
    let body = ''; for await (const part of request) body += part
    const input = JSON.parse(body)
    response.setHeader('content-type', 'application/json')
    if (request.url === '/v1/chat/completions') {
      const evidence = JSON.parse(input.messages.find(message => message.role === 'user').content).evidence
      const selectedOptionIds = [evidence.find(entry => entry.origin === 'output').options.find(option => option.kind === 'action').optionId]
      response.end(JSON.stringify({ model: input.model, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify({ selectedOptionIds, gaps: [] }) } }] }))
    } else {
      response.end(JSON.stringify({ model: input.model, answers: Object.fromEntries(Object.keys(input.questions).map(id => [id, { type: 'choice', choice: id === 'request_drift' ? 'attention' : 'unknown' }])) }))
    }
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-pipeline-'))
  let notificationOffset = 0
  const server = await startServer({ dataDir, now: () => Date.now() + notificationOffset })
  t.after(async () => { await server.close(); models.closeAllConnections(); await new Promise(resolve => models.close(resolve)); await rm(dataDir, { recursive: true, force: true }) })
  const client = await connectCore({ dataDir })
  const baseUrl = `http://127.0.0.1:${models.address().port}`
  await client.configureAnalysis({ llm: { baseUrl, model: 'extract' }, jev: { baseUrl, model: 'judge' } })
  const source = await client.registerSource({ registrationId: 'p', hostRunId: 'one', name: 'test' })
  const other = await client.registerSource({ registrationId: 'p', hostRunId: 'two', name: 'other' })
  await client.ingest(source, { tasks: [{ id: 'task', lifecycleId: 'life', status: 'running', title: 'Add a log' }], events: [
    { id: 'r', kind: 'input', text: 'Add a log' }, { id: 'a', kind: 'output', text: 'Rebuild all storage' },
  ].map(event => ({ ...event, taskId: 'task', taskLifecycleId: 'life', occurredAt: null, capturedAt: Date.now() })) })
  const deadline = Date.now() + 3000
  let snapshot
  do { snapshot = await client.snapshot(); if (snapshot.tasks[0]?.analysis?.checks?.some(check => check.id === 'request_drift' && check.status === 'attention')) break; await delay(10) } while (Date.now() < deadline)
  assert.equal(snapshot.tasks[0].analysis.checks.find(check => check.id === 'request_drift').status, 'attention', JSON.stringify(snapshot.tasks[0].analysis))
  assert.equal((await client.claimNotifications(other)).length, 0)
  assert.deepEqual(await client.claimNotifications(source), [])
  notificationOffset += 3_000
  const notifications = await client.claimNotifications(source)
  assert.equal(notifications.length, 1)
  await client.ackNotification(notifications[0].id, { status: 'submitted' })
  assert.equal((await client.claimNotifications(source)).length, 0)
  await client.close()
})

test('explicit credentials are omitted at the public ingress before reaching durable snapshots', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-private-capture-'))
  const server = await startServer({ dataDir })
  t.after(async () => { await server.close(); await rm(dataDir, { recursive: true, force: true }) })
  const client = await connectCore({ dataDir })
  await client.pause()
  const source = await client.registerSource({ registrationId: 'p', hostRunId: 'private', name: 'test' })
  await client.ingest(source, { tasks: [{ id: 'a', lifecycleId: 'l', title: 'Check HTTP', status: 'running' }], events: [
    { id: 'r', taskId: 'a', taskLifecycleId: 'l', kind: 'input', occurredAt: null, capturedAt: Date.now(), text: 'Check HTTP' },
    { id: 's', taskId: 'a', taskLifecycleId: 'l', kind: 'tool-start', occurredAt: null, capturedAt: Date.now(), text: 'curl result', data: { command: 'curl -H "Authorization: Bearer TEST_SENTINEL_NOT_A_SECRET" http://localhost' } },
  ] })
  const snapshot = await client.snapshot()
  assert.equal(JSON.stringify(snapshot).includes('TEST_SENTINEL_NOT_A_SECRET'), false)
  assert.equal(snapshot.tasks[0].events.find(event => event.id === 's').data.redacted, true)
  assert.equal(snapshot.tasks[0].coverage.complete, false)
  await client.close()
})

test('configuration revisions reject stale and same-value writes and permit only one concurrent CAS', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-config-cas-'))
  let server, client, peer
  t.after(async () => { const failures = []; for (const cleanup of [() => peer?.close(), () => client?.close(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await cleanup() } catch (error) { failures.push(error) } }; if (failures.length) throw new AggregateError(failures) })
  server = await startServer({ dataDir }); client = await connectCore({ dataDir }); peer = await connectCore({ dataDir })
  const http = (path, body) => fetch(`${server.url}${path}`, { method: body === undefined ? 'GET' : 'POST', headers: { authorization: `Bearer ${server.token}`, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
  const stateResponse = await http('/v1/analysis/config/state')
  assert.equal(stateResponse.status, 200)
  const initial = await stateResponse.json()
  assert.deepEqual(initial.config, {}); assert.equal(typeof initial.revision, 'string')
  assert.ok(initial.revision.length > 0); assert.ok(!JSON.stringify(initial).includes(server.token))
  assert.equal((await fetch(`${server.url}/v1/analysis/config/state`)).status, 401)
  const edited = { llm: { baseUrl: 'http://127.0.0.1:1', model: 'user-edited' } }
  assert.deepEqual(await client.configureAnalysis(edited), edited)
  const stale = await http('/v1/analysis/config/compare', { config: {}, revision: initial.revision })
  assert.equal(stale.status, 409); assert.equal((await stale.json()).code, 'analysis_config_conflict')
  const current = await client.analysisConfigState()
  assert.deepEqual(current.config, edited); assert.notEqual(current.revision, initial.revision)
  await client.configureAnalysis(edited)
  await assert.rejects(client.configureAnalysisIfCurrent({}, current.revision), error => error.code === 'analysis_config_conflict')
  const sameValue = await client.analysisConfigState(); assert.notEqual(sameValue.revision, current.revision)
  const one = { llm: { baseUrl: 'http://127.0.0.1:1', model: 'one' } }, two = { llm: { baseUrl: 'http://127.0.0.1:1', model: 'two' } }
  const writes = await Promise.allSettled([client.configureAnalysisIfCurrent(one, sameValue.revision), peer.configureAnalysisIfCurrent(two, sameValue.revision)])
  const successes = writes.filter(result => result.status === 'fulfilled'), failures = writes.filter(result => result.status === 'rejected')
  assert.equal(successes.length, 1); assert.equal(failures.length, 1); assert.equal(failures[0].reason.code, 'analysis_config_conflict')
  const final = await client.analysisConfigState()
  assert.deepEqual(final, successes[0].value); assert.deepEqual(await client.analysisConfig(), final.config)
  assert.deepEqual(JSON.parse(await readFile(join(dataDir, 'analysis-config.json'), 'utf8')), final.config)
  const duplicate = await client.configureAnalysisIfCurrent(final.config, final.revision)
  assert.notEqual(duplicate.revision, final.revision)
  await assert.rejects(peer.configureAnalysisIfCurrent({}, final.revision), error => error.code === 'analysis_config_conflict')
  const userEdit = { llm: { model: 'concurrent-user-edit' } }
  const racing = await Promise.allSettled([client.configureAnalysis(userEdit), peer.configureAnalysisIfCurrent({}, duplicate.revision)])
  assert.equal(racing[0].status, 'fulfilled')
  if (racing[1].status === 'rejected') assert.equal(racing[1].reason.code, 'analysis_config_conflict')
  const last = await client.analysisConfigState(); assert.deepEqual(last.config, userEdit)
  await peer.close(); peer = undefined
  await client.close(); await server.close(); server = await startServer({ dataDir }); client = await connectCore({ dataDir })
  const restarted = await client.analysisConfigState(); assert.deepEqual(restarted.config, last.config); assert.notEqual(restarted.revision, last.revision)
  await assert.rejects(client.configureAnalysisIfCurrent({}, final.revision), error => error.code === 'analysis_config_conflict')
})


test('a failed configuration file write leaves its revision usable and does not poison the shared writer queue', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-config-failure-'))
  let server, client
  t.after(async () => { const failures = []; for (const cleanup of [() => client?.close(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await cleanup() } catch (error) { failures.push(error) } }; if (failures.length) throw new AggregateError(failures) })
  server = await startServer({ dataDir }); client = await connectCore({ dataDir })
  const initial = await client.analysisConfigState(), path = join(dataDir, 'analysis-config.json')
  await mkdir(path)
  await assert.rejects(client.configureAnalysisIfCurrent({ llm: { model: 'failed-write' } }, initial.revision), /EISDIR|directory/)
  assert.deepEqual(await client.analysisConfigState(), initial)
  await rm(path, { recursive: true })
  const recovered = await client.configureAnalysisIfCurrent({ llm: { model: 'recovered' } }, initial.revision)
  assert.notEqual(recovered.revision, initial.revision); assert.equal(recovered.config.llm.model, 'recovered')
  await client.configureAnalysis({})
  assert.deepEqual((await client.analysisConfigState()).config, {})
})

test('Native Host and ClientRemote expose bounded configuration CAS and preserve its conflict code', async t => {
  const { req, clientRequire, resolveGatewayBrowserClientPath } = await import('./dsh-native-runtime.mjs')
  const { Context } = req('@deepseek-ai/cordis'), { createAdapter } = await import('../dsh/host.mjs'), { runInNewContext } = await import('node:vm')
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-config-'))
  let server, host, adapter, front, control, blocker
  t.after(async () => { const errors = []; for (const clean of [() => blocker?.close(), () => control?.close(), () => front?.fiber.dispose(), () => adapter?.close(), () => host?.fiber.dispose(), () => server?.close(), () => rm(dataDir, { recursive: true, force: true })]) { try { await clean() } catch (error) { errors.push(error) } }; if (errors.length) throw new AggregateError(errors) })
  server = await startServer({ dataDir }); host = new Context()
  await host.plugin(req('@deepseek-ai/dsh-llm').default)
  for (const name of ['dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop']) await host.plugin(req(`@deepseek-ai/${name}`).default, { agents: [] })
  await host.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  await host.plugin(req('@deepseek-ai/dsh-api-remote-access').default)
  await host.plugin(req('@deepseek-ai/dsh-api-gateway').default)
  const { Peer } = req('@deepseek-ai/dsh-client-connection')
  const peer = new Peer(host, { principal: { kind: 'owner' }, interactions: 'none' })
  adapter = createAdapter(host, { dataDir, registrationId: 'native-config' }, { flushMs: 50, heartbeatMs: 5000 })
  const end = Date.now() + 5000
  while (!adapter.connected) { assert.ok(Date.now() < end, 'actual Native adapter connected'); await delay(10) }
  const invoke = (method, args = {}) => host.typertGateway.invoke({ namespace: 'sentinel', method, args, peer })
  const initial = await invoke('analysisConfigState')
  const configured = await invoke('configureAnalysisIfCurrent', { config: { llm: { model: 'native' } }, revision: initial.revision })
  assert.equal(configured.config.llm.model, 'native')
  const conflict = await host.typertGateway.dispatchRpc('sentinel/configureAnalysisIfCurrent', { args: { config: {}, revision: initial.revision } }, undefined, peer)
  assert.equal(conflict.ok, false); assert.equal(conflict.error.code, 'analysis_config_conflict')
  for (const revision of ['', 'x'.repeat(2001), {}]) await assert.rejects(invoke('configureAnalysisIfCurrent', { config: {}, revision }), /boundary validation|修订|revision/)
  assert.deepEqual(await invoke('analysisConfigState'), configured)
  front = new Context(); front.provide('slots', { inject() { return () => {} } })
  await front.plugin(req('@deepseek-ai/dsh-typert-registry').default)
  front.provide('connection', { registerGenerationSource: () => () => {}, start: () => ({ stop() {} }), rpc: { open: async function *() {}, call: (_, endpoint, payload) => host.typertGateway.dispatchRpc(endpoint, JSON.parse(JSON.stringify(payload)), undefined, peer) } })
  let remoteFactory, factory
  runInNewContext(await readFile(await resolveGatewayBrowserClientPath(), 'utf8'), { window: { crypto: globalThis.crypto, __ModuleLoader__: { load(value) { remoteFactory = value.factory } } }, crypto: globalThis.crypto, setTimeout, clearTimeout, AbortController, AbortSignal, console })
  const remote = remoteFactory(req); await front.plugin({ inject: remote.inject, apply: remote.apply })
  const style = { remove() {} }
  runInNewContext(await readFile(new URL('../dsh/client.js', import.meta.url), 'utf8'), { window: { __ModuleLoader__: { load(value) { factory = value.factory } } }, document: { createElement: () => style, head: { appendChild() {} } }, setTimeout, clearTimeout, setInterval, clearInterval, AbortController, AbortSignal, console })
  const client = factory(name => name === 'react' ? clientRequire(name) : req(name)); await front.plugin({ inject: client.inject, apply: client.apply })
  while (!front.get('remote.sentinel')) { assert.ok(Date.now() < end, 'actual ClientRemote mount'); await delay(10) }
  const api = front.get('remote.sentinel'), before = await api.analysisConfigState()
  assert.equal(before.ok, true); assert.deepEqual(before.value, configured)
  const changed = await api.configureAnalysisIfCurrent({}, configured.revision)
  assert.equal(changed.ok, true); assert.deepEqual(changed.value.config, {})
  const clientConflict = await api.configureAnalysisIfCurrent({ llm: { model: 'must-not-override' } }, configured.revision)
  assert.equal(clientConflict.ok, false); assert.equal(clientConflict.error.code, 'analysis_config_conflict')
  assert.deepEqual((await api.analysisConfigState()).value, changed.value)
  control = await connectCore({ dataDir }); await control.pause()
  const idle = await host.agents.create({ sessionId: 'config-fault-idle', agentOptions: { provider: 'unused', model: 'unused' } }); t.after(() => idle.dispose())
  while (!(await adapter.snapshot()).tasks.some(task => task.id === 'config-fault-idle')) { assert.ok(Date.now() < end, 'Native idle task captured'); await delay(5) }
  const { DatabaseSync } = await import('node:sqlite')
  blocker = new DatabaseSync(join(dataDir, 'observations.sqlite')); blocker.exec('BEGIN IMMEDIATE')
  let applicationFailure, lockedSource
  try {
    await assert.rejects(control.configureAnalysisIfCurrent({ llm: { model: 'must-stay-unconfirmed' } }, changed.value.revision), error => { applicationFailure = error; return error.code === 'analysis_config_unconfirmed' })
    assert.ok(applicationFailure.causes.length >= 2 && applicationFailure.causes.every(cause => cause.includes('database is locked')))
    for (const request of [() => control.analysisConfigState(), () => control.analysisConfig(), () => control.configureAnalysisIfCurrent({}, changed.value.revision)]) await assert.rejects(request(), error => error.code === 'analysis_config_unconfirmed')
    lockedSource = (await adapter.snapshot()).adapter
    if (lockedSource.sourceKey === null) {
      const disconnected = await api.analysisConfigState()
      assert.equal(lockedSource.connected, false); assert.match(lockedSource.error, /database is locked/)
      assert.equal(disconnected.ok, false); assert.equal(disconnected.error.code, 'gateway/internal')
      assert.match(disconnected.error.message, /database is locked|共享核心.*连接/)
    }
  } finally { blocker.exec('ROLLBACK'); blocker.close(); blocker = undefined }
  while (!adapter.connected) { assert.ok(Date.now() < end, 'Native adapter reconnects within the original configuration fixture deadline'); await delay(10) }
  const nativeUnconfirmed = []
  for (const [method, request] of [['analysisConfigState', () => api.analysisConfigState()], ['analysisConfig', () => api.analysisConfig()], ['configureAnalysisIfCurrent', () => api.configureAnalysisIfCurrent({}, changed.value.revision)]]) {
    const reply = await request(); assert.equal(reply.ok, false); assert.equal(reply.error.code, 'analysis_config_unconfirmed', `${method}: ${reply.error.message}`)
    nativeUnconfirmed.push({ method, code: reply.error.code, message: reply.error.message })
  }
  t.diagnostic(`cas-public: ${JSON.stringify({ applicationFailure: { code: applicationFailure.code, message: applicationFailure.message, causes: applicationFailure.causes }, duringLock: { connected: lockedSource.connected, sourceKey: lockedSource.sourceKey, error: lockedSource.error }, reconnected: adapter.connected, nativeUnconfirmed })}`)
  assert.equal((await api.configureAnalysis({})).ok, true)
  assert.equal((await api.analysisConfigState()).ok, true)
  const { createRequire, syncBuiltinESMExports } = await import('node:module'), filesystem = createRequire(import.meta.url)('node:fs/promises')
  const originalChmod = filesystem.chmod, originalRm = filesystem.rm, configPath = join(dataDir, 'analysis-config.json')
  const stable = (await api.analysisConfigState()).value
  filesystem.chmod = async (path, ...args) => { if (String(path).startsWith(`${configPath}.`)) throw new Error('native precommit chmod failure'); return originalChmod(path, ...args) }
  filesystem.rm = async (path, ...args) => { if (String(path).startsWith(`${configPath}.`)) throw new Error('native temp cleanup failure'); return originalRm(path, ...args) }
  syncBuiltinESMExports()
  let fileFailure
  try { fileFailure = await api.configureAnalysisIfCurrent({ llm: { model: 'must-not-replace' } }, stable.revision) }
  finally { filesystem.chmod = originalChmod; filesystem.rm = originalRm; syncBuiltinESMExports() }
  assert.equal(fileFailure.ok, false); assert.equal(fileFailure.error.code, 'gateway/internal')
  assert.match(fileFailure.error.message, /native precommit chmod failure/)
  assert.deepEqual(fileFailure.error.details.causes, ['native precommit chmod failure', 'native temp cleanup failure'])
  assert.deepEqual((await api.analysisConfigState()).value, stable)
})

test('configuration apply failure restores the file, reports unconfirmed state and requires an explicit successful write before inference', async t => {
  const { DatabaseSync } = await import('node:sqlite')
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-config-apply-failure-'))
  let server, client, blocker, models
  const requests = []
  t.after(async () => { const errors = []; for (const cleanup of [() => blocker?.close(), () => client?.close(), () => server?.close(), async () => { models?.closeAllConnections(); if (models?.listening) await new Promise(resolve => models.close(resolve)) }, () => rm(dataDir, { recursive: true, force: true })]) { try { await cleanup() } catch (error) { errors.push(error) } }; if (errors.length) throw new AggregateError(errors) })
  models = createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(chunk)
    const body = JSON.parse(Buffer.concat(chunks)); requests.push({ path: request.url, model: body.model })
    response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify(request.url === '/v1/chat/completions' ? { choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] } : { answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown', probabilities: { attention: 0, clear: 0, unknown: 1 } }])) }))
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  server = await startServer({ dataDir }); client = await connectCore({ dataDir }); await client.pause()
  const baseUrl = `http://127.0.0.1:${models.address().port}`, prior = { llm: { baseUrl, model: 'before' }, jev: { baseUrl, model: 'judge' } }
  await client.configureAnalysis(prior)
  const sourceKey = await client.registerSource({ registrationId: 'apply-failure', hostRunId: 'own', name: 'own' })
  await client.ingest(sourceKey, { tasks: [{ id: 'own-task', lifecycleId: 'own-life', title: 'own', status: 'idle' }] })
  const initial = await client.analysisConfigState()
  // External storage contention only; no private database queries.
  blocker = new DatabaseSync(join(dataDir, 'observations.sqlite')); blocker.exec('BEGIN IMMEDIATE')
  let failure, disk
  try {
    try { await client.configureAnalysisIfCurrent({ ...prior, llm: { baseUrl, model: 'after' } }, initial.revision) } catch (error) { failure = error }
    assert.match(failure.message, /database is locked/); assert.equal(failure.code, 'analysis_config_unconfirmed')
    assert.ok(failure.causes.length >= 2 && failure.causes.every(cause => cause.includes('database is locked')))
    for (const read of [() => client.analysisConfigState(), () => client.analysisConfig(), () => client.configureAnalysisIfCurrent(prior, initial.revision)]) await assert.rejects(read(), error => error.code === 'analysis_config_unconfirmed')
    disk = JSON.parse(await readFile(join(dataDir, 'analysis-config.json'), 'utf8')); assert.deepEqual(disk, initial.config)
  } finally { blocker.exec('ROLLBACK'); blocker.close(); blocker = undefined }
  await client.configureAnalysis(prior)
  const published = await client.analysisConfigState(); assert.deepEqual(published.config, initial.config); assert.notEqual(published.revision, initial.revision)
  await client.ingest(sourceKey, { tasks: [{ id: 'own-task', lifecycleId: 'own-life', title: 'own', status: 'running' }], events: [
    { id: 'request', taskId: 'own-task', taskLifecycleId: 'own-life', kind: 'input', text: 'Read project metadata', occurredAt: null, capturedAt: Date.now() },
    { id: 'output', taskId: 'own-task', taskLifecycleId: 'own-life', kind: 'output', text: 'Reading project metadata now', occurredAt: null, capturedAt: Date.now() },
  ] })
  await client.resume()
  const end = Date.now() + 2000; while (!requests.some(request => request.path === '/v1/chat/completions')) { assert.ok(Date.now() < end, 'actual production inference requested'); await delay(5) }
  await mkdir(new URL('../.scratch/implementation/phase20/config-cas/', import.meta.url), { recursive: true })
  await import('node:fs/promises').then(({ writeFile }) => writeFile(new URL('../.scratch/implementation/phase20/config-cas/apply-failure-recovered.json', import.meta.url), JSON.stringify({ failure: { message: failure.message, code: failure.code, causes: failure.causes }, initial, published, disk, requests }, null, 2)))
  assert.deepEqual(published.config, initial.config)
  assert.deepEqual(disk, initial.config)
  assert.equal(requests.find(request => request.path === '/v1/chat/completions').model, 'before')
})

for (const cleanupAlsoFails of [false, true]) test(`precommit chmod failure ${cleanupAlsoFails ? 'and cleanup failure retain both errors' : 'retains old disk configuration and inference'}`, async t => {
  const { createRequire, syncBuiltinESMExports } = await import('node:module')
  const filesystem = createRequire(import.meta.url)('node:fs/promises'), originalChmod = filesystem.chmod, originalRm = filesystem.rm
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-config-file-io-')), configPath = join(dataDir, 'analysis-config.json')
  let server, client, models
  const requests = []
  const restore = () => { filesystem.chmod = originalChmod; filesystem.rm = originalRm; syncBuiltinESMExports() }
  t.after(async () => { restore(); const errors = []; for (const cleanup of [() => client?.close(), () => server?.close(), async () => { models?.closeAllConnections(); if (models?.listening) await new Promise(resolve => models.close(resolve)) }, () => rm(dataDir, { recursive: true, force: true })]) { try { await cleanup() } catch (error) { errors.push(error) } }; if (errors.length) throw new AggregateError(errors) })
  models = createServer(async (request, response) => {
    const chunks = []; for await (const chunk of request) chunks.push(chunk)
    const body = JSON.parse(Buffer.concat(chunks)); requests.push({ path: request.url, model: body.model })
    response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify(request.url === '/v1/chat/completions' ? { choices: [{ finish_reason: 'stop', message: { content: '{"selectedOptionIds":[],"gaps":[]}' } }] } : { answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown', probabilities: { attention: 0, clear: 0, unknown: 1 } }])) }))
  })
  await new Promise(resolve => models.listen(0, '127.0.0.1', resolve))
  server = await startServer({ dataDir }); client = await connectCore({ dataDir }); await client.pause()
  const baseUrl = `http://127.0.0.1:${models.address().port}`, prior = { llm: { baseUrl, model: 'before-chmod-failure' }, jev: { baseUrl, model: 'judge' } }
  await client.configureAnalysis(prior); const initial = await client.analysisConfigState()
  let failure
  filesystem.chmod = async (path, ...args) => { if (String(path) === configPath || String(path).startsWith(`${configPath}.`)) throw new Error('injected config chmod failure'); return originalChmod(path, ...args) }
  filesystem.rm = async (path, ...args) => { if (cleanupAlsoFails && String(path).startsWith(`${configPath}.`)) throw new Error('injected temporary cleanup failure'); return originalRm(path, ...args) }
  syncBuiltinESMExports()
  try { await client.configureAnalysisIfCurrent({ ...prior, llm: { baseUrl, model: 'must-not-be-persisted' } }, initial.revision) } catch (error) { failure = error } finally { restore() }
  assert.ok(failure)
  if (cleanupAlsoFails) { assert.match(failure.message, /chmod/); assert.deepEqual(failure.causes, ['injected config chmod failure', 'injected temporary cleanup failure']) }
  else assert.match(failure.message, /chmod/)
  assert.deepEqual(await client.analysisConfigState(), initial)
  assert.deepEqual(JSON.parse(await readFile(configPath, 'utf8')), prior)
  const sourceKey = await client.registerSource({ registrationId: 'file-io', hostRunId: 'own', name: 'own' })
  await client.ingest(sourceKey, { tasks: [{ id: 'io-task', lifecycleId: 'io-life', title: 'own', status: 'running' }], events: [
    { id: 'request', taskId: 'io-task', taskLifecycleId: 'io-life', kind: 'input', text: 'Read metadata', occurredAt: null, capturedAt: Date.now() },
    { id: 'action', taskId: 'io-task', taskLifecycleId: 'io-life', kind: 'output', text: 'Reading metadata', occurredAt: null, capturedAt: Date.now() },
  ] })
  await client.resume()
  const end = Date.now() + 2000; while (!requests.some(request => request.path === '/v1/chat/completions')) { assert.ok(Date.now() < end, 'unchanged production model used'); await delay(5) }
  assert.equal(requests.find(request => request.path === '/v1/chat/completions').model, 'before-chmod-failure')
})
