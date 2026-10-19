import { withModelSummary } from './helpers/model-summary.mjs'
import { req, workflowRoot } from './dsh-native-runtime.mjs'
import test from 'node:test'
import assert from 'node:assert/strict'
import { fork, execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { createServer } from 'node:http'
import { mkdtemp, readFile, stat, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import { connectCore } from '../src/core-client.mjs'
import { createCore } from '../src/core/index.mjs'
import { configureInstallationModels } from './helpers/installation-model-settings.mjs'

const fixturePath = fileURLToPath(new URL('./dsh-process-runtime.mjs', import.meta.url))
const alive = pid => { try { process.kill(pid, 0); return true } catch (error) { if (error.code === 'ESRCH') return false; throw error } }
const gone = async path => { try { await stat(path); return false } catch (error) { if (error.code === 'ENOENT') return true; throw error } }
async function until(predicate, deadline, description) {
  while (!(await predicate())) {
    assert.ok(Date.now() < deadline, `${description} exceeded its real wall-clock deadline`)
    await delay(100)
  }
}

async function nativeEvidence() {
  const artifacts = await Promise.all(['cordis', 'dsh-llm', 'dsh-session', 'dsh-session-projection', 'dsh-system-prompt', 'dsh-tools', 'dsh-agent', 'dsh-agent-loop'].map(async name => {
    const path = req.resolve(`@deepseek-ai/${name}`)
    return { name, path, sha256: createHash('sha256').update(await readFile(path)).digest('hex') }
  }))
  const head = cwd => execFileSync('git', ['rev-parse', 'HEAD'], { cwd, encoding: 'utf8' }).trim()
  return { node: process.version, workflowHead: head(workflowRoot), harnessHead: head(join(workflowRoot, 'deepseek-harness')), artifacts }
}

function spawnHost(dataDir, label, { readyMs = 20_000, requestMs = 10_000, fault } = {}) {
  const child = fork(fixturePath, [dataDir, label, ...(fault ? [fault] : [])], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    execArgv: process.execArgv.filter(value => !value.startsWith('--test')) })
  let serial = 0, initial, stderr = '', finished, bootstrapping = false, readyTimer, readyReject, resolveExit, exitTimers = 0
  const requests = new Map()
  child.stderr.on('data', value => { stderr = (stderr + value).slice(-8192) })
  child.stdout.resume()
  const clearReady = () => { clearTimeout(readyTimer); readyTimer = undefined }
  const settleRequest = (id, error, value) => {
    const request = requests.get(id)
    if (!request) return
    requests.delete(id); clearTimeout(request.timer)
    if (error) request.reject(error); else request.resolve(value)
  }
  const exit = new Promise(resolve => { resolveExit = resolve })
  const finish = (code, signal) => {
    if (finished) return
    finished = { code, signal }; clearReady()
    if (!initial) readyReject?.(new Error(`${label} exited before ready; ${stderr}`))
    for (const id of requests.keys()) settleRequest(id, new Error(`${label} fixture exited before RPC completed`))
    resolveExit(finished)
  }
  child.once('exit', finish)
  child.once('close', finish) // A failed spawn can close without emitting exit.
  const ready = new Promise((resolve, reject) => {
    readyReject = reject
    readyTimer = setTimeout(() => { clearReady(); reject(new Error(`${label} IPC ready deadline; ${stderr}`)) }, readyMs)
    child.once('error', error => { clearReady(); reject(error); for (const id of requests.keys()) settleRequest(id, error) })
    child.on('message', message => {
      if (message.type === 'ready') { initial = message.value; clearReady(); resolve(initial); return }
      if (message.type === 'bootstrapping') { bootstrapping = true; return }
      if (message.type === 'fatal') { clearReady(); reject(new Error(`${label}: ${message.error}`)); return }
      settleRequest(message.id, message.error ? new Error(message.error) : null, message.value)
    })
  })
  const waitExit = async (timeoutMs = 10_000) => {
    if (finished) return finished
    let timer
    exitTimers++
    try { return await Promise.race([exit, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} exit deadline`)), timeoutMs) })]) }
    finally { clearTimeout(timer); exitTimers-- }
  }
  return { child, ready, exit, get initial() { return initial }, get finished() { return finished }, get bootstrapping() { return bootstrapping },
    waitExit, diagnostics: () => ({ readyTimers: readyTimer ? 1 : 0, pendingRequests: requests.size, requestTimers: requests.size, exitTimers }),
    call(method, extra = {}) {
      if (finished || !child.connected) return Promise.reject(new Error(`${label} fixture disconnected`))
      return new Promise((resolve, reject) => {
        const id = ++serial, timer = setTimeout(() => settleRequest(id, new Error(`${label} ${method} deadline`)), requestMs)
        requests.set(id, { resolve, reject, timer })
        try { child.send({ id, method, ...extra }, error => { if (error) settleRequest(id, error) }) }
        catch (error) { settleRequest(id, error) }
      })
    },
    async killAbruptly() {
      assert.ok(initial, 'only an IPC-ready child owned by this fixture may be killed')
      assert.equal(child.kill('SIGKILL'), true)
      const result = await waitExit()
      assert.deepEqual(result, { code: null, signal: 'SIGKILL' })
    },
  }
}

async function cleanupHost(host, timeoutMs = 10_000) {
  if (host.finished) return { status: 'exited', exit: host.finished }
  try {
    if (host.initial) await host.call('close'); else if (host.child.connected) host.child.disconnect()
    await host.waitExit(timeoutMs)
    return { status: 'closed', exit: host.finished }
  } catch (error) {
    // Failure recovery uses the exact fork-owned handle, even before readiness.
    // This is distinct from killAbruptly(), the ready-only fault injection path.
    if (!host.finished) {
      host.child.kill('SIGKILL')
      try { await host.waitExit(Math.max(1000, timeoutMs)) }
      catch { return { status: 'unresolved', error: error.message, exit: host.finished } }
    }
    return { status: host.finished ? 'failure-cleanup' : 'unresolved', error: error.message, exit: host.finished }
  }
}

async function cleanupCore(dataDir, runtime, { sendSignal = (pid, signal) => process.kill(pid, signal), isAlive = alive } = {}) {
  let signalled = false
  let saved
  try { saved = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8')) }
  catch (error) {
    if (error.code !== 'ENOENT' || isAlive(runtime.pid)) return { status: 'unresolved', reason: 'runtime identity unavailable', signalled }
  }
  if (saved && (saved.instanceId !== runtime.instanceId || saved.pid !== runtime.pid || saved.url !== runtime.url || saved.token !== runtime.token)) {
    return { status: 'unresolved', reason: 'runtime identity changed', signalled }
  }
  if (saved && (saved.protocol !== 1 || runtime.protocol !== 1)) return { status: 'unresolved', reason: 'runtime protocol unavailable', signalled }
  if (isAlive(runtime.pid)) {
    try {
      const response = await fetch(`${runtime.url}/v1/health`, { headers: { authorization: `Bearer ${runtime.token}` }, redirect: 'error', signal: AbortSignal.timeout(500) })
      if (!response.ok) return { status: 'unresolved', reason: 'health identity unavailable', signalled }
      const health = await response.json()
      if (health.protocol !== 1 || health.instanceId !== runtime.instanceId) return { status: 'unresolved', reason: 'health identity changed', signalled }
      sendSignal(runtime.pid, 'SIGTERM'); signalled = true
      await until(() => !isAlive(runtime.pid), Date.now() + 10_000, 'isolated core failure cleanup')
    } catch { return { status: 'unresolved', reason: 'authenticated shutdown could not be confirmed', signalled } }
  }
  await rm(dataDir, { recursive: true, force: true })
  return { status: 'resolved', signalled }
}

async function ownedRuntime(t) {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-process-'))
  const client = await connectCore({ dataDir }), hosts = []
  // The private token remains in-process; IPC and diagnostics never receive it.
  const runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
  const { pid, instanceId, url } = runtime
  t.after(async () => {
    let unresolved = false
    for (const host of hosts) {
      const result = await cleanupHost(host)
      if (result.status === 'failure-cleanup') t.diagnostic('failure cleanup forcibly ended a fork-owned fixture; this is not normal shutdown evidence')
      if (result.status === 'unresolved') { unresolved = true; t.diagnostic(`unresolved fixture cleanup: ${result.error}`) }
    }
    await client.close()
    if (unresolved) { t.diagnostic(`unresolved cleanup: preserved isolated evidence directory ${dataDir}`); return }
    const result = await cleanupCore(dataDir, runtime)
    if (result.signalled) t.diagnostic('failure cleanup signalled an authenticated owned core; this is not normal shutdown evidence')
    if (result.status === 'unresolved') t.diagnostic(`unresolved cleanup (${result.reason}): preserved isolated evidence directory ${dataDir}`)
  })
  return { dataDir, client, pid, instanceId, url,
    async host(label) { const host = spawnHost(dataDir, label); hosts.push(host); await host.ready; return host },
    async assertStopped(deadline) {
      await until(async () => await gone(join(dataDir, 'runtime.json')) && !alive(pid), deadline, 'default core shutdown')
      assert.equal(await gone(join(dataDir, 'core.lock')), true)
      await assert.rejects(fetch(`${url}/v1/health`, { signal: AbortSignal.timeout(1000) }))
    },
  }
}

async function inferenceFixture() {
  let heldLabel = 'process-a', started = 0, cancelled = 0, completed = 0
  const held = new Set(), sockets = new Set()
  const server = createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    const body = JSON.parse(Buffer.concat(chunks).toString('utf8'))
    if (request.url === '/v1/chat/completions') {
      const observed = JSON.parse(body.messages.find(message => message.role === 'user').content)
      if (observed.evidence.some(entry => entry.text.includes(`${heldLabel} synthetic`))) {
        started++; held.add(response)
        response.once('close', () => { if (held.delete(response)) cancelled++ })
        return
      }
      completed++
      response.setHeader('content-type', 'application/json')
      response.end(JSON.stringify({ model: 'fixture-llm', choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(withModelSummary(body, { selectedOptionIds: [], gaps: [] })) } }] }))
    } else if (request.url === '/v1/systemone') {
      response.setHeader('content-type', 'application/json')
      response.end(JSON.stringify({ model: 'fixture-jev', answers: Object.fromEntries(Object.keys(body.questions).map(id => [id, { type: 'choice', choice: 'unknown' }])) }))
    } else { response.writeHead(404); response.end() }
  })
  server.keepAliveTimeout = 1000
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)) })
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve) })
  const baseUrl = `http://127.0.0.1:${server.address().port}`
  return { config: { llm: { baseUrl, model: 'fixture-llm' }, jev: { baseUrl, model: 'fixture-jev' } },
    hold(label) { heldLabel = label }, diagnostics: () => ({ held: held.size, started, cancelled, completed }),
    async close() { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)) },
  }
}

test('failed bootstrap cleanup ends its own unready child rather than leaking it after disconnect', async t => {
  const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-process-'))
  const host = spawnHost(dataDir, 'process-a', { readyMs: 200, requestMs: 5000, fault: 'stall-pre-ready' })
  t.after(async () => { if (!host.finished) { host.child.kill('SIGKILL'); await host.waitExit(1000) }; await rm(dataDir, { recursive: true, force: true }) })
  const readiness = assert.rejects(host.ready, /IPC ready deadline/)
  await until(() => host.bootstrapping, Date.now() + 5000, 'intentional pre-ready stall')
  const request = assert.rejects(host.call('state'), /exited before RPC/)
  await readiness
  const result = await cleanupHost(host, 30)
  assert.equal(result.status, 'failure-cleanup')
  assert.deepEqual(result.exit, { code: null, signal: 'SIGKILL' })
  assert.equal(alive(host.child.pid), false)
  await request
  assert.deepEqual(host.diagnostics(), { readyTimers: 0, pendingRequests: 0, requestTimers: 0, exitTimers: 0 })
  t.diagnostic('Intentional failed-bootstrap regression used forced cleanup of its own child; this is not normal resource acceptance evidence.')
})

test('core failure cleanup signals only an authenticated matching instance and preserves unconfirmed identity evidence', async t => {
  const token = 'fixture-private-token'
  let responseId = 'expected', healthy = true, unavailable = false
  const server = createServer((request, response) => {
    if (unavailable) { response.destroy(); return }
    if (request.headers.authorization !== `Bearer ${token}` || !healthy) { response.writeHead(401); response.end(); return }
    response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify({ protocol: 1, instanceId: responseId }))
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)) })
  const base = { pid: 12345, instanceId: 'expected', url: `http://127.0.0.1:${server.address().port}`, token, protocol: 1 }
  for (const [mode, expectedSignals] of [['matching', 1], ['wrong-instance', 0], ['unauthorized', 0], ['unreachable', 0], ['changed-runtime', 0]]) {
    responseId = mode === 'wrong-instance' ? 'another-instance' : 'expected'; healthy = mode !== 'unauthorized'
    unavailable = mode === 'unreachable'
    const dataDir = await mkdtemp(join(tmpdir(), 'sentinel-native-process-'))
    t.after(() => rm(dataDir, { recursive: true, force: true }))
    await writeFile(join(dataDir, 'runtime.json'), JSON.stringify(mode === 'changed-runtime' ? { ...base, instanceId: 'another-instance' } : base), { mode: 0o600 })
    let running = true
    const signals = []
    const result = await cleanupCore(dataDir, base, { isAlive: () => running,
      sendSignal(pid, signal) { signals.push({ pid, signal }); running = false } })
    assert.equal(signals.length, expectedSignals, mode)
    assert.deepEqual(signals, expectedSignals ? [{ pid: base.pid, signal: 'SIGTERM' }] : [], mode)
    assert.equal(result.status, mode === 'matching' ? 'resolved' : 'unresolved', mode)
    assert.equal(await gone(dataDir), mode === 'matching', mode)
  }
})

test('an owned native fixture closes its adapter resources within ten seconds and exits normally without controlling its actor', async t => {
  const environment = await nativeEvidence(), runtime = await ownedRuntime(t), host = await runtime.host('process-a')
  const startedAt = Date.now(), closed = await host.call('close')
  await until(() => !!host.finished, startedAt + 10_000, 'normal native fixture exit')
  const nativeExitMs = Date.now() - startedAt
  assert.deepEqual(host.finished, { code: 0, signal: null })
  assert.equal(closed.actorStatusAfterAdapter, 'running')
  assert.equal(closed.result.status, 'closed'); assert.equal(closed.result.timedOut, false)
  assert.equal(closed.adapter.adapter.activeRequests, 0); assert.equal(closed.adapter.adapter.timers, 0)
  assert.equal(closed.adapter.adapter.handlers, 0); assert.equal(closed.adapter.adapter.subscriptions, 0)
  await runtime.assertStopped(Date.now() + 14_000)
  assert.deepEqual(await nativeEvidence(), environment, 'SDK revisions/artifacts changed during native lifecycle validation')
  t.diagnostic(JSON.stringify({ nativeProcessNormal: { environment, corePid: runtime.pid, hostPid: host.child.pid,
    sourceKey: host.initial.sourceKey, adapterCloseMs: closed.elapsedMs, nativeExitMs, coreExitMs: Date.now() - startedAt } }))
})

test('abrupt native Host exits expire real leases, cancel held inference, preserve another Host, and save before last core process exit', async t => {
  const startedAt = Date.now(), environment = await nativeEvidence(), runtime = await ownedRuntime(t)
  const inference = await inferenceFixture()
  t.after(() => inference.close())
  const aRegistrationId = 'dsh:' + join(runtime.dataDir, 'profiles', 'process-a')
  await configureInstallationModels(runtime.client, aRegistrationId, inference.config)
  const a = await runtime.host('process-a')
  assert.equal(a.initial.registrationId, aRegistrationId)
  await until(() => inference.diagnostics().held === 1, Date.now() + 10_000, 'first held inference')
  const bRegistrationId = 'dsh:' + join(runtime.dataDir, 'profiles', 'process-b')
  await configureInstallationModels(runtime.client, bRegistrationId, inference.config)
  const b = await runtime.host('process-b')
  assert.equal(b.initial.registrationId, bRegistrationId)
  assert.notEqual(a.initial.hostRunId, b.initial.hostRunId); assert.notEqual(a.initial.lifecycleId, b.initial.lifecycleId)
  assert.equal(a.initial.taskId, b.initial.taskId)
  assert.equal(a.initial.profile, join(runtime.dataDir, 'profiles', 'process-a'))
  assert.equal(b.initial.profile, join(runtime.dataDir, 'profiles', 'process-b'))
  const plan = await runtime.client.sourcePlan()
  assert.equal(plan.entries.length, 2); assert.ok(plan.entries.every(entry => entry.status === 'connected'))
  assert.equal(plan.discovery.complete, false)
  await until(async () => (await runtime.client.diagnostics()).temporary.queueKeys >= 1, Date.now() + 10_000, 'shared native task queue')
  const before = await runtime.client.diagnostics()
  assert.equal(before.runtime.activeAnalyses, 1)
  const toolA = await a.call('tool', { callId: 'process-a:tool-1' }), toolB = await b.call('tool', { callId: 'process-b:tool-1' })
  assert.equal(toolA.value, 'process-a:tool-1'); assert.equal(toolB.value, 'process-b:tool-1')
  const killedAAt = Date.now()
  await a.killAbruptly()
  let disconnected
  await until(async () => {
    disconnected = await runtime.client.snapshot()
    return disconnected.sources.find(source => source.key === a.initial.sourceKey)?.status === 'disconnected'
  }, killedAAt + 30_000, 'abrupt source lease expiry')
  const firstLeaseMs = Date.now() - killedAAt
  assert.equal(disconnected.tasks.find(task => task.sourceKey === a.initial.sourceKey).status, 'unknown')
  assert.equal(disconnected.tasks.find(task => task.sourceKey === b.initial.sourceKey).status, 'running')
  await until(async () => {
    const value = await runtime.client.diagnostics()
    return value.temporary.queueKeys === 0 && value.temporary.queueMemberships === 0
      && value.runtime.activeAnalyses === 0 && value.runtime.cancelledUnsettledRequests === 0
      && value.http.activeRequests === 0 && inference.diagnostics().held === 0
  }, killedAAt + 30_000, 'abrupt source temporary resource cleanup')
  assert.equal(inference.diagnostics().cancelled, 1); assert.ok(inference.diagnostics().completed >= 1)
  assert.equal((await runtime.client.diagnostics()).instanceId, runtime.instanceId)
  const surviving = await b.call('state'), toolAfter = await b.call('tool', { callId: 'process-b:tool-2' })
  assert.equal(surviving.actorStatus, 'running'); assert.equal(surviving.providerAborted, false)
  assert.equal(toolAfter.value, 'process-b:tool-2'); assert.equal(toolAfter.signalAborted, false); assert.equal(toolAfter.providerAborted, false)
  await until(async () => (await runtime.client.snapshot()).tasks.find(task => task.sourceKey === b.initial.sourceKey)?.events.some(event => event.kind === 'tool-result' && event.text === 'process-b:tool-2'), Date.now() + 5000, 'surviving native activity capture')
  const cooling = (await runtime.client.snapshot()).tasks.find(task => task.sourceKey === b.initial.sourceKey)
  assert.equal(cooling.status, 'running')
  assert.ok(Number.isFinite(cooling.analysis.completedAt))
  assert.equal(cooling.analysis.nextEligibleAt, cooling.analysis.completedAt + 60_000)
  assert.ok(cooling.analysis.nextEligibleAt > Date.now(), 'the completed surviving round still has its real cooldown')
  inference.hold('process-b')
  const reconfiguredAt = Date.now()
  await configureInstallationModels(runtime.client, b.initial.registrationId, inference.config)
  const reconfigured = (await runtime.client.snapshot()).tasks.find(task => task.sourceKey === b.initial.sourceKey)
  assert.equal(reconfigured.analysis.completedAt, cooling.analysis.completedAt)
  assert.equal(reconfigured.analysis.nextEligibleAt, cooling.analysis.nextEligibleAt, 'configuration replacement preserves the surviving object completion cooldown')
  assert.equal(reconfigured.status, 'running'); assert.equal(alive(b.child.pid), true)
  await until(() => inference.diagnostics().held === 1, reconfigured.analysis.nextEligibleAt + 10_000, 'last held inference after surviving object cooldown')
  const heldAt = Date.now()
  assert.ok(heldAt >= reconfigured.analysis.nextEligibleAt, 'the replacement round cannot start before its public eligibility time')
  const stillSurviving = await b.call('state')
  assert.equal(stillSurviving.actorStatus, 'running'); assert.equal(stillSurviving.providerAborted, false)
  t.diagnostic(JSON.stringify({ processCooldown: { sourceKey: b.initial.sourceKey, taskKey: cooling.key, completedAt: cooling.analysis.completedAt,
    nextEligibleBefore: cooling.analysis.nextEligibleAt, nextEligibleAfter: reconfigured.analysis.nextEligibleAt, reconfiguredAt, heldAt,
    observationAllowanceMs: 10_000, hostAlive: alive(b.child.pid), actorStatus: stillSurviving.actorStatus, providerAborted: stillSurviving.providerAborted } }))
  const killedBAt = Date.now()
  await b.killAbruptly()
  await runtime.assertStopped(killedBAt + 30_000)
  assert.equal(inference.diagnostics().held, 0); assert.equal(inference.diagnostics().cancelled, 2)
  const restored = await createCore({ dataDir: runtime.dataDir })
  try {
    const saved = await restored.snapshot()
    assert.equal(saved.sources.length, 2); assert.ok(saved.sources.every(source => source.status === 'disconnected'))
    assert.equal(saved.tasks.length, 2); assert.ok(saved.tasks.every(task => task.status === 'unknown'))
    assert.ok(saved.tasks.find(task => task.sourceKey === b.initial.sourceKey).events.some(event => event.text === 'process-b:tool-2'))
    assert.ok(saved.tasks.find(task => task.sourceKey === a.initial.sourceKey).events.every(event => !event.text.startsWith('process-b')))
    assert.ok(saved.tasks.find(task => task.sourceKey === b.initial.sourceKey).events.every(event => !event.text.startsWith('process-a')))
  } finally { await restored.close() }
  assert.deepEqual(await nativeEvidence(), environment, 'SDK revisions/artifacts changed during native lifecycle validation')
  t.diagnostic(JSON.stringify({ nativeProcessAbrupt: { environment, corePid: runtime.pid, hostPids: [a.child.pid, b.child.pid],
    defaultSourceLeaseMs: 15_000, productionIdleGraceMs: 10_000, firstLeaseMs, lastExitMs: Date.now() - killedBAt,
    inference: inference.diagnostics(), totalMs: Date.now() - startedAt } }))
})
