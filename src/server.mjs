import { FORK_CONTEXT_FEATURE, FORK_STORAGE_FEATURE, FORK_LINEAGE_FEATURE, FORK_LEGACY_FEATURE } from './fork-proof.mjs'
import { createServer } from 'node:http'
import { randomBytes, randomUUID, timingSafeEqual } from 'node:crypto'
import { mkdir, readFile, writeFile, rename, chmod, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { createCore } from './core/index.mjs'
import { createAnalyzer } from './analysis/index.mjs'
import { PROTOCOL, GS_SUMMARY_FEATURE, SOURCE_OBSERVATION_FEATURE, CHATGPT_MAIN_FEATURE, CONVERSATION_MONITORING_FEATURE, MONITORING_FAILURES_FEATURE, MONITORING_NOTIFICATIONS_FEATURE, normalizeSource, normalizeBatch, normalizeAnalysisConfig } from './protocol.mjs'
import { normalizeEnrollment } from './enrollment-config.mjs'
import { HIERARCHY_FEATURE } from './hierarchy.mjs'
import { GS_FEATURE } from './gs-status.mjs'

export async function writePrivateJson(path, value) {
  const temporary = `${path}.${randomUUID()}.tmp`
  try {
    await writeFile(temporary, JSON.stringify(value), { mode: 0o600 })
    await chmod(temporary, 0o600)
    await rename(temporary, path)
  } catch (failure) {
    try { await rm(temporary, { force: true }) }
    catch (cleanup) { throw new AggregateError([failure, cleanup], failure.message) }
    throw failure
  }
}

async function readBody(request) {
  let size = 0
  const chunks = []
  for await (const chunk of request) {
    size += chunk.length
    if (size > 1_048_576) throw new Error('request exceeds 1 MiB')
    chunks.push(chunk)
  }
  try { return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') }
  catch { throw new Error('invalid JSON body') }
}
function authorized(request, token) {
  if (request.headers.origin) return false
  const actual = Buffer.from(request.headers.authorization ?? '')
  const expected = Buffer.from(`Bearer ${token}`)
  return actual.length === expected.length && timingSafeEqual(actual, expected)
}
function identifier(value) {
  if (typeof value !== 'string' || !value || value.length > 2000) throw new Error('invalid identifier')
  return value
}

async function acquireCoreLock(dataDir, instanceId) {
  const path = join(dataDir, 'core.lock')
  for (let attempt = 0; attempt < 2; attempt++) {
    try { await mkdir(path, { mode: 0o700 }) }
    catch (error) {
      if (error.code !== 'EEXIST') throw error
      let dead = false
      try {
        const owner = JSON.parse(await readFile(join(path, 'owner.json'), 'utf8'))
        if (Number.isInteger(owner.pid) && owner.pid > 0) {
          try { process.kill(owner.pid, 0) } catch (probe) { dead = probe.code === 'ESRCH' }
        }
      } catch {
        // A crash between mkdir and owner write must not block all future starts.
        dead = Date.now() - (await stat(path)).mtimeMs > 10_000
      }
      if (!dead) throw new Error('core already running or startup in progress for this data directory')
      await rm(path, { recursive: true, force: true })
      continue
    }
    try { await writePrivateJson(join(path, 'owner.json'), { pid: process.pid, instanceId }) }
    catch (error) { await rm(path, { recursive: true, force: true }); throw error }
    return async () => {
      try {
        const owner = JSON.parse(await readFile(join(path, 'owner.json'), 'utf8'))
        if (owner.instanceId === instanceId) await rm(path, { recursive: true, force: true })
      } catch (error) { if (error.code !== 'ENOENT') throw error }
    }
  }
  throw new Error('core startup lock changed; reconnect later')
}

export async function startServer({ dataDir, onIdle, ...options }) {
  await mkdir(dataDir, { recursive: true, mode: 0o700 })
  await chmod(dataDir, 0o700)
  const instanceId = randomUUID()
  const releaseLock = await acquireCoreLock(dataDir, instanceId)
  const configPath = join(dataDir, 'analysis-config.json')
  const sockets = new Set()
  const counts = { activeRequests: 0, totalRequests: 0, snapshotReads: 0, notificationClaims: 0 }
  let idlePending = false, stopping = false
  async function flushIdle() {
    if (!idlePending || counts.activeRequests || stopping || !core) return
    const diagnostics = await core.diagnostics()
    if (diagnostics.runtime.connectedSources) { idlePending = false; return }
    idlePending = false
    onIdle?.()
  }
  let config = {}, revision = randomUUID(), configWrites = Promise.resolve(), configConfirmed = true
  let core
  try {
    try { config = normalizeAnalysisConfig(JSON.parse(await readFile(configPath, 'utf8'))) }
    catch (error) { if (error.code !== 'ENOENT') throw new Error(`cannot read analysis configuration: ${error.message}`) }
    core = await createCore({ dataDir, ...options, analyzer: createAnalyzer(config), onIdle: () => { idlePending = true; void flushIdle() } })
  } catch (error) { await releaseLock(); throw error }
  const configState = () => {
    if (!configConfirmed) throw Object.assign(new Error('analysis configuration application is unconfirmed'), { code: 'analysis_config_unconfirmed' })
    return { config, revision }
  }
  function configure(next, expected) {
    const pending = configWrites.then(async () => {
      if (expected !== undefined) configState()
      if (expected !== undefined && expected !== revision) throw Object.assign(new Error('analysis configuration revision conflict'), { code: 'analysis_config_conflict' })
      await writePrivateJson(configPath, next)
      try { await core.setAnalyzer(createAnalyzer(next)) }
      catch (failure) {
        const failures = [failure]
        for (const restore of [() => writePrivateJson(configPath, config), () => core.setAnalyzer(createAnalyzer(config))]) { try { await restore() } catch (error) { failures.push(error) } }
        if (failures.length > 1) { configConfirmed = false; revision = randomUUID(); throw Object.assign(new AggregateError(failures, failure.message), { code: 'analysis_config_unconfirmed' }) }
        throw failure
      }
      config = next; revision = randomUUID(); configConfirmed = true
      return configState()
    })
    configWrites = pending.catch(() => {})
    return pending
  }
  const token = randomBytes(32).toString('hex')
  let closing
  const server = createServer(async (request, response) => {
    counts.activeRequests++; counts.totalRequests++
    try {
    response.setHeader('content-type', 'application/json; charset=utf-8')
    response.setHeader('cache-control', 'no-store')
    const send = (status, body) => { response.writeHead(status); response.end(JSON.stringify(body)) }
    const expectedHost = `127.0.0.1:${server.address()?.port}`
    if (request.headers.host !== expectedHost || !authorized(request, token)) return send(401, { error: 'private connection required' })
    if (stopping) return send(503, { error: 'core shutting down' })
    try {
      if (request.method === 'GET' && request.url === '/v1/health') return send(200, { protocol: PROTOCOL, instanceId, features: [FORK_CONTEXT_FEATURE, FORK_STORAGE_FEATURE, FORK_LINEAGE_FEATURE, FORK_LEGACY_FEATURE, HIERARCHY_FEATURE, GS_FEATURE, GS_SUMMARY_FEATURE, SOURCE_OBSERVATION_FEATURE, CHATGPT_MAIN_FEATURE, CONVERSATION_MONITORING_FEATURE, MONITORING_FAILURES_FEATURE, MONITORING_NOTIFICATIONS_FEATURE] })
      if (request.method === 'GET' && request.url.startsWith('/v1/notifications/native')) {
        const url = new URL(request.url, `http://${expectedHost}`), params = url.searchParams
        if (url.pathname !== '/v1/notifications/native') return send(404, { error: 'unknown endpoint' })
        if ([...params.keys()].some(key => params.getAll(key).length !== 1)) throw new Error('duplicate native notification query field')
        const query = Object.fromEntries(params)
        for (const key of ['afterSequence', 'limit']) if (query[key] !== undefined) {
          if (!/^\d+$/.test(query[key])) throw new Error('invalid native notification query integer')
          query[key] = Number(query[key])
        }
        return send(200, await core.nativeNotificationRecords(query))
      }
      if (request.method === 'GET' && request.url.startsWith('/v1/monitoring/failures')) {
        const url = new URL(request.url, `http://${expectedHost}`), params = url.searchParams
        if (url.pathname !== '/v1/monitoring/failures') return send(404, { error: 'unknown endpoint' })
        if ([...params.keys()].some(key => params.getAll(key).length !== 1)) throw new Error('duplicate failure receipt query field')
        const scope = Object.fromEntries(params)
        for (const key of ['mainId', 'lifecycleId']) if (scope[key] === '') scope[key] = null
        for (const key of ['afterSequence', 'limit']) if (scope[key] !== undefined) {
          if (!/^\d+$/.test(scope[key])) throw new Error('invalid failure receipt query integer')
          scope[key] = Number(scope[key])
        }
        return send(200, await core.failureReceipts(scope))
      }
      if (request.method === 'GET' && request.url.startsWith('/v1/conversation/monitoring')) {
        const url = new URL(request.url, `http://${expectedHost}`), params = url.searchParams
        if (url.pathname !== '/v1/conversation/monitoring') return send(404, { error: 'unknown endpoint' })
        if ([...params.keys()].length !== 2 || params.getAll('registrationId').length !== 1 || params.getAll('taskId').length !== 1) throw new Error('invalid conversation monitoring query')
        return send(200, await core.conversationMonitoring(Object.fromEntries(params)))
      }
      if (request.method === 'GET' && request.url.startsWith('/v1/gs/summary')) {
        const url = new URL(request.url, `http://${expectedHost}`)
        if (url.pathname !== '/v1/gs/summary') return send(404, { error: 'unknown endpoint' })
        const params = url.searchParams
        if ([...params.keys()].length !== 2 || params.getAll('sourceKey').length !== 1 || params.getAll('sessionId').length !== 1) throw new Error('invalid GS summary query')
        return send(200, await core.gsSummary({ sourceKey: identifier(params.get('sourceKey')), sessionId: identifier(params.get('sessionId')) }))
      }
      if (request.method === 'GET' && request.url === '/v1/snapshot') { counts.snapshotReads++; return send(200, await core.snapshot()) }
      if (request.method === 'GET' && request.url === '/v1/diagnostics') return send(200, { ...await core.diagnostics(), instanceId, http: { ...counts, activeRequests: counts.activeRequests - 1, connections: sockets.size, listening: server.listening, stopping } })
      if (request.method === 'GET' && request.url === '/v1/sources') return send(200, await core.sourcePlan())
      if (request.method === 'GET' && request.url === '/v1/analysis/config/state') return send(200, configState())
      if (request.method === 'GET' && request.url === '/v1/analysis/config') return send(200, configState().config)
      if (request.method !== 'POST') return send(404, { error: 'unknown endpoint' })
      const body = await readBody(request)
      let result
      switch (request.url) {
        case '/v1/source/register': result = await core.registerSource(normalizeSource(body)); break
        case '/v1/source/heartbeat': result = await core.heartbeat(identifier(body.sourceKey)); break
        case '/v1/source/ingest': result = await core.ingest(identifier(body.sourceKey), normalizeBatch(body.batch)); break
        case '/v1/source/unregister': result = await core.unregisterSource(identifier(body.sourceKey), typeof body.reason === 'string' ? body.reason.slice(0, 500) : 'adapter stopped'); break
        case '/v1/monitoring/pause': result = await core.pause(); break
        case '/v1/monitoring/resume': result = await core.resume(); break
        case '/v1/monitoring/collection-outcome': result = await core.recordCollectionOutcome(body); break
        case '/v1/conversation/monitoring': result = await core.setConversationMonitoring(body); break
        case '/v1/notifications/claim': counts.notificationClaims++; result = await core.claimNotifications(identifier(body.sourceKey)); break
        case '/v1/notifications/native/eligibility': result = await core.setNativeNotificationEligibility(body); break
        case '/v1/notifications/native/result': result = await core.recordNativeNotificationResult(body); break
        case '/v1/sources': result = await core.configureSources(normalizeEnrollment(body)); break
        case '/v1/notifications/ack': {
          if (!body.result || !['submitted', 'failed'].includes(body.result.status)) throw new Error('invalid notification result')
          result = await core.ackNotification(identifier(body.id), { status: body.result.status, ...(typeof body.result.error === 'string' ? { error: body.result.error.slice(0, 500) } : {}) })
          break
        }
        case '/v1/analysis/config': {
          result = (await configure(normalizeAnalysisConfig(body))).config
          break
        }
        case '/v1/analysis/config/compare': result = await configure(normalizeAnalysisConfig(body.config), identifier(body.revision)); break
        default: return send(404, { error: 'unknown endpoint' })
      }
      send(200, result ?? null)
    } catch (error) { send(['analysis_config_conflict', 'hierarchy_version_unsupported', 'conversation_monitoring_conflict', 'conversation_monitoring_capacity', 'native_capture_capacity', 'native_capture_body_unavailable'].includes(error.code) ? 409 : error.code === 'hierarchy_context_invalid' ? 422 : error.code === 'analysis_config_unconfirmed' ? 503 : 400, { error: String(error.message || 'request failed').slice(0, 500), ...(['analysis_config_conflict', 'analysis_config_unconfirmed', 'hierarchy_version_unsupported', 'hierarchy_context_invalid', 'conversation_monitoring_conflict', 'conversation_monitoring_capacity', 'native_capture_capacity', 'native_capture_body_unavailable'].includes(error.code) ? { code: error.code } : {}), ...(error instanceof AggregateError ? { causes: error.errors.map(cause => String(cause.message || cause).slice(0, 500)) } : {}) }) }
    } finally { counts.activeRequests--; await flushIdle() }
  })
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)) })
  server.keepAliveTimeout = 1000
  server.requestTimeout = 10_000
  server.headersTimeout = 10_000
  try {
    await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve) })
    const url = `http://127.0.0.1:${server.address().port}`
    await writePrivateJson(join(dataDir, 'runtime.json'), { protocol: PROTOCOL, url, token, pid: process.pid, instanceId, startedAt: Date.now() })
    return { url, token, instanceId, close() {
      if (closing) return closing
      stopping = true
      closing = (async () => {
        await new Promise(resolve => server.close(resolve))
        await core.close()
        let runtime
        try { runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8')) } catch {}
        if (runtime?.instanceId === instanceId) await rm(join(dataDir, 'runtime.json'), { force: true })
        await releaseLock()
      })()
      return closing
    } }
  } catch (error) {
    server.close()
    await core.close()
    await releaseLock()
    throw error
  }
}
