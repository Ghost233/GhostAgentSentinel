import { readFile, mkdir, rm, writeFile, stat } from 'node:fs/promises'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { join, resolve } from 'node:path'
import { homedir } from 'node:os'
import { setTimeout as delay } from 'node:timers/promises'
import { PROTOCOL, normalizeFailureScope, normalizeInstallationNotificationRecordsQuery } from './protocol.mjs'

export const defaultDataDir = join(homedir(), '.ghost-agent-sentinel')

async function request(runtime, path, body, signal, tracking) {
  signal?.throwIfAborted()
  const startedAt = Date.now(), started = performance.now()
  const deadline = new AbortController()
  const timer = setTimeout(() => deadline.abort(new Error('core request deadline exceeded')), 5000)
  const requestSignal = signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal
  let response, kind = 'transport'
  if (tracking) tracking.deadlineTimers++
  try {
  response = await fetch(`${runtime.url}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { authorization: `Bearer ${runtime.token}`, 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: requestSignal, redirect: 'error',
  })
  kind = 'decode'
  const result = await response.json()
  kind = 'http'
  if (!response.ok) throw Object.assign(new Error(result.error || `core HTTP ${response.status}`), { status: response.status, ...(typeof result.code === 'string' ? { code: result.code } : {}), ...(Array.isArray(result.causes) ? { causes: result.causes } : {}) })
  return result
  } catch (error) {
    if (error === null || typeof error !== 'object') throw error
    const code = error.code ?? error.cause?.code
    error.coreRequestFailure = Object.freeze({ operation: path.split('?')[0], startedAt, failedAt: Date.now(), elapsedMs: performance.now() - started, timeoutMs: 5000,
      kind: requestSignal.aborted ? requestSignal.reason === deadline.signal.reason ? 'timeout' : 'cancelled' : kind,
      name: ['Error', 'TypeError', 'SyntaxError', 'AbortError', 'TimeoutError'].includes(error.name) ? error.name : 'Error',
      ...(response ? { status: response.status } : {}),
      ...(['chatgpt_installation_connected', 'analysis_config_conflict', 'analysis_config_unconfirmed', 'hierarchy_version_unsupported', 'hierarchy_context_invalid', 'conversation_monitoring_conflict', 'conversation_monitoring_capacity', 'gs_summary_unsupported', 'ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'ETIMEDOUT', 'UND_ERR_SOCKET'].includes(code) ? { code } : {}) })
    throw error
  } finally { clearTimeout(timer); if (tracking) tracking.deadlineTimers-- }
}

async function liveRuntime(dataDir, signal) {
  signal?.throwIfAborted()
  try {
    const runtime = JSON.parse(await readFile(join(dataDir, 'runtime.json'), 'utf8'))
    if (runtime.protocol !== PROTOCOL || !/^http:\/\/127\.0\.0\.1:\d+$/.test(runtime.url) || !/^[a-f0-9]{64}$/.test(runtime.token)) return null
    const health = await request(runtime, '/v1/health', undefined, signal)
    if (health.instanceId !== runtime.instanceId || health.protocol !== PROTOCOL) return null
    return runtime
  } catch (error) { signal?.throwIfAborted(); return null }
}

async function ensureRuntime(dataDir, signal) {
  signal?.throwIfAborted()
  const existing = await liveRuntime(dataDir, signal)
  if (existing) return existing
  await mkdir(dataDir, { recursive: true, mode: 0o700 })
  const lock = join(dataDir, 'startup.lock')
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    signal?.throwIfAborted()
    let owner = false
    try { await mkdir(lock); owner = true }
    catch (error) { if (error.code !== 'EEXIST') throw error }
    if (owner) {
      try {
        await writeFile(join(lock, 'owner.json'), JSON.stringify({ pid: process.pid }), { mode: 0o600 })
        const ready = await liveRuntime(dataDir, signal)
        if (ready) return ready
        const child = spawn(process.execPath, [fileURLToPath(new URL('./daemon.mjs', import.meta.url)), '--serve', '--data-dir', dataDir], { detached: true, stdio: 'ignore', env: process.env })
        let spawnError
        child.on('error', error => { spawnError = error })
        child.unref()
        while (Date.now() < deadline) {
          signal?.throwIfAborted()
          if (spawnError) throw spawnError
          const runtime = await liveRuntime(dataDir, signal)
          if (runtime) return runtime
          if (child.exitCode !== null) throw new Error(`core exited during startup (${child.exitCode})`)
          await delay(50, undefined, { signal })
        }
        throw new Error('core startup timed out; run ghost-agent-sentinel --serve for diagnostics')
      } finally { await rm(lock, { recursive: true, force: true }) }
    }
    const ready = await liveRuntime(dataDir, signal)
    if (ready) return ready
    try {
      const { pid } = JSON.parse(await readFile(join(lock, 'owner.json'), 'utf8'))
      if (Number.isInteger(pid) && pid > 0) {
        try { process.kill(pid, 0) } catch (error) { if (error.code === 'ESRCH') await rm(lock, { recursive: true, force: true }) }
      }
    } catch {
      try { if (Date.now() - (await stat(lock)).mtimeMs > 10_000) await rm(lock, { recursive: true, force: true }) } catch {}
    }
    await delay(100, undefined, { signal })
  }
  throw new Error('core startup is busy; reconnect later')
}

export async function connectCore({ dataDir = defaultDataDir, signal } = {}) {
  signal?.throwIfAborted()
  dataDir = resolve(dataDir)
  const runtime = await ensureRuntime(dataDir, signal)
  let closed = false
  const active = new Map(), tracking = { deadlineTimers: 0 }
  const call = (path, body, requestSignal) => {
    if (closed) return Promise.reject(new Error('core client closed'))
    const controller = new AbortController()
    const combined = requestSignal ? AbortSignal.any([controller.signal, requestSignal]) : controller.signal
    const pending = request(runtime, path, body, combined, tracking)
    active.set(controller, pending)
    const remove = () => active.delete(controller)
    pending.then(remove, remove)
    return pending
  }
  const cancelPending = async (reason = new Error('core client requests cancelled')) => {
    const pending = [...active.values()]
    for (const controller of active.keys()) controller.abort(reason)
    await Promise.allSettled(pending)
  }
  return {
    health: requestSignal => call('/v1/health', undefined, requestSignal),
    registerSource: (descriptor, requestSignal) => call('/v1/source/register', descriptor, requestSignal),
    heartbeat: (sourceKey, requestSignal) => call('/v1/source/heartbeat', { sourceKey }, requestSignal),
    nativeOriginFence: (sourceKey, key) => call('/v1/source/native-origin', { sourceKey, key }),
    ingest: (sourceKey, batch, requestSignal) => call('/v1/source/ingest', { sourceKey, batch }, requestSignal),
    unregisterSource: (sourceKey, reason, requestSignal) => call('/v1/source/unregister', { sourceKey, reason }, requestSignal),
    purgeChatGPTInstallation: (scope, requestSignal) => call('/v1/chatgpt/installation/purge', scope, requestSignal),
    snapshot: requestSignal => call('/v1/snapshot', undefined, requestSignal),
    gsSummary: (sourceKey, sessionId, requestSignal) => call(`/v1/gs/summary?sourceKey=${encodeURIComponent(sourceKey)}&sessionId=${encodeURIComponent(sessionId)}`, undefined, requestSignal).catch(error => { if (error.status === 404) error.code = 'gs_summary_unsupported'; throw error }),
    diagnostics: requestSignal => call('/v1/diagnostics', undefined, requestSignal),
    sourcePlan: requestSignal => call('/v1/sources', undefined, requestSignal),
    configureSources: (entries, requestSignal) => call('/v1/sources', entries, requestSignal),
    pause: requestSignal => call('/v1/monitoring/pause', {}, requestSignal),
    resume: requestSignal => call('/v1/monitoring/resume', {}, requestSignal),
    conversationMonitoring: (scope, requestSignal) => call('/v1/conversation/monitoring?' + new URLSearchParams(scope), undefined, requestSignal),
    setConversationMonitoring: (control, requestSignal) => call('/v1/conversation/monitoring', control, requestSignal),
    installationMonitoring: (registrationId, requestSignal) => call('/v1/monitoring/installations?' + new URLSearchParams({ registrationId }), undefined, requestSignal),
    setInstallationMonitoring: (registrationId, paused, revision, requestSignal) => call('/v1/monitoring/installations', { registrationId, paused, revision }, requestSignal),
    // Identifiers cannot be empty: only an explicit null uses the empty query
    // value. Literal "null" stays a string, and undefined fields are omitted.
    failureReceipts: async (scope, requestSignal) => call('/v1/monitoring/failures?' + new URLSearchParams(Object.entries(normalizeFailureScope(scope)).map(([key, value]) => [key, value === null ? '' : String(value)])), undefined, requestSignal),
    recordCollectionOutcome: (outcome, requestSignal) => call('/v1/monitoring/collection-outcome', outcome, requestSignal),
    configureAnalysis: (config, requestSignal) => call('/v1/analysis/config', config, requestSignal),
    analysisConfig: requestSignal => call('/v1/analysis/config', undefined, requestSignal),
    analysisConfigState: requestSignal => call('/v1/analysis/config/state', undefined, requestSignal),
    configureAnalysisIfCurrent: (config, revision, requestSignal) => call('/v1/analysis/config/compare', { config, revision }, requestSignal),
    installationAnalysisConfig: (registrationId, requestSignal) => call('/v1/analysis/installations/config?' + new URLSearchParams({ registrationId }), undefined, requestSignal),
    configureInstallationAnalysis: (registrationId, config, revision, requestSignal) => call('/v1/analysis/installations/config', { registrationId, config, revision }, requestSignal),
    installationNotificationPreferences: (registrationId, requestSignal) => call('/v1/notifications/installations/preferences?' + new URLSearchParams({ registrationId }), undefined, requestSignal),
    installationNotificationRecords: async (registrationId, selection = {}, requestSignal) => call('/v1/notifications/installations/records?' + new URLSearchParams({ registrationId, ...normalizeInstallationNotificationRecordsQuery(selection) }), undefined, requestSignal),
    setInstallationNotificationPreference: (registrationId, kind, enabled, requestSignal) => call('/v1/notifications/installations/preferences', { registrationId, kind, enabled }, requestSignal),
    claimNotifications: (sourceKey, requestSignal) => call('/v1/notifications/claim', { sourceKey, submissionProtocol: 1 }, requestSignal),
    beginNotificationSubmission: (sourceKey, id, attemptToken, requestSignal) => call('/v1/notifications/submitting', { sourceKey, id, attemptToken }, requestSignal),
    waitNotificationSubmissionRevocation: (sourceKey, id, attemptToken, requestSignal) => call('/v1/notifications/revocation', { sourceKey, id, attemptToken }, requestSignal),
    setNativeNotificationEligibility: (facts, requestSignal) => call('/v1/notifications/native/eligibility', facts, requestSignal),
    nativeNotificationRecords: (query, requestSignal) => call('/v1/notifications/native?' + new URLSearchParams(query), undefined, requestSignal),
    recordNativeNotificationResult: (result, requestSignal) => call('/v1/notifications/native/result', result, requestSignal),
    ackNotification: (id, result, requestSignal) => call('/v1/notifications/ack', { id, result }, requestSignal),
    cancelPending,
    transportDiagnostics: () => ({ closed, pendingRequests: active.size, deadlineTimers: tracking.deadlineTimers }),
    close: async () => { closed = true; await cancelPending(new Error('core client closed')) },
  }
}
