import { homedir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import descriptors from './remote.cjs'
import { createObservation, hostIdentity } from './observation.mjs'
import { connectCore } from '../src/core-client.mjs'
import { HIERARCHY_FEATURE } from '../src/hierarchy.mjs'
import { GS_FEATURE } from '../src/gs-status.mjs'
import { GS_SUMMARY_FEATURE, INSTALLATION_NOTIFICATION_RECORDS_FEATURE, NOTIFICATION_REVOCATION_FEATURE } from '../src/protocol.mjs'
import { createHierarchyReader } from './hierarchy-reader.mjs'
import { readModelDirectory } from '../src/model-directory.mjs'
import { projectInstallationDiagnostics } from '../src/installation-diagnostics.mjs'

export const name = 'ghost-agent-sentinel'
export const inject = ['agents']
const adaptersKey = Symbol.for('ghost-agent-sentinel.dsh-adapters.v1')
const adapters = globalThis[adaptersKey] ??= new WeakMap()

const bounded = async (operation, ms, countTimer = () => {}) => {
  let timer
  countTimer(1)
  try {
    return await Promise.race([operation, new Promise((_, reject) => {
      timer = setTimeout(() => reject(Object.assign(new Error('adapter shutdown deadline'), { code: 'SENTINEL_SHUTDOWN_DEADLINE' })), ms)
    })])
  } finally { clearTimeout(timer); countTimer(-1) }
}

export function createAdapter(ctx, config = {}, { connect = connectCore, flushMs = 250, heartbeatMs = 3000, closeTimeoutMs = 5000, hierarchyReadTimeoutMs = 1000 } = {}) {
  const previous = adapters.get(ctx.root)
  // close synchronously detaches the old hooks/loop; connection replacement awaits its IO cleanup.
  const retiring = previous?.close() ?? Promise.resolve()
  const identity = hostIdentity(ctx.root), profile = ctx.get('profileContext')
  const descriptor = {
    registrationId: config.registrationId || (profile?.dir ? `dsh:${profile.dir}` : `dsh:unconfigured:${identity.hostRunId}`),
    hostRunId: identity.hostRunId,
    name: config.name || (profile?.name ? `DSH · ${profile.name}` : 'DSH Host'),
    capabilities: ['native-agent-events', 'native-tool-results', 'native-baseline'],
    enroll: !!(config.registrationId || profile?.dir),
    gaps: [],
  }
  const capabilityGaps = ['整机来源发现完整性未确认', '外部 ACP 子进程内部执行材料未覆盖', '桌面通知依赖存活的前端客户端和通知权限']
  const parentWaitAudit = () => {
    try { const supported = ctx.get('subagents')?.parentWaitAuditVersion === 1; return { supported, version: supported ? 1 : null } }
    catch { return { supported: false, version: null } }
  }
  if (profile?.dir || profile?.name) descriptor.profile = profile.dir || profile.name
  if (!config.registrationId && !profile?.dir) capabilityGaps.push('未配置稳定 registrationId；仅当前宿主运行身份可核对')
  const dataDir = config.dataDir || join(homedir(), '.ghost-agent-sentinel')
  const lifetime = new AbortController(), handlers = [], activeRequests = new Set(), settingsClients = new Set()
  let client, connectingClient, sourceKey, closed = false, nextRetry = 0, lastHeartbeat = 0, error = '正在连接共享核心'
  let communicationFailure
  let shutdownStatus = 'active', shutdownTimedOut = false, shutdownFailures = 0, shutdownFinalized = false, shutdownTimers = 0, lateSettledRequests = 0
  let sentTasks = new Map(), sentGaps = new Set(), baselineSent = false, tickPromise, timer, pendingClose, provideDispose, remoteFiber, closeEffectDispose, hierarchySupported = false, gsStatusSupported = false, gsSummarySupported = false, submissionRevocationSupported = false, sentHierarchyRevision = -1
  const collector = createObservation({ identity, hierarchy: true, readCatalog(session) {
    if (!ctx.get('subagents')) return undefined
    const projections = ctx.get('sessionProjections')
    if (typeof projections?.cachedSnapshot !== 'function') return undefined
    const snapshot = projections.cachedSnapshot(session, ['subagentCatalog'])
    return snapshot?.cursor === session.seq - 1 ? snapshot.values?.subagentCatalog : undefined
  }, readTitle(session) {
    const projections = ctx.get('sessionProjections')
    if (typeof projections?.cachedSnapshot !== 'function') return undefined
    return projections.cachedSnapshot(session, ['title'])?.values?.title
  } })
  const observe = (...args) => {
    try { return collector.observe(...args) }
    catch { collector.recordGap('事件摘取失败；原任务继续执行') }
  }
  const hierarchyReader = createHierarchyReader(ctx, { observe, recordGap: reason => collector.recordGap(reason), signal: lifetime.signal, timeoutMs: hierarchyReadTimeoutMs })
  const listen = (type, listener, options) => { handlers.push(ctx.on(type, listener, options)) }
  for (const type of ['agent/created', 'agent/status', 'agent/disposed', 'agent/inbox/spliced', 'agent/inbox/claimed', 'agent/assistant-stream']) listen(type, payload => { observe(type, payload); if (type === 'agent/created') hierarchyReader.schedule(payload.agent?.session?.id) })
  listen('tools/execute', (exec, next) => { observe('tools/execute', { exec }); return next() })
  listen('tools/result', (exec, result) => { observe('tools/result', { exec, result }) })
  listen('session/event', (session, event) => { observe('session/event', { session, event }); if (event.type === 'subagent/catalog' && event.seq >= (session.inheritedEventCount ?? 0)) { hierarchyReader.noteCatalog(session.id, event.data, event.time); hierarchyReader.schedule(session.id) } })
  for (const type of ['subagent/start', 'subagent/end']) listen(type, (activation, nativeParent) => {
    const child = ctx.agents.get(activation.id)
    hierarchyReader.noteActivation(type, activation, child, nativeParent)
  })
  for (const type of ['subagent/wait-start', 'subagent/wait-targets', 'subagent/wait-end', 'subagent/wait-gap', 'subagent/wait-caller-start', 'subagent/wait-caller-end']) listen(type, payload => {
    if (!parentWaitAudit().supported) { if (!closed) collector.recordGap('原生父等待审计版本未支持；未建立父等待'); return }
    observe(type, payload)
  })
  listen('user-questions/request', async (request, next) => {
    let waitId, owner
    try { if (!closed) { waitId = randomUUID(); owner = observe('user-questions/request-start', { agent: request.agent, waitId, callId: request.wait?.callId }) } } catch { if (!closed) collector.recordGap('原生用户问询观察失败；原请求继续执行') }
    try { return await next() }
    finally { try { if (!closed && owner) observe('user-questions/request-end', { agent: request.agent, waitId, ...owner }) } catch { if (!closed) collector.recordGap('原生用户问询观察失败；原请求继续执行') } }
  }, { global: true, prepend: true })
  const baseline = () => { try { const agents = ctx.agents.list(); collector.baseline(agents); for (const agent of agents) hierarchyReader.schedule(agent.session?.id) } catch { collector.recordGap('运行任务基线不可用') } }
  baseline()
  const track = operation => {
    const request = Promise.resolve(operation)
    activeRequests.add(request)
    const settled = () => { if (activeRequests.delete(request) && shutdownFinalized) lateSettledRequests++ }
    request.then(settled, settled)
    return request
  }
  const call = (target, method, ...args) => track(target[method](...args)).catch(error => {
    const failure = error?.coreRequestFailure
    if (failure && failure.kind !== 'cancelled') communicationFailure = communicationFailure?.active
      ? { ...communicationFailure, latest: { ...failure } }
      : { active: true, first: { ...failure }, latest: { ...failure }, connectionRecoveredAt: null }
    throw error
  })
  const waitForShutdown = (operation, ms) => bounded(operation, ms, change => { shutdownTimers += change })
  const withShutdownSignal = async (operation, ms) => {
    const controller = new AbortController()
    try { return await waitForShutdown(Promise.resolve().then(() => operation(controller.signal)), ms) }
    finally { controller.abort(new Error('adapter shutdown budget ended')) }
  }
  const shutdownFailure = error => {
    shutdownFailures++
    if (error?.code === 'SENTINEL_SHUTDOWN_DEADLINE') shutdownTimedOut = true
  }
  function transportBatch(batch) {
    if (hierarchySupported) return batch
    const supportedInputs = new Set((batch.hierarchyContext?.instructionBindings || []).filter(binding => binding.phase === 'consumed' && binding.origin === 'user-entry')
      .map(binding => JSON.stringify([binding.inputRef.taskId, binding.inputRef.lifecycleId, binding.inputRef.eventId])))
    const suppressed = batch.events.some(event => event.kind === 'input' && !supportedInputs.has(JSON.stringify([event.taskId, event.taskLifecycleId, event.id])))
    const reason = '旧共享核心不支持输入来源与消费阶段；未确认的输入未用于任务或授权判断'
    return { events: batch.events.map(event => event.kind !== 'input' || supportedInputs.has(JSON.stringify([event.taskId, event.taskLifecycleId, event.id])) ? event
      : { ...event, kind: 'gap', text: reason }), tasks: batch.tasks, gaps: suppressed ? [...new Set([...batch.gaps, reason])] : batch.gaps }
  }
  function pruneSent(batch) {
    const live = new Set(batch.tasks.map(task => task.lifecycleId))
    for (const id of sentTasks.keys()) if (!live.has(id)) sentTasks.delete(id)
    const currentGaps = new Set(batch.gaps)
    for (const gap of sentGaps) if (!currentGaps.has(gap)) sentGaps.delete(gap)
  }
  async function disconnect(reason) {
    const previousClient = client
    client = undefined; sourceKey = undefined
    if (!closed) {
      error = String(reason?.message || reason).slice(0, 200)
      collector.recordGap('共享核心连接曾中断；当前保留缓冲的覆盖边界需核对')
    }
    await previousClient?.close().catch(() => {})
  }
  async function runTick() {
    let communicationConfirmed = false, confirmedFailure
    try {
      await retiring
      if (closed) return
      if (!client) {
        const connected = await track(connect({ dataDir, signal: lifetime.signal }))
        if (closed) { await track(connected.close()); return }
        connectingClient = connected
        const key = await call(connected, 'registerSource', descriptor, lifetime.signal)
        if (closed) {
          // A replaced adapter must never unregister the replacement's stable source key.
          if (adapters.get(ctx.root) === service) await withShutdownSignal(signal => call(connected, 'unregisterSource', key, '适配器已关闭', signal), closeTimeoutMs).catch(shutdownFailure)
          await track(connected.close()); connectingClient = undefined; return
        }
        client = connected; connectingClient = undefined; sourceKey = key
        const features = typeof client.health === 'function' ? (await call(client, 'health', lifetime.signal))?.features : undefined
        hierarchySupported = features?.includes(HIERARCHY_FEATURE) === true
        gsStatusSupported = features?.includes(GS_FEATURE) === true
        gsSummarySupported = features?.includes(GS_SUMMARY_FEATURE) === true
        submissionRevocationSupported = features?.includes(NOTIFICATION_REVOCATION_FEATURE) === true
        sentHierarchyRevision = -1
        sentTasks = new Map(); sentGaps = new Set(); baselineSent = false
        baseline()
        if (closed) return
        lastHeartbeat = 0
      }
      if (Date.now() - lastHeartbeat >= heartbeatMs) {
        await call(client, 'heartbeat', sourceKey, lifetime.signal)
        communicationConfirmed = true
        confirmedFailure = communicationFailure
        if (closed) return
        lastHeartbeat = Date.now()
      }
      await hierarchyReader.drain()
      if (closed) return
      const batch = collector.batch(); pruneSent(batch)
      const tasks = batch.tasks.filter(task => sentTasks.get(task.lifecycleId) !== JSON.stringify(task))
      const gaps = batch.gaps.filter(gap => !sentGaps.has(gap))
      const hierarchyChanged = hierarchySupported && batch.hierarchyContext.revision !== sentHierarchyRevision
      if (!baselineSent || batch.events.length || tasks.length || gaps.length || hierarchyChanged) {
        const outgoing = transportBatch(batch)
        const result = await call(client, 'ingest', sourceKey, { ...outgoing, tasks, gaps: outgoing.gaps.filter(gap => !sentGaps.has(gap)) }, lifetime.signal)
        communicationConfirmed = true
        confirmedFailure = communicationFailure
        if (hierarchySupported && (result?.hierarchyContext?.schemaVersion !== 1 || result.hierarchyContext.acceptedRevision !== batch.hierarchyContext.revision)) throw Object.assign(new Error('层级上下文回执不匹配'), { code: 'hierarchy_context_unconfirmed' })
        if (closed) return
        collector.acknowledge(batch.events.map(event => event.id), tasks)
        for (const task of tasks) sentTasks.set(task.lifecycleId, JSON.stringify(task))
        for (const gap of gaps) sentGaps.add(gap)
        pruneSent(collector.batch())
        baselineSent = true
        if (hierarchySupported) sentHierarchyRevision = batch.hierarchyContext.revision
      }
      error = ''; nextRetry = 0
      if (communicationConfirmed && confirmedFailure === communicationFailure && communicationFailure?.active) communicationFailure = { ...communicationFailure, active: false, connectionRecoveredAt: Date.now() }
    } catch (err) {
      if (!closed) { await disconnect(err); nextRetry = Date.now() + Math.max(flushMs, 1000) }
      else if (connectingClient) { await track(connectingClient.close()).catch(() => {}); connectingClient = undefined }
    }
  }
  function tick() {
    if (closed || tickPromise || Date.now() < nextRetry) return
    tickPromise = runTick().finally(() => { tickPromise = undefined })
  }
  async function invoke(method, ...args) {
    if (closed || !client || !sourceKey) throw new Error(error || '共享核心未连接')
    const requested = args.pop()
    const signal = requested ? AbortSignal.any([lifetime.signal, requested]) : lifetime.signal
    return call(client, method, ...args, signal)
  }
  async function installationCall(method, args, requested, scoped = true) {
    if (closed) throw new Error('观察连接已关闭')
    if (!descriptor.enroll) throw new Error('未配置稳定 registrationId；本安装配置身份未确认')
    const signal = AbortSignal.any([lifetime.signal, AbortSignal.timeout(10000), ...(requested ? [requested] : [])])
    let target = client, temporary = false, failure
    try {
      if (!target) { target = await track(connect({ dataDir, signal })); temporary = true; settingsClients.add(target) }
      signal.throwIfAborted()
      if (typeof target[method] !== 'function') throw new Error('共享核心尚不支持本安装设置')
      return await track(target[method](...(scoped ? [descriptor.registrationId, ...args] : args), signal))
    } catch (error) {
      failure = error
      if (['analysis_config_conflict', 'analysis_config_unconfirmed', 'installation_monitoring_conflict'].includes(error?.code) || Array.isArray(error?.causes)) { error.code ??= 'gateway/internal'; error.isDSHRemoteError = true; error.details = error.causes ? { causes: error.causes } : {} }
      throw error
    } finally {
      if (temporary) {
        try { await waitForShutdown(track(target.close()), closeTimeoutMs) }
        catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], failure.message) : cleanup }
        finally { settingsClients.delete(target) }
      }
    }
  }
  async function setInstallationPaused(paused, signal) {
    const current = await installationCall('installationMonitoring', [], signal)
    return installationCall('setInstallationMonitoring', [paused, current.revision], signal)
  }
  const service = {
    get connected() { return !closed && !!sourceKey && !error },
    diagnostics() {
      return { adapter: { closed, connected: service.connected, timers: (timer ? 1 : 0) + shutdownTimers, activeRequests: activeRequests.size,
        flushTimers: timer ? 1 : 0, shutdownTimers,
        handlers: handlers.length, subscriptions: remoteFiber ? 1 : 0, sentTasks: sentTasks.size, sentGaps: sentGaps.size, tickInFlight: !!tickPromise,
        shutdownStatus, shutdownTimedOut, shutdownFailures, lateSettledRequests,
        ...(communicationFailure ? { communicationFailure: structuredClone(communicationFailure) } : {}) }, collector: collector.diagnostics(), hierarchyReader: hierarchyReader.diagnostics() }
    },
    async snapshot(signal) {
      let state
      try { state = await invoke('snapshot', signal) }
      catch (err) {
        if (signal?.aborted) throw err
        state = { protocol: 1, generatedAt: Date.now(), monitoring: 'unknown', overview: { coverage: 'unknown' }, sources: [], tasks: [] }
        if (!closed) error = String(err.message || err).slice(0, 200)
      }
      let monitoring = null
      if (descriptor.enroll) try { monitoring = await installationCall('installationMonitoring', [], signal) } catch (error) { if (signal?.aborted) throw error }
      const batch = collector.batch()
      return { ...state, adapter: { connected: service.connected, hostRunId: identity.hostRunId, sourceKey: sourceKey ?? null,
        registrationId: descriptor.enroll ? descriptor.registrationId : null, monitoring,
        error: error || null, hierarchyContext: { supported: hierarchySupported, acceptedRevision: sentHierarchyRevision >= 0 ? sentHierarchyRevision : null, code: hierarchySupported ? null : 'hierarchy_context_unsupported' }, gsStatus: { supported: gsStatusSupported, code: gsStatusSupported ? null : 'gs_status_unsupported' }, parentWaitAudit: parentWaitAudit(), bufferedEvents: batch.events.length, gaps: [...capabilityGaps, parentWaitAudit().supported ? '原生父等待需逐次核对实际 await 调用者和开闭；join 审计不提供豁免' : '原生等待子代理开闭审计不可观察；不能证明父级正在等待子代理', ...batch.gaps, ...(hierarchySupported ? [] : ['共享核心未支持层级上下文；当前仅平铺观察']), ...(gsStatusSupported ? [] : ['共享核心未支持GS状态与等待规则；不能声明相关检查已覆盖'])] } }
    },
    gsSummary(sessionId, signal) {
      if (closed || !client || !sourceKey) return Promise.reject(new Error('共享核心未连接'))
      if (typeof sessionId !== 'string' || !sessionId || sessionId.length > 2000 || /[\u0000-\u001f]/.test(sessionId)) return Promise.reject(new Error('会话标识无效'))
      if (!gsSummarySupported) return Promise.resolve({ protocol: 1, generatedAt: null, sourceKey, sessionId, sourceStatus: 'unknown', scopeKnown: false,
        counts: { green: 0, yellow: 0, red: 0, gray: 0 }, coverage: { complete: false, gaps: ['共享核心未支持 GS 计数摘要；当前计数不可判断'] }, gsStatus: { supported: false } })
      return invoke('gsSummary', sourceKey, sessionId, signal)
    },
    pause(signal) { return setInstallationPaused(true, signal) }, resume(signal) { return setInstallationPaused(false, signal) },
    modelDirectory({ baseUrl }, requested) {
      if (closed || !descriptor.enroll) return Promise.reject(new Error('本安装设置资格未确认'))
      const signal = AbortSignal.any([lifetime.signal, ...(requested ? [requested] : [])])
      return track((async () => {
        signal.throwIfAborted()
        const result = await readModelDirectory(baseUrl, { signal })
        signal.throwIfAborted()
        if (closed) throw new Error('观察连接已关闭')
        return result
      })())
    },
    installationDiagnostics(requested) {
      const signal = AbortSignal.any([lifetime.signal, ...(requested ? [requested] : [])])
      return track((async () => {
        const setting = await installationCall('installationAnalysisConfig', [], signal)
        const snapshot = await installationCall('snapshot', [], signal, false)
        signal.throwIfAborted()
        if (closed) throw new Error('观察连接已关闭')
        const collectionError = communicationFailure?.active ? { stage: 'core', code: communicationFailure.latest.code ?? null } : null
        return projectInstallationDiagnostics(setting, snapshot, { collectionError })
      })())
    },
    analysisConfigState(signal) { return installationCall('installationAnalysisConfig', [], signal) },
    async configureAnalysisIfCurrent(value, revision, signal) {
      try { return await installationCall('configureInstallationAnalysis', [value, revision], signal) }
      catch (error) {
        const requestFailure = error?.coreRequestFailure
        const rejected = !(error instanceof AggregateError) && !Array.isArray(error?.errors) && !Array.isArray(error?.causes)
          && error?.code !== 'analysis_config_unconfirmed' && requestFailure?.operation === '/v1/analysis/installations/config'
          && requestFailure.kind === 'http' && requestFailure.status >= 400 && requestFailure.status <= 599
        const failure = error && typeof error === 'object' ? error : new Error(String(error))
        failure.code ??= 'gateway/internal'; failure.isDSHRemoteError = true
        failure.details = { ...failure.details, saveOutcome: rejected ? 'rejected' : 'unknown' }
        throw failure
      }
    },
    async analysisConfig(signal) { return (await service.analysisConfigState(signal)).config },
    installationNotificationPreferences(signal) { return installationCall('installationNotificationPreferences', [], signal) },
    async setInstallationNotificationPreference(kind, enabled, signal) {
      try { return await installationCall('setInstallationNotificationPreference', [kind, enabled], signal) }
      catch (error) {
        const requestFailure = error?.coreRequestFailure
        const rejected = !(error instanceof AggregateError) && !Array.isArray(error?.errors) && !Array.isArray(error?.causes)
          && !['analysis_config_unconfirmed', 'notification_revocation_unconfirmed'].includes(error?.code) && requestFailure?.operation === '/v1/notifications/installations/preferences' && requestFailure.kind === 'http' && requestFailure.status >= 400 && requestFailure.status <= 599
        const failure = error && typeof error === 'object' ? error : new Error(String(error))
        failure.code ??= 'gateway/internal'; failure.isDSHRemoteError = true
        failure.details = { ...failure.details, saveOutcome: rejected ? 'rejected' : 'unknown' }
        throw failure
      }
    },
    beginNotificationSubmission(id, attemptToken, signal) {
      if (!submissionRevocationSupported) return Promise.reject(new Error('共享核心尚不支持通知撤销确认；未提交通知'))
      return invoke('beginNotificationSubmission', sourceKey, id, attemptToken, signal)
    },
    waitNotificationSubmissionRevocation(id, attemptToken, signal) {
      if (!submissionRevocationSupported) return Promise.reject(new Error('共享核心尚不支持通知撤销确认；未提交通知'))
      return invoke('waitNotificationSubmissionRevocation', sourceKey, id, attemptToken, signal)
    },
    async installationNotificationRecords(selection = {}, signal) {
      const health = await installationCall('health', [], signal, false)
      if (!health.features?.includes(INSTALLATION_NOTIFICATION_RECORDS_FEATURE)) throw new Error('共享核心尚不支持本安装通知记录')
      return installationCall('installationNotificationRecords', [selection], signal)
    },
    sourcePlan(signal) { return invoke('sourcePlan', signal) }, configureSources(value, signal) { return invoke('configureSources', value, signal) },
    claimNotifications(selection, signal) {
      if (selection?.submissionProtocol !== 1 || Object.keys(selection).length !== 1 || !submissionRevocationSupported) return Promise.reject(new Error('通知客户端或共享核心不支持撤销确认；未领取通知'))
      return invoke('claimNotifications', sourceKey, signal)
    },
    ackNotification(id, result, signal) { return invoke('ackNotification', id, { ...result, sourceKey }, signal) },
    close() {
      if (pendingClose) return pendingClose
      closed = true; shutdownStatus = 'closing'; clearInterval(timer); timer = undefined; lifetime.abort()
      for (const dispose of handlers.splice(0)) { try { dispose() } catch {} }
      const disposedReader = hierarchyReader.close()
      const disposedProvider = provideDispose?.(); provideDispose = undefined
      const disposedRemote = remoteFiber?.dispose(); remoteFiber = undefined
      closeEffectDispose?.(); closeEffectDispose = undefined
      const currentClients = () => [...new Set([client, connectingClient, ...settingsClients].filter(Boolean))]
      const cancelled = currentClients().map(target => target.cancelPending ? track(target.cancelPending()) : undefined)
      pendingClose = (async () => {
        const deadline = Date.now() + closeTimeoutMs * 2
        const remaining = () => Math.max(1, deadline - Date.now())
        let sealed = false
        try {
          const results = await waitForShutdown(Promise.allSettled([retiring, tickPromise, disposedReader, disposedProvider, disposedRemote, ...cancelled, ...activeRequests]), Math.min(closeTimeoutMs, remaining()))
          for (const result of results) if (result.status === 'rejected' && result.reason !== lifetime.signal.reason) shutdownFailure(result.reason)
        } catch (error) { shutdownFailure(error) }
        try {
          if (client && sourceKey) {
            // The final flush and unregister share one bounded deadline.
            await withShutdownSignal(async signal => {
              const batch = transportBatch(collector.batch())
              if (batch.events.length || batch.tasks.length || batch.gaps.length) {
                const receipt = await call(client, 'ingest', sourceKey, batch, signal)
                if (hierarchySupported && (receipt?.hierarchyContext?.schemaVersion !== 1 || receipt.hierarchyContext.acceptedRevision !== batch.hierarchyContext.revision)) throw new Error('关闭时层级上下文回执不匹配')
              }
              if (sealed || signal.aborted) return
              await call(client, 'unregisterSource', sourceKey, 'DSH 适配器关闭', signal)
            }, Math.min(closeTimeoutMs, remaining()))
          }
        } catch (error) { shutdownFailure(error) /* Failed transport is ultimately closed by the source lease. */ }
        sealed = true
        try {
          const results = await waitForShutdown(Promise.allSettled(currentClients().map(target => track(target.close()))), remaining())
          for (const result of results) if (result.status === 'rejected') shutdownFailure(result.reason)
        } catch (error) { shutdownFailure(error) }
        client = undefined; connectingClient = undefined; sourceKey = undefined; collector.close(); sentTasks.clear(); sentGaps.clear()
        shutdownFinalized = true
        shutdownStatus = shutdownFailures || activeRequests.size || tickPromise ? 'incomplete' : 'closed'
        if (adapters.get(ctx.root) === service) adapters.delete(ctx.root)
        return { status: shutdownStatus, timedOut: shutdownTimedOut, failures: shutdownFailures,
          pendingRequests: activeRequests.size, tickInFlight: !!tickPromise }
      })()
      return pendingClose
    },
  }
  adapters.set(ctx.root, service)
  service.typertRemote = Object.freeze({ service, serviceKey: 'sentinel', namespace: 'sentinel' })
  provideDispose = ctx.provide('sentinel', service)
  remoteFiber = ctx.inject(['typert'], child => { child.effect(() => child.typert.register({ package: name, face: 'host', schemas: [],
    model: { services: [], events: [], objects: [] }, invocations: descriptors() })) })
  closeEffectDispose = ctx.effect(() => () => { if (!closed) return service.close() })
  timer = setInterval(tick, flushMs); timer.unref?.()
  tick()
  return service
}

export function apply(ctx, config) { createAdapter(ctx, config) }

export default { name, inject, apply }
